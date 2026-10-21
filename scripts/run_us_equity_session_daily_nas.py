#!/usr/bin/env python3
"""Run the US full-market daily directly into one session Parquet package."""

from __future__ import annotations

import argparse
import gzip
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

from quant_stack.data.providers.yfinance import LiveYFinanceTransport, load_us_instrument_snapshot
from quant_stack.data.session_parquet import install_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.us_market_indexes import US_MARKET_INDEXES, normalize_index_records
from scripts.run_daily_session_parquet import _us_cache_records
from scripts.run_us_equity_core_daily_nas import wait_for_session_ready
from scripts.run_us_equity_isolated_daily import (
    _load_watermark,
    _write_watermark,
    next_completed_session,
)


def _path(config: dict[str, object], key: str) -> Path:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"US session daily config requires {key}")
    return Path(value)


def load_config(path: Path) -> dict[str, object]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema_version") != "quantstack.us-equity-session-daily.v1":
        raise ValueError("unsupported US session daily config")
    allowed = {
        "schema_version",
        "snapshot",
        "state",
        "initial_watermark",
        "work_root",
        "session_parquet_root",
        "yfinance_runtime_root",
        "yfinance_proxy_url",
        "session_readiness_symbol",
        "session_readiness_retry_seconds",
        "session_readiness_max_attempts",
        "provider_threads",
        "provider_batch_size",
        "provider_batch_start_interval_seconds",
        "market_index_root",
    }
    unknown = sorted(set(config).difference(allowed))
    if unknown:
        raise ValueError(f"US session daily config contains legacy fields: {unknown}")
    for key in (
        "snapshot",
        "state",
        "work_root",
        "session_parquet_root",
        "yfinance_runtime_root",
        "market_index_root",
    ):
        _path(config, key)
    date.fromisoformat(str(config["initial_watermark"]))
    proxy = urlsplit(str(config.get("yfinance_proxy_url", "")))
    if proxy.scheme != "http" or proxy.hostname != "127.0.0.1" or proxy.port != 7890:
        raise ValueError("US session daily requires the local Mihomo route")
    if not str(config.get("session_readiness_symbol", "SPY")).strip():
        raise ValueError("session readiness symbol must not be empty")
    if int(config.get("session_readiness_retry_seconds", 3600)) < 60:
        raise ValueError("session readiness retry interval must be at least 60 seconds")
    if not 1 <= int(config.get("session_readiness_max_attempts", 8)) <= 24:
        raise ValueError("session readiness attempts must be between 1 and 24")
    if not 1 <= int(config.get("provider_threads", 48)) <= 48:
        raise ValueError("provider threads must be between 1 and 48")
    if not 1 <= int(config.get("provider_batch_size", 128)) <= 512:
        raise ValueError("provider batch size must be between 1 and 512")
    if float(config.get("provider_batch_start_interval_seconds", 45)) < 0:
        raise ValueError("provider batch start interval must not be negative")
    return config


def run(config: dict[str, object], *, now: datetime | None = None) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    snapshot_path = _path(config, "snapshot")
    snapshot = load_us_instrument_snapshot(snapshot_path)
    state_path = _path(config, "state")
    watermark = _load_watermark(
        state_path,
        date.fromisoformat(str(config["initial_watermark"])),
        snapshot.snapshot_id,
    )
    session = next_completed_session(watermark, observed_at)
    if session is None:
        return {"status": "noop", "watermark": watermark.isoformat(), "provider_requested": False}
    readiness = wait_for_session_ready(config, session=session)
    work_root = _path(config, "work_root")
    cache_root = work_root / "session-provider-cache" / session.isoformat()
    cache_root.mkdir(parents=True, exist_ok=True)
    attempted: set[str] = set()
    for path in cache_root.glob("batch-*.json.gz"):
        payload = json.loads(gzip.decompress(path.read_bytes()))
        attempted.update(str(item).upper() for item in payload["requested_symbols"])
    symbols = [
        item.provider_symbol
        for item in snapshot.instruments
        if item.provider_symbol not in attempted
    ]
    batch_size = int(config.get("provider_batch_size", 128))
    threads = int(config.get("provider_threads", 48))
    transport_threads: bool | int = False if threads == 1 else threads
    transport = LiveYFinanceTransport(
        runtime_root=_path(config, "yfinance_runtime_root"),
        proxy_url=str(config["yfinance_proxy_url"]),
        minimum_interval_seconds=None,
        include_actions=True,
    )
    started = time.perf_counter()
    batch_number = len(list(cache_root.glob("batch-*.json.gz"))) + 1
    batch_interval = float(config.get("provider_batch_start_interval_seconds", 45))
    previous_batch_started: float | None = None

    def wait_for_batch_start() -> None:
        nonlocal previous_batch_started
        observed = time.monotonic()
        if previous_batch_started is not None:
            time.sleep(max(0.0, batch_interval - (observed - previous_batch_started)))
        previous_batch_started = time.monotonic()

    for offset in range(0, len(symbols), batch_size):
        batch = symbols[offset : offset + batch_size]
        wait_for_batch_start()
        rows = transport.fetch_daily_bars_batch(
            batch, session, session, include_actions=True, threads=transport_threads
        )
        audit = transport.audit_snapshot()
        outcomes = list(audit.get("request_outcomes", []))
        if len(outcomes) != len(batch):
            raise RuntimeError("Yahoo batch did not return one explicit outcome per symbol")
        symbol_outcomes = [
            {"symbol": symbol, "status": status}
            for symbol, status in zip(batch, outcomes, strict=True)
        ]
        provider_errors = [item for item in symbol_outcomes if item["status"] == "provider_error"]
        if provider_errors:
            raise RuntimeError("Yahoo batch contains provider errors; session install is blocked")
        payload = {
            "schema_version": "quantstack.yfinance-daily-batch-cache.v2",
            "session_date": session.isoformat(),
            "requested_symbols": batch,
            "records": rows,
            "symbol_outcomes": symbol_outcomes,
        }
        atomic_replace_bytes(
            cache_root / f"batch-{batch_number:04d}.json.gz",
            gzip.compress(json.dumps(payload, separators=(",", ":")).encode(), mtime=0),
        )
        batch_number += 1
    records, omissions, requested, provider_errors = _us_cache_records(
        cache_root, snapshot_path, session
    )
    result = install_session_package(
        records=records,
        output_root=_path(config, "session_parquet_root"),
        market="US",
        session=session,
        dataset_id="us_equity_daily",
        version=f"{session.isoformat()}.session-v1",
        adjustment="raw",
        requested_instruments=requested,
        omissions=omissions,
        provider_errors=provider_errors,
    )
    index_root = _path(config, "market_index_root")
    index_target = (
        index_root / "sessions" / "market=US" / f"session={session.isoformat()}"
    )
    index_provider_symbols = 0
    if not index_target.exists():
        index_symbols = list(US_MARKET_INDEXES)
        index_provider_symbols = len(index_symbols)
        wait_for_batch_start()
        index_rows = transport.fetch_daily_bars_batch(
            index_symbols,
            session,
            session,
            include_actions=False,
            threads=transport_threads,
        )
        normalized_indexes = normalize_index_records(index_rows)
        returned = {str(row["symbol"]) for row in normalized_indexes}
        index_omissions = sorted(set(index_symbols).difference(returned))
        install_session_package(
            records=normalized_indexes,
            output_root=index_root / "sessions",
            market="US",
            session=session,
            dataset_id="us_market_indexes_daily",
            version=f"{session.isoformat()}.session-v1",
            adjustment="raw",
            requested_instruments=len(index_symbols),
            omissions=index_omissions,
        )
    _write_watermark(state_path, session, snapshot.snapshot_id)
    report = {
        "schema_version": "quantstack.us-session-daily-run.v1",
        "status": "completed",
        "session": session.isoformat(),
        "rows": result.row_count,
        "instruments": result.instrument_count,
        "omissions": result.omission_count,
        "provider_symbols_requested_current_run": len(symbols),
        "provider_retries": 0,
        "provider_threads": threads,
        "provider_internal_threads": transport_threads,
        "provider_batch_size": batch_size,
        "market_index_symbols": len(US_MARKET_INDEXES),
        "market_index_provider_symbols_requested_current_run": index_provider_symbols,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "session_readiness": readiness,
        "files_installed": 2,
        "persistence": "single_session_parquet",
    }
    atomic_replace_bytes(
        work_root / "reports" / f"session-run-{session.isoformat()}.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(load_config(args.config)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
