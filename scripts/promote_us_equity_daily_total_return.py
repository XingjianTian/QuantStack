#!/usr/bin/env python3
"""Promote the oldest action-complete US daily session into the SS candidate."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.batch import CanonicalBatchRepository
from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.us_adjustment_incremental import (
    publish_us_total_return_incremental,
)
from quant_stack.data.us_corporate_actions import iter_us_raw_series
from quant_stack.data.us_total_return_dataset import (
    load_us_total_return_dataset,
    us_total_return_relation_sql,
)
from scripts.run_us_equity_isolated_daily import next_completed_session
from scripts.sync_ss_daily_snapshot import sync_daily_snapshot


def _path(config: dict[str, object], key: str) -> Path:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"US total-return promotion config requires {key}")
    return Path(value)


def load_config(path: Path) -> dict[str, object]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema_version") != "quantstack.us-total-return-daily-promotion.v1":
        raise ValueError("unsupported US total-return promotion config")
    for key in (
        "snapshot",
        "isolated_root",
        "action_evidence_root",
        "data_root",
        "work_root",
        "state",
    ):
        _path(config, key)
    date.fromisoformat(str(config["initial_watermark"]))
    prefix = str(config.get("dataset_version_prefix", ""))
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", prefix) is None:
        raise ValueError("US total-return dataset version prefix is invalid")
    recoveries = config.get("recovery_sessions", {})
    if not isinstance(recoveries, dict):
        raise ValueError("US total-return recovery sessions must be a mapping")
    for token, recovery in recoveries.items():
        date.fromisoformat(str(token))
        if not isinstance(recovery, dict):
            raise ValueError("US total-return recovery entry must be a mapping")
        for key in ("base_dataset_version", "dataset_version"):
            if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", str(recovery.get(key, ""))) is None:
                raise ValueError(f"US total-return recovery requires a valid {key}")
    action_cache_roots = config.get("action_cache_roots", [])
    if not isinstance(action_cache_roots, list) or not all(
        isinstance(item, str) and item for item in action_cache_roots
    ):
        raise ValueError("US total-return action cache roots must be a list of paths")
    if str(config.get("ss_base_url") or "").strip():
        _path(config, "ss_report_root")
        if "ss_report_gid" in config:
            int(config["ss_report_gid"])
    return config


def _state(path: Path, *, initial: date, snapshot_id: str) -> tuple[date, str | None]:
    if not path.is_file():
        return initial, None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("snapshot_id") != snapshot_id:
        raise ValueError("US total-return promotion state snapshot changed")
    return date.fromisoformat(str(payload["watermark"])), payload.get("dataset_version")


def _available_sessions(action_evidence_root: Path) -> list[date]:
    sessions: list[date] = []
    if not action_evidence_root.is_dir():
        return sessions
    for path in action_evidence_root.iterdir():
        if not path.is_dir():
            continue
        try:
            sessions.append(date.fromisoformat(path.name))
        except ValueError:
            continue
    return sorted(sessions)


def _available_session_packages(root: Path) -> list[date]:
    market_root = root / "market=US"
    if not market_root.is_dir():
        return []
    sessions: list[date] = []
    for path in market_root.glob("session=*"):
        try:
            sessions.append(date.fromisoformat(path.name.removeprefix("session=")))
        except ValueError:
            continue
    return sorted(sessions)


def _candidate(data_root: Path, dataset_version: str = "latest") -> tuple[str, Path, date, int]:
    repository = CanonicalBatchRepository(data_root, consumer_access="candidate")
    plan = repository.prepare_metadata("us_equity_total_return", dataset_version)
    try:
        version = plan.snapshot.manifest.version
        bars_path = plan.payload_path
        dataset = load_us_total_return_dataset(bars_path)
        if dataset.watermark is not None and dataset.instrument_count is not None:
            watermark = date.fromisoformat(dataset.watermark)
            instrument_count = dataset.instrument_count
        else:
            relation = us_total_return_relation_sql(bars_path)
            watermark = plan.connection.execute(
                f"SELECT max(trading_date) FROM {relation}"
            ).fetchone()[0]
            instrument_count = plan.connection.execute(
                f"SELECT count(DISTINCT instrument_id) FROM {relation}"
            ).fetchone()[0]
    finally:
        plan.connection.close()
    if not isinstance(watermark, date):
        raise ValueError("US total-return candidate has no watermark")
    return version, bars_path, watermark, int(instrument_count)


def _require_session_coverage(
    *,
    session: date,
    observed_instruments: int,
    expected_instruments: int,
    label: str,
) -> float:
    if expected_instruments < 1:
        raise ValueError("US total-return base candidate has no instruments")
    missing = expected_instruments - observed_instruments
    coverage = observed_instruments / expected_instruments
    if coverage < 0.99 or missing > 100:
        raise ValueError(
            f"US total-return {label} coverage is insufficient for {session}: "
            f"{observed_instruments}/{expected_instruments} ({coverage:.2%}), "
            f"missing {missing}"
        )
    return coverage


def _daily_records(
    *,
    isolated_root: Path,
    action_evidence_root: Path,
    snapshot_path: Path,
    session: date,
) -> tuple[list[dict[str, object]], list[str]]:
    snapshot = load_us_instrument_snapshot(snapshot_path)
    instruments = {item.provider_symbol: item.instrument_id for item in snapshot.instruments}
    records: list[dict[str, object]] = []
    action_quarantines: list[str] = []
    prefix = f"{session.isoformat()}.full-market-daily-v1"
    for symbol, rows in iter_us_raw_series(isolated_root, dataset_version_prefix=prefix):
        instrument_id = instruments.get(symbol)
        if instrument_id is None:
            raise ValueError(f"US daily symbol is outside the configured snapshot: {symbol}")
        matches = [item for item in rows if str(item["Date"])[:10] == session.isoformat()]
        if len(matches) != 1:
            raise ValueError(f"US daily session row is incomplete for {symbol}")
        evidence_path = action_evidence_root / session.isoformat() / f"{symbol}.json.gz"
        if not evidence_path.is_file():
            action_quarantines.append(symbol)
            continue
        evidence = json.loads(gzip.decompress(evidence_path.read_bytes()))
        if (
            evidence.get("schema_version") != "quantstack.us-daily-action-evidence.v1"
            or evidence.get("symbol") != symbol
            or evidence.get("instrument_id") != instrument_id
            or evidence.get("session") != session.isoformat()
        ):
            raise ValueError(f"US daily action evidence identity conflicts for {symbol}")
        records.append(
            {
                **matches[0],
                "instrument_id": instrument_id,
                "Dividends": evidence["Dividends"],
                "Stock Splits": evidence["Stock Splits"],
            }
        )
    if not records:
        raise ValueError("US total-return promotion found no action-complete daily rows")
    return records, action_quarantines


def _action_cache_records(
    roots: tuple[Path, ...], *, session: date
) -> dict[str, tuple[object, object]]:
    records: dict[str, tuple[object, object]] = {}
    for root in roots:
        paths = sorted(root.glob("batch-*.json.gz"))
        paths.extend(sorted(root.glob("*/batch-*.json.gz")))
        for path in paths:
            payload = json.loads(gzip.decompress(path.read_bytes()))
            if payload.get("session_date") != session.isoformat():
                continue
            for row in payload.get("records", []):
                if str(row.get("Date", ""))[:10] != session.isoformat():
                    continue
                if "Dividends" not in row or "Stock Splits" not in row:
                    continue
                symbol = str(row["ticker"]).upper()
                action = (row["Dividends"], row["Stock Splits"])
                if symbol in records and records[symbol] != action:
                    raise ValueError(f"US daily action cache conflicts for {symbol}")
                records[symbol] = action
    return records


def _session_parquet_records(
    *,
    package_root: Path,
    action_evidence_root: Path,
    action_cache_roots: tuple[Path, ...],
    session: date,
) -> tuple[list[dict[str, object]], list[str], list[str]]:
    package = package_root / "market=US" / f"session={session.isoformat()}"
    inspect_session_package(package)
    import pyarrow.parquet as pq

    records: list[dict[str, object]] = []
    action_quarantines: list[str] = []
    cached_actions = _action_cache_records(action_cache_roots, session=session)
    for row in pq.ParquetFile(package / "bars.parquet").read().to_pylist():
        instrument_id = str(row["instrument_id"])
        symbol = instrument_id.split(".", maxsplit=1)[1]
        dividend = row.get("cash_dividend")
        split = row.get("split_ratio")
        if dividend is None or split is None:
            evidence_path = action_evidence_root / session.isoformat() / f"{symbol}.json.gz"
            if evidence_path.is_file():
                evidence = json.loads(gzip.decompress(evidence_path.read_bytes()))
                dividend = evidence["Dividends"]
                split = evidence["Stock Splits"]
            elif symbol in cached_actions:
                dividend, split = cached_actions[symbol]
            else:
                action_quarantines.append(symbol)
                continue
        records.append(
            {
                "instrument_id": instrument_id,
                "Date": session.isoformat(),
                "Open": row["open"],
                "High": row["high"],
                "Low": row["low"],
                "Close": row["close"],
                "Adj Close": row.get("provider_adj_close", row["close"]),
                "Volume": row["volume"],
                "Dividends": dividend,
                "Stock Splits": split,
            }
        )
    manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
    return (
        records,
        [str(item) for item in manifest["omissions"]],
        action_quarantines,
    )


def promote(config: dict[str, object], *, now: datetime | None = None) -> dict[str, object]:
    snapshot_path = _path(config, "snapshot")
    snapshot = load_us_instrument_snapshot(snapshot_path)
    state_path = _path(config, "state")
    watermark, state_dataset_version = _state(
        state_path,
        initial=date.fromisoformat(str(config["initial_watermark"])),
        snapshot_id=snapshot.snapshot_id,
    )
    recovery: dict[str, object] | None = None
    recovery_sessions = dict(config.get("recovery_sessions", {}))
    for token in sorted(recovery_sessions):
        candidate_session = date.fromisoformat(str(token))
        candidate_recovery = recovery_sessions[token]
        if (
            candidate_session == watermark
            and state_dataset_version != candidate_recovery["dataset_version"]
        ):
            recovery = candidate_recovery
            session = candidate_session
            break
    else:
        session_root = config.get("session_parquet_root")
        available = (
            _available_session_packages(Path(str(session_root)))
            if session_root
            else _available_sessions(_path(config, "action_evidence_root"))
        )
        sessions = [item for item in available if item > watermark]
        session = sessions[0] if sessions else None
    if session is None:
        return {
            "schema_version": "quantstack.us-total-return-daily-promotion-run.v1",
            "status": "noop",
            "watermark": watermark.isoformat(),
            "provider_requested": False,
        }
    base_dataset_version = str(recovery["base_dataset_version"]) if recovery else "latest"
    base_version, base_path, base_watermark, base_instruments = _candidate(
        _path(config, "data_root"), base_dataset_version
    )
    expected_session = next_completed_session(base_watermark, now or datetime.now(UTC))
    if expected_session != session:
        raise ValueError("US total-return promotion would skip a market session")
    if recovery is None and base_watermark != watermark:
        raise ValueError("US total-return promotion state and current candidate diverged")
    dataset_version = (
        str(recovery["dataset_version"])
        if recovery
        else f"{config['dataset_version_prefix']}.{session.strftime('%Y%m%d')}"
    )
    session_root = config.get("session_parquet_root")
    if session_root:
        daily_records, formal_omissions, action_quarantines = _session_parquet_records(
            package_root=Path(str(session_root)),
            action_evidence_root=_path(config, "action_evidence_root"),
            action_cache_roots=tuple(
                Path(str(item)) for item in config.get("action_cache_roots", [])
            ),
            session=session,
        )
    else:
        daily_records, action_quarantines = _daily_records(
            isolated_root=_path(config, "isolated_root"),
            action_evidence_root=_path(config, "action_evidence_root"),
            snapshot_path=snapshot_path,
            session=session,
        )
        formal_omissions = []
    price_instruments = len(daily_records) + len(action_quarantines)
    if session_root:
        if action_quarantines:
            raise ValueError(
                "US total-return session package action coverage is incomplete: "
                f"{len(daily_records)}/{price_instruments}, "
                f"missing {len(action_quarantines)}"
            )
        requested = price_instruments + len(formal_omissions)
        price_coverage = price_instruments / requested
        action_complete_coverage = 1.0
    else:
        price_coverage = _require_session_coverage(
            session=session,
            observed_instruments=price_instruments,
            expected_instruments=base_instruments,
            label="price",
        )
        action_complete_coverage = _require_session_coverage(
            session=session,
            observed_instruments=len(daily_records),
            expected_instruments=base_instruments,
            label="action-complete",
        )
    result, receipt, proof = publish_us_total_return_incremental(
        base_bars_path=base_path,
        daily_records=daily_records,
        session=session,
        work_root=_path(config, "work_root"),
        data_root=_path(config, "data_root"),
        base_dataset_version=base_version,
        dataset_version=dataset_version,
        environment_id="nas-us-total-return-daily",
    )
    state = {
        "schema_version": "quantstack.us-total-return-daily-promotion-state.v1",
        "snapshot_id": snapshot.snapshot_id,
        "watermark": session.isoformat(),
        "dataset_version": dataset_version,
    }
    atomic_replace_bytes(state_path, (json.dumps(state, indent=2, sort_keys=True) + "\n").encode())
    report = {
        "schema_version": "quantstack.us-total-return-daily-promotion-run.v1",
        "status": "completed",
        "recovery": recovery is not None,
        "session": session.isoformat(),
        "base_dataset_version": base_version,
        "dataset_version": dataset_version,
        "rows": result.row_count,
        "instruments": result.instrument_count,
        "action_instruments": result.action_instruments,
        "action_quarantined_instruments": len(action_quarantines),
        "action_quarantines": action_quarantines,
        "formal_omitted_instruments": len(formal_omissions),
        "base_instruments": base_instruments,
        "daily_price_instruments": price_instruments,
        "daily_price_coverage": price_coverage,
        "daily_action_complete_coverage": action_complete_coverage,
        "candidate_eligible": receipt.publication_state.candidate_eligible,
        "receipt_replay_verified": proof["receipt_replay_verified"],
        "provider_requested": False,
        "full_history_provider_rebuilt": False,
        "candidate_projection_rewritten_locally": False,
    }
    report_path = _path(config, "work_root") / "reports" / f"promote-{session}.json"
    atomic_replace_bytes(
        report_path, (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    )
    return report


def run(
    config: dict[str, object],
    *,
    now: datetime | None = None,
    sync_snapshot=sync_daily_snapshot,
) -> dict[str, object]:
    """Promote local data, then idempotently converge the US Daily Snapshot.

    Snapshot convergence also runs after a promotion no-op. This makes a
    transient SS failure retryable without republishing or Provider access.
    """

    report = promote(config, now=now)
    ss_base_url = str(config.get("ss_base_url") or "").strip()
    if not ss_base_url:
        return report
    snapshot = load_us_instrument_snapshot(_path(config, "snapshot"))
    watermark, dataset_version = _state(
        _path(config, "state"),
        initial=date.fromisoformat(str(config["initial_watermark"])),
        snapshot_id=snapshot.snapshot_id,
    )
    if not dataset_version:
        raise ValueError("US snapshot sync requires a published total-return dataset")
    snapshot_report = sync_snapshot(
        market="US",
        ss_base_url=ss_base_url,
        dataset_version=dataset_version,
        expected_watermark=watermark,
    )
    combined = {**report, "ss_daily_snapshot": snapshot_report}
    report_root = _path(config, "ss_report_root")
    report_root.mkdir(parents=True, exist_ok=True)
    report_path = report_root / f"us-ss-daily-snapshot-{watermark}.json"
    atomic_replace_bytes(
        report_path,
        (json.dumps(combined, indent=2, sort_keys=True) + "\n").encode(),
    )
    if config.get("ss_report_gid") is not None:
        os.chown(report_path, -1, int(config["ss_report_gid"]))
        os.chmod(report_path, 0o640)
    return combined


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(load_config(args.config)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
