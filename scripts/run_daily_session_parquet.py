#!/usr/bin/env python3
"""Build or inspect the lightweight CN/US daily session Parquet package."""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import math
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.session_parquet import (
    inspect_session_package,
    install_session_package,
    replay_session_package,
)


def _canonical_row(row: dict[str, object], *, market: str, session: date) -> dict[str, object]:
    return {
        **row,
        "market": market,
        "trading_date": session,
        "open": row.get("open", row.get("Open")),
        "high": row.get("high", row.get("High")),
        "low": row.get("low", row.get("Low")),
        "close": row.get("close", row.get("Close")),
        "volume": row.get("volume", row.get("Volume")),
    }


def _parquet_records(paths: list[Path], *, market: str, session: date) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for path in paths:
        for raw in pq.ParquetFile(path).read().to_pylist():
            token = raw.get("trading_date", raw.get("Date"))
            observed = session if token is None else date.fromisoformat(str(token)[:10])
            if observed == session:
                result.append(_canonical_row(dict(raw), market=market, session=session))
    return result


def _us_cache_records(
    cache_root: Path, snapshot_path: Path, session: date
) -> tuple[list[dict[str, object]], list[str], int, list[dict[str, object]]]:
    snapshot = load_us_instrument_snapshot(snapshot_path)
    identities = {
        instrument.provider_symbol.upper(): instrument.instrument_id
        for instrument in snapshot.instruments
    }
    requested: set[str] = set()
    selected: dict[str, dict[str, object]] = {}
    no_bar: set[str] = set()
    provider_errors: list[dict[str, object]] = []
    has_v2 = False

    def valid_price(row: dict[str, object]) -> bool:
        try:
            open_, high, low, close, volume = (
                float(row[field]) for field in ("Open", "High", "Low", "Close", "Volume")
            )
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            all(math.isfinite(value) for value in (open_, high, low, close, volume))
            and volume >= 0
            and high >= max(open_, close)
            and low <= min(open_, close)
            and low <= high
        )

    for path in sorted(cache_root.glob("batch-*.json.gz")):
        payload = json.loads(gzip.decompress(path.read_bytes()))
        if payload.get("session_date") != session.isoformat():
            raise ValueError(f"cache session mismatch: {path}")
        batch_requested = [str(item).upper() for item in payload["requested_symbols"]]
        requested.update(batch_requested)
        if payload.get("schema_version") == "quantstack.yfinance-daily-batch-cache.v2":
            has_v2 = True
            outcomes = {
                str(item["symbol"]).upper(): str(item["status"])
                for item in payload.get("symbol_outcomes", [])
            }
            if set(outcomes) != set(batch_requested):
                raise ValueError(f"cache outcomes do not cover the requested batch: {path}")
            no_bar.update(symbol for symbol, status in outcomes.items() if status == "no_bar")
            provider_errors.extend(
                {"symbol": symbol, "status": status, "batch": path.name}
                for symbol, status in outcomes.items()
                if status == "provider_error"
            )
            unknown = sorted(set(outcomes.values()) - {"returned", "no_bar", "provider_error"})
            if unknown:
                raise ValueError(f"cache contains unsupported symbol outcomes: {unknown}")
        for raw in payload.get("records", []):
            symbol = str(raw["ticker"]).upper()
            if str(raw.get("Date", ""))[:10] != session.isoformat():
                continue
            if not valid_price(raw):
                continue
            instrument_id = identities.get(symbol)
            if instrument_id is None:
                raise ValueError(f"cache symbol is absent from snapshot: {symbol}")
            row = _canonical_row(dict(raw), market="US", session=session)
            row.update(
                instrument_id=instrument_id,
                symbol=symbol,
                provider_adj_close=raw.get("Adj Close"),
                cash_dividend=raw.get("Dividends", 0.0),
                split_ratio=raw.get("Stock Splits", 0.0),
            )
            selected[symbol] = row
    unresolved = requested.difference(selected)
    if provider_errors:
        return list(selected.values()), [], len(requested), provider_errors
    # V1 cache is accepted only as migration input from the already completed campaign.
    # V2 daily caches must explicitly identify every legal no-bar omission.
    if has_v2 and unresolved != no_bar:
        missing = sorted(unresolved.symmetric_difference(no_bar))
        raise ValueError(f"cache outcome coverage conflicts with returned rows: {missing}")
    omissions = sorted(identities[symbol] for symbol in unresolved)
    return list(selected.values()), omissions, len(requested), []


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build")
    build.add_argument("--market", choices=("CN", "US"), required=True)
    build.add_argument("--session", type=date.fromisoformat, required=True)
    build.add_argument("--dataset-id", required=True)
    build.add_argument("--version", required=True)
    build.add_argument("--adjustment", required=True)
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--input-parquet", type=Path, action="append", default=[])
    build.add_argument("--input-parquet-glob", action="append", default=[])
    build.add_argument("--us-cache-root", type=Path)
    build.add_argument("--snapshot", type=Path)

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--package", type=Path, required=True)

    replay = subparsers.add_parser("replay")
    replay.add_argument("--package", type=Path, required=True)
    replay.add_argument("--candidate-parquet", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "inspect":
        result = inspect_session_package(args.package)
        print(json.dumps(result.__dict__, default=str, sort_keys=True))
        return 0
    if args.command == "replay":
        equivalent = replay_session_package(args.package, args.candidate_parquet)
        print(json.dumps({"equivalent": equivalent}, sort_keys=True))
        return 0 if equivalent else 1

    parquet_paths = list(args.input_parquet)
    for pattern in args.input_parquet_glob:
        parquet_paths.extend(Path(item) for item in sorted(glob.glob(pattern)))
    records = _parquet_records(parquet_paths, market=args.market, session=args.session)
    omissions: list[str] = []
    requested: int | None = None
    if args.us_cache_root:
        if args.market != "US" or args.snapshot is None:
            raise ValueError("US cache input requires --market US and --snapshot")
        cached, omissions, requested, provider_errors = _us_cache_records(
            args.us_cache_root, args.snapshot, args.session
        )
        by_id = {str(row["instrument_id"]): row for row in records}
        by_id.update({str(row["instrument_id"]): row for row in cached})
        records = list(by_id.values())
        if requested is not None:
            cached_ids = {str(row["instrument_id"]) for row in cached}
            base_ids = {str(row["instrument_id"]) for row in records}.difference(cached_ids)
            requested += len(base_ids)
    result = install_session_package(
        records=records,
        output_root=args.output_root,
        market=args.market,
        session=args.session,
        dataset_id=args.dataset_id,
        version=args.version,
        adjustment=args.adjustment,
        requested_instruments=requested,
        omissions=omissions,
        provider_errors=provider_errors if args.us_cache_root else (),
    )
    print(json.dumps(result.__dict__, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
