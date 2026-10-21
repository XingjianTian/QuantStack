#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date
from importlib import import_module
from pathlib import Path

from quant_stack.data.backfill.models import BackfillUniverseSource
from quant_stack.data.backfill.universe import build_historical_universe
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes

DEFAULT_DATES = (
    date(2015, 1, 5),
    date(2017, 1, 3),
    date(2019, 1, 2),
    date(2021, 1, 4),
    date(2023, 1, 3),
    date(2025, 1, 2),
    date(2026, 8, 3),
)
NATIVE_SCHEMA_VERSION = "baostock.query_all_stock.code-tradestatus-code_name.v1"
SNAPSHOT_SET_VERSION = "2015-2026.medium-v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build deterministic medium CN backfill universe from BaoStock snapshots"
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--universe-version", default="2026-08-04.medium-v1")
    parser.add_argument("--target-count", type=int, default=360)
    parser.add_argument("--seed", type=int, default=2303)
    parser.add_argument(
        "--reuse-snapshots",
        action="store_true",
        help="Rebuild from immutable snapshots already present under --data-root",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        type=date.fromisoformat,
        default=DEFAULT_DATES,
    )
    return parser


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def main() -> None:
    args = _parser().parse_args()
    dates = tuple(args.dates)
    if dates != tuple(sorted(set(dates))):
        raise ValueError("snapshot dates must be unique and ascending")
    storage = StagingFileSystemStorage(args.data_root)
    snapshots: list[tuple[date, list[dict[str, str]]]] = []
    if args.reuse_snapshots:
        for observed_date in dates:
            path = args.data_root / (
                Path("pilot/universe-snapshots")
                / f"version={SNAPSHOT_SET_VERSION}"
                / f"snapshot={observed_date}.json"
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload["observed_date"] != observed_date.isoformat():
                raise ValueError("stored snapshot observed date mismatch")
            if tuple(payload["fields"]) != ("code", "tradeStatus", "code_name"):
                raise ValueError("stored BaoStock query_all_stock schema drift")
            records = payload["records"]
            snapshots.append((observed_date, records))
    else:
        client = import_module("baostock")
        login = client.login()
        if login.error_code != "0":
            raise ConnectionError("BaoStock login failed")
        try:
            for observed_date in dates:
                result = client.query_all_stock(day=observed_date.isoformat())
                if result.error_code != "0":
                    raise ConnectionError("BaoStock query_all_stock failed")
                if tuple(result.fields) != ("code", "tradeStatus", "code_name"):
                    raise ValueError("BaoStock query_all_stock schema drift")
                records = []
                while result.error_code == "0" and result.next():
                    records.append(
                        dict(zip(result.fields, result.get_row_data(), strict=True))
                    )
                snapshots.append((observed_date, records))
        finally:
            client.logout()
    sources: list[BackfillUniverseSource] = []
    for observed_date, records in snapshots:
        snapshot_version = f"{observed_date}.baostock-all-stock-v1"
        raw = storage.write_raw_object(
            Path("pilot/universe-snapshots")
            / f"version={SNAPSHOT_SET_VERSION}"
            / f"snapshot={observed_date}.json",
            _json_bytes(
                {
                    "schema_version": "1.0.0",
                    "snapshot_version": snapshot_version,
                    "observed_date": observed_date.isoformat(),
                    "provider_contract": {
                        "provider": "baostock",
                        "provider_version": "0.9.3",
                        "endpoint": "query_all_stock",
                        "native_schema_version": NATIVE_SCHEMA_VERSION,
                    },
                    "fields": ["code", "tradeStatus", "code_name"],
                    "records": records,
                    "evidence_scope": "provider_observed_membership_only",
                }
            ),
        )
        eligible_codes = [str(item["code"]) for item in records]
        sources.append(
            BackfillUniverseSource(
                snapshot_version=snapshot_version,
                observed_date=observed_date,
                provider_version="0.9.3",
                native_schema_version=NATIVE_SCHEMA_VERSION,
                raw_uri=raw.uri,
                raw_sha256=raw.sha256,
                row_count=len(records),
                xshg_count=sum(
                    code.startswith("sh.")
                    and code[3:6] in {"600", "601", "603", "605", "688"}
                    for code in eligible_codes
                ),
                xshe_count=sum(
                    code.startswith("sz.")
                    and code[3:6] in {"000", "001", "002", "003", "300", "301"}
                    for code in eligible_codes
                ),
            )
        )
    universe = build_historical_universe(
        snapshots,
        sources,
        universe_id="cn-xshg-xshe-medium-historical",
        version=args.universe_version,
        selection_seed=args.seed,
        target_count=args.target_count,
    )
    payload = _json_bytes(universe.model_dump(mode="json", exclude_computed_fields=True))
    atomic_replace_bytes(
        args.output,
        payload,
        validate=lambda path: type(universe).model_validate_json(path.read_text()),
    )
    summary = {
        "universe_version": universe.version,
        "content_sha256": universe.content_sha256,
        "instruments": len(universe.instruments),
        "xshg": sum(item.venue == "XSHG" for item in universe.instruments),
        "xshe": sum(item.venue == "XSHE" for item in universe.instruments),
        "historical_only_observed": sum(
            "historical_only_observed" in item.selection_bucket
            for item in universe.instruments
        ),
        "snapshots": len(sources),
        "snapshot_versions": list(universe.snapshot_versions),
    }
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
