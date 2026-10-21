#!/usr/bin/env python3
"""Run a read-only calendar/lifecycle/corporate-fact audit over unadjusted bars."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

GENERATOR_VERSION = "cn-research-audit-v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(payload))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _copy_parquet(connection: duckdb.DuckDBPyConnection, relation: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        "COPY (SELECT * FROM "
        f"{relation}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(path)],
    )


def audit_dataset(
    *,
    bars_index: Path,
    calendar_parquet: Path,
    lifecycle_parquet: Path,
    actions_parquet: Path,
    output_root: Path,
    baseline_inventory: Path | None,
    observed_at: datetime,
) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE bars AS SELECT * FROM duckdb_scan(?)", [str(bars_index)]
        )
    except duckdb.Error:
        connection.close()
        connection = duckdb.connect(str(bars_index), read_only=True)
        connection.execute("CREATE TEMP TABLE bars AS SELECT * FROM pilot_bars")
    try:
        connection.execute(
            "CREATE TEMP TABLE calendar_days AS "
            "SELECT CAST(date AS DATE) AS date, venue, state "
            "FROM read_parquet(?)",
            [str(calendar_parquet)],
        )
        connection.execute(
            "CREATE TEMP TABLE lifecycle AS "
            "SELECT instrument_id, symbol, venue, "
            "CAST(listing_date AS DATE) AS listing_date, "
            "CAST(delisting_date AS DATE) AS delisting_date "
            "FROM read_parquet(?)",
            [str(lifecycle_parquet)],
        )
        connection.execute(
            "CREATE TEMP TABLE action_dates AS "
            "SELECT DISTINCT instrument_id, CAST(effective_date AS DATE) AS effective_date "
            "FROM read_parquet(?)",
            [str(actions_parquet)],
        )
        connection.execute(
            "CREATE TEMP TABLE expected AS "
            "SELECT l.instrument_id, l.symbol, l.venue, c.date, c.state, "
            "l.listing_date, l.delisting_date, a.effective_date AS action_date, "
            "b.trading_date AS bar_date "
            "FROM lifecycle l JOIN calendar_days c ON c.venue=l.venue "
            "LEFT JOIN action_dates a ON a.instrument_id=l.instrument_id "
            "AND a.effective_date=c.date "
            "LEFT JOIN bars b ON b.instrument_id=l.instrument_id AND b.trading_date=c.date"
        )
        connection.execute(
            "CREATE TEMP TABLE classified AS "
            "SELECT *, CASE "
            "WHEN bar_date IS NOT NULL THEN 'bar_present' "
            "WHEN listing_date IS NOT NULL AND date < listing_date THEN 'verified_pre_listing' "
            "WHEN delisting_date IS NOT NULL AND date > delisting_date "
            "THEN 'verified_post_delisting' "
            "WHEN state='weekend' THEN 'calendar_weekend' "
            "WHEN state IN ('holiday','temporary_closure') THEN 'calendar_holiday' "
            "WHEN action_date IS NOT NULL THEN 'corporate_action_boundary' "
            "WHEN state='session' AND listing_date IS NOT NULL "
            "THEN 'unexpected_missing_on_session' "
            "ELSE 'unknown_missing' END AS coverage_status "
            "FROM expected"
        )
        audit_path = output_root / "normalized/coverage-audit.parquet"
        _copy_parquet(connection, "classified", audit_path)
        summary_rows = connection.execute(
            "SELECT coverage_status, count(*) AS rows, "
            "count(DISTINCT instrument_id) AS instruments "
            "FROM classified GROUP BY 1 ORDER BY 1"
        ).fetchall()
        annual_rows = connection.execute(
            "SELECT year(date), coverage_status, count(*) FROM classified "
            "GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
        unexpected_bars = connection.execute(
            "SELECT b.instrument_id, b.trading_date, "
            "CASE WHEN l.listing_date IS NOT NULL AND b.trading_date < l.listing_date "
            "OR l.delisting_date IS NOT NULL AND b.trading_date > l.delisting_date "
            "THEN 'unexpected_bar_outside_lifecycle' "
            "WHEN c.state <> 'session' THEN 'unexpected_bar_on_non_session' END AS status "
            "FROM bars b LEFT JOIN lifecycle l ON l.instrument_id=b.instrument_id "
            "LEFT JOIN calendar_days c ON c.venue=b.venue AND c.date=b.trading_date "
            "WHERE (l.listing_date IS NOT NULL AND b.trading_date < l.listing_date) "
            "OR (l.delisting_date IS NOT NULL AND b.trading_date > l.delisting_date) "
            "OR (c.state IS NOT NULL AND c.state <> 'session')"
        ).fetchall()
        duplicate_count = connection.execute(
            "SELECT count(*) FROM (SELECT instrument_id, trading_date FROM bars "
            "GROUP BY 1,2 HAVING count(*)>1)"
        ).fetchone()[0]
        bar_count, bar_instruments, first_bar, last_bar = connection.execute(
            "SELECT count(*), count(DISTINCT instrument_id), min(trading_date), "
            "max(trading_date) FROM bars"
        ).fetchone()
    finally:
        connection.close()

    baseline_counts: dict[str, int] = {}
    if baseline_inventory is not None:
        inventory = _read_json(baseline_inventory)
        for shard in inventory.get("shards", []):
            for value in shard.get("missing_classifications", {}).values():
                baseline_counts[value] = baseline_counts.get(value, 0) + 1
    summary = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_equity_daily_research_coverage_audit",
        "generator_version": GENERATOR_VERSION,
        "bars_index_uri": "quant-research://backfill/cn_equity_daily_backfill_pilot/version=2015-2026.medium-v2/",
        "bars_rows": int(bar_count),
        "bars_instruments": int(bar_instruments),
        "bars_first_date": first_bar.isoformat(),
        "bars_last_date": last_bar.isoformat(),
        "duplicate_instrument_date_keys": int(duplicate_count),
        "classification_counts": {
            status: {"rows": int(rows), "instruments": int(instruments)}
            for status, rows, instruments in summary_rows
        },
        "annual_counts": [
            {"year": int(year), "status": status, "rows": int(rows)}
            for year, status, rows in annual_rows
        ],
        "unexpected_bar_count": len(unexpected_bars),
        "baseline_missing_counts": baseline_counts,
        "after_missing_counts": {
            status: data["rows"]
            for status, data in {
                status: {"rows": int(rows), "instruments": int(instruments)}
                for status, rows, instruments in summary_rows
            }.items()
            if status != "bar_present"
        },
        "verified_suspension_or_no_trade_rows": 0,
        "suspension_source_status": (
            "unavailable; no suspension feed was used to relabel missing bars"
        ),
        "input_files": {
            "bars_index_sha256": _sha256(bars_index),
            "calendar_sha256": _sha256(calendar_parquet),
            "lifecycle_sha256": _sha256(lifecycle_parquet),
            "actions_sha256": _sha256(actions_parquet),
        },
        "read_only": True,
        "observed_at": observed_at.astimezone(UTC).isoformat(),
    }
    summary_path = output_root / "evidence/coverage-audit-summary.json"
    quarantine_path = output_root / "quarantine/coverage-anomalies.json"
    _write_json(summary_path, summary)
    _write_json(
        quarantine_path,
        {
            "schema_version": "1.0.0",
            "dataset_id": "cn_equity_daily_research_coverage_audit",
            "rows": [
                {
                    "instrument_id": instrument_id,
                    "trading_date": trading_date.isoformat(),
                    "status": status,
                }
                for instrument_id, trading_date, status in unexpected_bars[:10000]
            ],
            "total_anomalies": len(unexpected_bars),
            "truncated": len(unexpected_bars) > 10000,
        },
    )
    files = [audit_path, summary_path, quarantine_path]
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_equity_daily_research_coverage_audit",
        "generator_version": GENERATOR_VERSION,
        "immutable": True,
        "read_only": True,
        "row_count": sum(item[1] for item in summary_rows),
        "files": [
            {
                "path": path.relative_to(output_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in files
        ],
        "offline_rebuild_command": (
            "python scripts/audit_cn_research_dataset.py audit --bars-index <index> "
            "--calendar <calendar.parquet> --lifecycle <lifecycle.parquet> "
            "--actions <actions.parquet> --output <output-root>"
        ),
        "created_at": observed_at.astimezone(UTC).isoformat(),
    }
    _write_json(output_root / "manifest.json", manifest)
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    audit = subparsers.add_parser("audit")
    audit.add_argument("--bars-index", required=True, type=Path)
    audit.add_argument("--calendar", required=True, type=Path)
    audit.add_argument("--lifecycle", required=True, type=Path)
    audit.add_argument("--actions", required=True, type=Path)
    audit.add_argument("--output", required=True, type=Path)
    audit.add_argument("--baseline-inventory", type=Path)
    audit.add_argument("--observed-at", type=datetime.fromisoformat)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = audit_dataset(
        bars_index=args.bars_index.expanduser().resolve(),
        calendar_parquet=args.calendar.expanduser().resolve(),
        lifecycle_parquet=args.lifecycle.expanduser().resolve(),
        actions_parquet=args.actions.expanduser().resolve(),
        output_root=args.output.expanduser().resolve(),
        baseline_inventory=args.baseline_inventory.expanduser().resolve()
        if args.baseline_inventory
        else None,
        observed_at=(args.observed_at or datetime.now(UTC)),
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
