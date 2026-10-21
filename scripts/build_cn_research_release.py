#!/usr/bin/env python3
"""Build the multi-table CN research release and query proof on NAS."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

GENERATOR_VERSION = "cn-research-release-v1"
RELEASE_VERSION = "2026-08-05.research-v1"


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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sql_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _metrics(connection: duckdb.DuckDBPyConnection, table: str) -> dict[str, Any]:
    total, instruments, first_date, last_date = connection.execute(
        f"SELECT count(*), count(DISTINCT instrument_id), min(trading_date), "
        f"max(trading_date) FROM {table}"
    ).fetchone()
    duplicates = connection.execute(
        f"SELECT count(*) FROM (SELECT instrument_id,trading_date FROM {table} "
        "GROUP BY 1,2 HAVING count(*)>1)"
    ).fetchone()[0]
    return {
        "rows": int(total),
        "instruments": int(instruments),
        "first_date": first_date.isoformat() if first_date else None,
        "last_date": last_date.isoformat() if last_date else None,
        "duplicate_instrument_date_keys": int(duplicates),
    }


def _copy_release_files(
    *,
    release_root: Path,
    calendar_root: Path,
    lifecycle_root: Path,
    factors_root: Path,
    audit_root: Path,
    bars_manifest: Path,
) -> dict[str, Path]:
    component_roots = {
        "calendar": calendar_root,
        "lifecycle": lifecycle_root,
        "corporate_actions": factors_root,
        "coverage_audit": audit_root,
    }
    destinations: dict[str, Path] = {}
    for component, source_root in component_roots.items():
        if not source_root.is_dir():
            raise FileNotFoundError(f"component root does not exist: {source_root}")
        destination_root = release_root / component
        for source in sorted(path for path in source_root.rglob("*") if path.is_file()):
            relative = source.relative_to(source_root)
            destination = destination_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
    bars_destination = release_root / "references/bars-mirror-manifest.json"
    bars_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(bars_manifest, bars_destination)
    destinations.update(
        {
            "calendar": release_root / "calendar/normalized/calendar-days.parquet",
            "lifecycle": release_root / "lifecycle/normalized/instrument-lifecycle.parquet",
            "actions": release_root / "corporate_actions/normalized/corporate-actions.parquet",
            "factors": release_root / "corporate_actions/normalized/adjustment-factors.parquet",
            "audit": release_root / "coverage_audit/normalized/coverage-audit.parquet",
            "audit_summary": release_root / "coverage_audit/evidence/coverage-audit-summary.json",
            "audit_quarantine": release_root / "coverage_audit/quarantine/coverage-anomalies.json",
            "lifecycle_quarantine": release_root / "lifecycle/quarantine/lifecycle-conflicts.json",
            "factors_quarantine": release_root
            / "corporate_actions/quarantine/corporate-actions-factors-conflicts.json",
            "lifecycle_evidence": release_root / "lifecycle/evidence/lifecycle-evidence.json",
            "factors_evidence": release_root
            / "corporate_actions/evidence/corporate-actions-factors-evidence.json",
            "bars_manifest": bars_destination,
        }
    )
    for name, path in destinations.items():
        if not path.is_file():
            raise FileNotFoundError(f"release input missing after copy ({name}): {path}")
    return destinations


def _build_database(
    *,
    temporary: Path,
    bars_index: Path,
    files: dict[str, Path],
) -> dict[str, Any]:
    connection = duckdb.connect(str(temporary))
    try:
        connection.execute(f"ATTACH {_sql_literal(bars_index)} AS bars_source (READ_ONLY)")
        connection.execute("CREATE TABLE bars_unadjusted AS SELECT * FROM bars_source.pilot_bars")
        connection.execute(
            "CREATE TABLE calendar_days AS SELECT CAST(date AS DATE) AS date, venue, state, "
            "is_session, timezone, open_at, close_at, calendar_version FROM read_parquet(?)",
            [str(files["calendar"])],
        )
        connection.execute(
            "CREATE TABLE instrument_lifecycle AS SELECT * FROM read_parquet(?)",
            [str(files["lifecycle"])],
        )
        connection.execute(
            "CREATE TABLE corporate_actions AS SELECT * FROM read_parquet(?)",
            [str(files["actions"])],
        )
        connection.execute(
            "CREATE TABLE adjustment_factors AS SELECT * FROM read_parquet(?)",
            [str(files["factors"])],
        )
        connection.execute(
            "CREATE TABLE coverage_audit AS SELECT * FROM read_parquet(?)",
            [str(files["audit"])],
        )
        quarantine = _load_json(files["audit_quarantine"])
        connection.execute(
            "CREATE TABLE quarantine_scopes(scope VARCHAR, instrument_id VARCHAR, "
            "trading_date DATE, status VARCHAR, reason VARCHAR)"
        )
        quarantine_rows = [
            (
                "coverage_audit",
                row.get("instrument_id"),
                row.get("trading_date"),
                row.get("status"),
                "unexpected_bar_or_non_session",
            )
            for row in quarantine.get("rows", [])
        ]
        lifecycle_quarantine = _load_json(files["lifecycle_quarantine"])
        quarantine_rows.extend(
            (
                "lifecycle_conflicts",
                row.get("instrument_id"),
                row.get("trading_date"),
                "conflict",
                row.get("reason", "lifecycle_conflict"),
            )
            for row in lifecycle_quarantine.get("rows", [])
        )
        factors_quarantine = _load_json(files["factors_quarantine"])
        quarantine_rows.extend(
            (
                "factor_conflicts",
                row.get("instrument_id"),
                row.get("effective_date"),
                "conflict",
                row.get("reason", "corporate_actions_factor_conflict"),
            )
            for row in factors_quarantine.get("rows", [])
        )
        if quarantine_rows:
            connection.executemany(
                "INSERT INTO quarantine_scopes VALUES (?, ?, ?, ?, ?)",
                quarantine_rows,
            )
        connection.execute(
            "CREATE TABLE research_release_metadata(key VARCHAR PRIMARY KEY, value VARCHAR)"
        )
        metadata = {
            "release_version": RELEASE_VERSION,
            "generator_version": GENERATOR_VERSION,
            "bars_adjustment": "unadjusted",
            "pit_safe": "false",
            "qfq_enabled": "false",
            "hfq_enabled": "false",
            "canonical_eligible": "false",
            "ss_candidate": "false",
            "production_pointer_updated": "false",
            "deploy_or_cutover": "false",
            "query_index_rebuildable": "true",
            "mac_offline": "true",
            "per_symbol_http_queries": "0",
            "ss_database_accessed": "false",
            "factor_direction_validation": "blocked",
            "pit_safe_lifecycle": "blocked_by_current_snapshot_source",
            "suspension_feed": "unavailable",
            "consumer_policy": "research-only",
            "immutable": "true",
            "read_only_inputs": "true",
        }
        connection.executemany(
            "INSERT INTO research_release_metadata VALUES (?, ?)",
            sorted(metadata.items()),
        )
        connection.execute(
            "CREATE INDEX bars_unadjusted_instrument_date "
            "ON bars_unadjusted(instrument_id,trading_date)"
        )
        connection.execute("CREATE INDEX calendar_days_venue_date ON calendar_days(venue,date)")
        connection.execute(
            "CREATE INDEX lifecycle_instrument_listing "
            "ON instrument_lifecycle(instrument_id,listing_date)"
        )
        connection.execute(
            "CREATE INDEX actions_instrument_date "
            "ON corporate_actions(instrument_id,effective_date)"
        )
        connection.execute(
            "CREATE INDEX factors_instrument_date "
            "ON adjustment_factors(instrument_id,effective_date)"
        )
        connection.execute(
            "CREATE INDEX audit_instrument_date ON coverage_audit(instrument_id,date)"
        )
        connection.execute("CHECKPOINT")
        return _metrics(connection, "bars_unadjusted")
    finally:
        connection.close()


def _query_proof(
    index_path: Path, *, release_root: Path, metrics: dict[str, Any]
) -> dict[str, Any]:
    connection = duckdb.connect(str(index_path), read_only=True)
    started = time.perf_counter()
    try:
        instrument_date = connection.execute(
            "SELECT instrument_id,trading_date,close FROM bars_unadjusted "
            "WHERE instrument_id=? AND trading_date BETWEEN ? AND ? "
            "ORDER BY trading_date LIMIT 5",
            ["XSHG.600005", "2015-01-01", "2026-08-04"],
        ).fetchall()
        annual = connection.execute(
            "SELECT year(trading_date),count(*) FROM bars_unadjusted GROUP BY 1 ORDER BY 1"
        ).fetchall()
        venues = connection.execute(
            "SELECT venue,count(*),count(DISTINCT instrument_id) FROM bars_unadjusted "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
        lifecycle_intervals = connection.execute(
            "SELECT count(*) FROM instrument_lifecycle WHERE listing_date IS NOT NULL"
        ).fetchone()[0]
        pre_post = connection.execute(
            "SELECT coverage_status,count(*) FROM coverage_audit "
            "WHERE coverage_status IN ('verified_pre_listing','verified_post_delisting') "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
        session_missing = connection.execute(
            "SELECT count(*) FROM coverage_audit "
            "WHERE coverage_status='unexpected_missing_on_session'"
        ).fetchone()[0]
        action_window = connection.execute(
            "SELECT count(*) FROM bars_unadjusted b JOIN corporate_actions a "
            "ON a.instrument_id=b.instrument_id AND a.effective_date=b.trading_date"
        ).fetchone()[0]
        duplicates = connection.execute(
            "SELECT count(*) FROM (SELECT instrument_id,trading_date FROM bars_unadjusted "
            "GROUP BY 1,2 HAVING count(*)>1)"
        ).fetchone()[0]
        sample_sql = (
            "SELECT instrument_id,trading_date,close FROM bars_unadjusted "
            "ORDER BY md5(instrument_id),trading_date LIMIT 50"
        )
        repeat_a = connection.execute(sample_sql).fetchall()
        repeat_b = connection.execute(sample_sql).fetchall()
        table_counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "bars_unadjusted",
                "calendar_days",
                "instrument_lifecycle",
                "corporate_actions",
                "adjustment_factors",
                "coverage_audit",
                "quarantine_scopes",
                "research_release_metadata",
            )
        }
        metadata = dict(
            connection.execute(
                "SELECT key, value FROM research_release_metadata ORDER BY key"
            ).fetchall()
        )
    finally:
        connection.close()
    usage = shutil.disk_usage(release_root)
    return {
        "schema_version": "1.0.0",
        "index_uri": "quant-research://cn_equity_daily_research_release/query/version=2026-08-05.research-v1/research-release.duckdb",
        "index_bytes": index_path.stat().st_size,
        "table_counts": {key: int(value) for key, value in table_counts.items()},
        "bars_metrics": metrics,
        "instrument_date_rows": len(instrument_date),
        "annual_rows": [{"year": int(year), "rows": int(rows)} for year, rows in annual],
        "venue_rows": [
            {"venue": venue, "rows": int(rows), "instruments": int(instruments)}
            for venue, rows, instruments in venues
        ],
        "lifecycle_interval_rows": int(lifecycle_intervals),
        "pre_post_lifecycle_rows": [
            {"status": status, "rows": int(rows)} for status, rows in pre_post
        ],
        "session_missing_rows": int(session_missing),
        "corporate_action_window_rows": int(action_window),
        "duplicate_instrument_date_keys": int(duplicates),
        "qfq_hfq_comparison": {
            "status": "blocked",
            "reason": "factor direction validation incomplete",
        },
        "release_metadata": metadata,
        "repeat_query_equal": repeat_a == repeat_b,
        "mac_offline": True,
        "query_index_rebuildable": True,
        "per_symbol_http_queries": 0,
        "ss_database_accessed": False,
        "capacity_bytes": {"total": usage.total, "used": usage.used, "available": usage.free},
        "query_ms": round((time.perf_counter() - started) * 1000, 3),
        "cleanup_policy": (
            "temporary index removed after atomic replacement; no source files removed"
        ),
    }


def build_release(
    *,
    release_root: Path,
    bars_index: Path,
    bars_manifest: Path,
    calendar_root: Path,
    lifecycle_root: Path,
    factors_root: Path,
    audit_root: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    release_root.mkdir(parents=True, exist_ok=True)
    before = shutil.disk_usage(release_root)
    files = _copy_release_files(
        release_root=release_root,
        calendar_root=calendar_root,
        lifecycle_root=lifecycle_root,
        factors_root=factors_root,
        audit_root=audit_root,
        bars_manifest=bars_manifest,
    )
    index_dir = release_root / "query"
    index_dir.mkdir(parents=True, exist_ok=True)
    index_path = index_dir / "research-release.duckdb"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=index_dir, prefix=".research-release.duckdb.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    started = time.perf_counter()
    try:
        metrics = _build_database(temporary=temporary, bars_index=bars_index, files=files)
        if metrics != {
            "rows": 903363,
            "instruments": 500,
            "first_date": "2015-01-05",
            "last_date": "2026-08-04",
            "duplicate_instrument_date_keys": 0,
        }:
            raise ValueError(f"bars_unadjusted source metrics mismatch: {metrics}")
        os.replace(temporary, index_path)
    finally:
        temporary.unlink(missing_ok=True)
    build_ms = round((time.perf_counter() - started) * 1000, 3)
    proof = _query_proof(index_path, release_root=release_root, metrics=metrics)
    proof["build_ms"] = build_ms
    proof["capacity_before"] = {
        "total": before.total,
        "used": before.used,
        "available": before.free,
    }
    proof_path = index_dir / "nas-query-proof.json"
    _write_json(proof_path, proof)
    release_files = [path for path in release_root.rglob("*") if path.is_file()]
    release_files = [
        path
        for path in release_files
        if path.relative_to(release_root).as_posix()
        not in {
            "query/research-release.duckdb",
            "query/nas-query-proof.json",
            "research-release-manifest.json",
        }
    ]
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_equity_daily_research_release",
        "release_version": RELEASE_VERSION,
        "release_uri": f"quant-research://cn_equity_daily_research_release/version={RELEASE_VERSION}/",
        "generator_version": GENERATOR_VERSION,
        "bars_component": {
            "uri": "quant-research://backfill/cn_equity_daily_backfill_pilot/version=2015-2026.medium-v2/",
            "manifest_sha256": _sha256(files["bars_manifest"]),
            "rows": metrics["rows"],
            "instruments": metrics["instruments"],
            "first_date": metrics["first_date"],
            "last_date": metrics["last_date"],
            "adjustment": "unadjusted",
        },
        "calendar_version": "cn-historical-calendar-sources-v1",
        "lifecycle_version": "cn-instrument-lifecycle-v1",
        "corporate_actions_factors_version": "cn-corporate-actions-factors-v1",
        "audit_version": "cn-research-audit-v1",
        "pit_safe": False,
        "qfq_enabled": False,
        "hfq_enabled": False,
        "canonical_eligible": False,
        "ss_candidate": False,
        "production_pointer_updated": False,
        "deploy_or_cutover": False,
        "consumer_policy": "research-only; no canonical or Stock Screener consumer reads",
        "gaps": {
            "suspension_feed": "unavailable",
            "factor_direction_validation": "blocked",
            "pit_safe_lifecycle": "blocked_by_current_snapshot_source",
        },
        "quarantine_scopes": ["coverage_anomalies", "lifecycle_conflicts", "factor_conflicts"],
        "query_index_uri": "quant-research://cn_equity_daily_research_release/query/version=2026-08-05.research-v1/research-release.duckdb",
        "query_proof_uri": "quant-research://cn_equity_daily_research_release/query/version=2026-08-05.research-v1/nas-query-proof.json",
        "immutable": True,
        "read_only_inputs": True,
        "files": [
            {
                "path": path.relative_to(release_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in sorted(release_files)
        ],
        "created_at": observed_at.astimezone(UTC).isoformat(),
        "offline_rebuild_command": (
            "python scripts/build_cn_research_release.py build "
            "--release-root <release-root> --bars-index <bars-index> "
            "--bars-manifest <bars-manifest.json> --calendar-root <calendar-root> "
            "--lifecycle-root <lifecycle-root> --factors-root <factors-root> "
            "--audit-root <audit-root>"
        ),
    }
    _write_json(release_root / "research-release-manifest.json", manifest)
    return {"manifest": manifest, "proof": proof}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    for name in (
        "release-root",
        "bars-index",
        "bars-manifest",
        "calendar-root",
        "lifecycle-root",
        "factors-root",
        "audit-root",
    ):
        build.add_argument(f"--{name}", required=True, type=Path)
    build.add_argument("--observed-at", type=datetime.fromisoformat)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = build_release(
        release_root=args.release_root.expanduser().resolve(),
        bars_index=args.bars_index.expanduser().resolve(),
        bars_manifest=args.bars_manifest.expanduser().resolve(),
        calendar_root=args.calendar_root.expanduser().resolve(),
        lifecycle_root=args.lifecycle_root.expanduser().resolve(),
        factors_root=args.factors_root.expanduser().resolve(),
        audit_root=args.audit_root.expanduser().resolve(),
        observed_at=(args.observed_at or datetime.now(UTC)),
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
