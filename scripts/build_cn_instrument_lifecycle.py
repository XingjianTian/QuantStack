#!/usr/bin/env python3
"""Fetch or replay effective-dated lifecycle facts for the controlled CN universe."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from quant_stack.data.providers.cn.live import (
    BAOSTOCK_BLACKLIST_ERROR_CODE,
    BaoStockAccessRejected,
    BaoStockRequestBudget,
)
from quant_stack.data.providers.cn.live.providers import _scoped_baostock_socket_timeout

GENERATOR_VERSION = "cn-instrument-lifecycle-v1"
PROVIDER = "baostock"
ENDPOINT = "query_stock_basic"
PROVIDER_VERSION = "0.9.3"
RAW_URI_PREFIX = "quant-research://cn_equity_research/instrument_lifecycle/raw/"


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


def _parse_date(value: object) -> date | None:
    if value in (None, "", "null"):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _load_universe(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    instruments = payload.get("instruments")
    if not isinstance(instruments, list) or len(instruments) != 500:
        raise ValueError("lifecycle build requires the 500-instrument medium universe")
    return instruments


def _result_rows(result: object) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    error_code = str(getattr(result, "error_code", ""))
    if error_code != "0":
        raise ConnectionError(
            "BaoStock lifecycle query failed: " + str(getattr(result, "error_msg", error_code))
        )
    fields = tuple(str(field) for field in getattr(result, "fields", ()))
    rows: list[dict[str, str]] = []
    while result.next():  # type: ignore[attr-defined]
        values = tuple(str(value) for value in result.get_row_data())  # type: ignore[attr-defined]
        if len(values) != len(fields):
            raise ValueError("BaoStock lifecycle row does not match native schema")
        rows.append(dict(zip(fields, values, strict=True)))
    return fields, rows


def fetch_stock_basic(
    *,
    observed_at: datetime,
    request_budget: BaoStockRequestBudget | None = None,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if version(PROVIDER) != PROVIDER_VERSION:
        raise RuntimeError(f"{PROVIDER} version mismatch")
    import importlib

    client = importlib.import_module(PROVIDER)
    budget = request_budget or BaoStockRequestBudget.from_default()
    with budget.session_lock():
        budget.wait_for_batch_slot()
        budget.ensure_capacity(2)  # login + query_stock_basic
        budget.begin_batch(1)
        try:
            with _scoped_baostock_socket_timeout(client, 30.0):
                logged_in = False
                try:
                    budget.reserve(1)
                    login = client.login()
                    login_code = str(getattr(login, "error_code", ""))
                    login_message = str(getattr(login, "error_msg", login_code))
                    if login_code != "0":
                        if login_code == BAOSTOCK_BLACKLIST_ERROR_CODE or "黑名单" in login_message:
                            budget.mark_blocked(error_code=login_code, message=login_message)
                            raise BaoStockAccessRejected(login_code, login_message)
                        raise ConnectionError(
                            f"BaoStock lifecycle login failed: {login_code} {login_message}"
                        )
                    logged_in = True
                    budget.reserve(1)
                    result = client.query_stock_basic()
                    fields, rows = _result_rows(result)
                finally:
                    if logged_in:
                        client.logout()
        finally:
            budget.complete_batch()
    return (
        {
            "schema_version": "1.0.0",
            "provider": PROVIDER,
            "provider_version": PROVIDER_VERSION,
            "endpoint": ENDPOINT,
            "native_fields": list(fields),
            "retrieved_at": observed_at.astimezone(UTC).isoformat(),
            "row_count": len(rows),
            "rows": rows,
        },
        rows,
    )


def _normalize_lifecycle(
    universe: list[dict[str, Any]],
    rows: list[dict[str, str]],
    *,
    raw_uri: str,
    raw_sha256: str,
    observed_at: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_code: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        code = str(row.get("code", "")).lower()
        if code:
            by_code.setdefault(code, []).append(row)
    normalized: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    for instrument in universe:
        provider_symbol = str(instrument["provider_symbol"]).lower()
        matches = by_code.get(provider_symbol, [])
        conflict = len(matches) > 1
        source = matches[0] if matches else {}
        listing_date = _parse_date(source.get("ipoDate"))
        delisting_date = _parse_date(source.get("outDate"))
        raw_status = str(source.get("status", ""))
        active_status = {"1": "active", "0": "inactive"}.get(raw_status, "unknown")
        invalid_range = (
            listing_date is not None
            and delisting_date is not None
            and delisting_date < listing_date
        )
        if conflict or invalid_range:
            evidence_status = "blocked"
            quarantine.append(
                {
                    "instrument_id": instrument["instrument_id"],
                    "reason": (
                        "duplicate_provider_rows"
                        if conflict
                        else "invalid_effective_date_range"
                    ),
                    "raw_rows": matches,
                }
            )
        elif not matches or listing_date is None:
            evidence_status = "unavailable"
        elif delisting_date is None:
            evidence_status = "partial"
        else:
            evidence_status = "verified"
        limitations = [
            "query_stock_basic is a provider snapshot and is not a historical name/status timeline",
            "missing outDate is not treated as proof of active status",
            "bars are never used to infer listing or delisting dates",
        ]
        if not matches:
            limitations.append("provider returned no row for this controlled instrument")
        normalized.append(
            {
                "lifecycle_version": GENERATOR_VERSION,
                "instrument_id": instrument["instrument_id"],
                "symbol": instrument["symbol"],
                "venue": instrument["venue"],
                "provider_symbol": provider_symbol,
                "provider": PROVIDER,
                "endpoint": ENDPOINT,
                "provider_version": PROVIDER_VERSION,
                "provider_name": source.get("code_name") or None,
                "provider_status_raw": raw_status or None,
                "active_status": active_status,
                "listing_date": listing_date.isoformat() if listing_date else None,
                "delisting_date": delisting_date.isoformat() if delisting_date else None,
                "status_effective_from": None,
                "status_effective_to": None,
                "effective_date_semantics": "provider_fields_ipoDate_outDate_when_present",
                "evidence_status": evidence_status,
                "raw_uri": raw_uri,
                "raw_sha256": raw_sha256,
                "observed_at": observed_at.astimezone(UTC).isoformat(),
                "source_metadata": {
                    "native_fields": sorted(source),
                    "row_count_for_provider_symbol": len(matches),
                },
                "limitations": limitations,
            }
        )
    return normalized, quarantine


def build_lifecycle(
    *,
    universe_path: Path,
    output_root: Path,
    raw_path: Path | None,
    observed_at: datetime,
) -> dict[str, Any]:
    universe = _load_universe(universe_path)
    if raw_path is None:
        raw_payload, rows = fetch_stock_basic(
            observed_at=observed_at,
            request_budget=BaoStockRequestBudget.for_data_root(output_root),
        )
        raw_path = output_root / "raw/baostock-query_stock_basic.json"
        _write_json(raw_path, raw_payload)
    else:
        raw_path = raw_path.expanduser().resolve()
        raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        rows = raw_payload.get("rows", [])
        if raw_payload.get("provider") != PROVIDER or raw_payload.get("endpoint") != ENDPOINT:
            raise ValueError("lifecycle raw artifact does not match BaoStock source contract")
        output_raw_path = output_root / "raw" / raw_path.name
        if raw_path != output_raw_path:
            output_raw_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(raw_path, output_raw_path)
            raw_path = output_raw_path
    raw_sha256 = _sha256(raw_path)
    raw_uri = RAW_URI_PREFIX + raw_path.name
    normalized, quarantine = _normalize_lifecycle(
        universe,
        rows,
        raw_uri=raw_uri,
        raw_sha256=raw_sha256,
        observed_at=observed_at,
    )
    normalized_path = output_root / "normalized/instrument-lifecycle.json"
    evidence_path = output_root / "evidence/lifecycle-evidence.json"
    quarantine_path = output_root / "quarantine/lifecycle-conflicts.json"
    _write_json(normalized_path, {"schema_version": "1.0.0", "rows": normalized})
    _write_json(
        evidence_path,
        {
            "schema_version": "1.0.0",
            "dataset_id": "instrument_lifecycle",
            "lifecycle_version": GENERATOR_VERSION,
            "provider": PROVIDER,
            "endpoint": ENDPOINT,
            "provider_version": PROVIDER_VERSION,
            "raw_uri": raw_uri,
            "raw_sha256": raw_sha256,
            "universe_count": len(universe),
            "normalized_count": len(normalized),
            "verified_count": sum(row["evidence_status"] == "verified" for row in normalized),
            "partial_count": sum(row["evidence_status"] == "partial" for row in normalized),
            "unavailable_count": sum(
                row["evidence_status"] == "unavailable" for row in normalized
            ),
            "blocked_count": sum(row["evidence_status"] == "blocked" for row in normalized),
            "pit_safe": False,
            "limitations": [
                "BaoStock stock basic is a current snapshot, not a historical lifecycle feed",
                "name/status changes require first-party announcement ingestion before PIT use",
                "missing or conflicting facts remain explicit and quarantined",
            ],
            "observed_at": observed_at.astimezone(UTC).isoformat(),
        },
    )
    _write_json(
        quarantine_path,
        {
            "schema_version": "1.0.0",
            "dataset_id": "instrument_lifecycle",
            "quarantine_count": len(quarantine),
            "rows": quarantine,
        },
    )
    parquet_path: Path | None = None
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        parquet_path = output_root / "normalized/instrument-lifecycle.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(normalized), parquet_path, compression="zstd")
    except ImportError:
        parquet_path = None
    files = [raw_path, normalized_path, evidence_path, quarantine_path]
    if parquet_path is not None:
        files.append(parquet_path)
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "instrument_lifecycle",
        "lifecycle_version": GENERATOR_VERSION,
        "generator_python": platform.python_version(),
        "provider": PROVIDER,
        "endpoint": ENDPOINT,
        "provider_version": PROVIDER_VERSION,
        "universe_count": len(universe),
        "row_count": len(normalized),
        "quarantine_count": len(quarantine),
        "pit_safe": False,
        "immutable": True,
        "offline_rebuild_command": (
            "python scripts/build_cn_instrument_lifecycle.py build "
            "--universe config/cn_backfill_medium_universe_v1.json "
            "--raw <raw-artifact> --output <output-root>"
        ),
        "files": [
            {
                "path": path.relative_to(output_root).as_posix()
                if output_root in path.parents
                else f"raw/{path.name}",
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in files
        ],
        "created_at": observed_at.astimezone(UTC).isoformat(),
    }
    _write_json(output_root / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--universe", required=True, type=Path)
    build.add_argument("--output", required=True, type=Path)
    build.add_argument("--raw", type=Path)
    build.add_argument("--observed-at", type=datetime.fromisoformat)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = build_lifecycle(
        universe_path=args.universe.expanduser().resolve(),
        output_root=args.output.expanduser().resolve(),
        raw_path=args.raw,
        observed_at=(args.observed_at or datetime.now(UTC)),
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
