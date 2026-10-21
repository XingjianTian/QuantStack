#!/usr/bin/env python3
"""Build a versioned, offline-rebuildable XSHG/XSHE historical calendar."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

GENERATOR_VERSION = "cn-historical-calendar-v1"


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


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _load_sources(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "source_set_id",
        "calendar_library",
        "venues",
        "official_sources",
        "temporary_closure_dates",
        "unsupported_special_half_days",
    }
    missing = sorted(required.difference(payload))
    if missing:
        raise ValueError(f"calendar source config missing fields: {', '.join(missing)}")
    library = payload["calendar_library"]
    if library["distribution"] != "exchange-calendars":
        raise ValueError("historical calendar requires exchange-calendars")
    if library["version"] != version("exchange-calendars"):
        raise RuntimeError(
            "exchange-calendars version mismatch: "
            f"expected {library['version']}, got {version('exchange-calendars')}"
        )
    if library["calendar_name"] != "XSHG":
        raise ValueError("the controlled CN calendar source must be XSHG")
    if tuple(payload["venues"]) != ("XSHG", "XSHE"):
        raise ValueError("the controlled CN calendar must cover XSHG and XSHE")
    return payload


def _source_evidence(source_config: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = []
    for item in source_config["official_sources"]:
        normalized = json.dumps(item, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        evidence.append(
            {
                **item,
                "evidence_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
                "retrieval_mode": "official_url_recorded_for_offline_rebuild",
            }
        )
    return evidence


def build_calendar(
    *,
    start: date,
    end: date,
    source_config_path: Path,
    output_root: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    if end < start:
        raise ValueError("calendar end must not precede start")
    source_config = _load_sources(source_config_path)
    library = source_config["calendar_library"]

    import exchange_calendars as xc

    calendar = xc.get_calendar(library["calendar_name"])
    if str(calendar.tz) != library["timezone"]:
        raise ValueError("exchange calendar timezone does not match source contract")
    sessions = {
        timestamp.date()
        for timestamp in calendar.sessions_in_range(start.isoformat(), end.isoformat())
    }
    temporary_closures = set(source_config["temporary_closure_dates"])
    unsupported_half_days = set(source_config["unsupported_special_half_days"])
    if unsupported_half_days:
        raise ValueError(
            "unsupported special half-days require an explicit source implementation: "
            + ", ".join(sorted(unsupported_half_days))
        )

    normalized_rows: list[dict[str, Any]] = []
    for day in _date_range(start, end):
        iso_day = day.isoformat()
        if day in sessions:
            state = "session"
            is_session = True
            open_at = f"{iso_day}T{library['normal_open']}"
            close_at = f"{iso_day}T{library['normal_close']}"
        elif day.weekday() >= 5:
            state = "weekend"
            is_session = False
            open_at = close_at = None
        elif iso_day in temporary_closures:
            state = "temporary_closure"
            is_session = False
            open_at = close_at = None
        else:
            state = "holiday"
            is_session = False
            open_at = close_at = None
        for venue in source_config["venues"]:
            normalized_rows.append(
                {
                    "calendar_version": source_config["source_set_id"],
                    "date": iso_day,
                    "venue": venue,
                    "state": state,
                    "is_session": is_session,
                    "timezone": library["timezone"],
                    "open_at": open_at,
                    "close_at": close_at,
                    "source_kind": "exchange_calendars_plus_first_party_notices",
                    "source_calendar": library["calendar_name"],
                    "source_library_version": library["version"],
                }
            )

    unknown = [row for row in normalized_rows if row["state"] == "unknown"]
    if unknown:
        raise ValueError(f"calendar build left {len(unknown)} unknown rows")
    session_rows = [row for row in normalized_rows if row["is_session"]]
    if not session_rows:
        raise ValueError("calendar build produced no sessions")

    raw_payload = {
        "schema_version": "1.0.0",
        "raw_kind": "exchange_calendars_session_snapshot",
        "source_set_id": source_config["source_set_id"],
        "calendar_name": library["calendar_name"],
        "library_distribution": library["distribution"],
        "library_version": library["version"],
        "timezone": library["timezone"],
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "sessions": sorted(day.isoformat() for day in sessions),
        "official_sources": _source_evidence(source_config),
    }
    normalized_payload = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_historical_calendar",
        "calendar_version": source_config["source_set_id"],
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "venues": source_config["venues"],
        "timezone": library["timezone"],
        "normal_open": library["normal_open"],
        "normal_close": library["normal_close"],
        "rows": normalized_rows,
    }
    evidence_payload = {
        "schema_version": "1.0.0",
        "evidence_id": f"{source_config['source_set_id']}:{start.isoformat()}:{end.isoformat()}",
        "source_set_id": source_config["source_set_id"],
        "library_contract": library,
        "official_sources": _source_evidence(source_config),
        "classification_policy": source_config["classification_policy"],
        "source_completeness": "recorded_first_party_indexes_and_representative_notices;"
        "the deterministic session set is supplied by the pinned library",
        "unknown_rows": len(unknown),
        "session_rows": len(session_rows),
        "holiday_rows": sum(row["state"] == "holiday" for row in normalized_rows),
        "weekend_rows": sum(row["state"] == "weekend" for row in normalized_rows),
        "temporary_closure_rows": sum(
            row["state"] == "temporary_closure" for row in normalized_rows
        ),
        "observed_at": observed_at.astimezone(UTC).isoformat(),
    }

    raw_path = output_root / "raw/source-snapshot.json"
    normalized_path = output_root / "normalized/calendar-days.json"
    evidence_path = output_root / "evidence/calendar-evidence.json"
    _write_json(raw_path, raw_payload)
    _write_json(normalized_path, normalized_payload)
    _write_json(evidence_path, evidence_payload)

    parquet_path: Path | None = None
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        parquet_path = output_root / "normalized/calendar-days.parquet"
        table = pa.Table.from_pylist(normalized_rows)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_path, compression="zstd")
    except ImportError:
        parquet_path = None

    files = [raw_path, normalized_path, evidence_path]
    if parquet_path is not None:
        files.append(parquet_path)
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_historical_calendar",
        "calendar_version": source_config["source_set_id"],
        "generator_version": GENERATOR_VERSION,
        "generator_python": platform.python_version(),
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "venues": source_config["venues"],
        "timezone": library["timezone"],
        "rows": len(normalized_rows),
        "sessions_per_venue": len(sessions),
        "unknown_rows": len(unknown),
        "immutable": True,
        "offline_rebuild_command": (
            "python scripts/build_cn_historical_calendar.py build "
            f"--start {start.isoformat()} --end {end.isoformat()} "
            f"--sources {source_config_path.as_posix()} --output {output_root.as_posix()}"
        ),
        "files": [
            {
                "path": path.relative_to(output_root).as_posix(),
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
    build.add_argument("--start", required=True, type=_parse_date)
    build.add_argument("--end", required=True, type=_parse_date)
    build.add_argument(
        "--sources",
        type=Path,
        default=Path("config/cn_historical_calendar_sources_v1.json"),
    )
    build.add_argument("--output", required=True, type=Path)
    build.add_argument("--observed-at", type=datetime.fromisoformat)
    return parser


def main() -> None:
    args = _parser().parse_args()
    observed_at = args.observed_at or datetime.now(UTC)
    result = build_calendar(
        start=args.start,
        end=args.end,
        source_config_path=args.sources.expanduser().resolve(),
        output_root=args.output.expanduser().resolve(),
        observed_at=observed_at,
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
