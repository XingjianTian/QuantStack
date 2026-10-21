#!/usr/bin/env python3
"""Install one CN session package from an already completed legacy daily run.

This migration is deliberately zero-Provider. It reads the normalized batch
Parquets already present in the retained tar archives, validates their declared
row coverage, and installs one current session package. It does not recalculate
archive hashes or verify the retired HMAC envelope.
"""

from __future__ import annotations

import argparse
import json
import tarfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_stack.data.session_parquet import install_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes


def _batch_records(archive: Path, *, expected_rows: int) -> list[dict[str, object]]:
    with tarfile.open(archive, mode="r") as bundle:
        members = [
            item
            for item in bundle.getmembers()
            if item.isfile()
            and item.name.startswith("artifacts/normalized/bars_1d/market=CN/")
            and item.name.endswith("/bars.parquet")
        ]
        if len(members) != 1:
            raise ValueError(f"legacy archive must contain one CN bars Parquet: {archive}")
        stream = bundle.extractfile(members[0])
        if stream is None:
            raise ValueError(f"legacy archive bars Parquet is unreadable: {archive}")
        table = pq.read_table(pa.BufferReader(stream.read()))
    if table.num_rows != expected_rows:
        raise ValueError(
            f"legacy archive row count differs from its manifest: {archive} "
            f"({table.num_rows} != {expected_rows})"
        )
    return [dict(item) for item in table.to_pylist()]


def recover(
    *,
    legacy_run_root: Path,
    output_root: Path,
    session: date,
) -> dict[str, object]:
    manifest_path = legacy_run_root / "archive-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "1.0.0":
        raise ValueError("unsupported legacy daily archive manifest")
    if manifest.get("session_date") != session.isoformat():
        raise ValueError("legacy daily archive session does not match requested recovery")
    entries = list(manifest.get("entries", []))
    if not entries:
        raise ValueError("legacy daily archive manifest contains no batches")
    archive_paths = [str(item["archive_relative_path"]) for item in entries]
    if len(archive_paths) != len(set(archive_paths)):
        raise ValueError("legacy daily archive manifest repeats a batch archive")

    records: list[dict[str, object]] = []
    for item in entries:
        records.extend(
            _batch_records(
                legacy_run_root / str(item["archive_relative_path"]),
                expected_rows=int(item["records_received"]),
            )
        )
    requested = int(manifest["active_universe_instruments"])
    if len(records) != requested:
        raise ValueError(
            "zero-Provider legacy recovery requires complete returned coverage: "
            f"{len(records)} != {requested}"
        )
    package = install_session_package(
        records=records,
        output_root=output_root,
        market="CN",
        session=session,
        dataset_id="cn_equity_daily",
        version=f"{session.isoformat()}.session-v1",
        adjustment="raw",
        requested_instruments=requested,
    )
    return {
        "schema_version": "quantstack.cn-session-legacy-recovery.v1",
        "status": "completed",
        "session": session.isoformat(),
        "legacy_batches_read": len(entries),
        "rows": package.row_count,
        "instruments": package.instrument_count,
        "omissions": package.omission_count,
        "package": str(package.path),
        "installed": package.installed,
        "provider_requested": False,
        "business_files_written": 2 if package.installed else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-run-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--scheduler-summary", type=Path)
    args = parser.parse_args()
    result = recover(
        legacy_run_root=args.legacy_run_root.resolve(),
        output_root=args.output_root.resolve(),
        session=args.session,
    )
    if args.report is not None:
        atomic_replace_bytes(
            args.report,
            (json.dumps(result, indent=2, sort_keys=True) + "\n").encode(),
        )
    if args.scheduler_summary is not None:
        summary = {
            "schema_version": "quantstack.cn-session-daily-run.v1",
            "status": "completed",
            "session_date": args.session.isoformat(),
            "records_received": result["rows"],
            "distinct_instruments": result["instruments"],
            "omissions": result["omissions"],
            "persistence": "single_session_parquet",
            "provider_retries": 0,
            "provider_requested": False,
            "recovered_from_legacy_archives": True,
            "catalog_published": False,
            "production_pointer_updated": False,
        }
        atomic_replace_bytes(
            args.scheduler_summary,
            (json.dumps(summary, indent=2, sort_keys=True) + "\n").encode(),
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
