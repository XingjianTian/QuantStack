#!/usr/bin/env python3
"""Replay one accepted US Core 500 daily session without Provider access."""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date
from pathlib import Path

from quant_stack.data.models import DatasetLayer
from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.run_us_equity_core_daily import (
    DATASET_ID,
    TRANSFER_SCHEMA,
    _extract_transfer,
    _import_archives,
)
from scripts.run_us_equity_isolated_daily import _load_watermark
from scripts.run_us_equity_isolated_scale import _key


def replay(args: argparse.Namespace) -> dict[str, object]:
    scope_label = getattr(args, "scope_label", "US Core 500")
    transfer_schema = getattr(args, "transfer_schema", TRANSFER_SCHEMA)
    dataset_version_prefix = getattr(args, "dataset_version_prefix", "core-daily-v2")
    state_version_prefix = getattr(
        args, "state_dataset_version_prefix", dataset_version_prefix
    )
    workers = int(getattr(args, "local_import_workers", 1))
    if not 1 <= workers <= 8:
        raise ValueError("local import workers must be between 1 and 8")
    snapshot = load_us_instrument_snapshot(args.snapshot)
    watermark = _load_watermark(
        args.state,
        args.initial_watermark,
        snapshot.snapshot_id,
        state_version_prefix,
    )
    key = _key(args.hmac_key_file)
    args.scratch_parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=getattr(args, "temporary_prefix", "us-core-periodic-replay-"),
        dir=args.scratch_parent,
    ) as temp:
        manifest, archives = _extract_transfer(args.transfer, Path(temp))
        if (
            manifest.get("schema_version") != transfer_schema
            or manifest.get("dataset_id") != DATASET_ID
            or manifest.get("instrument_snapshot_id") != snapshot.snapshot_id
        ):
            raise ValueError("periodic replay transfer does not match the accepted snapshot")
        session = date.fromisoformat(str(manifest["session_date"]))
        if session != args.session or session > watermark:
            raise ValueError("periodic replay requires one already accepted session")
        expected_names = sorted(str(item) for item in manifest["archives"])
        if [path.name for path in archives] != expected_names:
            raise ValueError("periodic replay archive inventory does not match its manifest")
        imports = _import_archives(
            archives,
            data_root=args.isolated_root,
            hmac_key=key,
            workers=workers,
        )

    if any(not item.idempotent_replay for item in imports):
        raise RuntimeError("periodic replay encountered a previously unaccepted archive")

    storage = LocalFileSystemStorage(args.isolated_root)
    rows: list[dict[str, object]] = []
    for imported in imports:
        normalized = next(
            item
            for item in imported.envelope.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        rows.extend(storage.read_dataset(normalized.storage_uri))
    keys = [(str(row["instrument_id"]), row["ts_event"].date()) for row in rows]
    expected_instruments = {str(item) for item in manifest["instrument_ids"]}
    if (
        len(keys) != len(expected_instruments)
        or len(keys) != len(set(keys))
        or {item[0] for item in keys} != expected_instruments
        or {item[1] for item in keys} != {session}
    ):
        raise RuntimeError(f"{scope_label} periodic replay independent readback failed")

    watermark_after = _load_watermark(
        args.state,
        args.initial_watermark,
        snapshot.snapshot_id,
        state_version_prefix,
    )
    if watermark_after != watermark:
        raise RuntimeError("periodic replay changed the authoritative watermark")
    report = {
        "schema_version": (
            "quantstack.us-equity-periodic-replay.v1"
            if transfer_schema != TRANSFER_SCHEMA
            else "quantstack.us-core-periodic-replay.v1"
        ),
        "status": "completed",
        "session_date": session.isoformat(),
        "idempotent_replays": len(imports),
        "rows": len(rows),
        "instruments": len(expected_instruments),
        "duplicate_keys": 0,
        "local_import_workers": workers,
        "watermark_before": watermark.isoformat(),
        "watermark_after": watermark_after.isoformat(),
        "provider_requested": False,
        "history_rebuilt": False,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--initial-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--transfer", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--local-import-workers", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(replay(args), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
