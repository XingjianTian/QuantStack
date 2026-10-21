#!/usr/bin/env python3
"""Legacy archive replay retained for pre-Phase-2.17 manual recovery only."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

from pydantic import ValidationError

from quant_stack.daily_cli import FullMarketArchiveManifest
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.run_daily_archive_legacy import (
    _load_config,
    _remote_import,
    _remote_verify,
)


def run(config_path: Path, *, session: date) -> dict[str, object]:
    config = _load_config(config_path)
    run_root = config.work_root / f"daily-{session.strftime('%Y%m%d')}"
    summary_path = run_root / "runner-summary.json"
    manifest_path = run_root / "remote-archive-manifest.json"
    if not summary_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError("completed daily run evidence is unavailable")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest = FullMarketArchiveManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if summary.get("status") != "completed" or manifest.session_date != session:
        raise ValueError("periodic replay requires one completed matching daily session")

    manifest_address = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    remote_manifest = f"{config.remote_staging_root}/inbox/{manifest_address}.tar"
    token = session.strftime("%Y%m%d")
    replay = _remote_import(
        config=config,
        remote_manifest=remote_manifest,
        session_token=token,
        suffix="periodic-replay",
    )
    readback = _remote_verify(
        config=config,
        remote_manifest=remote_manifest,
        session_token=f"{token}-periodic",
    )
    expected = len(manifest.entries)
    if (
        replay.get("imported_archives") != expected
        or replay.get("idempotent_replays") != expected
        or readback.get("verified_receipts") != expected
        or readback.get("persisted_bar_rows") != manifest.records_received
        or readback.get("duplicate_keys") != 0
        or readback.get("source_commit") != summary.get("source_commit")
    ):
        raise RuntimeError("periodic NAS idempotent replay verification failed")
    report = {
        "schema_version": "quantstack.cn-periodic-replay.v1",
        "status": "completed",
        "session_date": session.isoformat(),
        "idempotent_replays": replay["idempotent_replays"],
        "verified_receipts": readback["verified_receipts"],
        "persisted_bar_rows": readback["persisted_bar_rows"],
        "provider_requested": False,
        "qfq_rebuilt": False,
    }
    atomic_replace_bytes(
        run_root / "periodic-replay-report.json",
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.config, session=args.session), ensure_ascii=False))
    except (FileNotFoundError, RuntimeError, ValueError, ValidationError) as exc:
        print(str(exc))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
