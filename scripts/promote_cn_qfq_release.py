#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from quant_stack.data.qfq_release import promote_isolated_qfq_release, restore_qfq_release
from quant_stack.data.storage.atomic import atomic_replace_bytes


def _write_report(path: Path, report: dict[str, object], *, shared_gid: int | None) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    if shared_gid is not None:
        os.chown(path, -1, shared_gid)
        os.chmod(path, 0o640)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote one verified isolated qfq worker import with rollback"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    promote = subparsers.add_parser("promote")
    promote.add_argument("--source-data-root", type=Path, required=True)
    promote.add_argument("--central-data-root", type=Path, required=True)
    promote.add_argument("--import-receipt", type=Path, required=True)
    promote.add_argument("--allowed-source-commit", action="append", required=True)
    promote.add_argument("--snapshot-dir", type=Path, required=True)
    promote.add_argument("--dataset-id", required=True)
    promote.add_argument("--dataset-version", required=True)
    promote.add_argument("--expected-row-count", type=int, required=True)
    promote.add_argument("--expected-instrument-count", type=int, required=True)
    promote.add_argument("--expected-watermark", type=date.fromisoformat, required=True)
    promote.add_argument("--ss-environment", type=Path)
    promote.add_argument("--report", type=Path, required=True)
    promote.add_argument("--report-gid", type=int)

    rollback = subparsers.add_parser("rollback")
    rollback.add_argument("--central-data-root", type=Path, required=True)
    rollback.add_argument("--snapshot-dir", type=Path, required=True)
    rollback.add_argument("--ss-environment", type=Path)
    rollback.add_argument("--report", type=Path, required=True)
    rollback.add_argument("--report-gid", type=int)
    args = parser.parse_args()

    if args.command == "promote":
        result = promote_isolated_qfq_release(
            source_data_root=args.source_data_root,
            central_data_root=args.central_data_root,
            import_receipt_path=args.import_receipt,
            allowed_source_commits=args.allowed_source_commit,
            snapshot_dir=args.snapshot_dir,
            expected_dataset_id=args.dataset_id,
            expected_dataset_version=args.dataset_version,
            expected_row_count=args.expected_row_count,
            expected_instrument_count=args.expected_instrument_count,
            expected_watermark=args.expected_watermark,
            ss_environment_path=args.ss_environment,
        )
        report = result.as_dict()
    else:
        report = restore_qfq_release(
            central_data_root=args.central_data_root,
            snapshot_dir=args.snapshot_dir,
            ss_environment_path=args.ss_environment,
        )
    _write_report(args.report, report, shared_gid=args.report_gid)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
