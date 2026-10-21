#!/usr/bin/env python3
"""Compatibility wrapper for the unified CN/US Daily Snapshot synchronizer."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.sync_ss_daily_snapshot import (
    DEFAULT_ANALYTICS_VERSION,
)
from scripts.sync_ss_daily_snapshot import (
    sync_daily_snapshot as _sync_daily_snapshot,
)


def sync_daily_snapshot(**kwargs: Any) -> dict[str, Any]:
    return _sync_daily_snapshot(
        market="CN",
        analytics_version=kwargs.pop("analytics_version", DEFAULT_ANALYTICS_VERSION),
        **kwargs,
    )


def write_report(path: Path, report: dict[str, Any], *, gid: int | None = None) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    if gid is not None:
        os.chown(path, -1, gid)
        os.chmod(path, 0o640)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ss-base-url", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--expected-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--analytics-version", default=DEFAULT_ANALYTICS_VERSION)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-gid", type=int)
    args = parser.parse_args()
    report = sync_daily_snapshot(
        ss_base_url=args.ss_base_url,
        dataset_version=args.dataset_version,
        expected_watermark=args.expected_watermark,
        analytics_version=args.analytics_version,
    )
    write_report(args.report, report, gid=args.report_gid)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
