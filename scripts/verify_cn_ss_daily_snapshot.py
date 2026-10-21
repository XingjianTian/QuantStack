#!/usr/bin/env python3
"""Compatibility CLI for unified analytics-ready Daily Snapshot verification."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.sync_ss_daily_snapshot import DEFAULT_ANALYTICS_VERSION
from scripts.verify_ss_daily_snapshot import verify_daily_snapshot as _verify_daily_snapshot


def verify_daily_snapshot(**kwargs: Any) -> dict[str, Any]:
    return _verify_daily_snapshot(
        market="CN",
        analytics_version=kwargs.pop("analytics_version", DEFAULT_ANALYTICS_VERSION),
        **kwargs,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ss-base-url", required=True)
    parser.add_argument("--scan-id", required=True)
    parser.add_argument("--expected-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--analytics-version", default=DEFAULT_ANALYTICS_VERSION)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-gid", type=int)
    args = parser.parse_args()
    report = verify_daily_snapshot(
        ss_base_url=args.ss_base_url,
        scan_id=args.scan_id,
        expected_watermark=args.expected_watermark,
        analytics_version=args.analytics_version,
    )
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    if args.report_gid is not None:
        os.chown(args.report, -1, args.report_gid)
        os.chmod(args.report, 0o640)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
