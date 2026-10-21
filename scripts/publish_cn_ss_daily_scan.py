#!/usr/bin/env python3
"""Compatibility CLI for the unified CN/US SS daily scan publisher."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.publish_ss_daily_scan import publish_daily_scan as _publish_daily_scan


def publish_daily_scan(**kwargs: Any) -> dict[str, Any]:
    return _publish_daily_scan(market="CN", **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ss-base-url", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-gid", type=int)
    args = parser.parse_args()
    report = publish_daily_scan(
        ss_base_url=args.ss_base_url,
        expected_version=args.expected_version,
        expected_watermark=args.expected_watermark,
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
