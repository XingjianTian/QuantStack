#!/usr/bin/env python3
"""Freeze the R11 US manifest omissions into an offline R12 recovery queue."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.r12_us_recovery import (
    build_frozen_queue,
    manifest_paths_from_raw_analysis,
    write_frozen_queue,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--raw-analysis", type=Path)
    source.add_argument("--manifest", type=Path, action="append", default=[])
    source.add_argument("--ledger", type=Path)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--historical-ledger", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    manifests = args.manifest
    if args.raw_analysis is not None:
        manifests = manifest_paths_from_raw_analysis(args.raw_analysis)
    queue = build_frozen_queue(
        snapshot_path=args.snapshot,
        manifest_paths=manifests,
        omission_ledger=args.ledger,
        historical_ledger=args.historical_ledger,
    )
    write_frozen_queue(args.output, queue)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "queue_id": queue["queue_id"],
                "counts": queue["counts"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
