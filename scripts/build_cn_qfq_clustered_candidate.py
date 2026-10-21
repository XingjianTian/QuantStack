#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.clustered_parquet import build_clustered_cn_qfq_candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an immutable instrument-clustered CN qfq isolated candidate."
    )
    parser.add_argument("--source-descriptor", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--row-group-size", type=int, default=32768)
    args = parser.parse_args()
    result = build_clustered_cn_qfq_candidate(
        source_descriptor=args.source_descriptor,
        output_root=args.output_root,
        version=args.version,
        row_group_size=args.row_group_size,
    )
    print(
        json.dumps(
            {
                "root": str(result.root),
                "clustered_descriptor": str(result.clustered_descriptor),
                "baseline_descriptor": str(result.baseline_descriptor),
                "report": str(result.report),
                "created": result.created,
                "row_count": result.row_count,
                "instrument_count": result.instrument_count,
                "row_group_count": result.row_group_count,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
