from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.clustered_parquet import (
    build_range_partitioned_us_total_return_candidate,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an immutable range-partitioned US total-return candidate."
    )
    parser.add_argument("--source-descriptor", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--instruments-per-partition", type=int, default=32)
    parser.add_argument("--row-group-size", type=int, default=8192)
    args = parser.parse_args()
    result = build_range_partitioned_us_total_return_candidate(
        source_descriptor=args.source_descriptor,
        output_root=args.output_root,
        version=args.version,
        instruments_per_partition=args.instruments_per_partition,
        row_group_size=args.row_group_size,
    )
    print(
        json.dumps(
            {
                "root": str(result.root),
                "descriptor": str(result.descriptor),
                "report": str(result.report),
                "created": result.created,
                "row_count": result.row_count,
                "instrument_count": result.instrument_count,
                "partition_count": result.partition_count,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
