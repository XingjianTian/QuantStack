"""Import snapshot-driven US equity archive shards into isolated storage."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.import_us_equity_core_shards import run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--shard-root", type=Path)
    source.add_argument("--archive-root", type=Path, action="append")
    parser.add_argument("--scratch-parent", type=Path)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--local-import-workers", type=int, default=1)
    parser.add_argument(
        "--verification-mode",
        choices=("readback_only", "full_replay"),
        default="full_replay",
    )
    args = parser.parse_args()
    if args.shard_root is not None and args.scratch_parent is None:
        parser.error("--scratch-parent is required with --shard-root")
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
