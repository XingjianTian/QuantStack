from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.backfill.finalization import finalize_and_publish_backfill


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay-only full-market backfill finalize and Catalog registration"
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--parquet-root", type=Path, action="append", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--environment-id", default="nas-catalog-publisher")
    args = parser.parse_args()
    _, report = finalize_and_publish_backfill(
        data_root=args.data_root,
        plan_path=args.plan,
        universe_path=args.universe,
        progress_path=args.progress,
        parquet_roots=args.parquet_root,
        environment_id=args.environment_id,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
