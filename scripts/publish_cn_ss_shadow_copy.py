from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.shadow_publication import publish_unadjusted_ss_shadow_copy


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish an existing CN unadjusted Parquet as immutable SS shadow-only data"
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--source-data-root",
        type=Path,
        required=True,
        help="Explicit read-only source root, separate and non-nested from --data-root",
    )
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--target-version", required=True)
    parser.add_argument("--environment-id", default="local-shadow-publisher")
    args = parser.parse_args()
    receipt, report = publish_unadjusted_ss_shadow_copy(
        data_root=args.data_root,
        source_data_root=args.source_data_root,
        source_manifest_path=args.source_manifest,
        target_version=args.target_version,
        environment_id=args.environment_id,
    )
    print(
        json.dumps(
            {"receipt_id": receipt.receipt_id, **report},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
