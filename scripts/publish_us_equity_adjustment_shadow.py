from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.us_adjustment import publish_us_adjustment_shadow


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish one US adjustment research shadow")
    parser.add_argument("--shadow-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--environment-id", default="us-adjustment-shadow-publisher")
    args = parser.parse_args()
    _, proof = publish_us_adjustment_shadow(
        shadow_root=args.shadow_root,
        data_root=args.data_root,
        dataset_id=args.dataset_id,
        dataset_version=args.dataset_version,
        environment_id=args.environment_id,
    )
    print(json.dumps(proof, sort_keys=True))


if __name__ == "__main__":
    main()
