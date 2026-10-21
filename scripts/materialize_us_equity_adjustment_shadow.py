from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.us_corporate_actions import (
    materialize_us_adjustment_from_action_evidence,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize US adjustment research shadows")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--dataset-version-prefix", required=True)
    parser.add_argument("--action-package", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            materialize_us_adjustment_from_action_evidence(
                data_root=args.data_root,
                dataset_version_prefix=args.dataset_version_prefix,
                action_package=args.action_package,
                output_root=args.output_root,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
