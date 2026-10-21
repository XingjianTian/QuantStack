"""Build US corporate-action evidence from the authenticated historical Raw layer."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.us_corporate_actions import (
    build_us_corporate_action_evidence_from_raw,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--dataset-version-prefix", required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build_us_corporate_action_evidence_from_raw(
                data_root=args.data_root,
                dataset_version_prefix=args.dataset_version_prefix,
                snapshot=load_us_instrument_snapshot(args.snapshot),
                start=args.start,
                end=args.end,
                package_path=args.package,
                report_path=args.report,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
