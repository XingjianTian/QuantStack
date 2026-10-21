"""Replay one accepted US full-market session without Provider access."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from scripts import run_us_equity_core_daily as daily
from scripts.run_us_equity_core_periodic_replay import replay as core_replay


def replay(args: argparse.Namespace) -> dict[str, object]:
    args.scope_label = "US full-market"
    args.transfer_schema = daily.FULL_MARKET_TRANSFER_SCHEMA
    args.dataset_version_prefix = "full-market-daily-v1"
    args.temporary_prefix = "us-equity-periodic-replay-"
    args.state_dataset_version_prefix = None
    return core_replay(args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--initial-watermark", type=date.fromisoformat, required=True)
    parser.add_argument("--transfer", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--local-import-workers", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(replay(args), sort_keys=True))


if __name__ == "__main__":
    main()
