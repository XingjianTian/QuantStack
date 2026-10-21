"""Operate the single-node NAS US full-market daily chain."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.run_us_equity_core_daily_nas import (
    FULL_MARKET_CONFIG_SCHEMA,
    plan_daily,
    run_daily,
    run_weekly,
)
from scripts.run_us_equity_core_daily_nas import load_config as load_shared_config


def load_config(path: Path) -> dict[str, object]:
    payload = load_shared_config(path)
    if payload.get("schema_version") != FULL_MARKET_CONFIG_SCHEMA:
        raise ValueError("full-market runner requires the US equity NAS config schema")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "daily", "weekly"))
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    result = (
        plan_daily(config)
        if args.command == "plan"
        else run_daily(config)
        if args.command == "daily"
        else run_weekly(config)
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
