from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.us_corporate_actions import run_us_corporate_action_campaign


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect one compact US Core action package")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--request-interval-seconds", type=float)
    parser.add_argument("--yfinance-runtime-root", type=Path)
    parser.add_argument("--yfinance-proxy-url")
    parser.add_argument("--fixture", type=Path)
    args = parser.parse_args()
    print(json.dumps(run_us_corporate_action_campaign(args), sort_keys=True))


if __name__ == "__main__":
    main()
