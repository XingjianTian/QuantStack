#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from quant_stack.data.providers.yfinance import LiveYFinanceTransport
from quant_stack.data.us_market_indexes import US_MARKET_INDEXES, install_index_history


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--proxy-url", required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    transport = LiveYFinanceTransport(
        runtime_root=args.runtime_root,
        proxy_url=args.proxy_url,
        minimum_interval_seconds=None,
        include_actions=False,
    )
    rows = transport.fetch_daily_bars_batch(
        list(US_MARKET_INDEXES),
        args.as_of - timedelta(days=90),
        args.as_of,
        include_actions=False,
        threads=4,
    )
    path = install_index_history(rows, args.output_root)
    print(json.dumps({"path": str(path), "rows": len(rows)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
