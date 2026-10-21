"""Build snapshot-driven US equity historical archives with strict serial requests."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

from quant_stack.data.providers.yfinance import YFinanceTransport, require_live_yfinance_interval
from scripts import run_us_equity_core_backfill as core


def _configure(args: argparse.Namespace) -> argparse.Namespace:
    args.minimum_instruments = getattr(args, "minimum_instruments", 1)
    args.maximum_instruments = getattr(args, "maximum_instruments", 10_000)
    args.max_archives_per_shard = getattr(args, "max_archives_per_shard", 100)
    args.scope_label = "US full-market"
    args.transfer_filename_prefix = "us-equity-archives"
    args.continue_terminal_failures = True
    args.include_actions = True
    args.provider_failure_window = getattr(args, "provider_failure_window", 20)
    args.provider_failure_limit = getattr(args, "provider_failure_limit", 5)
    args.skip_transfer_shards = getattr(args, "skip_transfer_shards", False)
    return args


def run(
    args: argparse.Namespace,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, object]:
    configured = _configure(args)
    kwargs: dict[str, object] = {}
    if transport_factory is not None:
        kwargs["transport_factory"] = transport_factory
    if sleeper is not None:
        kwargs["sleeper"] = sleeper
    return core.run(configured, **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--task-state-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--transfer-shard-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--environment-id", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--execution-time", type=datetime.fromisoformat)
    parser.add_argument("--request-interval-seconds", type=float)
    parser.add_argument("--minimum-instruments", type=int, default=1)
    parser.add_argument("--maximum-instruments", type=int, default=10_000)
    parser.add_argument("--max-archives-per-shard", type=int, default=100)
    parser.add_argument("--provider-failure-window", type=int, default=20)
    parser.add_argument("--provider-failure-limit", type=int, default=5)
    parser.add_argument("--yfinance-runtime-root", type=Path)
    parser.add_argument("--yfinance-proxy-url")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--skip-transfer-shards", action="store_true")
    args = parser.parse_args()
    if args.fixture is None and not args.yfinance_proxy_url:
        parser.error("live full-market backfill requires --yfinance-proxy-url")
    if args.fixture is None and args.request_interval_seconds is not None:
        try:
            require_live_yfinance_interval(args.request_interval_seconds)
        except ValueError as exc:
            parser.error(str(exc))
    if not 1 <= args.minimum_instruments <= args.maximum_instruments:
        parser.error("instrument bounds must be positive and ordered")
    if not 1 <= args.max_archives_per_shard <= 100:
        parser.error("--max-archives-per-shard must be between 1 and 100")
    if (args.provider_failure_window == 0) != (args.provider_failure_limit == 0):
        parser.error("provider failure window and limit must both be zero or positive")
    if args.provider_failure_window < 0 or args.provider_failure_limit < 0:
        parser.error("provider failure window and limit must be non-negative")
    if args.provider_failure_limit > args.provider_failure_window:
        parser.error("provider failure limit cannot exceed its window")
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
