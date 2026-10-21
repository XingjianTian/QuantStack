"""Operate one snapshot-driven US full-market daily session."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

from quant_stack.data.providers.yfinance import YFinanceTransport
from scripts import run_us_equity_core_daily as core

TRANSFER_SCHEMA = core.FULL_MARKET_TRANSFER_SCHEMA


def _configure(args: argparse.Namespace) -> argparse.Namespace:
    args.scope_label = "US full-market"
    args.dataset_version_prefix = "full-market-daily-v1"
    args.transfer_schema = TRANSFER_SCHEMA
    args.temporary_prefix = "us-equity-daily-"
    args.state_dataset_version_prefix = None
    args.minimum_session_coverage = 0.99
    args.maximum_session_omissions = 100
    return args


def collect(
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
    return core.collect(configured, **kwargs)


def accept(args: argparse.Namespace) -> dict[str, object]:
    return core.accept(_configure(args))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--snapshot", type=Path, required=True)
    collect_parser.add_argument("--session", type=date.fromisoformat, required=True)
    collect_parser.add_argument("--staging-root", type=Path, required=True)
    collect_parser.add_argument("--task-state-root", type=Path, required=True)
    collect_parser.add_argument("--archive-root", type=Path, required=True)
    collect_parser.add_argument("--transfer", type=Path, required=True)
    collect_parser.add_argument("--report", type=Path, required=True)
    collect_parser.add_argument("--hmac-key-file", type=Path, required=True)
    collect_parser.add_argument("--commit-sha", required=True)
    collect_parser.add_argument("--environment-id", required=True)
    collect_parser.add_argument("--request-interval-seconds", type=float)
    collect_parser.add_argument("--yfinance-runtime-root", type=Path)
    collect_parser.add_argument("--yfinance-proxy-url")
    collect_parser.add_argument("--fixture", type=Path)
    collect_parser.add_argument("--now", type=datetime.fromisoformat)
    collect_parser.add_argument("--provider-failure-window", type=int, default=20)
    collect_parser.add_argument("--provider-failure-limit", type=int, default=5)
    collect_parser.add_argument("--action-evidence-root", type=Path)
    collect_parser.add_argument(
        "--action-session", dest="action_sessions", action="append", type=date.fromisoformat
    )

    accept_parser = subparsers.add_parser("accept")
    accept_parser.add_argument("--snapshot", type=Path, required=True)
    accept_parser.add_argument("--state", type=Path, required=True)
    accept_parser.add_argument("--initial-watermark", type=date.fromisoformat, required=True)
    accept_parser.add_argument("--transfer", type=Path, required=True)
    accept_parser.add_argument("--scratch-parent", type=Path, required=True)
    accept_parser.add_argument("--isolated-root", type=Path, required=True)
    accept_parser.add_argument("--hmac-key-file", type=Path, required=True)
    accept_parser.add_argument("--report", type=Path, required=True)
    accept_parser.add_argument(
        "--verification-mode",
        choices=("readback_only", "full_replay"),
        default="full_replay",
    )
    accept_parser.add_argument("--local-import-workers", type=int, default=1)
    args = parser.parse_args()
    result = collect(args) if args.command == "collect" else accept(args)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
