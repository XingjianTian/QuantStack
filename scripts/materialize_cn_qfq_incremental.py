#!/usr/bin/env python3
"""Build or append the CN qfq stable-numerator v2 candidate."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.cn_qfq_incremental import (
    append_stable_session,
    build_factor_ledger,
    build_stable_base,
    merge_factor_delta,
    write_incremental_manifest,
)


def _bootstrap(args: argparse.Namespace) -> dict[str, object]:
    ledger = build_factor_ledger(
        factor_path=args.full_factors.resolve(),
        output_path=args.ledger.resolve(),
        watermark=args.watermark,
    )
    stable = build_stable_base(
        raw_bars_path=args.raw.resolve(),
        factor_ledger_path=args.ledger.resolve(),
        output_path=args.stable_base.resolve(),
    )
    return {"status": "completed", "mode": "bootstrap", "ledger": ledger, "stable": stable}


def _append(args: argparse.Namespace) -> dict[str, object]:
    ledger = merge_factor_delta(
        current_ledger_path=args.current_ledger.resolve(),
        factor_delta_path=args.factor_delta.resolve(),
        factor_plan_path=args.factor_plan.resolve(),
        output_path=args.next_ledger.resolve(),
    )
    stable_daily = args.stable_daily.resolve()
    stable = append_stable_session(
        daily_raw_path=args.daily_raw.resolve(),
        factor_ledger_path=args.next_ledger.resolve(),
        output_path=stable_daily,
    )
    existing = [path.resolve() for path in args.prior_stable_daily]
    manifest = write_incremental_manifest(
        output_path=args.manifest.resolve(),
        watermark=args.session,
        factor_ledger_path=args.next_ledger.resolve(),
        stable_base_path=args.stable_base.resolve(),
        stable_daily_paths=[*existing, stable_daily],
    )
    return {
        "status": "completed",
        "mode": "append",
        "ledger": {
            "watermark": ledger["watermark"],
            "last_request_count": ledger["last_request_count"],
        },
        "stable": stable,
        "manifest": manifest,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    bootstrap = subparsers.add_parser("bootstrap")
    bootstrap.add_argument("--raw", type=Path, required=True)
    bootstrap.add_argument("--full-factors", type=Path, required=True)
    bootstrap.add_argument("--watermark", type=date.fromisoformat, required=True)
    bootstrap.add_argument("--ledger", type=Path, required=True)
    bootstrap.add_argument("--stable-base", type=Path, required=True)

    append = subparsers.add_parser("append")
    append.add_argument("--session", type=date.fromisoformat, required=True)
    append.add_argument("--current-ledger", type=Path, required=True)
    append.add_argument("--factor-delta", type=Path, required=True)
    append.add_argument("--factor-plan", type=Path, required=True)
    append.add_argument("--next-ledger", type=Path, required=True)
    append.add_argument("--daily-raw", type=Path, required=True)
    append.add_argument("--stable-base", type=Path, required=True)
    append.add_argument("--prior-stable-daily", type=Path, action="append", default=[])
    append.add_argument("--stable-daily", type=Path, required=True)
    append.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()

    report = _bootstrap(args) if args.mode == "bootstrap" else _append(args)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
