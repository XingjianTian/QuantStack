#!/usr/bin/env python3
"""Plan a bounded CN factor delta from existing BaoStock daily Raw evidence."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.cn_qfq_incremental import (
    build_factor_query_plan,
    factor_last_checked,
    factor_universe_from_raw,
    load_daily_native_records,
    load_daily_session_records,
    previous_close_by_instrument,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--session-ordinal", type=int, required=True)
    parser.add_argument("--raw-baseline", type=Path, required=True)
    parser.add_argument("--factor-ledger", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--daily-native", type=Path, action="append")
    source.add_argument("--daily-session", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    native = (
        load_daily_session_records(args.daily_session.resolve())
        if args.daily_session is not None
        else load_daily_native_records([path.resolve() for path in args.daily_native])
    )
    by_id = {
        item["instrument_id"]: item
        for item in factor_universe_from_raw(args.raw_baseline.resolve())
    }
    for instrument_id, record in native.items():
        by_id.setdefault(
            instrument_id,
            {
                "instrument_id": instrument_id,
                "provider_symbol": str(record["code"]),
            },
        )
    universe = list(by_id.values())
    plan = build_factor_query_plan(
        session=args.session,
        session_ordinal=args.session_ordinal,
        universe=universe,
        daily_native_records=native,
        previous_closes=previous_close_by_instrument(
            args.raw_baseline.resolve(), before=args.session
        ),
        last_factor_checks=factor_last_checked(args.factor_ledger.resolve()),
    )
    atomic_replace_bytes(
        args.output.resolve(),
        (json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {key: plan[key] for key in ("session", "request_count", "request_reduction_ratio")},
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
