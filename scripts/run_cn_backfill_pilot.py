#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from quant_stack.data.backfill import BackfillPilotRunner, BackfillUniverse
from quant_stack.data.backfill.runner import build_annual_batch_shards
from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import BaoStockLiveProvider, BaoStockRequestBudget


def _recent_complete_candidate() -> date:
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    candidate = now.date() if now.hour >= 16 else now.date() - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an isolated, versioned BaoStock historical backfill"
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("config/cn_backfill_medium_universe_v1.json"),
    )
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2015, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=_recent_complete_candidate())
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=1)
    return parser


def main() -> None:
    args = _parser().parse_args()
    universe = BackfillUniverse.model_validate_json(args.universe.read_text(encoding="utf-8"))
    instruments = tuple(
        Instrument(
            instrument_id=item.instrument_id,
            symbol=item.symbol,
            market="CN",
            venue=item.venue,
            provider_symbol=item.provider_symbol,
            provider_exchange_alias=item.provider_symbol.split(".", 1)[0],
            currency="CNY",
            timezone="Asia/Shanghai",
        )
        for item in universe.instruments
    )
    provider = BaoStockLiveProvider(
        instruments,
        request_budget=BaoStockRequestBudget.for_data_root(args.data_root),
    )

    def fetch(ids: list[str] | tuple[str, ...], start: date, end: date):
        rows = list(provider.fetch_bars("CN", ids, start, end, "1d"))
        print(
            json.dumps(
                {
                    "event": "provider_batch_complete",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "instruments": len(ids),
                    "native_rows": len(rows),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return rows

    shards = build_annual_batch_shards(
        universe,
        dataset_version=args.dataset_version,
        start=args.start,
        end=args.end,
        batch_size=args.batch_size,
    )
    report = BackfillPilotRunner(
        args.data_root,
        universe,
        fetch,
        dataset_version=args.dataset_version,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff_seconds,
    ).run(shards)
    print(json.dumps(report.model_dump(mode="json"), ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
