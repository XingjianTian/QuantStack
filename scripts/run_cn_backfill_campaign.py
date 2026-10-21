#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import quant_stack
from quant_stack.data.backfill import (
    BackfillCampaignPlan,
    BackfillPilotRunner,
    BackfillUniverse,
    RuntimeProvenanceError,
    build_backfill_campaign_plan,
    build_campaign_progress,
    finalize_campaign,
    requeue_campaign_failures,
    run_campaign_wave,
    run_runtime_provenance_preflight,
)
from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import BaoStockLiveProvider, BaoStockRequestBudget
from quant_stack.data.storage.atomic import atomic_replace_bytes


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a resumable CN historical campaign")
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan")
    plan.add_argument("--universe", required=True, type=Path)
    plan.add_argument("--campaign-id", required=True)
    plan.add_argument("--dataset-version", required=True)
    plan.add_argument("--start", required=True, type=date.fromisoformat)
    plan.add_argument("--end", required=True, type=date.fromisoformat)
    plan.add_argument("--batch-size", type=int, default=50)
    plan.add_argument("--created-at", type=datetime.fromisoformat)
    plan.add_argument("--output", required=True, type=Path)

    clear = commands.add_parser(
        "clear-provider-block",
        help="Clear a local BaoStock cooldown after an operator verifies a safe egress",
    )
    clear.add_argument("--data-root", required=True, type=Path)
    clear.add_argument("--reason", required=True)

    for name in ("progress", "wave", "finalize", "requeue-failures"):
        command = commands.add_parser(name)
        command.add_argument("plan", type=Path)
        command.add_argument("--universe", required=True, type=Path)
        command.add_argument("--data-root", required=True, type=Path)
        command.add_argument("--max-attempts", type=int, default=3)
        command.add_argument("--backoff-seconds", type=float, default=1)
        command.add_argument("--progress-output", required=True, type=Path)
        if name in {"wave", "finalize", "requeue-failures"}:
            command.add_argument(
                "--source-commit",
                required=True,
                help="full Git SHA of the checkout that is allowed to make Provider-side effects",
            )
            command.add_argument(
                "--provenance-output",
                required=True,
                type=Path,
                help="isolated JSON report for the runtime provenance preflight",
            )
            command.add_argument(
                "--repo-root",
                type=Path,
                default=Path.cwd(),
                help="checkout root whose HEAD and tracked status must match --source-commit",
            )
            command.add_argument(
                "--environment-id",
                default="cn-backfill-worker",
                help="sanitized worker environment label stored in the preflight report",
            )
        if name == "wave":
            command.add_argument("--max-shards", required=True, type=int)
            command.add_argument(
                "--shard-id",
                help="optional explicit shard selector; required for a bounded single-shard canary",
            )
            command.add_argument(
                "--max-batches",
                type=int,
                default=144,
                help="daily pacing ceiling; one campaign shard is one serialized provider batch",
            )
            command.add_argument(
                "--max-requests",
                type=int,
                default=49_000,
                help="daily BaoStock request ceiling (hard provider ceiling is 50,000)",
            )
        if name == "requeue-failures":
            command.add_argument(
                "--shard-id",
                help="optional explicit shard selector; use it for a bounded single-shard canary",
            )
            command.add_argument("--error-class", required=True, action="append")
            command.add_argument("--additional-attempts", type=int, default=3)
    return parser


def _write_json(path: Path, payload: object) -> None:
    encoded = (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    atomic_replace_bytes(
        path, encoded, validate=lambda candidate: json.loads(candidate.read_text())
    )


def _load(args: argparse.Namespace) -> tuple[BackfillUniverse, BackfillCampaignPlan]:
    universe = BackfillUniverse.model_validate_json(args.universe.read_text(encoding="utf-8"))
    plan = BackfillCampaignPlan.model_validate_json(args.plan.read_text(encoding="utf-8"))
    return universe, plan


def _preflight(args: argparse.Namespace) -> None:
    """Write a fail-closed provenance report before any live campaign mutation."""

    try:
        report = run_runtime_provenance_preflight(
            declared_source_commit=args.source_commit,
            repo_root=args.repo_root,
            environment_id=args.environment_id,
            entrypoint_path=Path(__file__).resolve(),
            module_origin_paths=(Path(quant_stack.__file__).resolve(),),
        )
    except RuntimeProvenanceError as exc:
        _write_json(args.provenance_output, exc.report.model_dump())
        raise SystemExit(f"runtime provenance preflight failed: {exc}") from exc
    _write_json(args.provenance_output, report.model_dump())


def _runner(
    args: argparse.Namespace,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
    *,
    live: bool,
) -> BackfillPilotRunner:
    if live:
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

        def fetch(ids, start, end):
            return list(provider.fetch_bars("CN", ids, start, end, "1d"))

    else:

        def fetch(_ids, _start, _end):
            raise RuntimeError("progress inspection must not fetch Provider data")

    return BackfillPilotRunner(
        args.data_root,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff_seconds,
    )


def main() -> None:
    args = _parser().parse_args()
    if args.command == "clear-provider-block":
        budget = BaoStockRequestBudget.for_data_root(args.data_root)
        budget.clear_block(reason=args.reason)
        print(json.dumps({"event": "baostock_block_cleared", "reason": args.reason}))
        return
    if args.command == "plan":
        universe = BackfillUniverse.model_validate_json(args.universe.read_text(encoding="utf-8"))
        plan = build_backfill_campaign_plan(
            universe,
            campaign_id=args.campaign_id,
            dataset_version=args.dataset_version,
            start=args.start,
            end=args.end,
            batch_size=args.batch_size,
            created_at=args.created_at,
        )
        _write_json(
            args.output,
            plan.model_dump(mode="json", exclude_computed_fields=True),
        )
        print(plan.model_dump_json(exclude={"shards"}))
        return

    if args.command in {"wave", "finalize", "requeue-failures"}:
        _preflight(args)

    universe, plan = _load(args)
    runner = _runner(args, universe, plan, live=args.command in {"wave", "finalize"})
    if args.command == "progress":
        result = build_campaign_progress(runner, universe, plan)
    elif args.command == "wave":
        result = run_campaign_wave(
            runner,
            universe,
            plan,
            max_shards=args.max_shards,
            max_batches=args.max_batches,
            max_requests=args.max_requests,
            shard_id=args.shard_id,
        )
    elif args.command == "requeue-failures":
        result = requeue_campaign_failures(
            runner,
            universe,
            plan,
            allowed_error_classes=tuple(args.error_class),
            additional_attempts=args.additional_attempts,
            shard_id=args.shard_id,
        )
    else:
        result = finalize_campaign(runner, universe, plan)
    payload = result.model_dump(mode="json")
    _write_json(args.progress_output, payload)
    print(result.model_dump_json(exclude={"shards", "results", "shard_results"}))


if __name__ == "__main__":
    main()
