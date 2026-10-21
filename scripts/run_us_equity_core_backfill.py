from __future__ import annotations

import argparse
import json
import tarfile
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.calendars import USExchangeCalendar
from quant_stack.data.models import DatasetLayer
from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    ReplayYFinanceTransport,
    YFinanceProvider,
    YFinanceTransport,
    load_us_instrument_snapshot,
    require_live_yfinance_interval,
    resolve_yfinance_interval,
    run_yfinance_daily_task,
    yfinance_runtime_root,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import FileTaskStore, TaskState, TaskStateFile
from quant_stack.data.worker_bundle import build_worker_envelope, create_worker_archive
from scripts.run_us_equity_isolated_scale import _bundle_evidence, _key

DATASET_ID = "yfinance_us_equity_daily"


class _ActionsTransport:
    def __init__(self, transport: YFinanceTransport) -> None:
        self.transport = transport

    def fetch_daily_bars(self, symbols, start, end):
        return self.transport.fetch_daily_bars_with_actions(symbols, start, end)

    def fetch_daily_bars_with_actions(self, symbols, start, end):
        return self.transport.fetch_daily_bars_with_actions(symbols, start, end)

    def audit_snapshot(self):
        return self.transport.audit_snapshot()


def _terminal_failures(args: argparse.Namespace) -> dict[str, TaskStateFile]:
    failures: dict[str, TaskStateFile] = {}
    for path in args.task_state_root.glob("task-*.json"):
        state = TaskStateFile.model_validate_json(path.read_text(encoding="utf-8"))
        task = state.task
        if (
            task.state in {TaskState.QUARANTINED, TaskState.PERMANENTLY_FAILED}
            and task.dataset_id == DATASET_ID
            and task.dataset_version.startswith(f"{args.dataset_version}.")
            and task.start_date == args.start
            and task.end_date == args.end
            and len(task.instrument_scope) == 1
        ):
            failures[task.instrument_scope[0]] = state
    return failures


def _failure_evidence(
    state: TaskStateFile, staging_root: Path, *, ordinal: int, venue: str
) -> dict[str, object]:
    attempt = state.attempts[-1]
    context = attempt.failure_context
    failed_checks: list[str] = []
    if context and context.quality_report_uri:
        report_path = staging_root / context.quality_report_uri.removeprefix("quant-data://")
        quality = json.loads(report_path.read_text(encoding="utf-8"))
        failed_checks = [str(item) for item in quality.get("failed_checks", [])]
    return {
        "ordinal": ordinal,
        "symbol": state.task.instrument_scope[0],
        "venue": venue,
        "task_id": state.task.task_id,
        "state": state.task.state,
        "failure_stage": context.failure_stage if context else "unknown",
        "error_type": attempt.retry.error_type,
        "error_message": attempt.retry.error_message,
        "failed_checks": failed_checks,
    }


def _is_provider_terminal_failure(state: TaskStateFile) -> bool:
    failure_type = state.attempts[-1].retry.error_type if state.attempts else None
    return bool(
        state.attempts
        and state.attempts[-1].failure_context
        and state.attempts[-1].failure_context.failure_stage == "provider_fetch"
        and failure_type not in {"YFinanceRateLimitError", "YFinanceCircuitOpenError"}
    )


def common_sessions(start: date, end: date) -> tuple[date, ...]:
    xnys = tuple(
        item.trading_date for item in USExchangeCalendar("XNYS").sessions("US", start, end)
    )
    xnas = tuple(
        item.trading_date for item in USExchangeCalendar("XNAS").sessions("US", start, end)
    )
    if not xnys or xnys != xnas:
        raise ValueError("XNYS/XNAS session window is empty or inconsistent")
    return xnys


def create_transfer_shards(
    archives: list[Path],
    shard_root: Path,
    *,
    max_archives: int = 100,
    filename_prefix: str = "us-core-archives",
) -> list[dict[str, object]]:
    if not 1 <= max_archives <= 100:
        raise ValueError("max_archives must be between 1 and 100")
    shard_root.mkdir(parents=True, exist_ok=True)
    shards: list[dict[str, object]] = []
    for offset in range(0, len(archives), max_archives):
        members = archives[offset : offset + max_archives]
        shard_path = shard_root / f"{filename_prefix}-{offset // max_archives + 1:02d}.tar"
        with tarfile.open(shard_path, mode="w") as bundle:
            for archive in members:
                bundle.add(archive, arcname=archive.name, recursive=False)
        shards.append(
            {
                "ordinal": len(shards) + 1,
                "name": shard_path.name,
                "archive_count": len(members),
                "size_bytes": shard_path.stat().st_size,
                "first_archive": members[0].name,
                "last_archive": members[-1].name,
            }
        )
    return shards


def run(
    args: argparse.Namespace,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    scope_label = getattr(args, "scope_label", "US Core 500")
    proxy_url = getattr(args, "yfinance_proxy_url", None)
    runtime_root = yfinance_runtime_root(args.yfinance_runtime_root, proxy_url=proxy_url)
    interval = resolve_yfinance_interval(runtime_root, args.request_interval_seconds)
    if args.fixture is None and transport_factory is None and not proxy_url:
        raise ValueError(f"live {scope_label} backfill requires --yfinance-proxy-url")
    if args.fixture is None and transport_factory is None:
        require_live_yfinance_interval(interval)
    snapshot = load_us_instrument_snapshot(args.snapshot)
    instrument_count = len(snapshot.instruments)
    if not args.minimum_instruments <= instrument_count <= args.maximum_instruments:
        raise ValueError(
            f"{scope_label} snapshot instrument count is outside bounds: {instrument_count}"
        )
    if args.start > args.end:
        raise ValueError("start must not be later than end")
    sessions = common_sessions(args.start, args.end)
    expected_dates = {item.isoformat() for item in sessions}
    key = _key(args.hmac_key_file)
    args.archive_root.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    actual_requests = 0
    execution_time = args.execution_time or datetime.now(UTC)
    continue_terminal_failures = getattr(args, "continue_terminal_failures", False)
    include_actions = getattr(args, "include_actions", False)
    failure_window_size = getattr(args, "provider_failure_window", 0)
    failure_limit = getattr(args, "provider_failure_limit", 0)
    provider_outcomes: deque[bool] = deque(maxlen=failure_window_size or None)
    terminal_failures = {
        symbol: state
        for symbol, state in _terminal_failures(args).items()
        if state.task.state is TaskState.QUARANTINED
        or (continue_terminal_failures and _is_provider_terminal_failure(state))
    }

    for index, instrument in enumerate(snapshot.instruments):
        symbol = instrument.provider_symbol
        if symbol in terminal_failures:
            failures.append(
                _failure_evidence(
                    terminal_failures[symbol],
                    args.staging_root.resolve(),
                    ordinal=index + 1,
                    venue=instrument.canonical_venue,
                )
            )
            continue
        transport = (
            transport_factory(symbol)
            if transport_factory
            else ReplayYFinanceTransport(args.fixture, include_actions=include_actions)
            if args.fixture
            else LiveYFinanceTransport(
                runtime_root=args.yfinance_runtime_root,
                proxy_url=proxy_url,
                minimum_interval_seconds=interval,
                include_actions=include_actions,
            )
        )
        if include_actions and transport_factory is not None:
            transport = _ActionsTransport(transport)
        execution = run_yfinance_daily_task(
            args.staging_root,
            args.task_state_root,
            symbols=[symbol],
            start=args.start,
            end=args.end,
            dataset_version=f"{args.dataset_version}.{symbol.lower()}",
            execution_time=execution_time,
            transport=transport,
            instrument_snapshot=snapshot,
            max_attempts=1,
        )
        if execution.task.state is TaskState.QUARANTINED:
            state = FileTaskStore(args.task_state_root).load(execution.task.task_id)
            if state is None:
                raise RuntimeError(f"{scope_label} quarantine state is missing for {symbol}")
            failures.append(
                _failure_evidence(
                    state,
                    args.staging_root.resolve(),
                    ordinal=index + 1,
                    venue=instrument.canonical_venue,
                )
            )
            actual_requests += 1
            if index + 1 < instrument_count and (args.fixture or transport_factory):
                sleeper(interval)
            continue
        if execution.task.state is not TaskState.SUCCEEDED:
            if continue_terminal_failures and execution.task.state is TaskState.PERMANENTLY_FAILED:
                state = FileTaskStore(args.task_state_root).load(execution.task.task_id)
                if state is None:
                    raise RuntimeError(f"{scope_label} failure state is missing for {symbol}")
                if not _is_provider_terminal_failure(state):
                    attempt = state.attempts[-1]
                    raise RuntimeError(
                        f"{scope_label} backfill stopped for {symbol}: "
                        f"{attempt.retry.error_type} at "
                        f"{attempt.failure_context.failure_stage}"
                    )
                failures.append(
                    _failure_evidence(
                        state,
                        args.staging_root.resolve(),
                        ordinal=index + 1,
                        venue=instrument.canonical_venue,
                    )
                )
                actual_requests += 1
                if failure_window_size:
                    provider_outcomes.append(True)
                    if (
                        len(provider_outcomes) == failure_window_size
                        and sum(provider_outcomes) >= failure_limit
                    ):
                        raise RuntimeError(
                            f"{scope_label} provider failure budget reached: "
                            f"{sum(provider_outcomes)}/{failure_window_size}"
                        )
                if index + 1 < instrument_count and (args.fixture or transport_factory):
                    sleeper(interval)
                continue
            raise RuntimeError(
                f"{scope_label} backfill failed for {symbol}: {execution.task.state}"
            )
        bundle_path, bundle, raw_payload = _bundle_evidence(
            args.staging_root.resolve(), args.task_state_root, execution.task.task_id
        )
        manifest = next(
            item
            for item in bundle.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        raw_records = raw_payload["records"]
        row_dates = [str(item["Date"])[:10] for item in raw_records]
        unique_dates = set(row_dates)
        if (
            not row_dates
            or len(row_dates) != len(unique_dates)
            or manifest.row_count != len(row_dates)
            or not unique_dates.issubset(expected_dates)
            or any("Close" not in item or "Adj Close" not in item for item in raw_records)
            or (
                include_actions
                and any(
                    "Dividends" not in item or "Stock Splits" not in item for item in raw_records
                )
            )
        ):
            raise RuntimeError(f"{scope_label} backfill coverage failed for {symbol}")
        envelope = build_worker_envelope(
            data_root=args.staging_root.resolve(),
            publication_bundle_path=bundle_path,
            source_commit=args.commit_sha,
            environment_id=args.environment_id,
            provider_contract=YFinanceProvider(instrument_snapshot=snapshot).runtime_contract,
            provider_transport_audit=raw_payload["transport_audit"],
            created_at=bundle.created_at,
        )
        archive = create_worker_archive(
            data_root=args.staging_root.resolve(),
            envelope=envelope,
            output_path=args.archive_root / f"{index + 1:04d}-{symbol}.tar",
            hmac_key=key,
        )
        if not execution.deduplicated:
            actual_requests += 1
            if failure_window_size:
                provider_outcomes.append(False)
        entries.append(
            {
                "ordinal": index + 1,
                "symbol": symbol,
                "venue": instrument.canonical_venue,
                "task_id": execution.task.task_id,
                "bundle_id": envelope.bundle_id,
                "archive_name": archive.path.name,
                "archive_size_bytes": archive.size_bytes,
                "deduplicated": execution.deduplicated,
                "records_received": manifest.row_count,
                "first_observed_session": min(unique_dates),
                "last_observed_session": max(unique_dates),
                "expected_window_sessions": len(sessions),
                "missing_window_sessions": len(sessions) - len(unique_dates),
                "coverage_basis": "provider_observed_history_not_listing_lifecycle",
                "completed_requests": raw_payload["transport_audit"]["completed_requests"],
                "parallel_requests": raw_payload["transport_audit"]["parallel_requests"],
            }
        )
        if (
            not execution.deduplicated
            and index + 1 < instrument_count
            and (args.fixture or transport_factory)
        ):
            sleeper(interval)

    archives = sorted(args.archive_root.glob("*.tar"))
    if len(archives) != len(entries):
        raise RuntimeError(f"archive count does not match successful {scope_label} instruments")
    skip_transfer_shards = getattr(args, "skip_transfer_shards", False)
    shards = (
        []
        if skip_transfer_shards
        else create_transfer_shards(
            archives,
            args.transfer_shard_root,
            max_archives=args.max_archives_per_shard,
            filename_prefix=getattr(args, "transfer_filename_prefix", "us-core-archives"),
        )
    )
    report = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "replay" if args.fixture else "live",
        "dataset_id": DATASET_ID,
        "dataset_version": args.dataset_version,
        "instrument_snapshot_id": snapshot.snapshot_id,
        "membership_basis": snapshot.membership_basis,
        "survivorship_bias": True,
        "instrument_count": instrument_count,
        "successful_instruments": len(entries),
        "quarantined_instruments": sum(item["state"] == TaskState.QUARANTINED for item in failures),
        "failed_instruments": sum(
            item["state"] == TaskState.PERMANENTLY_FAILED for item in failures
        ),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "expected_session_count": len(sessions),
        "request_interval_seconds": interval,
        "network_route": "explicit_proxy" if proxy_url else "fixture",
        "parallel_requests": False,
        "provider_retries": 0,
        "provider_failure_window": failure_window_size,
        "provider_failure_limit": failure_limit,
        "actual_requests": actual_requests,
        "deduplicated_tasks": sum(bool(item["deduplicated"]) for item in entries),
        "records_received": sum(int(item["records_received"]) for item in entries),
        "archives": len(archives),
        "archive_transport": "nas_local_direct" if skip_transfer_shards else "transfer_shards",
        "transfer_shards": shards,
        "license_scope": YFinanceProvider.license_scope,
        "price_semantics": "unadjusted_ohlcv; adjusted_close_raw_only",
        "corporate_actions_in_raw": include_actions,
        "entries": entries,
        "terminal_failures": failures,
        "quarantines": [item for item in failures if item["state"] == TaskState.QUARANTINED],
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the US Core 500 ten-year archives")
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
    parser.add_argument("--minimum-instruments", type=int, default=500)
    parser.add_argument("--maximum-instruments", type=int, default=505)
    parser.add_argument("--max-archives-per-shard", type=int, default=100)
    parser.add_argument("--yfinance-runtime-root", type=Path)
    parser.add_argument("--yfinance-proxy-url")
    parser.add_argument("--fixture", type=Path)
    args = parser.parse_args()
    if args.request_interval_seconds is not None and args.request_interval_seconds < 0:
        parser.error("--request-interval-seconds must be non-negative")
    if args.fixture is None and not args.yfinance_proxy_url:
        parser.error("live core backfill requires --yfinance-proxy-url")
    if args.fixture is None and args.request_interval_seconds is not None:
        try:
            require_live_yfinance_interval(args.request_interval_seconds)
        except ValueError as exc:
            parser.error(str(exc))
    if not 1 <= args.max_archives_per_shard <= 100:
        parser.error("--max-archives-per-shard must be between 1 and 100")
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
