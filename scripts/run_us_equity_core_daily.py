from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import tarfile
import tempfile
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from quant_stack.data.models import DatasetLayer
from quant_stack.data.providers.yfinance import (
    BufferedYFinanceTransport,
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
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import FileTaskStore, TaskState, TaskStateFile
from quant_stack.data.worker_bundle import (
    build_worker_envelope,
    create_worker_archive,
    import_worker_archive,
)
from scripts.run_us_equity_isolated_daily import (
    DATASET_ID,
    _load_watermark,
    _write_watermark,
    next_completed_session,
)
from scripts.run_us_equity_isolated_scale import _bundle_evidence, _key

TRANSFER_SCHEMA = "quantstack.us-core-daily-transfer.v1"
FULL_MARKET_TRANSFER_SCHEMA = "quantstack.us-equity-daily-transfer.v1"


def _action_evidence_path(root: Path, session: date, symbol: str) -> Path:
    return root / session.isoformat() / f"{symbol.upper()}.json.gz"


def _write_action_evidence(
    root: Path,
    *,
    symbol: str,
    instrument_id: str,
    requested_start: date,
    requested_end: date,
    records: list[dict[str, object]],
) -> None:
    for session in sorted(
        {
            date.fromisoformat(str(item["Date"])[:10])
            for item in records
            if "Dividends" in item and "Stock Splits" in item
        }
    ):
        row = next(item for item in records if str(item["Date"])[:10] == session.isoformat())
        payload = {
            "schema_version": "quantstack.us-daily-action-evidence.v1",
            "symbol": symbol.upper(),
            "instrument_id": instrument_id,
            "session": session.isoformat(),
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "Dividends": float(row["Dividends"]),
            "Stock Splits": float(row["Stock Splits"]),
        }
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        atomic_replace_bytes(
            _action_evidence_path(root, session, symbol),
            gzip.compress(body.encode(), mtime=0),
        )


class _ActionEvidenceCaptureTransport:
    """Capture action evidence while exposing only the price task's requested rows."""

    def __init__(
        self,
        base: YFinanceTransport,
        *,
        symbol: str,
        instrument_id: str,
        evidence_root: Path,
        evidence_start: date,
        evidence_end: date,
    ) -> None:
        self.base = base
        self.symbol = symbol
        self.instrument_id = instrument_id
        self.evidence_root = evidence_root
        self.evidence_start = evidence_start
        self.evidence_end = evidence_end
        self.requested = False

    def _fetch(self, start: date, end: date) -> list[dict[str, object]]:
        rows = [
            dict(item)
            for item in self.base.fetch_daily_bars_with_actions(
                [self.symbol], self.evidence_start, self.evidence_end
            )
        ]
        self.requested = True
        if any("Dividends" not in item or "Stock Splits" not in item for item in rows):
            raise RuntimeError(f"daily action evidence is incomplete for {self.symbol}")
        _write_action_evidence(
            self.evidence_root,
            symbol=self.symbol,
            instrument_id=self.instrument_id,
            requested_start=self.evidence_start,
            requested_end=self.evidence_end,
            records=rows,
        )
        return [item for item in rows if start <= date.fromisoformat(str(item["Date"])[:10]) <= end]

    def fetch_daily_bars(
        self, symbols: list[str], start: date, end: date
    ) -> list[dict[str, object]]:
        return self._fetch(start, end)

    def fetch_daily_bars_with_actions(
        self, symbols: list[str], start: date, end: date
    ) -> list[dict[str, object]]:
        return self._fetch(start, end)

    def capture(self) -> None:
        self._fetch(self.evidence_start, self.evidence_end)

    def audit_snapshot(self) -> dict[str, object]:
        return dict(self.base.audit_snapshot())


def _write_json(path: Path, payload: dict[str, object]) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def _require_completed_session(session: date, now: datetime) -> None:
    observed = next_completed_session(session - timedelta(days=1), now)
    if observed != session:
        raise ValueError(f"session is not the oldest completed US session: {session}")


def _quarantine_entry(
    task_store: FileTaskStore, task_id: str, symbol: str, instrument_id: str
) -> dict[str, object]:
    state = task_store.load(task_id)
    if state is None or not state.attempts:
        raise RuntimeError(f"daily quarantine state is missing for {symbol}")
    attempt = state.attempts[-1]
    context = attempt.failure_context
    return {
        "symbol": symbol,
        "instrument_id": instrument_id,
        "task_id": task_id,
        "state": "quarantined",
        "failure_stage": context.failure_stage if context else "unknown",
        "error_type": attempt.retry.error_type,
        "error_message": attempt.retry.error_message,
    }


def _no_session_row_entry(
    task_store: FileTaskStore, task_id: str, symbol: str, instrument_id: str
) -> dict[str, object]:
    state = task_store.load(task_id)
    if state is None or not state.attempts:
        raise RuntimeError(f"daily terminal state is missing for {symbol}")
    attempt = state.attempts[-1]
    if attempt.retry.error_type != "YFinanceEmptyResponseError":
        raise RuntimeError(f"daily provider failure is not an empty response for {symbol}")
    return {
        "symbol": symbol,
        "instrument_id": instrument_id,
        "task_id": task_id,
        "state": "no_session_row",
        "failure_stage": "provider_fetch",
        "error_type": attempt.retry.error_type,
        "error_message": attempt.retry.error_message,
    }


def _stale_session_row_entry(
    task_id: str,
    symbol: str,
    instrument_id: str,
    records: list[dict[str, object]],
) -> dict[str, object]:
    observed_dates = sorted({str(item.get("Date", ""))[:10] for item in records})
    return {
        "symbol": symbol,
        "instrument_id": instrument_id,
        "task_id": task_id,
        "state": "no_session_row",
        "failure_stage": "provider_coverage",
        "error_type": "YFinanceMissingRequestedSessionError",
        "error_message": "provider returned rows, but none matched the requested session",
        "observed_dates": observed_dates,
    }


def _existing_daily_tasks(
    root: Path, session: date, dataset_version_prefix: str = "core-daily-v2"
) -> dict[str, TaskStateFile]:
    prefix = f"{session.isoformat()}.{dataset_version_prefix}."
    result: dict[str, TaskStateFile] = {}
    for path in root.glob("task-*.json"):
        state = TaskStateFile.model_validate_json(path.read_text(encoding="utf-8"))
        task = state.task
        if (
            task.dataset_id == DATASET_ID
            and task.dataset_version.startswith(prefix)
            and task.start_date == session
            and task.end_date == session
            and task.state
            in {TaskState.SUCCEEDED, TaskState.QUARANTINED, TaskState.PERMANENTLY_FAILED}
            and len(task.instrument_scope) == 1
        ):
            symbol = task.instrument_scope[0]
            current = result.get(symbol)
            if current is None or state.updated_at > current.updated_at:
                result[symbol] = state
    return result


def _session_unavailable_task(state: TaskStateFile) -> bool:
    if not state.attempts:
        return False
    retry = state.attempts[-1].retry
    return bool(
        (
            state.task.state is TaskState.QUARANTINED
            and retry.error_type == "DataQualityError"
            and retry.error_message
            == "Dataset quality validation failed: dataset_non_empty"
        )
        or (
            state.task.state is TaskState.PERMANENTLY_FAILED
            and retry.error_type == "YFinanceEmptyResponseError"
        )
    )


def _stale_session_symbols(
    report_path: Path, *, session: date, recovery_generation: int
) -> set[str]:
    if recovery_generation < 1 or not report_path.is_file():
        return set()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if payload.get("session_date") != session.isoformat():
        return set()
    return {
        str(item["symbol"]).upper()
        for item in payload.get("quarantines", [])
        if item.get("error_type") == "YFinanceMissingRequestedSessionError"
    }


def _create_aggregate(output: Path, manifest: dict[str, object], archives: list[Path]) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(f".{output.name}.tmp")
    manifest_bytes = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode()
    with tarfile.open(temp, mode="w") as bundle:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(manifest_bytes)
        info.mtime = 0
        bundle.addfile(info, io.BytesIO(manifest_bytes))
        for archive in archives:
            bundle.add(archive, arcname=archive.name, recursive=False)
    os.replace(temp, output)
    return output.stat().st_size


def collect(
    args: argparse.Namespace,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    collection_started_at = time.perf_counter()
    scope_label = getattr(args, "scope_label", "US Core 500")
    dataset_version_prefix = getattr(args, "dataset_version_prefix", "core-daily-v2")
    transfer_schema = getattr(args, "transfer_schema", TRANSFER_SCHEMA)
    now = args.now or datetime.now(UTC)
    _require_completed_session(args.session, now)
    proxy_url = getattr(args, "yfinance_proxy_url", None)
    runtime_root = yfinance_runtime_root(args.yfinance_runtime_root, proxy_url=proxy_url)
    interval = resolve_yfinance_interval(runtime_root, args.request_interval_seconds)
    if args.fixture is None and transport_factory is None and not proxy_url:
        raise ValueError(f"live {scope_label} daily collection requires --yfinance-proxy-url")
    if args.fixture is None and transport_factory is None:
        require_live_yfinance_interval(interval)

    snapshot = load_us_instrument_snapshot(args.snapshot)
    key = _key(args.hmac_key_file)
    session_archive_root = args.archive_root / args.session.isoformat() / dataset_version_prefix
    session_archive_root.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    quarantines: list[dict[str, object]] = []
    archives: list[Path] = []
    actual_requests = 0
    task_store = FileTaskStore(args.task_state_root)
    existing_tasks = _existing_daily_tasks(
        args.task_state_root, args.session, dataset_version_prefix
    )
    failure_window_size = getattr(args, "provider_failure_window", 20)
    failure_limit = getattr(args, "provider_failure_limit", 5)
    recent_provider_failures: deque[bool] = deque(maxlen=failure_window_size)
    recovery_generation = int(getattr(args, "task_recovery_generation", 0))
    stale_recovery_symbols = _stale_session_symbols(
        args.report,
        session=args.session,
        recovery_generation=recovery_generation,
    )
    action_evidence_root = getattr(args, "action_evidence_root", None)
    action_sessions = tuple(getattr(args, "action_sessions", ()) or ())
    if action_sessions and action_evidence_root is None:
        raise ValueError("daily action sessions require an action evidence root")
    action_evidence_root = Path(action_evidence_root) if action_evidence_root else None
    price_requests = 0
    action_evidence_requests = 0
    reused_price_tasks = 0
    provider_threads = int(getattr(args, "provider_threads", 1))
    provider_batch_size = int(getattr(args, "provider_batch_size", 250))
    if provider_threads < 1 or provider_batch_size < 1:
        raise ValueError("provider_threads and provider_batch_size must be positive")
    batch_mode = args.fixture is None and transport_factory is None and provider_threads > 1
    batched_records: dict[str, list[dict[str, object]]] = {}
    provider_batch_count = 0
    provider_fetch_elapsed_seconds = 0.0
    provider_missing_symbols: list[str] = []
    provider_cached_symbols = 0
    action_evidence_failures: list[dict[str, str]] = []
    if batch_mode:
        symbols_to_fetch: list[str] = []
        symbols_needing_price: set[str] = set()
        symbols_needing_actions: set[str] = set()
        for instrument in snapshot.instruments:
            symbol = instrument.provider_symbol
            prior_state = existing_tasks.get(symbol)
            needs_price = prior_state is None
            if prior_state is not None:
                recovery_suffix = f".recovery-{recovery_generation}"
                needs_price = bool(
                    symbol in stale_recovery_symbols
                    or (
                        recovery_generation > 0
                        and _session_unavailable_task(prior_state)
                        and not prior_state.task.dataset_version.endswith(recovery_suffix)
                    )
                )
            needs_actions = bool(
                prior_state is not None
                and prior_state.task.state is TaskState.SUCCEEDED
                and symbol not in stale_recovery_symbols
                and action_evidence_root
                and any(
                    not _action_evidence_path(action_evidence_root, session, symbol).is_file()
                    for session in action_sessions
                )
            )
            if needs_price or needs_actions:
                symbols_to_fetch.append(symbol)
            if needs_price:
                symbols_needing_price.add(symbol)
            if needs_actions:
                symbols_needing_actions.add(symbol)
        batch_cache_root = (
            args.task_state_root.parent
            / "provider-batches"
            / args.session.isoformat()
            / dataset_version_prefix
        )
        if recovery_generation > 0:
            batch_cache_root /= f"recovery-{recovery_generation}"
        cached_paths = sorted(batch_cache_root.glob("batch-*.json.gz"))
        attempted_symbols: set[str] = set()
        for cache_path in cached_paths:
            payload = json.loads(gzip.decompress(cache_path.read_bytes()))
            attempted_symbols.update(str(item).upper() for item in payload["requested_symbols"])
            for row in payload.get("records", []):
                batched_records.setdefault(str(row["ticker"]).upper(), []).append(dict(row))
        provider_cached_symbols = len(set(symbols_to_fetch).intersection(attempted_symbols))
        symbols_to_fetch = [
            symbol for symbol in symbols_to_fetch if symbol not in attempted_symbols
        ]
        actual_requests = len(symbols_to_fetch)
        price_requests = len(set(symbols_to_fetch).intersection(symbols_needing_price))
        action_evidence_requests = len(
            set(symbols_to_fetch).intersection(symbols_needing_actions)
        )
        live_transport = LiveYFinanceTransport(
            runtime_root=args.yfinance_runtime_root,
            proxy_url=proxy_url,
            minimum_interval_seconds=interval,
            include_actions=bool(action_sessions),
        )
        provider_started_at = time.perf_counter()
        next_batch_number = len(cached_paths) + 1
        for offset in range(0, len(symbols_to_fetch), provider_batch_size):
            batch = symbols_to_fetch[offset : offset + provider_batch_size]
            rows = live_transport.fetch_daily_bars_batch(
                batch,
                args.session,
                args.session,
                include_actions=bool(action_sessions),
                threads=provider_threads,
            )
            provider_batch_count += 1
            for action_session in action_sessions:
                if action_session == args.session:
                    continue
                action_batch = [
                    symbol
                    for symbol in batch
                    if symbol in symbols_needing_actions
                    and action_evidence_root is not None
                    and not _action_evidence_path(
                        action_evidence_root, action_session, symbol
                    ).is_file()
                ]
                if not action_batch:
                    continue
                rows.extend(
                    live_transport.fetch_daily_bars_batch(
                        action_batch,
                        action_session,
                        action_session,
                        include_actions=True,
                        threads=provider_threads,
                    )
                )
                provider_batch_count += 1
            attempted_symbols.update(batch)
            for row in rows:
                batched_records.setdefault(str(row["ticker"]).upper(), []).append(dict(row))
            cache_payload = {
                "schema_version": "quantstack.yfinance-daily-batch-cache.v1",
                "session_date": args.session.isoformat(),
                "requested_symbols": batch,
                "records": rows,
            }
            batch_cache_root.mkdir(parents=True, exist_ok=True)
            atomic_replace_bytes(
                batch_cache_root / f"batch-{next_batch_number:04d}.json.gz",
                gzip.compress(
                    json.dumps(cache_payload, sort_keys=True, separators=(",", ":")).encode(),
                    mtime=0,
                ),
            )
            next_batch_number += 1
        provider_fetch_elapsed_seconds = round(time.perf_counter() - provider_started_at, 3)
        provider_missing_symbols = [
            symbol
            for symbol in attempted_symbols
            if not any(
                str(row.get("Date", ""))[:10] == args.session.isoformat()
                for row in batched_records.get(symbol, [])
            )
        ]
        if (
            len(provider_missing_symbols) >= failure_limit
            and len(provider_missing_symbols) / len(attempted_symbols) > 0.05
        ):
            raise RuntimeError(
                "daily batch provider failure budget reached: "
                f"{len(provider_missing_symbols)}/{len(attempted_symbols)}"
            )

    for index, instrument in enumerate(snapshot.instruments):
        symbol = instrument.provider_symbol
        recover_session_unavailable = symbol in stale_recovery_symbols
        if symbol in existing_tasks and existing_tasks[symbol].task.state in {
            TaskState.QUARANTINED,
            TaskState.PERMANENTLY_FAILED,
        }:
            prior_state = existing_tasks[symbol]
            terminal = prior_state.task
            recovery_suffix = f".recovery-{recovery_generation}"
            recover_session_unavailable = bool(
                recover_session_unavailable
                or (
                    recovery_generation > 0
                    and _session_unavailable_task(prior_state)
                    and not terminal.dataset_version.endswith(recovery_suffix)
                )
            )
            if not recover_session_unavailable:
                entry_factory = (
                    _quarantine_entry
                    if terminal.state is TaskState.QUARANTINED
                    else _no_session_row_entry
                )
                quarantines.append(
                    entry_factory(
                        task_store,
                        terminal.task_id,
                        symbol,
                        instrument.instrument_id,
                    )
                )
                continue
        base_transport = (
            BufferedYFinanceTransport(batched_records.get(symbol, []))
            if batch_mode
            else transport_factory(symbol)
            if transport_factory
            else ReplayYFinanceTransport(args.fixture)
            if args.fixture
            else LiveYFinanceTransport(
                runtime_root=args.yfinance_runtime_root,
                proxy_url=proxy_url,
                minimum_interval_seconds=interval,
                include_actions=False,
            )
        )
        transport: YFinanceTransport = base_transport
        capture: _ActionEvidenceCaptureTransport | None = None
        missing_action_sessions = (
            [
                session
                for session in action_sessions
                if not _action_evidence_path(action_evidence_root, session, symbol).is_file()
            ]
            if action_evidence_root
            else []
        )
        if missing_action_sessions and action_evidence_root is not None:
            capture = _ActionEvidenceCaptureTransport(
                base_transport,
                symbol=symbol,
                instrument_id=instrument.instrument_id,
                evidence_root=action_evidence_root,
                evidence_start=min(missing_action_sessions),
                evidence_end=max(missing_action_sessions),
            )
            transport = capture
        execution = run_yfinance_daily_task(
            args.staging_root,
            args.task_state_root,
            symbols=[symbol],
            start=args.session,
            end=args.session,
            dataset_version=(
                f"{args.session.isoformat()}.{dataset_version_prefix}.{symbol.lower()}"
                + (f".recovery-{recovery_generation}" if recover_session_unavailable else "")
            ),
            execution_time=now,
            transport=transport,
            instrument_snapshot=snapshot,
            max_attempts=1,
            include_actions=False,
            durable_task_state=not batch_mode,
        )
        requested = not execution.deduplicated
        if requested:
            if not batch_mode:
                actual_requests += 1
                price_requests += 1
                if capture and capture.requested:
                    action_evidence_requests += 1
        if execution.task.state is TaskState.QUARANTINED:
            quarantines.append(
                _quarantine_entry(
                    task_store, execution.task.task_id, symbol, instrument.instrument_id
                )
            )
            if requested and not batch_mode:
                recent_provider_failures.append(True)
                if (
                    len(recent_provider_failures) == failure_window_size
                    and sum(recent_provider_failures) >= failure_limit
                ):
                    raise RuntimeError(
                        "daily provider failure budget reached: "
                        f"{sum(recent_provider_failures)}/{failure_window_size}"
                    )
        elif execution.task.state is TaskState.PERMANENTLY_FAILED:
            terminal = _no_session_row_entry(
                task_store, execution.task.task_id, symbol, instrument.instrument_id
            )
            quarantines.append(terminal)
            if requested and not batch_mode:
                recent_provider_failures.append(True)
                if (
                    len(recent_provider_failures) == failure_window_size
                    and sum(recent_provider_failures) >= failure_limit
                ):
                    raise RuntimeError(
                        "daily provider failure budget reached: "
                        f"{sum(recent_provider_failures)}/{failure_window_size}"
                    )
        elif execution.task.state is TaskState.SUCCEEDED:
            if execution.deduplicated:
                reused_price_tasks += 1
            bundle_path, bundle, raw_payload = _bundle_evidence(
                args.staging_root.resolve(), args.task_state_root, execution.task.task_id
            )
            normalized = next(
                item
                for item in bundle.proposed_manifests
                if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
            )
            records = raw_payload["records"]
            session_records = [
                item
                for item in records
                if str(item.get("Date", ""))[:10] == args.session.isoformat()
            ]
            if records and not session_records:
                quarantines.append(
                    _stale_session_row_entry(
                        execution.task.task_id,
                        symbol,
                        instrument.instrument_id,
                        records,
                    )
                )
                if requested and not batch_mode:
                    recent_provider_failures.append(True)
                    if (
                        len(recent_provider_failures) == failure_window_size
                        and sum(recent_provider_failures) >= failure_limit
                    ):
                        raise RuntimeError(
                            "daily provider failure budget reached: "
                            f"{sum(recent_provider_failures)}/{failure_window_size}"
                        )
                continue
            if (
                normalized.row_count != 1
                or len(records) != 1
                or len(session_records) != 1
                or "Close" not in records[0]
                or "Adj Close" not in records[0]
            ):
                raise RuntimeError(f"{scope_label} daily coverage failed for {symbol}")
            if execution.deduplicated and capture is not None:
                try:
                    capture.capture()
                except ConnectionError as exc:
                    action_evidence_failures.append(
                        {
                            "symbol": symbol,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                        }
                    )
                if not batch_mode:
                    actual_requests += 1
                    action_evidence_requests += 1
            archive_path = session_archive_root / f"{index + 1:04d}-{symbol}.tar"
            if execution.deduplicated and archive_path.is_file():
                archives.append(archive_path)
            else:
                envelope = build_worker_envelope(
                    data_root=args.staging_root.resolve(),
                    publication_bundle_path=bundle_path,
                    source_commit=args.commit_sha,
                    environment_id=args.environment_id,
                    provider_contract=YFinanceProvider(
                        instrument_snapshot=snapshot, include_actions=False
                    ).runtime_contract,
                    provider_transport_audit=raw_payload["transport_audit"],
                    created_at=bundle.created_at,
                )
                archive = create_worker_archive(
                    data_root=args.staging_root.resolve(),
                    envelope=envelope,
                    output_path=archive_path,
                    hmac_key=key,
                )
                archives.append(archive.path)
            entries.append(
                {
                    "ordinal": index + 1,
                    "symbol": symbol,
                    "instrument_id": instrument.instrument_id,
                    "task_id": execution.task.task_id,
                    "archive_name": archive_path.name,
                    "deduplicated": execution.deduplicated,
                }
            )
            if requested:
                recent_provider_failures.append(False)
        else:
            raise RuntimeError(f"{scope_label} daily failed for {symbol}: {execution.task.state}")
        if (
            requested
            and index + 1 < len(snapshot.instruments)
            and (args.fixture or transport_factory)
        ):
            sleeper(interval)

    if not entries:
        raise RuntimeError(f"{scope_label} daily produced no eligible archives")
    expected_names = [str(item["archive_name"]) for item in entries]
    if sorted(path.name for path in archives) != sorted(expected_names):
        raise RuntimeError(f"{scope_label} daily archive set is incomplete")
    manifest = {
        "schema_version": transfer_schema,
        "dataset_id": DATASET_ID,
        "instrument_snapshot_id": snapshot.snapshot_id,
        "session_date": args.session.isoformat(),
        "source_commit": args.commit_sha,
        "eligible_instruments": len(entries),
        "quarantined_instruments": len(quarantines),
        "archives": expected_names,
        "instrument_ids": [str(item["instrument_id"]) for item in entries],
        "quarantines": quarantines,
        "action_evidence_quarantines": action_evidence_failures,
    }
    transfer_size = _create_aggregate(args.transfer, manifest, archives)
    collection_elapsed_seconds = round(time.perf_counter() - collection_started_at, 3)
    report = {
        **manifest,
        "status": "collected",
        "mode": "replay" if args.fixture else "live",
        "network_route": "explicit_proxy" if proxy_url else "fixture",
        "actual_requests": actual_requests,
        "deduplicated_tasks": sum(bool(item["deduplicated"]) for item in entries),
        "parallel_requests": batch_mode,
        "provider_threads": provider_threads if batch_mode else 1,
        "provider_batch_size": provider_batch_size if batch_mode else 1,
        "provider_batch_count": provider_batch_count if batch_mode else actual_requests,
        "provider_elapsed_seconds": provider_fetch_elapsed_seconds,
        "collection_elapsed_seconds": collection_elapsed_seconds,
        "target_elapsed_seconds": 20 * 60,
        "collection_target_met": collection_elapsed_seconds <= 20 * 60,
        "provider_missing_symbols": provider_missing_symbols,
        "provider_cached_symbols": provider_cached_symbols,
        "request_interval_seconds": interval,
        "provider_retries": 0,
        "price_requests": price_requests,
        "reused_price_tasks": reused_price_tasks,
        "action_evidence_requests": action_evidence_requests,
        "action_evidence_sessions": [item.isoformat() for item in action_sessions],
        "action_evidence_quarantined_instruments": len(action_evidence_failures),
        "action_evidence_quarantines": action_evidence_failures,
        "corporate_actions_in_raw": False,
        "corporate_actions_in_sidecars": bool(action_sessions),
        "aggregate_transfers": 1,
        "transfer_name": args.transfer.name,
        "transfer_size_bytes": transfer_size,
        "entries": entries,
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    _write_json(args.report, report)
    return report


def _extract_transfer(transfer: Path, scratch: Path) -> tuple[dict[str, object], list[Path]]:
    manifest: dict[str, object] | None = None
    archives: list[Path] = []
    names: set[str] = set()
    with tarfile.open(transfer, mode="r") as bundle:
        for member in bundle.getmembers():
            if not member.isfile() or Path(member.name).name != member.name or member.name in names:
                raise ValueError("daily transfer contains an unsafe or duplicate member")
            source = bundle.extractfile(member)
            if source is None:
                raise ValueError("daily transfer member is unreadable")
            names.add(member.name)
            if member.name == "manifest.json":
                manifest = json.loads(source.read())
            elif member.name.endswith(".tar"):
                path = scratch / member.name
                atomic_replace_bytes(path, source.read())
                archives.append(path)
            else:
                raise ValueError("daily transfer contains an unsupported member")
    if manifest is None or not archives:
        raise ValueError("daily transfer is incomplete")
    return manifest, sorted(archives)


def _import_archives(archives: list[Path], *, data_root: Path, hmac_key: bytes, workers: int):
    def import_one(path: Path):
        return import_worker_archive(
            archive_path=path,
            data_root=data_root,
            hmac_key=hmac_key,
        )

    if workers == 1:
        return [import_one(path) for path in archives]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(import_one, archives))


def accept(args: argparse.Namespace) -> dict[str, object]:
    scope_label = getattr(args, "scope_label", "US Core 500")
    transfer_schema = getattr(args, "transfer_schema", TRANSFER_SCHEMA)
    dataset_version_prefix = getattr(args, "dataset_version_prefix", "core-daily-v2")
    state_version_prefix = getattr(args, "state_dataset_version_prefix", dataset_version_prefix)
    verification_mode = getattr(args, "verification_mode", "full_replay")
    if verification_mode not in {"readback_only", "full_replay"}:
        raise ValueError(f"unsupported {scope_label} daily verification mode")
    workers = int(getattr(args, "local_import_workers", 1))
    if not 1 <= workers <= 8:
        raise ValueError("local import workers must be between 1 and 8")
    snapshot = load_us_instrument_snapshot(args.snapshot)
    minimum_session_coverage = float(
        getattr(args, "minimum_session_coverage", 0.0)
    )
    maximum_session_omissions = int(
        getattr(args, "maximum_session_omissions", len(snapshot.instruments))
    )
    if not 0.0 <= minimum_session_coverage <= 1.0:
        raise ValueError("minimum session coverage must be between zero and one")
    if maximum_session_omissions < 0:
        raise ValueError("maximum session omissions must be non-negative")
    watermark = _load_watermark(
        args.state,
        args.initial_watermark,
        snapshot.snapshot_id,
        state_version_prefix,
    )
    key = _key(args.hmac_key_file)
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    temp_prefix = getattr(args, "temporary_prefix", "us-core-daily-")
    with tempfile.TemporaryDirectory(prefix=temp_prefix, dir=args.scratch_parent) as temp:
        manifest, archives = _extract_transfer(args.transfer, Path(temp))
        if (
            manifest.get("schema_version") != transfer_schema
            or manifest.get("dataset_id") != DATASET_ID
            or manifest.get("instrument_snapshot_id") != snapshot.snapshot_id
        ):
            raise ValueError("daily transfer manifest does not match the accepted dataset snapshot")
        session = date.fromisoformat(str(manifest["session_date"]))
        expected_session = next_completed_session(
            watermark, datetime.combine(session + timedelta(days=1), datetime.min.time(), UTC)
        )
        if session > watermark and expected_session != session:
            raise ValueError("daily transfer is not the oldest missing session")
        expected_names = sorted(str(item) for item in manifest["archives"])
        if [path.name for path in archives] != expected_names:
            raise ValueError("daily transfer archive inventory does not match its manifest")
        first = _import_archives(
            archives,
            data_root=args.isolated_root,
            hmac_key=key,
            workers=workers,
        )
        replay = (
            _import_archives(
                archives,
                data_root=args.isolated_root,
                hmac_key=key,
                workers=workers,
            )
            if verification_mode == "full_replay"
            else []
        )
    if replay and any(not item.idempotent_replay for item in replay):
        raise RuntimeError(f"{scope_label} daily replay was not idempotent")
    storage = LocalFileSystemStorage(args.isolated_root)
    rows: list[dict[str, object]] = []
    for imported in first:
        normalized = next(
            item
            for item in imported.envelope.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        rows.extend(storage.read_dataset(normalized.storage_uri))
    keys = [(str(row["instrument_id"]), row["ts_event"].date()) for row in rows]
    instruments = {item[0] for item in keys}
    expected_instruments = {str(item) for item in manifest["instrument_ids"]}
    if (
        len(keys) != len(expected_instruments)
        or len(keys) != len(set(keys))
        or instruments != expected_instruments
        or {item[1] for item in keys} != {session}
    ):
        raise RuntimeError(f"{scope_label} daily independent readback audit failed")
    snapshot_instruments = {item.instrument_id for item in snapshot.instruments}
    missing_instruments = snapshot_instruments - instruments
    session_coverage = len(instruments) / len(snapshot_instruments)
    if (
        session_coverage < minimum_session_coverage
        or len(missing_instruments) > maximum_session_omissions
    ):
        raise RuntimeError(
            f"{scope_label} daily session coverage is insufficient: "
            f"{len(instruments)}/{len(snapshot_instruments)} "
            f"({session_coverage:.2%}), missing {len(missing_instruments)}"
        )
    if (args.isolated_root / "catalog/quant.duckdb").exists() or (
        args.isolated_root / "catalog/current-manifest-set.json"
    ).exists():
        raise RuntimeError(f"{scope_label} daily acceptance unexpectedly touched Catalog/current")
    _write_watermark(
        args.state,
        max(watermark, session),
        snapshot.snapshot_id,
        state_version_prefix,
    )
    report = {
        "schema_version": "1.0.0",
        "status": "passed",
        "dataset_id": DATASET_ID,
        "instrument_snapshot_id": snapshot.snapshot_id,
        "session_date": session.isoformat(),
        "watermark_before": watermark.isoformat(),
        "watermark_after": max(watermark, session).isoformat(),
        "aggregate_transfers": 1,
        "archives": len(archives),
        "new_imports": sum(not item.idempotent_replay for item in first),
        "resumed_imports": sum(item.idempotent_replay for item in first),
        "replay_imports": len(replay),
        "verification_mode": verification_mode,
        "local_import_workers": workers,
        "idempotent_replay_deferred": verification_mode == "readback_only",
        "action_evidence_quarantines": manifest.get(
            "action_evidence_quarantines", []
        ),
        "rows": len(rows),
        "instruments": len(instruments),
        "snapshot_instruments": len(snapshot_instruments),
        "session_coverage": session_coverage,
        "missing_instruments": len(missing_instruments),
        "duplicate_keys": 0,
        "quarantines": manifest["quarantines"],
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    _write_json(args.report, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Operate one US Core 500 daily session")
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
