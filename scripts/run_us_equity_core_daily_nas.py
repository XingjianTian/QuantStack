#!/usr/bin/env python3
"""Operate the single-node NAS US Core 500 daily chain."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from datetime import time as datetime_time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    load_us_instrument_snapshot,
)
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import TaskState, TaskStateFile
from scripts import run_us_equity_core_daily as daily
from scripts import run_us_equity_core_periodic_replay as periodic
from scripts.run_us_equity_isolated_daily import _load_watermark, next_completed_session

CORE_CONFIG_SCHEMA = "quantstack.us-core-daily-nas.v1"
FULL_MARKET_CONFIG_SCHEMA = "quantstack.us-equity-daily-nas.v1"
DEFAULT_READINESS_SYMBOL = "SPY"
DEFAULT_READINESS_RETRY_SECONDS = 60 * 60
DEFAULT_READINESS_MAX_ATTEMPTS = 8
DEFAULT_PROVIDER_THREADS = 48
DEFAULT_PROVIDER_BATCH_SIZE = 512


def _path(payload: dict[str, object], key: str) -> Path:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"US Core daily config requires {key}")
    return Path(value)


def load_config(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") not in {CORE_CONFIG_SCHEMA, FULL_MARKET_CONFIG_SCHEMA}:
        raise ValueError("unsupported US Core NAS daily config")
    for key in (
        "repo_root",
        "snapshot",
        "state",
        "work_root",
        "isolated_root",
        "hmac_key_file",
        "yfinance_runtime_root",
    ):
        _path(payload, key)
    if payload.get("schema_version") == FULL_MARKET_CONFIG_SCHEMA:
        if not str(payload.get("session_readiness_symbol", DEFAULT_READINESS_SYMBOL)).strip():
            raise ValueError("session readiness symbol must not be empty")
        _path(payload, "action_evidence_root")
        date.fromisoformat(str(payload["action_initial_watermark"]))
    proxy_url = str(payload.get("yfinance_proxy_url", ""))
    parsed = urlsplit(proxy_url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port != 7890:
        raise ValueError("NAS daily acquisition requires the local Mihomo route")
    date.fromisoformat(str(payload["initial_watermark"]))
    if payload.get("verification_mode") != "readback_only":
        raise ValueError("routine NAS daily verification must be readback_only")
    local_import_workers = int(payload.get("local_import_workers", 1))
    if not 1 <= local_import_workers <= 8:
        raise ValueError("local import workers must be between 1 and 8")
    source_commit = payload.get("source_commit")
    if source_commit is not None and not re.fullmatch(r"[0-9a-f]{40}", str(source_commit)):
        raise ValueError("source_commit must be a full Git commit when configured")
    if payload.get("schema_version") == FULL_MARKET_CONFIG_SCHEMA:
        if (
            int(payload.get("session_readiness_retry_seconds", DEFAULT_READINESS_RETRY_SECONDS))
            < 60
        ):
            raise ValueError("session readiness retry interval must be at least 60 seconds")
        if (
            not 1
            <= int(payload.get("session_readiness_max_attempts", DEFAULT_READINESS_MAX_ATTEMPTS))
            <= 24
        ):
            raise ValueError("session readiness attempts must be between 1 and 24")
        generations = payload.get("task_recovery_generations", {})
        if not isinstance(generations, dict):
            raise ValueError("task recovery generations must map sessions to positive integers")
        for session, generation in generations.items():
            date.fromisoformat(str(session))
            if int(generation) < 1:
                raise ValueError("task recovery generations must map sessions to positive integers")
        provider_threads = int(payload.get("provider_threads", DEFAULT_PROVIDER_THREADS))
        provider_batch_size = int(
            payload.get("provider_batch_size", DEFAULT_PROVIDER_BATCH_SIZE)
        )
        if not 2 <= provider_threads <= 48:
            raise ValueError("full-market provider threads must be between 2 and 48")
        if not 32 <= provider_batch_size <= 512:
            raise ValueError("full-market provider batch size must be between 32 and 512")
    return payload


def _source_commit(repo_root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _configured_source_commit(config: dict[str, object]) -> str:
    configured = config.get("source_commit")
    return str(configured) if configured else _source_commit(_path(config, "repo_root"))


def _report(path: Path, payload: dict[str, object]) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
    )


def _readiness_lag_seconds(session: date, observed_at: datetime) -> int:
    market_close = datetime.combine(
        session,
        datetime_time(hour=16),
        tzinfo=ZoneInfo("America/New_York"),
    )
    return max(0, int((observed_at - market_close.astimezone(UTC)).total_seconds()))


def wait_for_session_ready(
    config: dict[str, object],
    *,
    session: date,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    transport_factory: Callable[[], LiveYFinanceTransport] | None = None,
) -> dict[str, object]:
    """Wait for one liquid canary to expose the requested completed session."""

    symbol = str(config.get("session_readiness_symbol", DEFAULT_READINESS_SYMBOL)).upper()
    retry_seconds = int(
        config.get("session_readiness_retry_seconds", DEFAULT_READINESS_RETRY_SECONDS)
    )
    max_attempts = int(config.get("session_readiness_max_attempts", DEFAULT_READINESS_MAX_ATTEMPTS))
    work_root = _path(config, "work_root")
    report_path = work_root / "reports" / f"readiness-{session.isoformat()}.json"
    if report_path.is_file():
        existing = json.loads(report_path.read_text(encoding="utf-8"))
        if (
            existing.get("session_date") == session.isoformat()
            and existing.get("status") == "ready"
        ):
            return {
                **existing,
                "reused_ready_report": True,
                "provider_requests_current_run": 0,
            }
    transport = (
        transport_factory()
        if transport_factory is not None
        else LiveYFinanceTransport(
            runtime_root=_path(config, "yfinance_runtime_root"),
            proxy_url=str(config["yfinance_proxy_url"]),
            minimum_interval_seconds=None,
            include_actions=True,
        )
    )
    attempts: list[dict[str, object]] = []
    for attempt_number in range(1, max_attempts + 1):
        checked_at = clock().astimezone(UTC)
        error_type: str | None = None
        error_message: str | None = None
        records: list[dict[str, object]] = []
        try:
            records = [
                dict(item)
                for item in transport.fetch_daily_bars_with_actions([symbol], session, session)
            ]
        except ConnectionError as exc:
            error_type = type(exc).__name__
            error_message = str(exc)
        observed_dates = sorted({str(item.get("Date", ""))[:10] for item in records})
        ready = session.isoformat() in observed_dates
        attempts.append(
            {
                "attempt": attempt_number,
                "checked_at": checked_at.isoformat(),
                "checked_at_asia_shanghai": checked_at.astimezone(
                    ZoneInfo("Asia/Shanghai")
                ).isoformat(),
                "observed_dates": observed_dates,
                "status": "ready" if ready else "not_ready",
                "error_type": error_type,
                "error_message": error_message,
            }
        )
        final_attempt = attempt_number == max_attempts
        result = {
            "schema_version": "quantstack.us-session-readiness.v1",
            "session_date": session.isoformat(),
            "symbol": symbol,
            "status": "ready" if ready else "exhausted" if final_attempt else "waiting",
            "attempts": attempts,
            "retry_interval_seconds": retry_seconds,
            "provider_requests": len(attempts),
            "provider_requests_current_run": len(attempts),
            "reused_ready_report": False,
            "first_ready_observed_at": checked_at.isoformat() if ready else None,
            "first_ready_observed_at_asia_shanghai": (
                checked_at.astimezone(ZoneInfo("Asia/Shanghai")).isoformat() if ready else None
            ),
            "seconds_after_regular_close": (
                _readiness_lag_seconds(session, checked_at) if ready else None
            ),
        }
        _report(report_path, result)
        if ready:
            return result
        if attempt_number < max_attempts:
            sleeper(retry_seconds)
    raise RuntimeError(
        f"Yahoo session {session.isoformat()} was not ready after {max_attempts} checks"
    )


def _action_sessions(
    config: dict[str, object], *, session: date, observed_at: datetime
) -> list[date]:
    if config.get("schema_version") != FULL_MARKET_CONFIG_SCHEMA:
        return []
    sessions: list[date] = []
    watermark = date.fromisoformat(str(config["action_initial_watermark"]))
    while True:
        candidate = next_completed_session(watermark, observed_at)
        if candidate is None or candidate > session:
            return sessions
        sessions.append(candidate)
        watermark = candidate


def _incomplete_recovery_session(
    config: dict[str, object],
    *,
    snapshot_instruments: int,
    watermark: date,
) -> date | None:
    generations = dict(config.get("task_recovery_generations", {}))
    work_root = _path(config, "work_root")
    for token in sorted(generations):
        session = date.fromisoformat(str(token))
        if session > watermark:
            continue
        report_path = work_root / "reports" / f"accept-{session.isoformat()}.json"
        if not report_path.is_file():
            continue
        report = json.loads(report_path.read_text(encoding="utf-8"))
        observed = int(report.get("instruments", 0))
        missing = snapshot_instruments - observed
        if observed / snapshot_instruments < 0.99 or missing > 100:
            return session
    return None


def plan_daily(config: dict[str, object], *, now: datetime | None = None) -> dict[str, object]:
    """Build a zero-Provider recovery inventory for the next daily session."""

    full_market = config.get("schema_version") == FULL_MARKET_CONFIG_SCHEMA
    observed_at = now or datetime.now(UTC)
    snapshot = load_us_instrument_snapshot(_path(config, "snapshot"))
    state_path = _path(config, "state")
    initial_watermark = date.fromisoformat(str(config["initial_watermark"]))
    version_prefix = "full-market-daily-v1" if full_market else "core-daily-v2"
    watermark = _load_watermark(
        state_path,
        initial_watermark,
        snapshot.snapshot_id,
        None if full_market else version_prefix,
    )
    session = (
        _incomplete_recovery_session(
            config,
            snapshot_instruments=len(snapshot.instruments),
            watermark=watermark,
        )
        if full_market
        else None
    ) or next_completed_session(watermark, observed_at)
    if session is None:
        return {
            "schema_version": "quantstack.us-equity-daily-plan.v1",
            "status": "noop",
            "watermark": watermark.isoformat(),
            "provider_requested": False,
        }

    work_root = _path(config, "work_root")
    task_states: dict[str, TaskStateFile] = {}
    prefix = f"{session.isoformat()}.{version_prefix}."
    for path in (work_root / "tasks").glob("task-*.json"):
        state_file = TaskStateFile.model_validate_json(path.read_text(encoding="utf-8"))
        state = state_file.task
        if (
            state.dataset_id == daily.DATASET_ID
            and state.dataset_version.startswith(prefix)
            and len(state.instrument_scope) == 1
        ):
            symbol = state.instrument_scope[0]
            current = task_states.get(symbol)
            if current is None or state_file.updated_at > current.updated_at:
                task_states[symbol] = state_file

    symbols = {item.provider_symbol for item in snapshot.instruments}
    recovery_generation = int(
        dict(config.get("task_recovery_generations", {})).get(session.isoformat(), 0)
    )
    stale = daily._stale_session_symbols(
        work_root / "reports" / f"collect-{session.isoformat()}.json",
        session=session,
        recovery_generation=recovery_generation,
    )
    reused = {
        symbol for symbol, state in task_states.items() if state.task.state is TaskState.SUCCEEDED
    }.difference(stale)
    recovery_suffix = f".recovery-{recovery_generation}"
    recoverable = {
        symbol
        for symbol, state in task_states.items()
        if recovery_generation > 0
        and daily._session_unavailable_task(state)
        and not state.task.dataset_version.endswith(recovery_suffix)
    } | stale
    terminal = {
        symbol
        for symbol, state in task_states.items()
        if state.task.state in {TaskState.PERMANENTLY_FAILED, TaskState.QUARANTINED}
    }.difference(recoverable)
    price_missing = symbols.difference(reused, terminal)
    action_sessions = _action_sessions(config, session=session, observed_at=observed_at)
    action_only = set()
    if full_market:
        evidence_root = _path(config, "action_evidence_root")
        action_only = {
            symbol
            for symbol in reused
            if any(
                not daily._action_evidence_path(evidence_root, item, symbol).is_file()
                for item in action_sessions
            )
        }
    return {
        "schema_version": "quantstack.us-equity-daily-plan.v1",
        "status": "planned",
        "session_date": session.isoformat(),
        "universe_instruments": len(symbols),
        "reused_price_tasks": len(reused),
        "terminal_price_tasks": len(terminal),
        "recoverable_price_tasks": len(recoverable),
        "price_requests": len(price_missing),
        "action_evidence_requests": len(action_only),
        "combined_price_action_requests": len(price_missing) if full_market else 0,
        "total_provider_requests": len(price_missing | action_only),
        "duplicate_price_requests": 0,
        "action_evidence_sessions": [item.isoformat() for item in action_sessions],
        "provider_requested": False,
    }


def run_daily(
    config: dict[str, object],
    *,
    now: datetime | None = None,
    collect_fn: Callable[..., dict[str, object]] = daily.collect,
    accept_fn: Callable[..., dict[str, object]] = daily.accept,
    readiness_fn: Callable[..., dict[str, object]] = wait_for_session_ready,
    readiness_sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    full_market = config.get("schema_version") == FULL_MARKET_CONFIG_SCHEMA
    run_schema = (
        "quantstack.us-equity-daily-nas-run.v1"
        if full_market
        else "quantstack.us-core-daily-nas-run.v1"
    )
    transfer_prefix = "us-equity-daily" if full_market else "us-core-daily"
    scope_label = "US full-market" if full_market else "US Core 500"
    dataset_version_prefix = "full-market-daily-v1" if full_market else "core-daily-v2"
    transfer_schema = daily.FULL_MARKET_TRANSFER_SCHEMA if full_market else daily.TRANSFER_SCHEMA
    observed_at = now or datetime.now(UTC)
    snapshot_path = _path(config, "snapshot")
    state_path = _path(config, "state")
    snapshot = load_us_instrument_snapshot(snapshot_path)
    initial_watermark = date.fromisoformat(str(config["initial_watermark"]))
    watermark = _load_watermark(
        state_path,
        initial_watermark,
        snapshot.snapshot_id,
        None if full_market else dataset_version_prefix,
    )
    session = (
        _incomplete_recovery_session(
            config,
            snapshot_instruments=len(snapshot.instruments),
            watermark=watermark,
        )
        if full_market
        else None
    ) or next_completed_session(watermark, observed_at)
    work_root = _path(config, "work_root")
    if session is None:
        result = {
            "schema_version": run_schema,
            "status": "noop",
            "watermark": watermark.isoformat(),
            "provider_requested": False,
        }
        _report(work_root / "reports" / "latest-noop.json", result)
        return result

    token = session.isoformat()
    readiness = None
    if full_market:
        readiness = readiness_fn(
            config,
            session=session,
            sleeper=readiness_sleeper,
        )
    collection_observed_at = observed_at if now is not None else datetime.now(UTC)
    action_sessions = _action_sessions(
        config,
        session=session,
        observed_at=collection_observed_at,
    )
    transfer = work_root / "outbound" / f"{transfer_prefix}-{token}.tar"
    collect_args = SimpleNamespace(
        snapshot=snapshot_path,
        session=session,
        staging_root=work_root / "producer",
        task_state_root=work_root / "tasks",
        archive_root=work_root / "archives",
        transfer=transfer,
        report=work_root / "reports" / f"collect-{token}.json",
        hmac_key_file=_path(config, "hmac_key_file"),
        commit_sha=_configured_source_commit(config),
        environment_id="nas-us-equity-daily" if full_market else "nas-us-core-daily",
        scope_label=scope_label,
        dataset_version_prefix=dataset_version_prefix,
        transfer_schema=transfer_schema,
        request_interval_seconds=None,
        yfinance_runtime_root=_path(config, "yfinance_runtime_root"),
        yfinance_proxy_url=str(config["yfinance_proxy_url"]),
        fixture=None,
        now=collection_observed_at,
        action_evidence_root=(_path(config, "action_evidence_root") if full_market else None),
        action_sessions=tuple(action_sessions),
        task_recovery_generation=int(
            dict(config.get("task_recovery_generations", {})).get(token, 0)
        ),
        provider_threads=(
            int(config.get("provider_threads", DEFAULT_PROVIDER_THREADS)) if full_market else 1
        ),
        provider_batch_size=(
            int(config.get("provider_batch_size", DEFAULT_PROVIDER_BATCH_SIZE))
            if full_market
            else 1
        ),
    )
    collected = collect_fn(collect_args)
    accept_args = SimpleNamespace(
        snapshot=snapshot_path,
        state=state_path,
        initial_watermark=initial_watermark,
        transfer=transfer,
        scratch_parent=work_root / "scratch",
        isolated_root=_path(config, "isolated_root"),
        hmac_key_file=_path(config, "hmac_key_file"),
        report=work_root / "reports" / f"accept-{token}.json",
        verification_mode="readback_only",
        local_import_workers=int(config.get("local_import_workers", 1)),
        scope_label=scope_label,
        transfer_schema=transfer_schema,
        dataset_version_prefix=dataset_version_prefix,
        temporary_prefix="us-equity-daily-" if full_market else "us-core-daily-",
        state_dataset_version_prefix=None if full_market else dataset_version_prefix,
        minimum_session_coverage=0.99 if full_market else 0.0,
        maximum_session_omissions=100 if full_market else len(snapshot.instruments),
    )
    accepted = accept_fn(accept_args)
    result = {
        "schema_version": run_schema,
        "status": "completed",
        "session_date": token,
        "actual_requests": collected["actual_requests"],
        "request_interval_seconds": collected["request_interval_seconds"],
        "aggregate_transfers": collected["aggregate_transfers"],
        "rows": accepted["rows"],
        "instruments": accepted["instruments"],
        "duplicate_keys": accepted["duplicate_keys"],
        "verification_mode": accepted["verification_mode"],
        "watermark_after": accepted["watermark_after"],
        "provider_retries": 0,
        "session_readiness": readiness,
        "price_requests": collected.get("price_requests", collected["actual_requests"]),
        "action_evidence_requests": collected.get("action_evidence_requests", 0),
        "parallel_requests": collected.get("parallel_requests", False),
        "provider_threads": collected.get("provider_threads", 1),
        "provider_batch_size": collected.get("provider_batch_size", 1),
        "provider_batch_count": collected.get("provider_batch_count", 0),
        "provider_elapsed_seconds": collected.get("provider_elapsed_seconds"),
        "collection_elapsed_seconds": collected.get("collection_elapsed_seconds"),
        "target_elapsed_seconds": collected.get("target_elapsed_seconds", 20 * 60),
        "collection_target_met": collected.get("collection_target_met"),
    }
    _report(work_root / "reports" / f"run-{token}.json", result)
    return result


def run_weekly(
    config: dict[str, object],
    *,
    replay_fn: Callable[..., dict[str, object]] = periodic.replay,
) -> dict[str, object]:
    full_market = config.get("schema_version") == FULL_MARKET_CONFIG_SCHEMA
    transfer_prefix = "us-equity-daily" if full_market else "us-core-daily"
    dataset_version_prefix = "full-market-daily-v1" if full_market else "core-daily-v2"
    snapshot_path = _path(config, "snapshot")
    state_path = _path(config, "state")
    snapshot = load_us_instrument_snapshot(snapshot_path)
    initial_watermark = date.fromisoformat(str(config["initial_watermark"]))
    session = _load_watermark(
        state_path,
        initial_watermark,
        snapshot.snapshot_id,
        None if full_market else dataset_version_prefix,
    )
    work_root = _path(config, "work_root")
    args = SimpleNamespace(
        snapshot=snapshot_path,
        session=session,
        state=state_path,
        initial_watermark=initial_watermark,
        transfer=work_root / "outbound" / f"{transfer_prefix}-{session.isoformat()}.tar",
        scratch_parent=work_root / "periodic-scratch",
        isolated_root=_path(config, "isolated_root"),
        hmac_key_file=_path(config, "hmac_key_file"),
        report=work_root / "reports" / f"periodic-{session.isoformat()}.json",
        scope_label="US full-market" if full_market else "US Core 500",
        transfer_schema=(
            daily.FULL_MARKET_TRANSFER_SCHEMA if full_market else daily.TRANSFER_SCHEMA
        ),
        dataset_version_prefix=dataset_version_prefix,
        temporary_prefix=(
            "us-equity-periodic-replay-" if full_market else "us-core-periodic-replay-"
        ),
        local_import_workers=int(config.get("local_import_workers", 1)),
        state_dataset_version_prefix=None if full_market else dataset_version_prefix,
    )
    return replay_fn(args)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "daily", "weekly"))
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    result = (
        plan_daily(config)
        if args.command == "plan"
        else run_daily(config)
        if args.command == "daily"
        else run_weekly(config)
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
