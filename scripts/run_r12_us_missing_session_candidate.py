#!/usr/bin/env python3
"""Recover one fixed R12 US session into an isolated candidate tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping
from datetime import date
from math import ceil
from pathlib import Path
from typing import Any

from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.r12_us_recovery import (
    PINNED_SNAPSHOT_ID,
    PINNED_UNIVERSE_COUNT,
    _shared_proxy_runtime_receipt,
)
from scripts.run_us_equity_session_daily_nas import load_config, run

SESSIONS = (
    "2026-09-22",
    "2026-09-23",
    "2026-09-24",
    "2026-09-25",
    "2026-09-28",
)
BUDGET_SCHEMA = "quantstack.r12-us-missing-session-budget.v1"
CANDIDATE_SCHEMA = "quantstack.r12-us-missing-session-candidate.v1"
SESSION_REQUEST_RESERVATION = 49
TOTAL_REQUEST_RESERVATION = SESSION_REQUEST_RESERVATION * len(SESSIONS)
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _inside(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _require_candidate_isolation(
    candidate_root: Path,
    production_config: Mapping[str, object],
    source_root: Path,
) -> Path:
    candidate = candidate_root.resolve()
    protected = {
        Path(str(production_config[key])).resolve()
        for key in ("state", "work_root", "session_parquet_root", "market_index_root")
    }
    protected.add(source_root.resolve())
    for path in protected:
        if _inside(candidate, path) or _inside(path, candidate):
            raise ValueError(f"candidate and protected paths overlap: {path}")
    for child in ("control", "work", "session-parquet", "market-indexes", "receipts"):
        path = candidate / child
        if path.is_symlink():
            raise ValueError(f"candidate output path must not be a symlink: {path}")
        if path.resolve(strict=False).parent != candidate:
            raise ValueError(f"candidate output path escapes candidate root: {path}")
    return candidate


def _candidate_config(
    production: Mapping[str, object], candidate: Path
) -> dict[str, object]:
    return {
        "schema_version": "quantstack.us-equity-session-daily.v1",
        "snapshot": production["snapshot"],
        "state": str(candidate / "control/state.json"),
        "initial_watermark": "2026-09-21",
        "work_root": str(candidate / "work"),
        "session_parquet_root": str(candidate / "session-parquet"),
        "yfinance_runtime_root": production["yfinance_runtime_root"],
        "yfinance_proxy_url": production["yfinance_proxy_url"],
        "session_readiness_symbol": production.get("session_readiness_symbol", "SPY"),
        "session_readiness_retry_seconds": 3600,
        "session_readiness_max_attempts": 8,
        "provider_threads": 1,
        "provider_batch_size": 128,
        "provider_batch_start_interval_seconds": 45,
        "market_index_root": str(candidate / "market-indexes"),
    }


def _expected_session(candidate: Path) -> str:
    state = candidate / "control/state.json"
    if not state.is_file():
        return SESSIONS[0]
    payload = json.loads(state.read_text(encoding="utf-8"))
    watermark = str(payload.get("watermark", ""))
    if watermark not in SESSIONS:
        raise ValueError(f"candidate watermark is outside the fixed R12 scope: {watermark}")
    index = SESSIONS.index(watermark) + 1
    if index >= len(SESSIONS):
        raise ValueError("all fixed R12 US missing sessions are already complete")
    return SESSIONS[index]


def _reserve_session(path: Path, session: str) -> dict[str, Any]:
    if path.exists():
        ledger = json.loads(path.read_text(encoding="utf-8"))
        if ledger.get("schema_version") != BUDGET_SCHEMA:
            raise ValueError("unsupported US missing-session budget ledger")
    else:
        ledger = {
            "schema_version": BUDGET_SCHEMA,
            "maximum_reserved_requests": TOTAL_REQUEST_RESERVATION,
            "session_request_reservation": SESSION_REQUEST_RESERVATION,
            "sessions": {},
        }
    sessions = ledger["sessions"]
    if session in sessions:
        raise ValueError(f"zero-retry policy blocks already reserved session: {session}")
    if len(sessions) >= len(SESSIONS):
        raise ValueError("persistent US missing-session request budget is exhausted")
    sessions[session] = {
        "status": "reserved",
        "reserved_requests": SESSION_REQUEST_RESERVATION,
    }
    ledger["reserved_requests"] = len(sessions) * SESSION_REQUEST_RESERVATION
    _atomic_json(path, ledger)
    return ledger


def _finalize_session(path: Path, session: str, status: str) -> None:
    ledger = json.loads(path.read_text(encoding="utf-8"))
    entry = ledger["sessions"][session]
    if entry.get("status") != "reserved":
        raise ValueError("US missing-session budget reservation is not active")
    entry["status"] = status
    _atomic_json(path, ledger)


def recover_missing_session(
    *,
    production_config_path: Path,
    session: str,
    candidate_root: Path,
    source_root: Path,
    source_commit: str,
    source_archive: Path,
    source_archive_sha256: str,
    execute: bool = False,
    caller_holds_operation_lock: bool = False,
    runner: Callable[[dict[str, object]], dict[str, object]] = run,
) -> dict[str, Any]:
    if session not in SESSIONS:
        raise ValueError(f"session is outside the fixed R12 scope: {session}")
    date.fromisoformat(session)
    if _COMMIT.fullmatch(source_commit) is None:
        raise ValueError("source_commit must be a full lowercase commit SHA")
    if _sha256(source_archive) != source_archive_sha256:
        raise ValueError("source archive SHA-256 mismatch")
    source = source_root.resolve()
    if not (source / "scripts/run_r12_us_missing_session_candidate.py").is_file():
        raise ValueError("source root does not contain the R12 missing-session runner")
    production = load_config(production_config_path)
    snapshot = load_us_instrument_snapshot(Path(str(production["snapshot"])))
    if (
        snapshot.snapshot_id != PINNED_SNAPSHOT_ID
        or len(snapshot.instruments) != PINNED_UNIVERSE_COUNT
    ):
        raise ValueError("US missing-session recovery requires the pinned U=5079 snapshot")
    candidate = _require_candidate_isolation(candidate_root, production, source)
    config = _candidate_config(production, candidate)
    expected_session = _expected_session(candidate)
    if session != expected_session:
        raise ValueError(
            f"session must be the next fixed candidate session {expected_session}, got {session}"
        )
    computed_reservation = (
        ceil(PINNED_UNIVERSE_COUNT / int(config["provider_batch_size"]))
        + 1
        + int(config["session_readiness_max_attempts"])
    )
    if computed_reservation != SESSION_REQUEST_RESERVATION:
        raise ValueError("US missing-session request reservation contract changed")
    receipt = {
        "schema_version": CANDIDATE_SCHEMA,
        "mode": "execute" if execute else "dry_run",
        "session": session,
        "provider_calls": 0 if not execute else None,
        "authority_written": False,
        "candidate_root": str(candidate),
        "source": {
            "root": str(source),
            "commit": source_commit,
            "archive": str(source_archive.resolve()),
            "archive_sha256": source_archive_sha256,
        },
        "policy": {
            "fixed_sessions": list(SESSIONS),
            "provider_retries": 0,
            "provider_batch_size": config["provider_batch_size"],
            "provider_threads": config["provider_threads"],
            "transport_threads": False,
            "provider_batch_start_interval_seconds": config[
                "provider_batch_start_interval_seconds"
            ],
            "shared_runtime_root": config["yfinance_runtime_root"],
            "shared_proxy_route": True,
            "caller_operation_lock_required": True,
            "session_request_reservation": SESSION_REQUEST_RESERVATION,
            "session_request_reservation_formula": "ceil(5079/128)+1_index+8_readiness",
            "total_request_reservation": TOTAL_REQUEST_RESERVATION,
        },
        "candidate_config": config,
    }
    if not execute:
        return receipt
    if not caller_holds_operation_lock:
        raise ValueError("--execute requires caller_holds_operation_lock")
    runtime_receipt = _shared_proxy_runtime_receipt(
        Path(str(config["yfinance_runtime_root"])), str(config["yfinance_proxy_url"])
    )
    receipt["policy"]["shared_runtime_route"] = runtime_receipt
    candidate.mkdir(parents=True, exist_ok=True)
    config_path = candidate / "control/candidate-config.json"
    if config_path.exists():
        persisted = json.loads(config_path.read_text(encoding="utf-8"))
        if persisted != config:
            raise ValueError("candidate config changed after first execution")
    else:
        _atomic_json(config_path, config)
    ledger_path = candidate / "control/session-budget.json"
    _reserve_session(ledger_path, session)
    try:
        result = runner(config)
    except Exception:
        _finalize_session(ledger_path, session, "failed_no_retry")
        raise
    if result.get("status") != "completed" or result.get("session") != session:
        _finalize_session(ledger_path, session, "unexpected_result_no_retry")
        raise RuntimeError(f"US candidate produced unexpected session result: {result}")
    _finalize_session(ledger_path, session, "completed_requires_review")
    receipt["provider_calls"] = {
        "maximum": computed_reservation,
        "formula": "ceil(5079/128)+1_index+8_readiness",
    }
    receipt["result"] = result
    receipt["status"] = "requires_review"
    receipt_path = candidate / "receipts" / f"session-{session}.json"
    if receipt_path.exists() or receipt_path.is_symlink():
        raise ValueError(f"candidate receipt already exists: {receipt_path}")
    _atomic_json(receipt_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--production-config", type=Path, required=True)
    parser.add_argument("--session", choices=SESSIONS, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--caller-holds-operation-lock", action="store_true")
    args = parser.parse_args()
    result = recover_missing_session(
        production_config_path=args.production_config,
        session=args.session,
        candidate_root=args.candidate_root,
        source_root=args.source_root,
        source_commit=args.source_commit,
        source_archive=args.source_archive,
        source_archive_sha256=args.source_archive_sha256,
        execute=args.execute,
        caller_holds_operation_lock=args.caller_holds_operation_lock,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
