#!/usr/bin/env python3
"""Advance one isolated R12 CN qfq candidate under the raw task budget."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.cn_qfq_incremental import (
    build_factor_query_plan,
    factor_last_checked,
    load_daily_session_records,
)
from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.run_cn_qfq_session_daily import (
    _next_package,
    _read,
    _universe,
    _validate_provider_request_budget,
)
from scripts.run_cn_qfq_session_daily import (
    run as run_qfq,
)

RAW_SESSIONS = (
    date(2026, 9, 23),
    date(2026, 9, 24),
    date(2026, 9, 28),
    date(2026, 9, 29),
)
FACTOR_SESSIONS = (
    date(2026, 9, 22),
    date(2026, 9, 23),
    date(2026, 9, 24),
    date(2026, 9, 28),
    date(2026, 9, 29),
)
EXPECTED_SESSION_AFTER_WATERMARK = {
    date(2026, 9, 21): date(2026, 9, 22),
    date(2026, 9, 22): date(2026, 9, 23),
    date(2026, 9, 23): date(2026, 9, 24),
    date(2026, 9, 24): date(2026, 9, 28),
    date(2026, 9, 28): date(2026, 9, 29),
}
TASK_BUDGET_LIMIT = 44_000
MAX_FACTOR_QUERIES = 1_370
LOGIN_ALLOWANCE = 1
EXPECTED_STATE_UNIVERSE = {
    date(2026, 9, 21): 5_474,
    date(2026, 9, 22): 5_475,
    date(2026, 9, 23): 5_475,
    date(2026, 9, 24): 5_475,
    date(2026, 9, 28): 5_475,
}
EXPECTED_FACTOR_UNIVERSE = {
    date(2026, 9, 22): 5_475,
    date(2026, 9, 23): 5_475,
    date(2026, 9, 24): 5_475,
    date(2026, 9, 28): 5_475,
    date(2026, 9, 29): 5_476,
}
EXPECTED_FACTOR_UNIVERSE_DIGEST = {
    date(2026, 9, 22): "0c93e6c5bbc9e0d04a110f2e1932920a583e6cb4bd5415f56cc1db659147d448",
    date(2026, 9, 23): "0c93e6c5bbc9e0d04a110f2e1932920a583e6cb4bd5415f56cc1db659147d448",
    date(2026, 9, 24): "0c93e6c5bbc9e0d04a110f2e1932920a583e6cb4bd5415f56cc1db659147d448",
    date(2026, 9, 28): "0c93e6c5bbc9e0d04a110f2e1932920a583e6cb4bd5415f56cc1db659147d448",
    date(2026, 9, 29): "25e080d2aeed00647c2cfcce6aa19350715dd776a129d1ab38d4ca8110b2057c",
}


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _instrument_id_digest(instrument_ids: list[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(instrument_ids)) + "\n").encode()).hexdigest()


def _validate_frozen_factor_universe(
    session: date, universe: list[dict[str, str]]
) -> None:
    instrument_ids = [str(item["instrument_id"]) for item in universe]
    expected_count = EXPECTED_FACTOR_UNIVERSE[session]
    if len(instrument_ids) != expected_count or len(set(instrument_ids)) != expected_count:
        raise RuntimeError(
            "qfq factor universe count differs from the frozen session universe: "
            f"session={session} observed={len(instrument_ids)} "
            f"unique={len(set(instrument_ids))} expected={expected_count}"
        )
    observed_digest = _instrument_id_digest(instrument_ids)
    expected_digest = EXPECTED_FACTOR_UNIVERSE_DIGEST[session]
    if observed_digest != expected_digest:
        raise RuntimeError(
            "qfq factor universe digest differs from the frozen session lineage: "
            f"session={session}"
        )


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _require_candidate_path(path: Path, candidate_root: Path, *, label: str) -> Path:
    raw = path.expanduser()
    if raw.is_symlink():
        raise ValueError(f"{label} must not be a symlink")
    resolved = raw.resolve(strict=False)
    if not _is_relative_to(resolved, candidate_root):
        raise ValueError(f"{label} must be inside the qfq candidate root")
    current = resolved
    while current != candidate_root:
        if current.is_symlink():
            raise ValueError(f"{label} has a symlink in its write path")
        current = current.parent
    return resolved


def _validate_isolation(
    *,
    config_path: Path,
    state_path: Path,
    candidate_root: Path,
    authority_data_root: Path,
    expected_provider_control_root: Path,
    expected_instruments_sha256: str,
) -> dict[str, object]:
    candidate = candidate_root.expanduser().resolve(strict=False)
    authority = authority_data_root.expanduser().resolve()
    if candidate_root.is_symlink() or candidate == Path("/"):
        raise ValueError("qfq candidate root must be one bounded regular path")
    if (
        candidate == authority
        or _is_relative_to(candidate, authority)
        or _is_relative_to(authority, candidate)
    ):
        raise ValueError("qfq candidate and authority roots must not overlap")
    config_resolved = _require_candidate_path(config_path, candidate, label="qfq candidate config")
    state_resolved = _require_candidate_path(state_path, candidate, label="qfq candidate state")
    config = _read(config_resolved)
    if config.get("schema_version") != "quantstack.cn-qfq-session-daily.v1":
        raise ValueError("unsupported CN qfq candidate config")
    if str(config.get("ss_base_url") or "").strip():
        raise ValueError("R12 qfq candidate must disable SS publication")
    for key in ("session_root", "work_root", "data_root", "ss_report_root"):
        _require_candidate_path(Path(str(config[key])), candidate, label=f"config {key}")
    observed_control = Path(str(config["provider_control_root"])).expanduser().resolve()
    expected_control = expected_provider_control_root.expanduser().resolve()
    if observed_control != expected_control:
        raise ValueError("qfq candidate does not use the expected shared Provider control root")
    instruments_path = Path(str(config["instruments_path"])).expanduser().resolve()
    if (
        not instruments_path.is_file()
        or instruments_path.is_symlink()
        or _sha256(instruments_path) != expected_instruments_sha256
    ):
        raise ValueError("qfq candidate instruments snapshot does not match its frozen digest")
    state = _read(state_resolved)
    if state.get("schema_version") != "quantstack.cn-qfq-session-state.v1":
        raise ValueError("unsupported CN qfq candidate state")
    watermark = date.fromisoformat(str(state["watermark"]))
    expected_state_count = EXPECTED_STATE_UNIVERSE.get(watermark)
    if (
        expected_state_count is None
        or int(state.get("instrument_count", -1)) != expected_state_count
    ):
        raise ValueError("qfq candidate state does not bind the frozen prior universe")
    return {"candidate_root": candidate, "config": config, "state": state}


@contextmanager
def _shared_cron_lock(path: Path) -> Iterator[None]:
    if not path.is_file() or path.is_symlink():
        raise ValueError("shared CN cron lock must be one existing regular file")
    with path.open("r+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("shared CN cron lock is already held") from exc
        yield


def _ledger_delta(before: dict[str, object], after: dict[str, object]) -> int:
    def usage(payload: dict[str, object]) -> dict[str, int]:
        days = payload.get("days", {})
        if not isinstance(days, dict):
            raise ValueError("shared BaoStock budget days are malformed")
        return {
            str(key): int(value.get("used", 0))
            for key, value in days.items()
            if isinstance(value, dict)
        }

    old = usage(before)
    return sum(max(value - old.get(day, 0), 0) for day, value in usage(after).items())


def _recompute_allocated(payload: dict[str, object]) -> int:
    factor = payload.get("factor_reservations", {})
    if not isinstance(factor, dict):
        raise ValueError("factor reservations are malformed")
    factor_allocated = sum(
        int(value["allocated_requests"]) for value in factor.values() if isinstance(value, dict)
    )
    settlement = payload.get("raw_settlement")
    if isinstance(settlement, dict):
        raw_allocated = int(settlement["raw_actual_total"])
    else:
        raw = payload.get("reservations", {})
        if not isinstance(raw, dict):
            raise ValueError("raw reservations are malformed")
        raw_allocated = sum(
            int(value["allocated_requests"]) for value in raw.values() if isinstance(value, dict)
        )
    allocated = raw_allocated + factor_allocated
    payload["allocated"] = allocated
    return allocated


def _validation_session(validation: dict[str, object], session: date) -> dict[str, object]:
    sessions = validation.get("sessions")
    if not isinstance(sessions, dict):
        raise ValueError("independent raw validation has no session map")
    item = sessions.get(session.isoformat())
    if not isinstance(item, dict) or item.get("status") != "pass":
        raise ValueError(f"independent raw validation did not pass {session}")
    checks = item.get("checks")
    if (
        not isinstance(checks, dict)
        or not checks
        or not all(value is True for value in checks.values())
    ):
        raise ValueError(f"independent raw validation checks are incomplete for {session}")
    return item


def _reconcile_raw_budget(
    payload: dict[str, object],
    *,
    raw_candidate_root: Path,
    authority_session_root: Path,
    validation_receipt_path: Path,
    shared_budget: dict[str, object],
) -> dict[str, object]:
    if payload.get("schema_version") != "quantstack.r12-provider-budget.v1":
        raise ValueError("unsupported R12 Provider task budget")
    if int(payload.get("total_limit", 0)) != TASK_BUDGET_LIMIT:
        raise ValueError("R12 Provider task budget limit changed")
    validation = _read(validation_receipt_path)
    if (
        validation.get("schema_version") != "quantstack.r12-cn-candidate-independent-validation.v1"
        or validation.get("overall_status") != "pass"
    ):
        raise ValueError("independent raw candidate validation is not complete")
    raw = payload.get("reservations")
    if not isinstance(raw, dict):
        raise ValueError("raw reservations are malformed")
    proof_sessions: dict[str, object] = {}
    raw_actual_total = 0
    original_allocated = 0
    for session in RAW_SESSIONS:
        key = session.isoformat()
        reservation = raw.get(key)
        if not isinstance(reservation, dict) or reservation.get("status") != "completed":
            raise RuntimeError(f"raw reservation is not safely complete: {key}")
        actual = reservation.get("actual_shared_ledger_delta")
        if not isinstance(actual, int) or isinstance(actual, bool) or actual < 0:
            raise RuntimeError(f"raw reservation has unknown Provider usage: {key}")
        planned = int(reservation["allocated_requests"])
        if actual > planned:
            raise RuntimeError(f"raw reservation exceeded its planned budget: {key}")
        evidence_path = raw_candidate_root / f"evidence/cn-session-{key}.json"
        evidence = _read(evidence_path)
        if (
            evidence.get("status") != "completed"
            or evidence.get("session") != key
            or evidence.get("provider_retries") != 0
            or int(dict(evidence.get("budget", {})).get("actual_shared_ledger_delta", -1)) != actual
        ):
            raise RuntimeError(f"raw recovery evidence disagrees with its reservation: {key}")
        partition = evidence.get("object_partition")
        if not isinstance(partition, dict):
            raise RuntimeError(f"raw object partition is malformed: {key}")
        u_count = int(partition.get("u_count", -1))
        returned_count = int(partition.get("returned_count", -1))
        omission_count = int(partition.get("omission_count", -1))
        semantics = partition.get("omission_semantics")
        closed = (
            partition.get("union_equals_u") is True
            and int(partition.get("returned_omission_overlap", -1)) == 0
            and u_count == returned_count + omission_count
        )
        semantics_valid = (
            omission_count == 0 and partition.get("raw_complete") is True and semantics == "none"
        ) or (
            omission_count > 0
            and partition.get("raw_complete") is False
            and semantics == "unverified_omission"
        )
        if not closed or not semantics_valid:
            raise RuntimeError(f"raw object partition is not closed: {key}")
        package_path = raw_candidate_root / f"session-parquet/market=CN/session={key}"
        package = inspect_session_package(package_path)
        validation_item = _validation_session(validation, session)
        counts = validation_item.get("counts")
        digests = validation_item.get("digests")
        if not isinstance(counts, dict) or not isinstance(digests, dict):
            raise ValueError(f"independent raw validation is malformed for {key}")
        instruments_path = raw_candidate_root / f"work/daily-{session:%Y%m%d}/instruments.json"
        expected_counts = {
            "U": u_count,
            "R": returned_count,
            "O": omission_count,
            "rows": package.row_count,
        }
        observed_counts = {name: int(counts[name]) for name in expected_counts}
        if observed_counts != expected_counts:
            raise RuntimeError(f"independent raw counts drifted for {key}")
        current_digests = {
            "bars_sha256": _sha256(package_path / "bars.parquet"),
            "manifest_sha256": _sha256(package_path / "manifest.json"),
            "instruments_sha256": _sha256(instruments_path),
        }
        if any(digests.get(name) != value for name, value in current_digests.items()):
            raise RuntimeError(f"independent raw artifacts drifted for {key}")
        identity_digests = {
            "U": partition.get("u_sha256"),
            "R": partition.get("returned_sha256"),
            "O": partition.get("omission_sha256"),
        }
        if any(digests.get(name) != value for name, value in identity_digests.items()):
            raise RuntimeError(f"independent raw identity sets drifted for {key}")
        if (authority_session_root / f"market=CN/session={key}").exists():
            raise RuntimeError(f"authority already contains the R12 raw target: {key}")
        budget = evidence.get("budget")
        if not isinstance(budget, dict):
            raise ValueError(f"raw evidence budget is malformed for {key}")
        day = str(budget.get("day"))
        used_before = int(budget.get("used_before", -1))
        shared_days = shared_budget.get("days", {})
        shared_day = shared_days.get(day, {}) if isinstance(shared_days, dict) else {}
        if (
            not isinstance(shared_day, dict)
            or int(shared_day.get("used", -1)) < used_before + actual
        ):
            raise RuntimeError(f"shared Provider ledger does not cover raw usage for {key}")
        original_allocated += planned
        raw_actual_total += actual
        proof_sessions[key] = {
            "planned_requests": planned,
            "actual_shared_ledger_delta": actual,
            "evidence_sha256": _sha256(evidence_path),
            "bars_sha256": current_digests["bars_sha256"],
            "manifest_sha256": current_digests["manifest_sha256"],
            "instruments_sha256": current_digests["instruments_sha256"],
            "reported_budget_day": day,
        }
    if shared_budget.get("blocked"):
        raise RuntimeError("shared BaoStock budget ledger is blocked")
    shared_days = shared_budget.get("days", {})
    if not isinstance(shared_days, dict):
        raise ValueError("shared BaoStock budget days are malformed")
    shared_total_used = sum(
        int(value.get("used", 0)) for value in shared_days.values() if isinstance(value, dict)
    )
    if shared_total_used < raw_actual_total:
        raise RuntimeError("shared Provider ledger does not cover total raw usage")
    proof_core = {
        "raw_sessions": proof_sessions,
        "validation_receipt_sha256": _sha256(validation_receipt_path),
        "raw_actual_total": raw_actual_total,
        "original_allocated": original_allocated,
    }
    proof_digest = hashlib.sha256(
        json.dumps(proof_core, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    expected = {
        "schema_version": "quantstack.r12-raw-budget-settlement.v1",
        **proof_core,
        "released_capacity": original_allocated - raw_actual_total,
        "proof_sha256": proof_digest,
    }
    existing = payload.get("raw_settlement")
    if existing is not None:
        if not isinstance(existing, dict):
            raise ValueError("raw settlement is malformed")
        comparable = {key: existing.get(key) for key in expected}
        if comparable != expected:
            raise RuntimeError("raw settlement proof drifted")
    else:
        payload["raw_settlement"] = {
            **expected,
            "settled_at": datetime.now(UTC).isoformat(),
        }
    _recompute_allocated(payload)
    return payload


def _reserve_factor(
    payload: dict[str, object], *, session: date, query_count: int
) -> dict[str, object]:
    if not isinstance(payload.get("raw_settlement"), dict):
        raise RuntimeError("raw task budget must be reconciled before factor requests")
    if session not in FACTOR_SESSIONS:
        raise ValueError(f"session is outside the frozen R12 qfq set: {session}")
    if not 1 <= query_count <= MAX_FACTOR_QUERIES:
        raise RuntimeError(
            "factor query plan exceeds the frozen cap: "
            f"queries={query_count} cap={MAX_FACTOR_QUERIES}"
        )
    reservations = payload.setdefault("factor_reservations", {})
    if not isinstance(reservations, dict):
        raise ValueError("factor reservations are malformed")
    key = f"factor:{session.isoformat()}"
    if key in reservations:
        raise RuntimeError(f"factor budget is already reserved: {key}")
    allocated = query_count + LOGIN_ALLOWANCE
    current = _recompute_allocated(payload)
    if current + allocated > TASK_BUDGET_LIMIT:
        raise RuntimeError(
            "R12 Provider task budget exhausted before factor access: "
            f"allocated={current} planned={allocated} limit={TASK_BUDGET_LIMIT}"
        )
    reservation = {
        "status": "reserved",
        "session": session.isoformat(),
        "query_requests": query_count,
        "login_allowance": LOGIN_ALLOWANCE,
        "allocated_requests": allocated,
        "actual_shared_ledger_delta": None,
        "reserved_at": datetime.now(UTC).isoformat(),
    }
    reservations[key] = reservation
    _recompute_allocated(payload)
    return reservation


def _finish_factor(
    payload: dict[str, object],
    *,
    session: date,
    status: str,
    actual_shared_ledger_delta: int | None,
) -> None:
    reservations = payload["factor_reservations"]
    reservation = reservations[f"factor:{session.isoformat()}"]
    reservation.update(
        {
            "status": status,
            "actual_shared_ledger_delta": actual_shared_ledger_delta,
            "finished_at": datetime.now(UTC).isoformat(),
        }
    )
    _recompute_allocated(payload)


def _factor_plan(
    config: dict[str, object], state: dict[str, object]
) -> tuple[date, dict[str, object]]:
    watermark = date.fromisoformat(str(state["watermark"]))
    expected = EXPECTED_SESSION_AFTER_WATERMARK.get(watermark)
    if expected is None:
        raise RuntimeError(f"qfq candidate watermark is outside the frozen sequence: {watermark}")
    session_root = Path(str(config["session_root"]))
    observed_sessions = {
        date.fromisoformat(path.name.removeprefix("session="))
        for path in (session_root / "market=CN").glob("session=*")
    }
    if observed_sessions != set(FACTOR_SESSIONS):
        raise RuntimeError("qfq raw overlay does not contain the exact frozen five sessions")
    next_package = _next_package(session_root, watermark)
    if next_package is None:
        raise RuntimeError("qfq candidate has no next raw session")
    session, package = next_package
    if session != expected:
        raise RuntimeError(
            f"qfq candidate expected {expected} after {watermark}, observed {session}"
        )
    inspected = inspect_session_package(package)
    universe = _universe(Path(str(config["instruments_path"])))
    _validate_frozen_factor_universe(session, universe)
    plan = build_factor_query_plan(
        session=session,
        session_ordinal=session.toordinal(),
        universe=universe,
        daily_native_records=load_daily_session_records(inspected.path / "bars.parquet"),
        previous_closes={
            str(key): float(value) for key, value in dict(state["latest_raw_closes"]).items()
        },
        last_factor_checks=factor_last_checked(Path(str(state["factor_ledger_path"]))),
    )
    _validate_provider_request_budget(plan)
    return session, plan


def _validate_state_lineage(
    *,
    config: dict[str, object],
    state: dict[str, object],
    candidate_root: Path,
    baseline_dataset_path: Path,
    baseline_dataset_sha256: str,
    baseline_factor_ledger_path: Path,
    baseline_factor_ledger_sha256: str,
) -> None:
    repo = Path(str(config["repo_root"])).expanduser().resolve()
    if repo != Path(__file__).resolve().parents[1]:
        raise ValueError("qfq config repo_root does not match the executing source snapshot")
    watermark = date.fromisoformat(str(state["watermark"]))
    dataset_path = Path(str(state["dataset_path"])).expanduser().resolve()
    ledger_path = Path(str(state["factor_ledger_path"])).expanduser().resolve()
    if watermark == date(2026, 9, 21):
        expected_dataset = baseline_dataset_path.expanduser().resolve()
        expected_ledger = baseline_factor_ledger_path.expanduser().resolve()
        if (
            dataset_path != expected_dataset
            or ledger_path != expected_ledger
            or not expected_dataset.is_file()
            or expected_dataset.is_symlink()
            or not expected_ledger.is_file()
            or expected_ledger.is_symlink()
            or _sha256(expected_dataset) != baseline_dataset_sha256
            or _sha256(expected_ledger) != baseline_factor_ledger_sha256
        ):
            raise RuntimeError("qfq baseline state does not match the frozen 2026-09-21 inputs")
        return
    if watermark not in FACTOR_SESSIONS:
        raise RuntimeError("qfq candidate state watermark is outside the frozen sequence")
    data_root = Path(str(config["data_root"])).expanduser().resolve()
    work_root = Path(str(config["work_root"])).expanduser().resolve()
    expected_dataset = (
        data_root
        / "normalized/bars_1d/market=CN"
        / f"version=daily-through-{watermark:%Y%m%d}.qfq-session-v1/bars.parquet"
    )
    expected_ledger = work_root / f"session={watermark.isoformat()}/factor-ledger.json"
    if dataset_path != expected_dataset or ledger_path != expected_ledger:
        raise RuntimeError("qfq candidate state does not point to the previous frozen session")
    evidence_path = candidate_root / f"evidence/qfq-session-{watermark.isoformat()}.json"
    if not evidence_path.is_file() or evidence_path.is_symlink():
        raise RuntimeError("qfq candidate is missing the previous session evidence")
    evidence = _read(evidence_path)
    if (
        evidence.get("status") != "completed"
        or evidence.get("session") != watermark.isoformat()
        or evidence.get("dataset_sha256") != _sha256(expected_dataset)
        or evidence.get("factor_ledger_sha256") != _sha256(expected_ledger)
    ):
        raise RuntimeError("qfq previous-session lineage evidence drifted")


def _completed_qfq_summary(
    *,
    result: dict[str, object],
    state_path: Path,
    config: dict[str, object],
    session: date,
    query_count: int,
    reserved_requests: int,
    actual_shared_ledger_delta: int,
) -> dict[str, object]:
    if result.get("status") != "completed" or result.get("session") != session.isoformat():
        raise RuntimeError(f"qfq candidate did not complete its expected session: {result}")
    if actual_shared_ledger_delta > reserved_requests:
        raise RuntimeError("qfq candidate exceeded its reserved Provider budget")
    next_state = _read(state_path)
    if next_state.get("watermark") != session.isoformat():
        raise RuntimeError("qfq candidate state did not advance to the expected session")
    if int(next_state.get("instrument_count", -1)) != EXPECTED_FACTOR_UNIVERSE[session]:
        raise RuntimeError("qfq candidate state has the wrong cumulative instrument count")
    data_root = Path(str(config["data_root"])).expanduser().resolve()
    work_root = Path(str(config["work_root"])).expanduser().resolve()
    expected_dataset = (
        data_root
        / "normalized/bars_1d/market=CN"
        / f"version=daily-through-{session:%Y%m%d}.qfq-session-v1/bars.parquet"
    )
    expected_ledger = work_root / f"session={session.isoformat()}/factor-ledger.json"
    observed_dataset = Path(str(next_state["dataset_path"])).expanduser().resolve()
    observed_ledger = Path(str(next_state["factor_ledger_path"])).expanduser().resolve()
    if (
        observed_dataset != expected_dataset
        or observed_ledger != expected_ledger
        or not expected_dataset.is_file()
        or expected_dataset.is_symlink()
        or not expected_ledger.is_file()
        or expected_ledger.is_symlink()
    ):
        raise RuntimeError("qfq candidate state points outside the expected session outputs")
    return {
        "schema_version": "quantstack.r12-cn-qfq-candidate.v1",
        "status": "completed",
        "session": session.isoformat(),
        "query_requests": query_count,
        "reserved_requests": reserved_requests,
        "actual_shared_ledger_delta": actual_shared_ledger_delta,
        "provider_retries": 0,
        "instrument_count": int(next_state["instrument_count"]),
        "instruments_sha256": _sha256(Path(str(config["instruments_path"]))),
        "authority_written": False,
        "catalog_scope": "candidate_only",
        "dataset_sha256": _sha256(expected_dataset),
        "factor_ledger_sha256": _sha256(expected_ledger),
        "runner": result,
    }


def execute(
    *,
    config_path: Path,
    state_path: Path,
    qfq_candidate_root: Path,
    raw_candidate_root: Path,
    authority_data_root: Path,
    authority_session_root: Path,
    expected_provider_control_root: Path,
    expected_instruments_sha256: str,
    baseline_dataset_path: Path,
    baseline_dataset_sha256: str,
    baseline_factor_ledger_path: Path,
    baseline_factor_ledger_sha256: str,
    shared_budget_path: Path,
    task_budget_path: Path,
    validation_receipt_path: Path,
    cron_lock: Path,
    dry_run: bool = False,
) -> dict[str, object]:
    isolation = _validate_isolation(
        config_path=config_path,
        state_path=state_path,
        candidate_root=qfq_candidate_root,
        authority_data_root=authority_data_root,
        expected_provider_control_root=expected_provider_control_root,
        expected_instruments_sha256=expected_instruments_sha256,
    )
    candidate = isolation["candidate_root"]
    config = isolation["config"]
    state = isolation["state"]
    _validate_state_lineage(
        config=config,
        state=state,
        candidate_root=Path(candidate),
        baseline_dataset_path=baseline_dataset_path,
        baseline_dataset_sha256=baseline_dataset_sha256,
        baseline_factor_ledger_path=baseline_factor_ledger_path,
        baseline_factor_ledger_sha256=baseline_factor_ledger_sha256,
    )
    raw_candidate = raw_candidate_root.expanduser().resolve()
    if (
        raw_candidate_root.is_symlink()
        or raw_candidate == Path("/")
        or raw_candidate == candidate
        or _is_relative_to(raw_candidate, candidate)
        or _is_relative_to(candidate, raw_candidate)
    ):
        raise ValueError("raw and qfq candidate roots must not overlap")
    expected_task_budget = raw_candidate / "control/r12-provider-budget.json"
    if (
        task_budget_path.is_symlink()
        or task_budget_path.expanduser().resolve() != expected_task_budget
        or not expected_task_budget.is_file()
        or expected_task_budget.is_symlink()
    ):
        raise ValueError("task budget must be the raw candidate task ledger")
    expected_shared_budget = (
        expected_provider_control_root.expanduser().resolve() / "baostock-request-budget.json"
    )
    if (
        shared_budget_path.is_symlink()
        or shared_budget_path.expanduser().resolve() != expected_shared_budget
        or not expected_shared_budget.is_file()
        or expected_shared_budget.is_symlink()
    ):
        raise ValueError("shared budget must be the expected Provider control ledger")
    expected_cron_lock = expected_provider_control_root.expanduser().resolve().parent / "cron.lock"
    if cron_lock.is_symlink() or cron_lock.expanduser().resolve() != expected_cron_lock:
        raise ValueError("cron lock must be the shared CN daily lock")
    validation_receipt = validation_receipt_path.expanduser().resolve()
    if (
        validation_receipt_path.is_symlink()
        or not _is_relative_to(validation_receipt, raw_candidate)
        or not validation_receipt.is_file()
        or validation_receipt.is_symlink()
    ):
        raise ValueError("raw validation receipt must be a regular raw-candidate artifact")
    with _shared_cron_lock(cron_lock.resolve()):
        task_budget = _read(task_budget_path)
        shared_before = _read(shared_budget_path)
        _reconcile_raw_budget(
            task_budget,
            raw_candidate_root=raw_candidate,
            authority_session_root=authority_session_root.resolve(),
            validation_receipt_path=validation_receipt,
            shared_budget=shared_before,
        )
        session, plan = _factor_plan(config, state)
        query_count = int(plan["request_count"])
        if dry_run:
            preview_budget = json.loads(json.dumps(task_budget))
            reservation = _reserve_factor(preview_budget, session=session, query_count=query_count)
            return {
                "schema_version": "quantstack.r12-cn-qfq-candidate.v1",
                "status": "dry_run",
                "session": session.isoformat(),
                "query_requests": query_count,
                "reservation": reservation,
                "allocated_after": preview_budget["allocated"],
                "provider_requested": False,
                "authority_written": False,
            }
        reservation = _reserve_factor(task_budget, session=session, query_count=query_count)
        _write_json(task_budget_path, task_budget)
        started_at = datetime.now(UTC)
        try:
            result = run_qfq(
                config_path.resolve(),
                state_path.resolve(),
                max_provider_requests=query_count,
            )
            shared_after = _read(shared_budget_path)
            actual = _ledger_delta(shared_before, shared_after)
            summary = _completed_qfq_summary(
                result=result,
                state_path=state_path,
                config=config,
                session=session,
                query_count=query_count,
                reserved_requests=int(reservation["allocated_requests"]),
                actual_shared_ledger_delta=actual,
            )
            _write_json(
                Path(candidate) / f"evidence/qfq-session-{session.isoformat()}.json",
                summary,
            )
        except Exception as exc:
            shared_after = _read(shared_budget_path)
            actual = _ledger_delta(shared_before, shared_after)
            _finish_factor(
                task_budget,
                session=session,
                status="failed",
                actual_shared_ledger_delta=actual,
            )
            _write_json(task_budget_path, task_budget)
            _write_json(
                Path(candidate) / "control/last-factor-attempt.json",
                {
                    "schema_version": "quantstack.r12-cn-qfq-attempt.v1",
                    "status": "failed",
                    "session": session.isoformat(),
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(UTC).isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:1000],
                    "reservation": reservation,
                    "actual_shared_ledger_delta": actual,
                },
            )
            raise
        _finish_factor(
            task_budget,
            session=session,
            status="completed",
            actual_shared_ledger_delta=actual,
        )
        _write_json(task_budget_path, task_budget)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--qfq-candidate-root", type=Path, required=True)
    parser.add_argument("--raw-candidate-root", type=Path, required=True)
    parser.add_argument("--authority-data-root", type=Path, required=True)
    parser.add_argument("--authority-session-root", type=Path, required=True)
    parser.add_argument("--expected-provider-control-root", type=Path, required=True)
    parser.add_argument("--expected-instruments-sha256", required=True)
    parser.add_argument("--baseline-dataset", type=Path, required=True)
    parser.add_argument("--baseline-dataset-sha256", required=True)
    parser.add_argument("--baseline-factor-ledger", type=Path, required=True)
    parser.add_argument("--baseline-factor-ledger-sha256", required=True)
    parser.add_argument("--shared-budget", type=Path, required=True)
    parser.add_argument("--task-budget", type=Path, required=True)
    parser.add_argument("--validation-receipt", type=Path, required=True)
    parser.add_argument("--cron-lock", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            execute(
                config_path=args.config,
                state_path=args.state,
                qfq_candidate_root=args.qfq_candidate_root,
                raw_candidate_root=args.raw_candidate_root,
                authority_data_root=args.authority_data_root,
                authority_session_root=args.authority_session_root,
                expected_provider_control_root=args.expected_provider_control_root,
                expected_instruments_sha256=args.expected_instruments_sha256,
                baseline_dataset_path=args.baseline_dataset,
                baseline_dataset_sha256=args.baseline_dataset_sha256,
                baseline_factor_ledger_path=args.baseline_factor_ledger,
                baseline_factor_ledger_sha256=args.baseline_factor_ledger_sha256,
                shared_budget_path=args.shared_budget,
                task_budget_path=args.task_budget,
                validation_receipt_path=args.validation_receipt,
                cron_lock=args.cron_lock,
                dry_run=args.dry_run,
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
