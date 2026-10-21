#!/usr/bin/env python3
"""Build one bounded CN raw recovery candidate without touching authority."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import inspect
import json
import os
import re
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo

import duckdb

from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.run_daily_incremental import (
    DailySchedulerConfig,
    _load_config,
    run,
)

TARGET_SESSIONS = frozenset(
    {
        date(2026, 9, 23),
        date(2026, 9, 24),
        date(2026, 9, 28),
        date(2026, 9, 29),
    }
)
DEFAULT_SESSION_BUDGET = 11_000
MAX_SESSION_BUDGET = 11_000
TASK_BUDGET_LIMIT = 44_000
_COMMIT = re.compile(r"^[a-f0-9]{40}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_IMPORTED_RUN_SOURCE = Path(inspect.getfile(run)).resolve()
_SOURCE_TREE_IGNORES = frozenset({
    ".git", ".pytest_cache", ".ruff_cache", "__pycache__", ".venv",
    ".local-data", ".ai-bridge", "node_modules", "secrets", "credentials", "generated",
})


def _private_source_path(path: PurePosixPath) -> bool:
    return (
        any(part in _SOURCE_TREE_IGNORES for part in path.parts)
        or path.name == ".env"
        or path.name.startswith(".env.") and not path.name.endswith(".example")
        or path.name.endswith((".local.json", ".local.yaml", ".local.yml", ".pem", ".key"))
        or ".private." in path.name
        or path.name.startswith("._")
    )


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


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_candidate_root(candidate_root: Path, production: DailySchedulerConfig) -> Path:
    candidate = candidate_root.expanduser().resolve(strict=False)
    if candidate_root.is_symlink():
        raise ValueError("candidate root must not be a symlink")
    protected = (
        production.work_root.expanduser().resolve(strict=False),
        production.session_parquet_root.expanduser().resolve(strict=False),
    )
    if any(
        candidate == item
        or _is_relative_to(candidate, item)
        or _is_relative_to(item, candidate)
        for item in protected
    ):
        raise ValueError("candidate root must be outside production work and session roots")
    if candidate == Path("/"):
        raise ValueError("candidate root must be bounded")
    return candidate


def _validate_candidate_layout(candidate: Path, session: date) -> None:
    session_text = session.isoformat()
    relative_paths = (
        "work",
        f"work/daily-{session:%Y%m%d}",
        f"work/daily-{session:%Y%m%d}/temporary-data",
        f"work/daily-{session:%Y%m%d}/state",
        f"work/daily-{session:%Y%m%d}/provider-cache",
        "session-parquet",
        "session-parquet/market=CN",
        f"session-parquet/market=CN/session={session_text}",
        "control",
        "evidence",
    )
    for relative in relative_paths:
        path = candidate / relative
        if path.is_symlink():
            raise ValueError(f"candidate output path must not be a symlink: {relative}")
        resolved = path.resolve(strict=False)
        if not _is_relative_to(resolved, candidate):
            raise ValueError(f"candidate output path escapes candidate root: {relative}")


def _validate_runtime_source_root(candidate_repo_root: Path) -> Path:
    if candidate_repo_root.is_symlink():
        raise ValueError("candidate repo root must not be a symlink")
    repo_root = candidate_repo_root.expanduser().resolve()
    wrapper_root = Path(__file__).resolve().parents[1]
    if repo_root != wrapper_root:
        raise ValueError("candidate repo root must be the running wrapper source root")
    expected_runner = repo_root / "scripts/run_daily_incremental.py"
    if _IMPORTED_RUN_SOURCE != expected_runner.resolve() or not expected_runner.is_file():
        raise ValueError("imported daily runner must come from the candidate repo root")
    return repo_root


def _verify_source_archive(
    source_archive: Path,
    *,
    expected_sha256: str,
    source_commit: str,
    candidate_repo_root: Path,
) -> dict[str, object]:
    if not _COMMIT.fullmatch(source_commit):
        raise ValueError("source commit must be an exact 40-character lowercase SHA")
    archive = source_archive.expanduser().resolve()
    if not archive.is_file() or archive.is_symlink():
        raise ValueError("source archive must be one regular file")
    if not _SHA256.fullmatch(expected_sha256):
        raise ValueError("source archive SHA-256 must be 64 lowercase hexadecimal characters")
    observed = _sha256(archive)
    if observed != expected_sha256:
        raise ValueError("source archive SHA-256 mismatch")
    if candidate_repo_root.is_symlink():
        raise ValueError("candidate repo root must not be a symlink")
    repo_root = candidate_repo_root.expanduser().resolve()
    if not repo_root.is_dir():
        raise ValueError("candidate repo root must be an extracted source directory")

    archive_files: dict[str, tuple[str, bool]] = {}
    tree_digest = hashlib.sha256()
    try:
        with tarfile.open(archive, "r:*") as source:
            archive_commit = source.pax_headers.get("comment")
            if archive_commit != source_commit:
                raise ValueError("source archive commit does not match source_commit")
            for member in source.getmembers():
                pure = PurePosixPath(member.name)
                if pure.is_absolute() or not pure.parts or ".." in pure.parts:
                    raise ValueError("source archive contains an unsafe path")
                if _private_source_path(pure):
                    raise ValueError(
                        "source archive contains private configuration or runtime files"
                    )
                relative = pure.as_posix().removeprefix("./")
                if not relative or relative in archive_files:
                    raise ValueError("source archive contains a duplicate or empty path")
                target = repo_root.joinpath(*PurePosixPath(relative).parts)
                if member.isdir():
                    if not target.is_dir() or target.is_symlink():
                        raise ValueError("candidate source directory does not match archive")
                    continue
                if not member.isfile():
                    raise ValueError(
                        "source archive may contain only regular files and directories"
                    )
                if (
                    not target.is_file()
                    or target.is_symlink()
                    or not _is_relative_to(target.resolve(), repo_root)
                ):
                    raise ValueError("candidate source file does not match archive")
                stream = source.extractfile(member)
                if stream is None:
                    raise ValueError("source archive regular file cannot be read")
                archive_hash = hashlib.sha256()
                while chunk := stream.read(1024 * 1024):
                    archive_hash.update(chunk)
                file_sha256 = archive_hash.hexdigest()
                if _sha256(target) != file_sha256:
                    raise ValueError("candidate source file content does not match archive")
                executable = bool(member.mode & 0o111)
                if bool(target.stat().st_mode & 0o111) != executable:
                    raise ValueError("candidate source executable mode does not match archive")
                archive_files[relative] = (file_sha256, executable)
    except tarfile.TarError as exc:
        raise ValueError("source archive is not a readable git archive") from exc

    actual_files: set[str] = set()
    for path in repo_root.rglob("*"):
        relative_path = path.relative_to(repo_root)
        if _private_source_path(PurePosixPath(relative_path.as_posix())):
            continue
        if path.is_symlink():
            raise ValueError("candidate source tree must not contain symlinks")
        if path.is_file():
            actual_files.add(relative_path.as_posix())
        elif not path.is_dir():
            raise ValueError("candidate source tree contains a special file")
    if actual_files != set(archive_files):
        raise ValueError("candidate source tree file set does not match archive")
    for relative in sorted(archive_files):
        file_sha256, executable = archive_files[relative]
        tree_digest.update(relative.encode())
        tree_digest.update(b"\0x\0" if executable else b"\0-\0")
        tree_digest.update(file_sha256.encode())
        tree_digest.update(b"\n")
    return {
        "schema_version": "quantstack.r12-source-receipt.v1",
        "source_commit": source_commit,
        "archive_path": str(archive),
        "archive_sha256": observed,
        "archive_type": "git_archive",
        "archive_commit": source_commit,
        "candidate_repo_root": str(repo_root),
        "source_tree_sha256": tree_digest.hexdigest(),
        "source_file_count": len(archive_files),
        "verified_at": datetime.now(UTC).isoformat(),
    }


def _budget_snapshot(control_root: Path) -> dict[str, object]:
    path = control_root / "baostock-request-budget.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "1.0.0":
        raise ValueError("unsupported BaoStock budget ledger")
    return payload


def _budget_preflight(control_root: Path, planned_requests: int) -> dict[str, object]:
    if not 1 <= planned_requests <= MAX_SESSION_BUDGET:
        raise ValueError(f"planned request budget must be from 1 to {MAX_SESSION_BUDGET}")
    payload = _budget_snapshot(control_root)
    timezone = str(payload.get("timezone", "Asia/Shanghai"))
    today = datetime.now(ZoneInfo(timezone)).date().isoformat()
    day = dict(payload.get("days", {})).get(today, {})
    used = int(day.get("used", 0)) if isinstance(day, dict) else 0
    soft_limit = int(payload.get("daily_limit", 49_000))
    if payload.get("blocked"):
        raise RuntimeError("BaoStock budget ledger is blocked")
    if used + planned_requests > soft_limit:
        raise RuntimeError(
            "BaoStock recovery budget preflight failed: "
            f"used={used} planned={planned_requests} limit={soft_limit}"
        )
    return {
        "ledger_path": str(control_root / "baostock-request-budget.json"),
        "day": today,
        "used_before": used,
        "planned_requests": planned_requests,
        "soft_limit": soft_limit,
        "hard_limit": int(payload.get("hard_daily_limit", 50_000)),
        "timezone": timezone,
    }


def _task_budget_path(candidate_root: Path) -> Path:
    return candidate_root / "control/r12-provider-budget.json"


def _reserve_task_budget(
    candidate_root: Path, *, session: date, planned_requests: int
) -> dict[str, object]:
    path = _task_budget_path(candidate_root)
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != "quantstack.r12-provider-budget.v1":
            raise ValueError("unsupported R12 Provider budget ledger")
    else:
        payload = {
            "schema_version": "quantstack.r12-provider-budget.v1",
            "total_limit": TASK_BUDGET_LIMIT,
            "allocated": 0,
            "reservations": {},
        }
    reservations = payload.get("reservations")
    if not isinstance(reservations, dict):
        raise ValueError("R12 Provider budget reservations are malformed")
    key = session.isoformat()
    if key in reservations:
        raise RuntimeError(f"R12 Provider budget is already reserved for {key}")
    allocated = int(payload.get("allocated", 0))
    total_limit = int(payload.get("total_limit", TASK_BUDGET_LIMIT))
    if total_limit != TASK_BUDGET_LIMIT or allocated + planned_requests > total_limit:
        raise RuntimeError(
            "R12 Provider task budget exhausted: "
            f"allocated={allocated} planned={planned_requests} limit={total_limit}"
        )
    reservation = {
        "status": "reserved",
        "allocated_requests": planned_requests,
        "reserved_at": datetime.now(UTC).isoformat(),
        "actual_shared_ledger_delta": None,
    }
    reservations[key] = reservation
    payload["allocated"] = allocated + planned_requests
    _write_json(path, payload)
    return {
        "ledger_path": str(path),
        "total_limit": total_limit,
        "allocated_before": allocated,
        "allocated_after": allocated + planned_requests,
        "reservation": reservation,
    }


def _finish_task_budget(
    candidate_root: Path,
    *,
    session: date,
    status: str,
    actual_shared_ledger_delta: int | None,
) -> None:
    path = _task_budget_path(candidate_root)
    payload = json.loads(path.read_text(encoding="utf-8"))
    reservations = payload["reservations"]
    reservation = reservations[session.isoformat()]
    reservation.update(
        {
            "status": status,
            "actual_shared_ledger_delta": actual_shared_ledger_delta,
            "finished_at": datetime.now(UTC).isoformat(),
        }
    )
    _write_json(path, payload)


def _ledger_used_by_day(payload: dict[str, object]) -> dict[str, int]:
    days = payload.get("days", {})
    if not isinstance(days, dict):
        raise ValueError("BaoStock budget ledger days are malformed")
    return {
        str(day): int(record.get("used", 0))
        for day, record in days.items()
        if isinstance(record, dict)
    }


def _ledger_delta(before: dict[str, object], after: dict[str, object]) -> int:
    before_days = _ledger_used_by_day(before)
    after_days = _ledger_used_by_day(after)
    return sum(
        max(used - before_days.get(day, 0), 0) for day, used in after_days.items()
    )


def _identity_digest(identities: set[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(identities)) + "\n").encode()).hexdigest()


def _object_partition(run_root: Path, package_root: Path) -> dict[str, object]:
    instruments_payload = json.loads((run_root / "instruments.json").read_text(encoding="utf-8"))
    instrument_rows = instruments_payload.get("instruments")
    if not isinstance(instrument_rows, list):
        raise ValueError("candidate instruments evidence is malformed")
    requested = {
        str(item["instrument_id"])
        for item in instrument_rows
        if isinstance(item, dict) and item.get("instrument_id")
    }
    if len(requested) != len(instrument_rows):
        raise ValueError("candidate instruments evidence has duplicate or missing identities")
    connection = duckdb.connect()
    try:
        returned = {
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT instrument_id FROM read_parquet(?, hive_partitioning=false)",
                [str(package_root / "bars.parquet")],
            ).fetchall()
        }
    finally:
        connection.close()
    manifest = json.loads((package_root / "manifest.json").read_text(encoding="utf-8"))
    omissions = {str(item) for item in manifest.get("omissions", [])}
    overlap = returned.intersection(omissions)
    unexpected = returned.union(omissions).difference(requested)
    unresolved = requested.difference(returned).difference(omissions)
    if overlap or unexpected or unresolved:
        raise ValueError(
            "candidate U/R/O partition does not close: "
            f"overlap={len(overlap)} unexpected={len(unexpected)} unresolved={len(unresolved)}"
        )
    return {
        "u_count": len(requested),
        "u_sha256": _identity_digest(requested),
        "returned_count": len(returned),
        "returned_sha256": _identity_digest(returned),
        "omission_count": len(omissions),
        "omission_sha256": _identity_digest(omissions),
        "returned_omission_overlap": 0,
        "union_equals_u": True,
        "omission_semantics": (
            "none" if not omissions else "unverified_omission"
        ),
        "raw_complete": not omissions,
    }


@contextmanager
def _shared_cron_lock(path: Path) -> Iterator[None]:
    if not path.is_file() or path.is_symlink():
        raise FileNotFoundError(f"shared cron lock does not exist: {path}")
    with path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("shared CN daily lock is already held") from exc
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _candidate_config(
    production: DailySchedulerConfig,
    *,
    candidate_root: Path,
    provider_control_root: Path,
    candidate_repo_root: Path,
    source_commit: str,
) -> DailySchedulerConfig:
    repo_root = candidate_repo_root.expanduser().resolve()
    if not (repo_root / "src/quant_stack").is_dir() or not (
        repo_root / "scripts/run_daily_incremental.py"
    ).is_file():
        raise ValueError("candidate repo root is not a QuantStack source snapshot")
    if not _COMMIT.fullmatch(source_commit):
        raise ValueError("source commit must be an exact 40-character lowercase SHA")
    return production.model_copy(
        update={
            "repo_root": repo_root,
            "source_commit": source_commit,
            "work_root": candidate_root / "work",
            "session_parquet_root": candidate_root / "session-parquet",
            "provider_control_root": provider_control_root,
            "reauthentication_replay": False,
            "catch_up_start_date": None,
            "environment_id": f"{production.environment_id}-r12-candidate",
        }
    )


def execute(
    *,
    production_config_path: Path,
    candidate_root: Path,
    candidate_repo_root: Path,
    source_commit: str,
    source_archive: Path,
    source_archive_sha256: str,
    cron_lock: Path,
    session: date,
    planned_requests: int = DEFAULT_SESSION_BUDGET,
    dry_run: bool = False,
) -> dict[str, object]:
    if session not in TARGET_SESSIONS:
        raise ValueError(f"session is outside the frozen R12 CN set: {session}")
    production = _load_config(production_config_path.resolve())
    candidate = _validate_candidate_root(candidate_root, production)
    _validate_candidate_layout(candidate, session)
    runtime_repo_root = _validate_runtime_source_root(candidate_repo_root)
    source_receipt = _verify_source_archive(
        source_archive,
        expected_sha256=source_archive_sha256,
        source_commit=source_commit,
        candidate_repo_root=runtime_repo_root,
    )
    control_root = (
        production.provider_control_root or production.work_root / "control"
    ).expanduser().resolve()
    expected_cron_lock = control_root.parent / "cron.lock"
    if (
        cron_lock.is_symlink()
        or cron_lock.expanduser().resolve() != expected_cron_lock
        or not expected_cron_lock.is_file()
        or expected_cron_lock.is_symlink()
    ):
        raise ValueError("cron lock must be the existing shared CN daily lock")
    config = _candidate_config(
        production,
        candidate_root=candidate,
        provider_control_root=control_root,
        candidate_repo_root=runtime_repo_root,
        source_commit=source_commit,
    )
    preview = run(config, session_date=session, dry_run=True)
    if dry_run:
        budget_before = _budget_preflight(control_root, planned_requests)
        return {
            "schema_version": "quantstack.r12-cn-recovery-candidate.v1",
            "status": "dry_run",
            "session": session.isoformat(),
            "candidate_root": str(candidate),
            "authoritative_session_root": str(production.session_parquet_root),
            "provider_control_root": str(control_root),
            "candidate_repo_root": str(config.repo_root),
            "source_commit": config.source_commit,
            "source_receipt": source_receipt,
            "budget": budget_before,
            "runner_preview": preview,
            "provider_requested": False,
            "authority_written": False,
        }

    with _shared_cron_lock(expected_cron_lock):
        _validate_candidate_layout(candidate, session)
        candidate.mkdir(parents=True, exist_ok=True)
        config_path = candidate / "control/candidate-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(config_path, config.model_dump(mode="json"))
        _write_json(candidate / "source-receipt.json", source_receipt)
        attempt_path = candidate / "control/last-attempt.json"
        budget_before = _budget_preflight(control_root, planned_requests)
        shared_budget_before = _budget_snapshot(control_root)
        task_budget = _reserve_task_budget(
            candidate, session=session, planned_requests=planned_requests
        )
        started_at = datetime.now(UTC)
        try:
            result = run(config, session_date=session)
        except Exception as exc:
            shared_budget_after = _budget_snapshot(control_root)
            actual_delta = _ledger_delta(shared_budget_before, shared_budget_after)
            _finish_task_budget(
                candidate,
                session=session,
                status="failed",
                actual_shared_ledger_delta=actual_delta,
            )
            _write_json(
                attempt_path,
                {
                    "schema_version": "quantstack.r12-cn-recovery-attempt.v1",
                    "status": "failed",
                    "session": session.isoformat(),
                    "started_at": started_at.isoformat(),
                    "finished_at": datetime.now(UTC).isoformat(),
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:1000],
                    "provider_retries": 0,
                    "task_budget": task_budget,
                    "actual_shared_ledger_delta": actual_delta,
                },
            )
            raise
        budget_after_payload = _budget_snapshot(control_root)
        actual_delta = _ledger_delta(shared_budget_before, budget_after_payload)
        budget_status = (
            "completed" if actual_delta <= planned_requests else "completed_budget_exceeded"
        )
        _finish_task_budget(
            candidate,
            session=session,
            status=budget_status,
            actual_shared_ledger_delta=actual_delta,
        )
    if result.get("status") != "completed" or result.get("session_date") != session.isoformat():
        raise RuntimeError(f"candidate runner did not complete expected session: {result}")
    _validate_candidate_layout(candidate, session)
    package = inspect_session_package(
        config.session_parquet_root / "market=CN" / f"session={session.isoformat()}"
    )
    run_root = config.work_root / f"daily-{session:%Y%m%d}"
    evidence = {}
    for name in ("universe-snapshot.json", "instruments.json", "spec.json", "plan.json"):
        path = run_root / name
        if not path.is_file():
            raise RuntimeError(f"candidate is missing frozen universe evidence: {name}")
        evidence[name] = {"path": str(path), "sha256": _sha256(path), "size": path.stat().st_size}
    object_partition = _object_partition(run_root, package.path)
    summary = {
        "schema_version": "quantstack.r12-cn-recovery-candidate.v1",
        "status": "completed" if actual_delta <= planned_requests else "budget_exceeded",
        "session": session.isoformat(),
        "candidate_root": str(candidate),
        "authoritative_session_root": str(production.session_parquet_root),
        "provider_control_root": str(control_root),
        "candidate_repo_root": str(config.repo_root),
        "source_commit": config.source_commit,
        "source_receipt": source_receipt,
        "provider_retries": 0,
        "budget": {
            **budget_before,
            "task": task_budget,
            "actual_shared_ledger_delta": actual_delta,
        },
        "package": {
            "path": str(package.path),
            "rows": package.row_count,
            "instruments": package.instrument_count,
            "omissions": package.omission_count,
        },
        "universe_evidence": evidence,
        "object_partition": object_partition,
        "authority_written": False,
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    _write_json(candidate / f"evidence/cn-session-{session.isoformat()}.json", summary)
    _write_json(attempt_path, {**summary, "finished_at": datetime.now(UTC).isoformat()})
    if actual_delta > planned_requests:
        raise RuntimeError(
            "CN recovery exceeded its reserved request budget; stop before the next session: "
            f"actual={actual_delta} reserved={planned_requests}"
        )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-config", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--candidate-repo-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--cron-lock", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--planned-requests", type=int, default=DEFAULT_SESSION_BUDGET)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        print(
            json.dumps(
                execute(
                    production_config_path=args.production_config,
                    candidate_root=args.candidate_root,
                    candidate_repo_root=args.candidate_repo_root,
                    source_commit=args.source_commit,
                    source_archive=args.source_archive,
                    source_archive_sha256=args.source_archive_sha256,
                    cron_lock=args.cron_lock,
                    session=args.session,
                    planned_requests=args.planned_requests,
                    dry_run=args.dry_run,
                ),
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps({"status": "failed", "error_type": type(exc).__name__, "error": str(exc)}),
            file=os.sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
