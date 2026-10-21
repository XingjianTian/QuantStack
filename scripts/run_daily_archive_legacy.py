"""Legacy cross-node archive runner; never used by the routine daily schedule."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from datetime import time as wall_time
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, ValidationError

from quant_stack.daily_cli import (
    FullMarketArchiveManifest,
    _assert_daily_release_gate,
    _assert_verified_session,
    _verified_calendar,
)
from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import _read_verified_archive

_CN_TIMEZONE = ZoneInfo("Asia/Shanghai")
_COMMIT = re.compile(r"^[a-f0-9]{40}$")


class DailySchedulerConfig(BaseModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    repo_root: Path
    python: Path
    work_root: Path
    calendar_raw: Path
    calendar_normalized: Path
    calendar_raw_uri: str
    calendar_normalized_uri: str
    calendar_source: str
    calendar_version: str
    hmac_key_file: Path
    environment_id: str
    source_commit: str | None = Field(default=None, pattern=r"^[a-f0-9]{40}$")
    execution_mode: Literal["remote_ssh", "local"] = "remote_ssh"
    remote_host: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    remote_staging_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_repo_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_python: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_data_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_hmac_key_file: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    catch_up_start_date: date | None = None
    batch_size: Literal[500] = 500
    min_batch_interval_seconds: Literal[5] = 5
    nas_verification_mode: Literal["full_replay", "readback_only"] = "full_replay"
    session_parquet_root: Path | None = None


def _run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 21_600,
    attempts: int = 1,
) -> subprocess.CompletedProcess[str]:
    failure: subprocess.SubprocessError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return subprocess.run(
                command,
                cwd=cwd,
                env=env,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.SubprocessError as exc:
            failure = exc
            if attempt < attempts:
                time.sleep(attempt * 2)
    assert failure is not None
    raise failure


def _ssh_command(config: DailySchedulerConfig, remote: str) -> list[str]:
    if config.execution_mode == "local":
        return ["/bin/sh", "-c", remote]
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=20",
        "-o",
        "ServerAliveInterval=10",
        "-o",
        "ServerAliveCountMax=2",
        config.remote_host,
        remote,
    ]


def _transfer_command(config: DailySchedulerConfig, *, archive: Path, report: Path) -> list[str]:
    command = [
        str(config.python),
        str(config.repo_root / "scripts/transfer_worker_bundle.py"),
        "--archive",
        str(archive),
    ]
    if config.execution_mode == "local":
        command.extend(["--local-staging-root", config.remote_staging_root])
    else:
        command.extend(
            [
                "--remote-host",
                config.remote_host,
                "--remote-staging-root",
                config.remote_staging_root,
            ]
        )
    command.extend(["--report", str(report)])
    return command


def _remote_preflight(config: DailySchedulerConfig) -> None:
    if config.session_parquet_root is not None:
        config.session_parquet_root.mkdir(parents=True, exist_ok=True)
        return
    if config.execution_mode == "local":
        Path(config.remote_staging_root).mkdir(parents=True, exist_ok=True)
        Path(config.remote_data_root).mkdir(parents=True, exist_ok=True)
    required = (
        config.remote_python,
        f"{config.remote_repo_root}/scripts/import_daily_archive_manifest.py",
        f"{config.remote_repo_root}/scripts/verify_daily_import.py",
        config.remote_hmac_key_file,
    )
    checks = " && ".join(f"test -r {shlex.quote(path)}" for path in required)
    checks += f" && test -x {shlex.quote(config.remote_python)}"
    checks += f" && test -w {shlex.quote(config.remote_staging_root)}"
    checks += f" && test -w {shlex.quote(config.remote_data_root)}"
    _run(_ssh_command(config, checks), cwd=config.repo_root, timeout=60, attempts=3)


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def _load_config(path: Path) -> DailySchedulerConfig:
    config = DailySchedulerConfig.model_validate_json(path.read_text(encoding="utf-8"))
    for required in (
        config.repo_root,
        config.python,
        config.calendar_raw,
        config.calendar_normalized,
        config.hmac_key_file,
    ):
        if not required.exists():
            raise FileNotFoundError(f"required scheduler path is unavailable: {required}")
    return config


def _source_commit(config: DailySchedulerConfig) -> str:
    if config.source_commit is not None:
        return config.source_commit
    dirty = _run(
        ["git", "status", "--porcelain"],
        cwd=config.repo_root,
    ).stdout.strip()
    if dirty:
        raise RuntimeError("daily scheduler requires a clean source checkout")
    commit = _run(["git", "rev-parse", "HEAD"], cwd=config.repo_root).stdout.strip()
    if not _COMMIT.fullmatch(commit):
        raise RuntimeError("daily scheduler could not resolve an exact source commit")
    return commit


def _calendar_preflight(
    config: DailySchedulerConfig,
    session_date: date,
    *,
    now: datetime | None = None,
) -> tuple[bool, str]:
    _assert_daily_release_gate(session_date, now=now)
    calendar, evidence = _verified_calendar(
        raw_path=config.calendar_raw,
        normalized_path=config.calendar_normalized,
        raw_uri=config.calendar_raw_uri,
        normalized_uri=config.calendar_normalized_uri,
        observed_at=now,
    )
    if evidence.source != config.calendar_source or evidence.version != config.calendar_version:
        raise ValueError("scheduler calendar identity does not match its configured evidence")
    try:
        _assert_verified_session(session_date=session_date, calendar=calendar)
    except ValueError as exc:
        return False, str(exc)
    return True, "verified complete trading session"


def _completed_session(config: DailySchedulerConfig, session_date: date) -> bool:
    summary_path = (
        config.work_root / f"daily-{session_date.strftime('%Y%m%d')}" / "runner-summary.json"
    )
    if not summary_path.is_file():
        return False
    try:
        return json.loads(summary_path.read_text(encoding="utf-8")).get("status") == "completed"
    except json.JSONDecodeError:
        return False


def _completed_acquisition(
    *,
    run_root: Path,
    session_date: date,
    manifest_path: Path,
    report_path: Path,
    hmac_key_file: Path,
) -> tuple[FullMarketArchiveManifest, str] | None:
    """Resume NAS delivery without repeating a completed Provider acquisition."""

    if not manifest_path.is_file() or not report_path.is_file():
        return None
    manifest = FullMarketArchiveManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if (
        manifest.session_date != session_date
        or report.get("session_date") != session_date.isoformat()
        or manifest.failed_batches != 0
        or manifest.succeeded_batches != manifest.planned_batches
        or report.get("succeeded_batches") != manifest.succeeded_batches
        or report.get("retryable_failed_batches") != 0
        or report.get("permanently_failed_batches") != 0
        or report.get("cancelled_batches") != 0
        or report.get("quarantined_batches") != 0
        or len(manifest.entries) != manifest.planned_batches
    ):
        raise RuntimeError("saved daily acquisition is not complete and resumable")
    archive_root = (run_root / "archives").resolve()
    archives: list[Path] = []
    for entry in manifest.entries:
        archive = (manifest_path.parent / entry.archive_relative_path).resolve()
        if archive.parent != archive_root or not archive.is_file():
            raise RuntimeError("saved daily acquisition archive is unavailable")
        archives.append(archive)
    envelope, _ = _read_verified_archive(archives[0], hmac_key_file.read_bytes())
    return manifest, envelope.source_commit


def _pending_sessions(
    config: DailySchedulerConfig,
    *,
    through_date: date,
    now: datetime,
) -> tuple[date, ...]:
    start = config.catch_up_start_date
    if start is None:
        return (through_date,)
    if through_date < start:
        return ()
    pending: list[date] = []
    current = start
    while current <= through_date:
        if current == now.date() and now.timetz().replace(tzinfo=None) < wall_time(18, 0):
            current += timedelta(days=1)
            continue
        eligible, _ = _calendar_preflight(config, current, now=now)
        if eligible and not _completed_session(config, current):
            pending.append(current)
        current += timedelta(days=1)
    return tuple(pending)


def _remote_import(
    *,
    config: DailySchedulerConfig,
    remote_manifest: str,
    session_token: str,
    suffix: str,
) -> dict[str, object]:
    report = f"{config.remote_staging_root}/reports/{session_token}-{suffix}.json"
    command = " ".join(
        shlex.quote(item)
        for item in (
            config.remote_python,
            f"{config.remote_repo_root}/scripts/import_daily_archive_manifest.py",
            "--manifest",
            remote_manifest,
            "--data-root",
            config.remote_data_root,
            "--hmac-key-file",
            config.remote_hmac_key_file,
            "--report",
            report,
        )
    )
    remote = f"mkdir -p {shlex.quote(config.remote_staging_root + '/reports')} && {command}"
    _run(_ssh_command(config, remote), cwd=config.repo_root, attempts=3)
    payload = _run(
        _ssh_command(config, f"cat {shlex.quote(report)}"),
        cwd=config.repo_root,
        attempts=3,
    ).stdout
    return json.loads(payload)


def _remote_verify(
    *,
    config: DailySchedulerConfig,
    remote_manifest: str,
    session_token: str,
) -> dict[str, object]:
    report = f"{config.remote_staging_root}/reports/{session_token}-readback.json"
    command = " ".join(
        shlex.quote(item)
        for item in (
            config.remote_python,
            f"{config.remote_repo_root}/scripts/verify_daily_import.py",
            "--manifest",
            remote_manifest,
            "--data-root",
            config.remote_data_root,
            "--report",
            report,
        )
    )
    _run(_ssh_command(config, command), cwd=config.repo_root, attempts=3)
    payload = _run(
        _ssh_command(config, f"cat {shlex.quote(report)}"),
        cwd=config.repo_root,
        attempts=3,
    ).stdout
    return json.loads(payload)


def _nas_delivery_is_valid(
    *,
    first: dict[str, object],
    replay: dict[str, object] | None,
    verification: dict[str, object],
    expected: int,
    expected_rows: int,
    source_commit: str,
    mode: Literal["full_replay", "readback_only"],
) -> bool:
    first_valid = (
        first.get("imported_archives") == expected
        and int(first.get("new_imports", 0)) + int(first.get("idempotent_replays", 0)) == expected
        and first.get("catalog_published") is False
        and first.get("production_pointer_updated") is False
    )
    readback_valid = (
        verification.get("verified_receipts") == expected
        and verification.get("persisted_bar_rows") == expected_rows
        and verification.get("duplicate_keys") == 0
        and verification.get("source_commit") == source_commit
        and verification.get("catalog_published") is False
        and verification.get("production_pointer_updated") is False
    )
    if mode == "readback_only":
        return first_valid and readback_valid and replay is None
    return (
        first_valid
        and readback_valid
        and replay is not None
        and replay.get("imported_archives") == expected
        and replay.get("idempotent_replays") == expected
    )


def run(config: DailySchedulerConfig, *, session_date, dry_run: bool = False) -> dict[str, object]:
    eligible, reason = _calendar_preflight(config, session_date)
    session_token = session_date.strftime("%Y%m%d")
    run_root = config.work_root / f"daily-{session_token}"
    summary_path = run_root / "runner-summary.json"
    if not eligible:
        payload = {
            "schema_version": "1.0.0",
            "status": "skipped",
            "session_date": session_date.isoformat(),
            "reason": reason,
            "provider_requested": False,
            "nas_written": False,
        }
        if not dry_run:
            _write_json(summary_path, payload)
        return payload
    commit = _source_commit(config)
    if dry_run:
        return {
            "schema_version": "1.0.0",
            "status": "dry_run",
            "session_date": session_date.isoformat(),
            "source_commit": commit,
            "batch_size": config.batch_size,
            "min_batch_interval_seconds": config.min_batch_interval_seconds,
            "remote_host_recorded": False,
            "canonical_eligible": False,
            "production_pointer_updated": False,
        }
    if summary_path.exists():
        previous = json.loads(summary_path.read_text(encoding="utf-8"))
        if previous.get("status") == "completed":
            return {**previous, "idempotent_scheduler_replay": True}

    _remote_preflight(config)

    run_root.mkdir(parents=True, exist_ok=True)
    snapshot = run_root / "universe-snapshot.json"
    instruments = run_root / "instruments.json"
    spec = run_root / "spec.json"
    plan = run_root / "plan.json"
    report = run_root / "run-report.json"
    archive_root = run_root / "archives"
    manifest_path = run_root / "archive-manifest.json"
    python = str(config.python)
    base = [python, "-m", "quant_stack.daily_cli"]
    live_env = os.environ.copy()
    live_env["QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS"] = str(
        config.min_batch_interval_seconds
    )
    completed_acquisition = (
        None
        if config.session_parquet_root is not None
        else _completed_acquisition(
            run_root=run_root,
            session_date=session_date,
            manifest_path=manifest_path,
            report_path=report,
            hmac_key_file=config.hmac_key_file,
        )
    )
    if completed_acquisition is None:
        _run(
            [
                *base,
                "build-live-universe",
                "--session",
                session_date.isoformat(),
                "--snapshot",
                str(snapshot),
                "--instruments",
                str(instruments),
                "--spec",
                str(spec),
                "--calendar-source",
                config.calendar_source,
                "--calendar-version",
                config.calendar_version,
                "--calendar-raw",
                str(config.calendar_raw),
                "--calendar-normalized",
                str(config.calendar_normalized),
                "--calendar-raw-uri",
                config.calendar_raw_uri,
                "--calendar-evidence-ref",
                config.calendar_normalized_uri,
                "--snapshot-evidence-ref",
                f"quant-data://evidence/universe/{session_date.isoformat()}.json",
                "--provider-control-root",
                str(config.work_root / "control"),
                "--batch-size",
                str(config.batch_size),
            ],
            cwd=config.repo_root,
            env=live_env,
        )
        _run([*base, "build-plan", str(spec), "--output", str(plan)], cwd=config.repo_root)
        _run(
            [
                *base,
                "run-live-full-market",
                str(plan),
                "--instruments",
                str(instruments),
                "--universe-snapshot",
                str(snapshot),
                "--calendar-raw",
                str(config.calendar_raw),
                "--calendar-normalized",
                str(config.calendar_normalized),
                "--calendar-raw-uri",
                config.calendar_raw_uri,
                "--calendar-normalized-uri",
                config.calendar_normalized_uri,
                "--data-root",
                str(run_root / "data"),
                "--provider-control-root",
                str(config.work_root / "control"),
                "--state-root",
                str(run_root / "state"),
                "--report",
                str(report),
                *(
                    ["--session-parquet-root", str(config.session_parquet_root)]
                    if config.session_parquet_root is not None
                    else [
                        "--archive-root",
                        str(archive_root),
                        "--archive-manifest",
                        str(manifest_path),
                        "--hmac-key-file",
                        str(config.hmac_key_file),
                    ]
                ),
                "--source-commit",
                commit,
                "--environment-id",
                config.environment_id,
            ],
            cwd=config.repo_root,
            env=live_env,
        )
        if config.session_parquet_root is not None:
            package = inspect_session_package(
                config.session_parquet_root / "market=CN" / f"session={session_date.isoformat()}"
            )
            summary = {
                "schema_version": "1.0.0",
                "status": "completed",
                "session_date": session_date.isoformat(),
                "source_commit": commit,
                "records_received": package.row_count,
                "distinct_instruments": package.instrument_count,
                "omissions": package.omission_count,
                "duplicate_keys": 0,
                "nas_verification_mode": "session_readback_only",
                "nas_idempotent_replay_deferred": True,
                "archives_created": 0,
                "catalog_published": False,
                "production_pointer_updated": False,
            }
            _write_json(summary_path, summary)
            return summary
        manifest = FullMarketArchiveManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        acquisition_commit = commit
    else:
        manifest, acquisition_commit = completed_acquisition
    remote_payload = manifest.model_dump(mode="json")
    for entry in remote_payload["entries"]:
        local_archive = (manifest_path.parent / entry["archive_relative_path"]).resolve()
        _run(
            _transfer_command(
                config,
                archive=local_archive,
                report=run_root / f"transfer-{entry['archive_sha256']}.json",
            ),
            cwd=config.repo_root,
        )
        entry["archive_relative_path"] = f"{entry['archive_sha256']}.tar"
    remote_manifest_local = run_root / "remote-archive-manifest.json"
    _write_json(remote_manifest_local, remote_payload)
    remote_manifest_sha = hashlib.sha256(remote_manifest_local.read_bytes()).hexdigest()
    _run(
        _transfer_command(
            config,
            archive=remote_manifest_local,
            report=run_root / "transfer-manifest.json",
        ),
        cwd=config.repo_root,
    )
    remote_manifest = f"{config.remote_staging_root}/inbox/{remote_manifest_sha}.tar"
    first = _remote_import(
        config=config,
        remote_manifest=remote_manifest,
        session_token=session_token,
        suffix="first-import",
    )
    second = None
    if config.nas_verification_mode == "full_replay":
        second = _remote_import(
            config=config,
            remote_manifest=remote_manifest,
            session_token=session_token,
            suffix="idempotent-replay",
        )
    verification = _remote_verify(
        config=config,
        remote_manifest=remote_manifest,
        session_token=session_token,
    )
    expected = len(manifest.entries)
    if not _nas_delivery_is_valid(
        first=first,
        replay=second,
        verification=verification,
        expected=expected,
        expected_rows=manifest.records_received,
        source_commit=acquisition_commit,
        mode=config.nas_verification_mode,
    ):
        raise RuntimeError("NAS daily import or idempotent replay verification failed")
    summary = {
        "schema_version": "1.0.0",
        "status": "completed",
        "session_date": session_date.isoformat(),
        "source_commit": acquisition_commit,
        "planned_batches": manifest.planned_batches,
        "succeeded_batches": manifest.succeeded_batches,
        "records_received": manifest.records_received,
        "duplicate_keys": manifest.duplicate_keys,
        "nas_new_imports": first["new_imports"],
        "nas_recovered_imports": first["idempotent_replays"],
        "nas_verification_mode": config.nas_verification_mode,
        "nas_idempotent_replays": (int(second["idempotent_replays"]) if second is not None else 0),
        "nas_idempotent_replay_deferred": second is None,
        "nas_verified_receipts": verification["verified_receipts"],
        "nas_verified_artifacts": verification["verified_artifacts"],
        "nas_persisted_bar_rows": verification["persisted_bar_rows"],
        "nas_distinct_instruments": verification["distinct_instruments"],
        "remote_host_recorded": False,
        "canonical_eligible": False,
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    _write_json(summary_path, summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one fail-closed QuantStack daily increment")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session", type=datetime.fromisoformat)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        config = _load_config(args.config)
        if args.session is not None:
            result = run(config, session_date=args.session.date(), dry_run=args.dry_run)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            return 0
        now = datetime.now(_CN_TIMEZONE)
        sessions = _pending_sessions(config, through_date=now.date(), now=now)
        results = [run(config, session_date=item, dry_run=args.dry_run) for item in sessions]
        print(
            json.dumps(
                {
                    "schema_version": "1.0.0",
                    "status": "completed",
                    "sessions_planned": [item.isoformat() for item in sessions],
                    "sessions_completed": len(results),
                    "results": results,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        ValidationError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
