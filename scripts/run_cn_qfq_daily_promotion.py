#!/usr/bin/env python3
"""Advance completed CN daily raw sessions through qfq, Catalog, and Stock Screener."""

from __future__ import annotations

import argparse
import fcntl
import json
import shlex
import subprocess
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, Field

from quant_stack.data.cn_operations_audit import _bars_profile, _factor_profile
from quant_stack.data.storage.atomic import atomic_replace_bytes


class PromotionConfig(BaseModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    repo_root: Path
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    python: Path
    work_root: Path
    daily_work_root: Path
    calendar_normalized: Path
    hmac_key_file: Path
    provider_control_root: Path
    factor_start_date: date
    execution_mode: Literal["remote_ssh", "local"] = "remote_ssh"
    remote_host: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    remote_staging_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_repo_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_python: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_import_data_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_hmac_key_file: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_central_data_root: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_raw_baseline_path: str = Field(pattern=r"^/[A-Za-z0-9_./=-]+$")
    remote_calendar_normalized: str = Field(pattern=r"^/[A-Za-z0-9_./=-]+$")
    remote_ss_checkout: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$")
    remote_read_gid: int = 997
    remote_publisher_image: str = "personal/quant-stack-api:cpdw-t098-eddafaf-audit"
    remote_docker_cli_image: str = "uusec/openresty-manager:latest"
    quant_base_url: str
    ss_base_url: str
    operations_audit_runtime_evidence: Path | None = None


class PromotionState(BaseModel):
    schema_version: Literal["quantstack.cn-qfq-daily-promotion-state.v1"]
    watermark: date
    raw_path: Path
    qfq_path: Path | None = None
    remote_qfq_path: str | None = None
    materialization_mode: Literal["worker_wan", "nas_local"] = "worker_wan"
    factor_path: Path | None = None
    dataset_version: str
    row_count: int
    instrument_count: int


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n"
        ).encode(),
    )


def _run(command: list[str], *, cwd: Path, timeout: int = 21_600) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    ).stdout


def _ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600) -> str:
    if config.execution_mode == "local":
        return _run(command, cwd=config.repo_root, timeout=timeout)
    remote = " ".join(shlex.quote(item) for item in command)
    return _run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=20",
            "-o",
            "ServerAliveInterval=10",
            "-o",
            "ServerAliveCountMax=6",
            config.remote_host,
            remote,
        ],
        cwd=config.repo_root,
        timeout=timeout,
    )


def _remote_python(config: PromotionConfig, script: str, *args: str) -> list[str]:
    return [
        "/usr/bin/env",
        f"PYTHONPATH={config.remote_repo_root}/src:{config.remote_repo_root}",
        config.remote_python,
        f"{config.remote_repo_root}/scripts/{script}",
        *args,
    ]


def _transfer_command(config: PromotionConfig, *, archive: Path, report: Path) -> list[str]:
    command = [
        str(config.python),
        "scripts/transfer_worker_bundle.py",
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


def _remote_publisher_python(
    config: PromotionConfig, script: str, *args: str, network_host: bool = False
) -> list[str]:
    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        "0:0",
    ]
    if network_host:
        command.extend(["--network", "host"])
    mount_paths = dict.fromkeys(
        (
            config.remote_repo_root,
            config.remote_import_data_root,
            config.remote_central_data_root,
            config.remote_ss_checkout,
        )
    )
    for path in mount_paths:
        command.extend(["-v", f"{path}:{path}"])
    return [
        *command,
        config.remote_publisher_image,
        "/usr/bin/env",
        f"PYTHONPATH={config.remote_repo_root}/src:{config.remote_repo_root}",
        "/app/.venv/bin/python",
        f"{config.remote_repo_root}/scripts/{script}",
        *args,
    ]


@contextmanager
def _single_flight(work_root: Path):
    lock_path = work_root / "control/qfq-daily-promotion.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another qfq daily promotion is already running") from exc
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _session_dates(calendar_path: Path) -> list[date]:
    payload = json.loads(calendar_path.read_text(encoding="utf-8"))
    return [
        date.fromisoformat(item["trading_date"])
        for item in payload["days"]
        if item["state"] == "session"
    ]


def next_completed_session(
    *, state: PromotionState, daily_work_root: Path, calendar_normalized: Path
) -> date | None:
    later = [item for item in _session_dates(calendar_normalized) if item > state.watermark]
    if not later:
        return None
    expected = later[0]
    summary = daily_work_root / f"daily-{expected:%Y%m%d}" / "runner-summary.json"
    if not summary.is_file():
        later_completed = [
            item
            for item in later[1:]
            if (daily_work_root / f"daily-{item:%Y%m%d}" / "runner-summary.json").is_file()
        ]
        if later_completed:
            raise RuntimeError(f"daily raw session {expected} is missing before a later session")
        return None
    payload = json.loads(summary.read_text(encoding="utf-8"))
    if payload.get("status") != "completed" or payload.get("session_date") != expected.isoformat():
        raise ValueError(f"daily raw summary is not complete for {expected}")
    if payload.get("duplicate_keys") != 0 or payload.get("catalog_published") is not False:
        raise ValueError(f"daily raw summary failed isolation checks for {expected}")
    return expected


def _daily_summary(daily_work_root: Path, session: date) -> dict[str, object]:
    path = daily_work_root / f"daily-{session:%Y%m%d}" / "runner-summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("status") != "completed" or payload.get("session_date") != session.isoformat():
        raise ValueError(f"daily raw summary is not complete for {session}")
    return payload


def _validate_readback_only_operations_audit(
    *,
    audit: dict[str, object],
    daily_summary: dict[str, object],
    candidate: PromotionState,
    session: date,
) -> None:
    token = session.isoformat()
    audit_daily = audit.get("daily")
    audit_promotions = audit.get("promotions")
    if not isinstance(audit_daily, dict) or not isinstance(audit_promotions, dict):
        raise ValueError("readback-only operations audit has no daily or promotion evidence")
    daily_evidence = audit_daily.get(token)
    promotion_evidence = audit_promotions.get(token)
    if not isinstance(daily_evidence, dict) or not isinstance(promotion_evidence, dict):
        raise ValueError("readback-only operations audit is missing matching session evidence")
    if (
        audit.get("status") != "passed"
        or audit.get("alerts") != []
        or audit.get("through_date") != token
        or audit.get("expected_sessions") != [token]
        or daily_evidence.get("session_date") != token
        or daily_evidence.get("source_commit") != daily_summary.get("source_commit")
        or daily_evidence.get("nas_verification_mode") != "readback_only"
        or promotion_evidence.get("session_date") != token
        or promotion_evidence.get("dataset_version") != candidate.dataset_version
    ):
        raise ValueError("readback-only operations audit did not match the promotion candidate")


def _run_readback_only_operations_audit(
    config: PromotionConfig,
    *,
    daily_summary: dict[str, object],
    candidate: PromotionState,
    session: date,
) -> None:
    runtime_evidence = config.operations_audit_runtime_evidence
    if runtime_evidence is None:
        raise RuntimeError("readback-only promotion requires operations audit runtime evidence")
    run_root = config.work_root / f"qfq-{session:%Y%m%d}"
    candidate_state = run_root / "operations-audit-candidate-state.json"
    report = run_root / "operations-audit.json"
    _write_json(candidate_state, candidate.model_dump(mode="json"))
    _run(
        [
            str(config.python),
            "scripts/audit_cn_operations.py",
            "--work-root",
            str(config.daily_work_root),
            "--expected-session",
            session.isoformat(),
            "--promotion-state",
            str(candidate_state),
            "--promotion-work-root",
            str(config.work_root),
            "--runtime-evidence",
            str(runtime_evidence),
            "--through",
            session.isoformat(),
            "--output",
            str(report),
        ],
        cwd=config.repo_root,
    )
    audit = json.loads(report.read_text(encoding="utf-8"))
    _validate_readback_only_operations_audit(
        audit=audit,
        daily_summary=daily_summary,
        candidate=candidate,
        session=session,
    )


def _promote_with_readback_audit_gate(
    config: PromotionConfig, state: PromotionState, session: date
) -> PromotionState:
    daily_summary = _daily_summary(config.daily_work_root, session)
    candidate = promote_one(config, state, session)
    if daily_summary.get("nas_verification_mode", "full_replay") == "readback_only":
        _run_readback_only_operations_audit(
            config,
            daily_summary=daily_summary,
            candidate=candidate,
            session=session,
        )
    return candidate


def _profile(path: Path) -> tuple[int, int]:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*), count(DISTINCT instrument_id) FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise ValueError("qfq profile returned no result")
    return int(row[0]), int(row[1])


def _promotion_evidence(
    *,
    session: date,
    dataset_version: str,
    raw_path: Path,
    qfq_path: Path | None,
    factor_path: Path,
    plan_path: Path,
    first_import: dict[str, object],
    replay_import: dict[str, object],
    first_promotion: dict[str, object],
    replay_promotion: dict[str, object],
    smoke: dict[str, object],
    daily_scan: dict[str, object],
    materialization: dict[str, object] | None = None,
) -> dict[str, object]:
    if materialization is None:
        if qfq_path is None:
            raise ValueError("worker-WAN promotion evidence requires a local qfq path")
        factors = _factor_profile(factor_path)
        factors.pop("instrument_ids", None)
        raw = _bars_profile(raw_path)
        qfq = _bars_profile(qfq_path)
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    else:
        factors = dict(materialization["factors"])
        raw = dict(materialization["raw"])
        qfq = dict(materialization["qfq"])
        plan = dict(materialization["plan"])
    import_evidence = {
        "bundle_id": first_import.get("bundle_id"),
        "first_idempotent_replay": first_import.get("import_idempotent_replay"),
        "replay_idempotent_replay": replay_import.get("import_idempotent_replay"),
        "import_resumed": first_import.get("import_resumed", False),
        "catalog_published_during_import": bool(
            first_import.get("catalog_published") or replay_import.get("catalog_published")
        ),
    }
    catalog_fields = (
        "status",
        "idempotent_replay",
        "dataset_id",
        "dataset_version",
        "row_count",
        "instrument_count",
        "first_date",
        "last_date",
        "catalog_replay_verified",
        "ss_exact_version_pinned",
    )
    return {
        "schema_version": "quantstack.cn-qfq-daily-promotion.v2",
        "status": "completed",
        "session_date": session.isoformat(),
        "dataset_version": dataset_version,
        "raw": raw,
        "factors": factors,
        "qfq": qfq,
        "plan": {
            "status": plan.get("status"),
            "strategy": plan.get("strategy"),
            "target_watermark": plan.get("target_watermark"),
            "target_instrument_count": plan.get("target_instrument_count"),
            "raw_inputs_unchanged": plan.get("raw_inputs_unchanged"),
        },
        "nas_import": import_evidence,
        "catalog": {
            "first": {key: first_promotion.get(key) for key in catalog_fields},
            "replay": {key: replay_promotion.get(key) for key in catalog_fields},
        },
        "stock_screener": smoke,
        "daily_scan": daily_scan,
        "materialization": (
            {
                "mode": "nas_local",
                "factor_input_archive_size_bytes": materialization.get(
                    "factor_input_archive_size_bytes"
                ),
                "wan_contains_raw_parquet": materialization.get("wan_contains_raw_parquet"),
                "wan_contains_qfq_parquet": materialization.get("wan_contains_qfq_parquet"),
                "qfq_release_archive_location": materialization.get("qfq_release_archive_location"),
                "exact_key_coverage": materialization.get("exact_key_coverage"),
            }
            if materialization is not None
            else {"mode": "worker_wan"}
        ),
    }


def _factor_checkpoint_complete(path: Path, *, session: date) -> bool:
    if not path.is_file():
        return False
    payload = json.loads(path.read_text(encoding="utf-8"))
    requests = payload.get("requests")
    return (
        payload.get("complete") is True
        and payload.get("request_end") == session.isoformat()
        and isinstance(requests, list)
        and len(requests) == payload.get("universe_count")
    )


def _remote_materialization(
    config: PromotionConfig, *, report_path: str, session: date, dataset_version: str
) -> dict[str, object] | None:
    try:
        payload = json.loads(_ssh(config, ["cat", report_path], timeout=60))
    except subprocess.SubprocessError:
        return None
    if (
        payload.get("status") != "completed"
        or payload.get("session_date") != session.isoformat()
        or payload.get("dataset_version") != dataset_version
        or payload.get("wan_contains_raw_parquet") is not False
        or payload.get("wan_contains_qfq_parquet") is not False
        or payload.get("qfq_release_archive_location") != "nas-local"
        or payload.get("replay_import_idempotent_replay") is not True
        or not isinstance(payload.get("first_import_idempotent_replay"), bool)
        or payload.get("import_resumed", False) is not payload.get("first_import_idempotent_replay")
    ):
        raise ValueError("completed NAS qfq materialization report is invalid")
    return payload


def _remote_import(
    config: PromotionConfig, *, archive_path: str, token: str, suffix: str
) -> dict[str, object]:
    report = f"{config.remote_staging_root}/reports/qfq-{token}-{suffix}.json"
    _ssh(
        config,
        _remote_python(
            config,
            "import_worker_bundle.py",
            "--archive",
            archive_path,
            "--data-root",
            config.remote_import_data_root,
            "--hmac-key-file",
            config.remote_hmac_key_file,
            "--report",
            report,
        ),
    )
    return json.loads(_ssh(config, ["cat", report], timeout=60))


def _completed_transport(
    config: PromotionConfig, *, run_root: Path, token: str
) -> tuple[dict[str, object], dict[str, object], str] | None:
    transfer_path = run_root / "transfer.json"
    if not transfer_path.is_file():
        return None
    transfer = json.loads(transfer_path.read_text(encoding="utf-8"))
    archive_sha256 = str(transfer.get("archive_sha256", ""))
    if not (
        len(archive_sha256) == 64
        and transfer.get("remote_hash_verified") is True
        and transfer.get("atomic_inbox_install") is True
    ):
        raise ValueError("completed qfq transport report is invalid")
    reports = []
    for suffix in ("first", "replay"):
        path = f"{config.remote_staging_root}/reports/qfq-{token}-{suffix}.json"
        reports.append(json.loads(_ssh(config, ["cat", path], timeout=60)))
    first, replay = reports
    if (
        first.get("archive_sha256") != archive_sha256
        or replay.get("archive_sha256") != archive_sha256
        or first.get("bundle_id") != replay.get("bundle_id")
        or first.get("source_commit") != replay.get("source_commit")
        or first.get("import_idempotent_replay") is not False
        or replay.get("import_idempotent_replay") is not True
    ):
        raise ValueError("completed qfq transport evidence did not converge")
    return first, replay, archive_sha256


def _compose_backend(config: PromotionConfig) -> None:
    _ssh(
        config,
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0:0",
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-v",
            f"{config.remote_ss_checkout}:{config.remote_ss_checkout}",
            "--entrypoint",
            "/usr/bin/docker",
            config.remote_docker_cli_image,
            "compose",
            "--project-directory",
            config.remote_ss_checkout,
            "--env-file",
            f"{config.remote_ss_checkout}/.env.docker",
            "-f",
            f"{config.remote_ss_checkout}/docker-compose.yml",
            "-f",
            f"{config.remote_ss_checkout}/docker-compose.release.yml",
            "-f",
            f"{config.remote_ss_checkout}/compose.quant.yaml",
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "600",
            "--no-deps",
            "--force-recreate",
            "backend",
            "frontend",
        ],
        timeout=900,
    )


def _rollback_command(
    config: PromotionConfig, *, snapshot: str, token: str, restore_ss: bool
) -> list[str]:
    arguments = [
        "rollback",
        "--central-data-root",
        config.remote_central_data_root,
        "--snapshot-dir",
        snapshot,
    ]
    if restore_ss:
        arguments.extend(["--ss-environment", f"{config.remote_ss_checkout}/.env.docker"])
    arguments.extend(
        [
            "--report",
            f"{config.remote_staging_root}/reports/qfq-{token}-rollback.json",
            "--report-gid",
            str(config.remote_read_gid),
        ]
    )
    return _remote_publisher_python(
        config,
        "promote_cn_qfq_release.py",
        *arguments,
    )


def _remote_snapshot_state(config: PromotionConfig, *, snapshot: str) -> str:
    database = shlex.quote(f"{snapshot}/catalog/quant.duckdb")
    pointer = shlex.quote(f"{snapshot}/catalog/current-manifest-set.json")
    ss_environment = shlex.quote(f"{snapshot}/ss/.env.docker")
    result = _ssh(
        config,
        [
            "sh",
            "-c",
            f"if test -f {database} && test -f {pointer}; then "
            f"test -f {ss_environment} && printf complete || printf catalog_only; "
            f"elif test -f {ss_environment}; then printf invalid; else printf absent; fi",
        ],
        timeout=60,
    )
    state = result.strip()
    if state not in {"complete", "catalog_only", "absent"}:
        raise RuntimeError("remote qfq rollback snapshot check returned an invalid result")
    return state


def _rollback_if_exposed(config: PromotionConfig, *, snapshot: str, token: str) -> bool:
    snapshot_state = _remote_snapshot_state(config, snapshot=snapshot)
    if snapshot_state == "absent":
        return False
    restore_ss = snapshot_state == "complete"
    _ssh(
        config,
        _rollback_command(
            config,
            snapshot=snapshot,
            token=token,
            restore_ss=restore_ss,
        ),
    )
    if restore_ss:
        _compose_backend(config)
    return True


def _publish_and_smoke(
    config: PromotionConfig,
    *,
    run_root: Path,
    token: str,
    session: date,
    dataset_version: str,
    snapshot: str,
    promote_command: list[str],
    promotion_report: str,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    incomplete = run_root / "promotion-incomplete.json"
    if incomplete.is_file():
        marker = json.loads(incomplete.read_text(encoding="utf-8"))
        if marker != {"dataset_version": dataset_version, "snapshot": snapshot}:
            raise ValueError("incomplete qfq promotion marker belongs to another release")
        _rollback_if_exposed(config, snapshot=snapshot, token=token)
        incomplete.unlink()

    _write_json(incomplete, {"dataset_version": dataset_version, "snapshot": snapshot})
    try:
        _ssh(config, promote_command)
        first = json.loads(_ssh(config, ["cat", promotion_report], timeout=60))
        _ssh(config, promote_command)
        replay = json.loads(_ssh(config, ["cat", promotion_report], timeout=60))
        if (
            first.get("status") != "published"
            or not isinstance(first.get("idempotent_replay"), bool)
            or replay.get("status") != "published"
            or replay.get("idempotent_replay") is not True
        ):
            raise RuntimeError("qfq promotion and replay did not converge")
        _compose_backend(config)
        smoke_report = f"{config.remote_staging_root}/reports/qfq-{token}-ss-smoke.json"
        _ssh(
            config,
            _remote_publisher_python(
                config,
                "smoke_cn_qfq_ss_release.py",
                "--quant-base-url",
                config.quant_base_url,
                "--ss-base-url",
                config.ss_base_url,
                "--ss-environment",
                f"{config.remote_ss_checkout}/.env.docker",
                "--expected-version",
                dataset_version,
                "--expected-watermark",
                session.isoformat(),
                "--report",
                smoke_report,
                "--report-gid",
                str(config.remote_read_gid),
                network_host=True,
            ),
            timeout=900,
        )
        smoke = json.loads(_ssh(config, ["cat", smoke_report], timeout=60))
        daily_scan_report = f"{config.remote_staging_root}/reports/qfq-{token}-ss-daily-scan.json"
        _ssh(
            config,
            _remote_publisher_python(
                config,
                "publish_cn_ss_daily_scan.py",
                "--ss-base-url",
                config.ss_base_url,
                "--expected-version",
                dataset_version,
                "--expected-watermark",
                session.isoformat(),
                "--report",
                daily_scan_report,
                "--report-gid",
                str(config.remote_read_gid),
                network_host=True,
            ),
            timeout=8_100,
        )
        daily_scan = json.loads(_ssh(config, ["cat", daily_scan_report], timeout=60))
        if (
            daily_scan.get("status") != "completed"
            or daily_scan.get("dataset_version") != dataset_version
            or daily_scan.get("watermark") != session.isoformat()
        ):
            raise RuntimeError("Stock Screener daily scan report did not converge")
        scan_id = str(daily_scan.get("scan_id") or "")
        if not scan_id:
            raise RuntimeError("Stock Screener daily scan report has no scan_id")
        daily_snapshot = _verify_daily_snapshot(
            config,
            token=token,
            session=session,
            scan_id=scan_id,
        )
        daily_scan = {**daily_scan, "daily_snapshot": daily_snapshot}
    except Exception:
        try:
            _rollback_if_exposed(config, snapshot=snapshot, token=token)
        except Exception as rollback_error:
            raise RuntimeError(
                "qfq promotion failed and rollback could not be confirmed; "
                "the incomplete marker was retained for restart reconciliation"
            ) from rollback_error
        incomplete.unlink()
        raise
    incomplete.unlink()
    return first, replay, smoke, daily_scan


def _verify_daily_snapshot(
    config: PromotionConfig,
    *,
    token: str,
    session: date,
    scan_id: str,
) -> dict[str, object]:
    report = f"{config.remote_staging_root}/reports/qfq-{token}-ss-snapshot.json"
    _ssh(
        config,
        _remote_publisher_python(
            config,
            "verify_cn_ss_daily_snapshot.py",
            "--ss-base-url",
            config.ss_base_url,
            "--scan-id",
            scan_id,
            "--expected-watermark",
            session.isoformat(),
            "--report",
            report,
            "--report-gid",
            str(config.remote_read_gid),
            network_host=True,
        ),
        timeout=900,
    )
    payload = json.loads(_ssh(config, ["cat", report], timeout=60))
    if (
        payload.get("status") != "passed"
        or payload.get("scan_id") != scan_id
        or payload.get("watermark") != session.isoformat()
    ):
        raise RuntimeError("Daily Snapshot readback did not converge")
    return payload


def promote_one(config: PromotionConfig, state: PromotionState, session: date) -> PromotionState:
    token = session.strftime("%Y%m%d")
    run_root = config.work_root / f"qfq-{token}"
    run_root.mkdir(parents=True, exist_ok=True)
    daily_paths = sorted(
        (config.daily_work_root / f"daily-{token}" / "data/normalized/bars_1d/market=CN").glob(
            "version=*/bars.parquet"
        )
    )
    if not daily_paths:
        raise FileNotFoundError(f"daily raw Parquet inputs are missing for {session}")
    raw = run_root / f"raw-through-{token}.parquet"
    factors = run_root / f"qfq-factors-through-{token}.json"
    plan = run_root / "promotion-plan.json"
    factor_input = run_root / (f"qfq-factor-input-{token}-{config.source_commit[:12]}.tar")
    dataset_version = f"daily-through-{token}.qfq-release-v1"
    python = str(config.python)
    daily_args = [item for path in daily_paths for item in ("--daily", str(path))]
    remote_materialization_report = (
        f"{config.remote_staging_root}/reports/qfq-{token}-materialization.json"
    )
    materialization = _remote_materialization(
        config,
        report_path=remote_materialization_report,
        session=session,
        dataset_version=dataset_version,
    )
    if materialization is None:
        if not raw.is_file():
            _run(
                [
                    python,
                    "scripts/build_cn_raw_union.py",
                    "--historical",
                    str(state.raw_path),
                    *daily_args,
                    "--expected-session",
                    session.isoformat(),
                    "--output",
                    str(raw),
                ],
                cwd=config.repo_root,
            )
        if not _factor_checkpoint_complete(factors, session=session):
            _run(
                [
                    python,
                    "scripts/acquire_cn_qfq_factors.py",
                    "--raw-bars",
                    str(raw),
                    "--output",
                    str(factors),
                    "--start",
                    config.factor_start_date.isoformat(),
                    "--end",
                    session.isoformat(),
                    "--provider-control-root",
                    str(config.provider_control_root),
                ],
                cwd=config.repo_root,
            )
        daily_import_report = run_root / "daily-import-report.json"
        remote_daily_report = f"{config.remote_staging_root}/reports/{token}-first-import.json"
        _write_json(
            daily_import_report,
            json.loads(_ssh(config, ["cat", remote_daily_report], timeout=60)),
        )
        if not factor_input.is_file():
            _run(
                [
                    python,
                    "scripts/build_cn_qfq_factor_input.py",
                    "--factor",
                    str(factors),
                    "--daily-import-report",
                    str(daily_import_report),
                    "--session",
                    session.isoformat(),
                    "--dataset-version",
                    dataset_version,
                    "--source-commit",
                    config.source_commit,
                    "--hmac-key-file",
                    str(config.hmac_key_file),
                    "--output",
                    str(factor_input),
                ],
                cwd=config.repo_root,
            )
        transfer_report = run_root / "factor-input-transfer.json"
        transfer = json.loads(
            _run(
                _transfer_command(
                    config,
                    archive=factor_input,
                    report=transfer_report,
                ),
                cwd=config.repo_root,
                timeout=1_800,
            )
        )
        if int(transfer.get("size_bytes", 10 * 1024 * 1024 + 1)) > 10 * 1024 * 1024:
            raise ValueError("qfq factor input transfer exceeds the 10MB WAN limit")
        remote_archive = f"{config.remote_staging_root}/inbox/{transfer['archive_sha256']}.tar"
        current_qfq = (
            f"{config.remote_central_data_root}/normalized/bars_1d/market=CN/"
            f"version={state.dataset_version}/bars.parquet"
        )
        _ssh(
            config,
            _remote_python(
                config,
                "materialize_cn_qfq_release.py",
                "--input-archive",
                remote_archive,
                "--data-root",
                config.remote_import_data_root,
                "--raw-baseline",
                config.remote_raw_baseline_path,
                "--current-qfq",
                current_qfq,
                "--calendar-normalized",
                config.remote_calendar_normalized,
                "--daily-reports-root",
                f"{config.remote_staging_root}/reports",
                "--work-root",
                f"{config.remote_staging_root}/materialized/qfq-{token}",
                "--hmac-key-file",
                config.remote_hmac_key_file,
                "--source-commit",
                config.source_commit,
                "--dataset-version",
                dataset_version,
                "--report",
                remote_materialization_report,
            ),
        )
        materialization = _remote_materialization(
            config,
            report_path=remote_materialization_report,
            session=session,
            dataset_version=dataset_version,
        )
        if materialization is None:
            raise RuntimeError("NAS qfq materialization did not produce a report")
    else:
        if not raw.is_file() or not factors.is_file():
            raise RuntimeError("NAS materialization completed but local audit inputs are missing")

    source_commit = str(materialization["source_commit"])
    first = {
        "bundle_id": materialization["bundle_id"],
        "source_commit": source_commit,
        "import_idempotent_replay": materialization["first_import_idempotent_replay"],
        "import_resumed": materialization.get("import_resumed", False),
        "catalog_published": False,
    }
    replay = {
        "bundle_id": materialization["bundle_id"],
        "source_commit": source_commit,
        "import_idempotent_replay": materialization["replay_import_idempotent_replay"],
        "catalog_published": False,
    }
    receipt = f"{config.remote_import_data_root}/{materialization['import_receipt_relative_path']}"
    qfq_profile = dict(materialization["qfq"])
    rows = int(qfq_profile["rows"])
    instruments = int(qfq_profile["instruments"])
    _write_json(plan, materialization["plan"])
    snapshot = f"{config.remote_central_data_root}/catalog/rollback_snapshots/{dataset_version}"
    promotion_report = f"{config.remote_staging_root}/reports/qfq-{token}-promotion.json"
    promote_command = _remote_publisher_python(
        config,
        "promote_cn_qfq_release.py",
        "promote",
        "--source-data-root",
        config.remote_import_data_root,
        "--central-data-root",
        config.remote_central_data_root,
        "--import-receipt",
        receipt,
        "--allowed-source-commit",
        source_commit,
        "--snapshot-dir",
        snapshot,
        "--dataset-id",
        "cn_equity_daily",
        "--dataset-version",
        dataset_version,
        "--expected-row-count",
        str(rows),
        "--expected-instrument-count",
        str(instruments),
        "--expected-watermark",
        session.isoformat(),
        "--ss-environment",
        f"{config.remote_ss_checkout}/.env.docker",
        "--report",
        promotion_report,
        "--report-gid",
        str(config.remote_read_gid),
    )
    first_promotion, replay_promotion, smoke, daily_scan = _publish_and_smoke(
        config,
        run_root=run_root,
        token=token,
        session=session,
        dataset_version=dataset_version,
        snapshot=snapshot,
        promote_command=promote_command,
        promotion_report=promotion_report,
    )
    summary = _promotion_evidence(
        session=session,
        dataset_version=dataset_version,
        raw_path=raw,
        qfq_path=None,
        factor_path=factors,
        plan_path=plan,
        first_import=first,
        replay_import=replay,
        first_promotion=first_promotion,
        replay_promotion=replay_promotion,
        smoke=smoke,
        daily_scan=daily_scan,
        materialization=materialization,
    )
    _write_json(run_root / "summary.json", summary)
    return PromotionState(
        schema_version="quantstack.cn-qfq-daily-promotion-state.v1",
        watermark=session,
        raw_path=raw,
        qfq_path=None,
        remote_qfq_path=(
            f"{config.remote_central_data_root}/normalized/bars_1d/market=CN/"
            f"version={dataset_version}/bars.parquet"
        ),
        materialization_mode="nas_local",
        factor_path=factors,
        dataset_version=dataset_version,
        row_count=rows,
        instrument_count=instruments,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    try:
        config = PromotionConfig.model_validate_json(args.config.read_text(encoding="utf-8"))
        state = PromotionState.model_validate_json(args.state.read_text(encoding="utf-8"))
        with _single_flight(config.work_root):
            completed: list[str] = []
            while (
                session := next_completed_session(
                    state=state,
                    daily_work_root=config.daily_work_root,
                    calendar_normalized=config.calendar_normalized,
                )
            ) is not None:
                state = _promote_with_readback_audit_gate(config, state, session)
                _write_json(args.state, state.model_dump(mode="json"))
                completed.append(session.isoformat())
        print(json.dumps({"status": "completed", "sessions": completed}, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, ensure_ascii=False),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
