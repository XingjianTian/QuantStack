from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from scripts.promote_cn_qfq_release import _write_report
from scripts.run_cn_qfq_daily_promotion import (
    PromotionConfig,
    PromotionState,
    _completed_transport,
    _compose_backend,
    _promote_with_readback_audit_gate,
    _promotion_evidence,
    _publish_and_smoke,
    _remote_materialization,
    _remote_publisher_python,
    _ssh,
    _transfer_command,
    _validate_readback_only_operations_audit,
    next_completed_session,
)
from scripts.smoke_cn_qfq_ss_release import smoke_release


def _state(tmp_path: Path) -> PromotionState:
    return PromotionState(
        schema_version="quantstack.cn-qfq-daily-promotion-state.v1",
        watermark=date(2026, 8, 14),
        raw_path=tmp_path / "raw.parquet",
        qfq_path=tmp_path / "qfq.parquet",
        dataset_version="daily-through-20260814.qfq-release-v1",
        row_count=10,
        instrument_count=2,
    )


def _config(tmp_path: Path) -> PromotionConfig:
    return PromotionConfig(
        repo_root=tmp_path / "repo",
        source_commit="a" * 40,
        python=tmp_path / "python",
        work_root=tmp_path / "work",
        daily_work_root=tmp_path / "daily",
        calendar_normalized=tmp_path / "calendar.json",
        hmac_key_file=tmp_path / "hmac.key",
        provider_control_root=tmp_path / "control",
        factor_start_date=date(2015, 1, 1),
        remote_host="nas",
        remote_staging_root="/data/staging",
        remote_repo_root="/data/repo",
        remote_python="/data/python",
        remote_import_data_root="/data/imported",
        remote_hmac_key_file="/data/hmac.key",
        remote_central_data_root="/data/central",
        remote_raw_baseline_path=(
            "/data/central/canonical/cn_equity_daily/version=2015-2026.full-market-v1/bars.parquet"
        ),
        remote_calendar_normalized="/data/calendar/normalized.json",
        remote_ss_checkout="/data/ss",
        quant_base_url="http://quant",
        ss_base_url="http://ss",
    )


def _calendar(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "days": [
                    {"trading_date": "2026-08-15", "state": "weekend"},
                    {"trading_date": "2026-08-17", "state": "session"},
                    {"trading_date": "2026-08-18", "state": "session"},
                ]
            }
        ),
        encoding="utf-8",
    )


def _summary(root: Path, session: str, **overrides: object) -> None:
    path = root / f"daily-{session.replace('-', '')}" / "runner-summary.json"
    path.parent.mkdir(parents=True)
    payload: dict[str, object] = {
        "status": "completed",
        "session_date": session,
        "duplicate_keys": 0,
        "catalog_published": False,
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _bars(path: Path, adjustment: str) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 'XSHG.600000' AS instrument_id, DATE '2026-08-17' "
            "AS trading_date, struct_pack(adjustment := ?) AS lineage) "
            f"TO '{path}' (FORMAT PARQUET)",
            [adjustment],
        )
    finally:
        connection.close()


def test_daily_promotion_consumes_only_the_next_verified_completed_session(
    tmp_path: Path,
) -> None:
    calendar = tmp_path / "calendar.json"
    daily = tmp_path / "daily"
    _calendar(calendar)
    assert (
        next_completed_session(
            state=_state(tmp_path),
            daily_work_root=daily,
            calendar_normalized=calendar,
        )
        is None
    )
    _summary(daily, "2026-08-17")
    assert next_completed_session(
        state=_state(tmp_path),
        daily_work_root=daily,
        calendar_normalized=calendar,
    ) == date(2026, 8, 17)


def test_local_promotion_execution_avoids_ssh(monkeypatch, tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"execution_mode": "local"})
    observed: list[list[str]] = []

    def fake_run(command, *, cwd, timeout=21_600):
        observed.append(command)
        return "local\n"

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._run", fake_run)
    assert _ssh(config, ["printf", "local"]) == "local\n"
    assert observed == [["printf", "local"]]
    transfer = _transfer_command(
        config,
        archive=tmp_path / "factor.tar",
        report=tmp_path / "transfer.json",
    )
    assert "--local-staging-root" in transfer
    assert "--remote-host" not in transfer


def test_daily_promotion_fails_closed_on_missing_or_invalid_session(tmp_path: Path) -> None:
    calendar = tmp_path / "calendar.json"
    daily = tmp_path / "daily"
    _calendar(calendar)
    _summary(daily, "2026-08-18")
    with pytest.raises(RuntimeError, match="2026-08-17 is missing"):
        next_completed_session(
            state=_state(tmp_path),
            daily_work_root=daily,
            calendar_normalized=calendar,
        )
    _summary(daily, "2026-08-17", duplicate_keys=1)
    with pytest.raises(ValueError, match="isolation checks"):
        next_completed_session(
            state=_state(tmp_path),
            daily_work_root=daily,
            calendar_normalized=calendar,
        )


def _audit_payload(
    *, session: str, source_commit: str, dataset_version: str
) -> dict[str, Any]:
    return {
        "status": "passed",
        "alerts": [],
        "through_date": session,
        "expected_sessions": [session],
        "daily": {
            session: {
                "session_date": session,
                "source_commit": source_commit,
                "nas_verification_mode": "readback_only",
            }
        },
        "promotions": {
            session: {
                "session_date": session,
                "dataset_version": dataset_version,
            }
        },
    }


def test_readback_only_audit_gate_accepts_matching_session_and_source(tmp_path: Path) -> None:
    session = date(2026, 8, 17)
    source_commit = "b" * 40
    candidate = _state(tmp_path).model_copy(
        update={
            "watermark": session,
            "dataset_version": "daily-through-20260817.qfq-release-v1",
        }
    )
    _validate_readback_only_operations_audit(
        audit=_audit_payload(
            session=session.isoformat(),
            source_commit=source_commit,
            dataset_version=candidate.dataset_version,
        ),
        daily_summary={"source_commit": source_commit},
        candidate=candidate,
        session=session,
    )


@pytest.mark.parametrize(
    "case", ["failed", "stale", "mismatched_source", "mismatched_dataset"]
)
def test_readback_only_audit_gate_rejects_failed_or_stale_evidence(
    tmp_path: Path, case: str
) -> None:
    session = date(2026, 8, 17)
    source_commit = "b" * 40
    candidate = _state(tmp_path).model_copy(
        update={
            "watermark": session,
            "dataset_version": "daily-through-20260817.qfq-release-v1",
        }
    )
    audit = _audit_payload(
        session=session.isoformat(),
        source_commit=source_commit,
        dataset_version=candidate.dataset_version,
    )
    if case == "failed":
        audit["status"] = "failed"
        audit["alerts"] = ["daily_rows_reconciled:2026-08-17"]
    elif case == "stale":
        audit["through_date"] = "2026-08-14"
    elif case == "mismatched_source":
        audit["daily"][session.isoformat()]["source_commit"] = "c" * 40
    else:
        audit["promotions"][session.isoformat()]["dataset_version"] = "stale-version"
    with pytest.raises(ValueError, match="did not match"):
        _validate_readback_only_operations_audit(
            audit=audit,
            daily_summary={"source_commit": source_commit},
            candidate=candidate,
            session=session,
        )


def test_readback_only_missing_audit_prevents_candidate_return(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = date(2026, 8, 17)
    config = _config(tmp_path)
    _summary(
        config.daily_work_root,
        session.isoformat(),
        nas_verification_mode="readback_only",
        source_commit="b" * 40,
    )
    candidate = _state(tmp_path).model_copy(update={"watermark": session})
    monkeypatch.setattr(
        "scripts.run_cn_qfq_daily_promotion.promote_one", lambda *_args: candidate
    )
    with pytest.raises(RuntimeError, match="requires operations audit runtime evidence"):
        _promote_with_readback_audit_gate(config, _state(tmp_path), session)


def test_full_replay_promotion_retains_existing_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = date(2026, 8, 17)
    config = _config(tmp_path)
    _summary(config.daily_work_root, session.isoformat())
    candidate = _state(tmp_path).model_copy(update={"watermark": session})
    monkeypatch.setattr(
        "scripts.run_cn_qfq_daily_promotion.promote_one", lambda *_args: candidate
    )

    def unexpected_audit(*_args, **_kwargs):
        raise AssertionError("full replay must not invoke the readback-only audit gate")

    monkeypatch.setattr(
        "scripts.run_cn_qfq_daily_promotion._run_readback_only_operations_audit",
        unexpected_audit,
    )
    assert _promote_with_readback_audit_gate(config, _state(tmp_path), session) == candidate


def test_daily_promotion_resumes_a_completed_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_root = tmp_path / "qfq-20260817"
    run_root.mkdir()
    archive_sha256 = "1" * 64
    (run_root / "transfer.json").write_text(
        json.dumps(
            {
                "archive_sha256": archive_sha256,
                "remote_hash_verified": True,
                "atomic_inbox_install": True,
            }
        ),
        encoding="utf-8",
    )
    reports = {
        "first": {
            "archive_sha256": archive_sha256,
            "bundle_id": "worker-fixture",
            "source_commit": "b" * 40,
            "import_idempotent_replay": False,
        },
        "replay": {
            "archive_sha256": archive_sha256,
            "bundle_id": "worker-fixture",
            "source_commit": "b" * 40,
            "import_idempotent_replay": True,
        },
    }

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        del config, timeout
        suffix = Path(command[1]).stem.rsplit("-", 1)[-1]
        return json.dumps(reports[suffix])

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    completed = _completed_transport(_config(tmp_path), run_root=run_root, token="20260817")
    assert completed == (reports["first"], reports["replay"], archive_sha256)


def test_daily_promotion_resumes_only_a_completed_nas_local_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = {
        "status": "completed",
        "session_date": "2026-08-19",
        "dataset_version": "daily-through-20260819.qfq-release-v1",
        "wan_contains_raw_parquet": False,
        "wan_contains_qfq_parquet": False,
        "qfq_release_archive_location": "nas-local",
        "first_import_idempotent_replay": False,
        "replay_import_idempotent_replay": True,
        "import_resumed": False,
    }
    monkeypatch.setattr(
        "scripts.run_cn_qfq_daily_promotion._ssh",
        lambda *_args, **_kwargs: json.dumps(report),
    )
    assert (
        _remote_materialization(
            _config(tmp_path),
            report_path="/data/staging/reports/qfq-20260819-materialization.json",
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
        )
        == report
    )

    report["wan_contains_qfq_parquet"] = True
    with pytest.raises(ValueError, match="materialization report is invalid"):
        _remote_materialization(
            _config(tmp_path),
            report_path="/data/staging/reports/qfq-20260819-materialization.json",
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
        )

    report["wan_contains_qfq_parquet"] = False
    report["first_import_idempotent_replay"] = True
    report["import_resumed"] = True
    assert (
        _remote_materialization(
            _config(tmp_path),
            report_path="/data/staging/reports/qfq-20260819-materialization.json",
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
        )
        == report
    )


@pytest.mark.parametrize(
    "failure_at", ["first_report", "replay", "smoke", "daily_scan", "daily_snapshot"]
)
def test_publication_fault_windows_restore_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_at: str
) -> None:
    config = _config(tmp_path)
    run_root = tmp_path / "run"
    run_root.mkdir()
    snapshot_exists = False
    promote_calls = 0
    report_reads = 0
    rollback_calls = 0

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        nonlocal snapshot_exists, promote_calls, report_reads, rollback_calls
        del config, timeout
        joined = " ".join(command)
        if command[:2] == ["sh", "-c"]:
            return "complete" if snapshot_exists else "absent"
        if command == ["promote"]:
            promote_calls += 1
            snapshot_exists = True
            if failure_at == "replay" and promote_calls == 2:
                raise subprocess.CalledProcessError(255, command)
            return ""
        if command == ["cat", "/promotion.json"]:
            report_reads += 1
            if failure_at == "first_report" and report_reads == 1:
                raise subprocess.CalledProcessError(255, command)
            return json.dumps(
                {
                    "status": "published",
                    "idempotent_replay": report_reads > 1,
                }
            )
        if "promote_cn_qfq_release.py rollback" in joined:
            rollback_calls += 1
            return ""
        if "smoke_cn_qfq_ss_release.py" in joined:
            if failure_at == "smoke":
                raise subprocess.CalledProcessError(255, command)
            return ""
        if "publish_cn_ss_daily_scan.py" in joined and failure_at == "daily_scan":
            raise subprocess.CalledProcessError(255, command)
        if "verify_cn_ss_daily_snapshot.py" in joined and failure_at == "daily_snapshot":
            raise subprocess.CalledProcessError(255, command)
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-smoke.json"]:
            return json.dumps({"status": "passed"})
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-daily-scan.json"]:
            return json.dumps(
                {
                    "status": "completed",
                    "dataset_version": "daily-through-20260819.qfq-release-v1",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-snapshot.json"]:
            return json.dumps(
                {
                    "status": "passed",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        return ""

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    with pytest.raises(subprocess.CalledProcessError):
        _publish_and_smoke(
            config,
            run_root=run_root,
            token="20260819",
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
            snapshot="/data/central/catalog/rollback_snapshots/release",
            promote_command=["promote"],
            promotion_report="/promotion.json",
        )
    assert rollback_calls == 1
    assert not (run_root / "promotion-incomplete.json").exists()


def test_publication_restart_reconciles_incomplete_marker_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    run_root = tmp_path / "run"
    run_root.mkdir()
    dataset_version = "daily-through-20260819.qfq-release-v1"
    snapshot = "/data/central/catalog/rollback_snapshots/release"
    (run_root / "promotion-incomplete.json").write_text(
        json.dumps({"dataset_version": dataset_version, "snapshot": snapshot}),
        encoding="utf-8",
    )
    report_reads = 0
    rollback_calls = 0

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        nonlocal report_reads, rollback_calls
        del config, timeout
        joined = " ".join(command)
        if command[:2] == ["sh", "-c"]:
            return "complete"
        if "promote_cn_qfq_release.py rollback" in joined:
            rollback_calls += 1
            return ""
        if command == ["cat", "/promotion.json"]:
            report_reads += 1
            return json.dumps(
                {
                    "status": "published",
                    "idempotent_replay": report_reads > 1,
                }
            )
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-smoke.json"]:
            return json.dumps({"status": "passed"})
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-daily-scan.json"]:
            return json.dumps(
                {
                    "status": "completed",
                    "dataset_version": "daily-through-20260819.qfq-release-v1",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-snapshot.json"]:
            return json.dumps(
                {
                    "status": "passed",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        return ""

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    first, replay, smoke, daily_scan = _publish_and_smoke(
        config,
        run_root=run_root,
        token="20260819",
        session=date(2026, 8, 19),
        dataset_version=dataset_version,
        snapshot=snapshot,
        promote_command=["promote"],
        promotion_report="/promotion.json",
    )
    assert rollback_calls == 1
    assert first["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert smoke["status"] == "passed"
    assert daily_scan["watermark"] == "2026-08-19"
    assert daily_scan["daily_snapshot"]["status"] == "passed"
    assert not (run_root / "promotion-incomplete.json").exists()


def test_first_promote_partial_snapshot_uses_catalog_only_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    run_root = tmp_path / "run"
    run_root.mkdir()
    rollback_commands: list[list[str]] = []

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        del config, timeout
        joined = " ".join(command)
        if command == ["promote"]:
            raise subprocess.CalledProcessError(1, command)
        if command[:2] == ["sh", "-c"]:
            return "catalog_only"
        if "promote_cn_qfq_release.py rollback" in joined:
            rollback_commands.append(command)
            return ""
        return ""

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    with pytest.raises(subprocess.CalledProcessError):
        _publish_and_smoke(
            config,
            run_root=run_root,
            token="20260819",
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
            snapshot="/data/central/catalog/rollback_snapshots/release",
            promote_command=["promote"],
            promotion_report="/promotion.json",
        )
    assert len(rollback_commands) == 1
    assert "--ss-environment" not in rollback_commands[0]
    assert not (run_root / "promotion-incomplete.json").exists()


def test_publication_restart_reconciles_catalog_only_snapshot_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    run_root = tmp_path / "run"
    run_root.mkdir()
    dataset_version = "daily-through-20260819.qfq-release-v1"
    snapshot = "/data/central/catalog/rollback_snapshots/release"
    (run_root / "promotion-incomplete.json").write_text(
        json.dumps({"dataset_version": dataset_version, "snapshot": snapshot}),
        encoding="utf-8",
    )
    report_reads = 0
    rollback_commands: list[list[str]] = []

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        nonlocal report_reads
        del config, timeout
        joined = " ".join(command)
        if command[:2] == ["sh", "-c"]:
            return "catalog_only"
        if "promote_cn_qfq_release.py rollback" in joined:
            rollback_commands.append(command)
            return ""
        if command == ["cat", "/promotion.json"]:
            report_reads += 1
            return json.dumps({"status": "published", "idempotent_replay": report_reads > 1})
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-smoke.json"]:
            return json.dumps({"status": "passed"})
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-daily-scan.json"]:
            return json.dumps(
                {
                    "status": "completed",
                    "dataset_version": "daily-through-20260819.qfq-release-v1",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        if command == ["cat", "/data/staging/reports/qfq-20260819-ss-snapshot.json"]:
            return json.dumps(
                {
                    "status": "passed",
                    "watermark": "2026-08-19",
                    "scan_id": "scan-20260819",
                }
            )
        return ""

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    first, replay, smoke, daily_scan = _publish_and_smoke(
        config,
        run_root=run_root,
        token="20260819",
        session=date(2026, 8, 19),
        dataset_version=dataset_version,
        snapshot=snapshot,
        promote_command=["promote"],
        promotion_report="/promotion.json",
    )
    assert len(rollback_commands) == 1
    assert "--ss-environment" not in rollback_commands[0]
    assert first["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert smoke["status"] == "passed"
    assert daily_scan["watermark"] == "2026-08-19"
    assert daily_scan["daily_snapshot"]["status"] == "passed"
    assert not (run_root / "promotion-incomplete.json").exists()


def test_normal_daily_path_uses_factor_input_and_nas_materializer() -> None:
    source = Path(__file__).parents[1] / "scripts/run_cn_qfq_daily_promotion.py"
    promote_source = source.read_text(encoding="utf-8").split("def promote_one", 1)[1]
    assert "build_cn_qfq_factor_input.py" in promote_source
    assert "materialize_cn_qfq_release.py" in promote_source
    assert "build_cn_qfq_worker_archive.py" not in promote_source
    assert "package_cn_qfq_release.py" not in promote_source


def test_qfq_promotion_timer_runs_after_daily_worker() -> None:
    root = Path(__file__).parents[1]
    daily_service = (root / "deploy/systemd/quantstack-daily.service").read_text()
    service = (root / "deploy/systemd/quantstack-cn-qfq-promotion.service").read_text()
    timer = (root / "deploy/systemd/quantstack-cn-qfq-promotion.timer").read_text()
    assert (
        "ExecStartPost=/usr/bin/systemctl --user start --no-block "
        "quantstack-cn-qfq-promotion.service" in daily_service
    )
    assert "After=network-online.target quantstack-daily.service" in service
    assert "run_cn_qfq_session_daily.py" in service
    assert "PYTHONPATH=${QUANTSTACK_QFQ_REPO}/src:${QUANTSTACK_QFQ_REPO}" in service
    assert "OnCalendar=Mon..Fri *-*-* 18:30:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer


def test_operations_audit_follows_promotion_state_and_keeps_daily_history() -> None:
    root = Path(__file__).parents[1]
    service = (root / "deploy/systemd/quantstack-cn-operations-audit.service").read_text()
    assert "PYTHONPATH=${QUANTSTACK_CN_AUDIT_REPO}/src:${QUANTSTACK_CN_AUDIT_REPO}" in service
    assert "--promotion-state ${QUANTSTACK_CN_PROMOTION_STATE}" in service
    assert "--promotion-work-root ${QUANTSTACK_CN_PROMOTION_WORK_ROOT}" in service
    assert "--history-root ${QUANTSTACK_CN_AUDIT_HISTORY_ROOT}" in service
    assert "--raw ${QUANTSTACK_CN_RAW}" not in service


def test_promotion_summary_preserves_daily_release_evidence(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    qfq = tmp_path / "qfq.parquet"
    factors = tmp_path / "factors.json"
    plan = tmp_path / "plan.json"
    _bars(raw, "none")
    _bars(qfq, "qfq")
    factors.write_text(
        json.dumps(
            {
                "complete": True,
                "universe_count": 1,
                "request_end": "2026-08-17",
                "request_policy": {
                    "concurrency": 1,
                    "request_interval_ms": 50,
                    "batch_size": 500,
                    "batch_interval_seconds": 5,
                },
                "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
            }
        ),
        encoding="utf-8",
    )
    plan.write_text(
        json.dumps(
            {
                "status": "ready",
                "strategy": "full_rebuild_required",
                "target_watermark": "2026-08-17",
                "target_instrument_count": 1,
                "raw_inputs_unchanged": True,
            }
        ),
        encoding="utf-8",
    )
    version = "daily-through-20260817.qfq-release-v1"
    catalog = {
        "status": "published",
        "dataset_id": "cn_equity_daily",
        "dataset_version": version,
        "row_count": 1,
        "instrument_count": 1,
        "first_date": "2026-08-17",
        "last_date": "2026-08-17",
        "catalog_replay_verified": True,
        "ss_exact_version_pinned": True,
    }
    evidence = _promotion_evidence(
        session=date(2026, 8, 17),
        dataset_version=version,
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        plan_path=plan,
        first_import={
            "bundle_id": "worker-fixture",
            "import_idempotent_replay": False,
            "catalog_published": False,
        },
        replay_import={"import_idempotent_replay": True, "catalog_published": False},
        first_promotion={**catalog, "idempotent_replay": False},
        replay_promotion={**catalog, "idempotent_replay": True},
        smoke={"status": "passed", "dataset_version": version},
        daily_scan={
            "status": "completed",
            "dataset_version": version,
            "watermark": "2026-08-17",
            "scan_id": "scan-fixture",
        },
    )
    assert evidence["raw"]["end_date"] == "2026-08-17"
    assert evidence["factors"]["request_policy"]["concurrency"] == 1
    assert evidence["catalog"]["replay"]["idempotent_replay"] is True
    assert "instrument_ids" not in evidence["factors"]


def test_production_publication_runs_in_the_root_publisher_container(tmp_path: Path) -> None:
    command = _remote_publisher_python(
        _config(tmp_path), "promote_cn_qfq_release.py", "promote", network_host=True
    )
    assert command[:5] == ["docker", "run", "--rm", "--user", "0:0"]
    assert "QUANT_DATA_SHARED_GID=997" not in command
    assert "--network" in command
    assert "personal/quant-stack-api:cpdw-t098-eddafaf-audit" in command
    assert "/app/.venv/bin/python" in command
    assert command[-2].endswith("/scripts/promote_cn_qfq_release.py")
    assert command[-1] == "promote"


def test_backend_recreate_waits_for_container_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands: list[list[str]] = []

    def fake_ssh(config: PromotionConfig, command: list[str], *, timeout: int = 21_600):
        del config, timeout
        commands.append(command)
        return ""

    monkeypatch.setattr("scripts.run_cn_qfq_daily_promotion._ssh", fake_ssh)
    _compose_backend(_config(tmp_path))
    assert "--wait" in commands[0]
    assert commands[0][commands[0].index("--wait-timeout") + 1] == "600"
    assert commands[0][-2:] == ["backend", "frontend"]


def test_publisher_report_applies_the_existing_reader_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr("scripts.promote_cn_qfq_release.os.chown", lambda *args: calls.append(args))
    monkeypatch.setattr("scripts.promote_cn_qfq_release.os.chmod", lambda *args: calls.append(args))
    report = tmp_path / "promotion.json"
    _write_report(report, {"status": "published"}, shared_gid=997)
    assert json.loads(report.read_text()) == {"status": "published"}
    assert calls == [(report, -1, 997), (report, 0o640)]


def test_ss_smoke_requires_exact_pin_and_matching_lake_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment = tmp_path / ".env.docker"
    version = "daily-through-20260814.qfq-release-v1"
    environment.write_text(
        "SS_QUANTSTACK_READ_MODE=quantstack_primary\n"
        "SS_QUANTSTACK_PRIMARY_ENABLED=true\n"
        "SS_QUANTSTACK_SHADOW_ENABLED=false\n"
        "SS_QUANTSTACK_BARS_DATASET_ID=cn_equity_daily\n"
        f"SS_QUANTSTACK_DATASET_VERSION={version}\n"
        "SS_QUANTSTACK_ALLOW_CANDIDATE=false\n"
        "SS_QUANTSTACK_API_KEY=fixture\n",
        encoding="utf-8",
    )

    def fake_json(url: str, **kwargs):
        if "/metadata" in url:
            return {"dataset_version": version}
        if "/market/batch/bars" in url:
            instrument = kwargs["payload"]["instrument_ids"][0]
            return {
                "dataset_version": version,
                "items": [
                    {
                        "instrument_id": instrument,
                        "ts_event": "2026-08-14T07:00:00Z",
                        "adjustment_semantics": "qfq",
                    }
                ],
            }
        if "/history" in url:
            return [{"date": "2026-08-14"}]
        return {"status": "ok"}

    class Frontend:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Opener:
        def open(self, *_args, **_kwargs):
            return Frontend()

    monkeypatch.setattr("scripts.smoke_cn_qfq_ss_release._json_request", fake_json)
    monkeypatch.setattr("urllib.request.build_opener", lambda *_args: Opener())
    report = smoke_release(
        quant_base_url="http://quant",
        ss_base_url="http://ss",
        ss_environment_path=environment,
        expected_version=version,
        expected_watermark=date(2026, 8, 14),
    )
    assert report["status"] == "passed"
    assert report["frontend_http_status"] == 200
