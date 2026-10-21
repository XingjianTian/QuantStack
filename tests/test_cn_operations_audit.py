from __future__ import annotations

import json
from pathlib import Path

import duckdb

from quant_stack.data.cn_operations_audit import audit_cn_operations
from scripts import capture_cn_operations_runtime as runtime_capture
from scripts.capture_cn_operations_runtime import _latest_price_date, _stale_daily_tasks


def _bars(path: Path, adjustment: str, instrument_id: str = "XSHG.600000") -> None:
    connection = duckdb.connect()
    adjustment_sql = "'" + adjustment.replace("'", "''") + "'"
    try:
        connection.execute(
            f"COPY (SELECT '{instrument_id}' AS instrument_id, '600000' AS symbol, "
            "'XSHG' AS venue, DATE '2026-08-14' AS trading_date, "
            "TIMESTAMPTZ '2026-08-14 00:00:00+08' AS ts_event, 10.0 AS open, "
            "11.0 AS high, 9.0 AS low, 10.5 AS close, 100.0 AS volume, "
            "1050.0 AS amount, 'CNY' AS currency, "
            f"struct_pack(adjustment := {adjustment_sql}) AS lineage) "
            f"TO '{str(path)}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_cn_operations_audit_reconciles_one_report(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    qfq = tmp_path / "qfq.parquet"
    _bars(raw, "none")
    _bars(qfq, "qfq")
    work = tmp_path / "work"
    _write(
        work / "daily-20260814" / "runner-summary.json",
        {
            "status": "completed",
            "duplicate_keys": 0,
            "records_received": 1,
            "nas_persisted_bar_rows": 1,
            "planned_batches": 1,
            "succeeded_batches": 1,
            "nas_new_imports": 1,
            "nas_idempotent_replays": 1,
            "nas_verified_receipts": 1,
            "canonical_eligible": False,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
    )
    factors = tmp_path / "factors.json"
    _write(
        factors,
        {
            "complete": True,
            "universe_count": 1,
            "request_end": "2026-08-14",
            "request_policy": {
                "concurrency": 1,
                "request_interval_ms": 50,
                "batch_size": 500,
                "batch_interval_seconds": 5,
            },
            "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
        },
    )
    runtime = tmp_path / "runtime.json"
    _write(
        runtime,
        {
            "daily_timer": {"enabled": True, "active": True},
            "operations_timer": {"enabled": True, "active": True},
            "tailscale_service": {"enabled": True, "active": True},
            "operator_linger": True,
            "weekly_reconciliation": {"scheduled": True},
            "provider_budget": {"blocked": None},
            "nas_containers": {"quant-stack-api": "healthy"},
            "stale_tasks": [],
            "nas_isolated": {
                "first_import": "passed",
                "idempotent_replay": "passed",
                "readback": "passed",
                "exact_key_coverage": True,
                "latest_anchor_max_close_drift": 0.0,
            },
            "ss_kline": {"last_date": "2026-08-14"},
        },
    )
    promotion_work = tmp_path / "promotion"
    promotion_summary = {
        "schema_version": "quantstack.cn-qfq-daily-promotion.v2",
        "status": "completed",
        "session_date": "2026-08-14",
        "dataset_version": "daily-through-20260814.qfq-release-v1",
        "raw": {
            "rows": 1,
            "unique_keys": 1,
            "instruments": 1,
            "start_date": "2026-08-14",
            "end_date": "2026-08-14",
            "adjustments": ["none"],
        },
        "factors": {
            "complete": True,
            "universe_count": 1,
            "request_count": 1,
            "request_end": "2026-08-14",
            "request_policy": {
                "concurrency": 1,
                "request_interval_ms": 50,
                "batch_size": 500,
                "batch_interval_seconds": 5,
            },
            "statuses": {"ok": 1},
            "unique_instrument_ids": 1,
        },
        "qfq": {
            "rows": 1,
            "unique_keys": 1,
            "instruments": 1,
            "start_date": "2026-08-14",
            "end_date": "2026-08-14",
            "adjustments": ["qfq"],
        },
        "plan": {
            "status": "ready",
            "strategy": "full_rebuild_required",
            "target_watermark": "2026-08-14",
            "target_instrument_count": 1,
            "raw_inputs_unchanged": True,
        },
        "nas_import": {
            "bundle_id": "worker-fixture",
            "first_idempotent_replay": False,
            "replay_idempotent_replay": True,
            "catalog_published_during_import": False,
        },
        "catalog": {
            key: {
                "status": "published",
                "idempotent_replay": key == "replay",
                "dataset_id": "cn_equity_daily",
                "dataset_version": "daily-through-20260814.qfq-release-v1",
                "row_count": 1,
                "instrument_count": 1,
                "first_date": "2026-08-14",
                "last_date": "2026-08-14",
                "catalog_replay_verified": True,
                "ss_exact_version_pinned": True,
            }
            for key in ("first", "replay")
        },
        "stock_screener": {
            "status": "passed",
            "dataset_version": "daily-through-20260814.qfq-release-v1",
            "watermark": "2026-08-14",
            "read_mode": "quantstack_primary",
            "shadow_enabled": False,
            "frontend_http_status": 200,
        },
    }
    promotion_path = promotion_work / "qfq-20260814" / "summary.json"
    _write(promotion_path, promotion_summary)

    report = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
    )

    assert report["status"] == "passed"
    assert report["alerts"] == []
    assert list(report["promotions"]) == ["2026-08-14"]
    assert report["late_revision_policy"] == "new_immutable_version_only"

    nas_local_report = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=None,
        qfq_path=None,
        factor_path=None,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
        artifact_evidence={
            "raw": promotion_summary["raw"],
            "qfq": promotion_summary["qfq"],
            "factors": promotion_summary["factors"],
            "exact_key_coverage": True,
        },
    )
    assert nas_local_report["status"] == "passed"
    assert nas_local_report["key_coverage"]["evidence_source"] == ("nas_local_materialization")

    daily_summary_path = work / "daily-20260814" / "runner-summary.json"
    daily_summary = json.loads(daily_summary_path.read_text(encoding="utf-8"))
    daily_summary.update(
        {
            "nas_verification_mode": "readback_only",
            "nas_idempotent_replays": 0,
            "nas_idempotent_replay_deferred": True,
            "nas_recovered_imports": 0,
        }
    )
    _write(daily_summary_path, daily_summary)
    lightweight_report = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
    )
    assert lightweight_report["status"] == "passed"
    assert lightweight_report["alerts"] == []

    _write(
        daily_summary_path,
        {
            "schema_version": "quantstack.cn-session-daily-run.v1",
            "status": "completed",
            "session_date": "2026-08-14",
            "records_received": 1,
            "distinct_instruments": 1,
            "omissions": 0,
            "persistence": "single_session_parquet",
            "provider_retries": 0,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
    )
    session_report = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
    )
    assert session_report["status"] == "passed"
    assert session_report["alerts"] == []

    runtime_payload = json.loads(runtime.read_text(encoding="utf-8"))
    runtime_payload["ss_kline"]["last_date"] = "2026-08-06"
    _write(runtime, runtime_payload)
    stale_ss = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
    )
    assert "ss_kline_last_date" in stale_ss["alerts"]
    runtime_payload["ss_kline"]["last_date"] = "2026-08-14"
    _write(runtime, runtime_payload)

    promotion_summary["stock_screener"]["watermark"] = "2026-08-13"
    _write(promotion_path, promotion_summary)
    failed = audit_cn_operations(
        work_root=work,
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
        promotion_work_root=promotion_work,
    )
    assert "qfq_promotion_stock_screener:2026-08-14" in failed["alerts"]


def test_cn_operations_audit_rejects_equal_count_wrong_keys_and_factor_ids(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw.parquet"
    qfq = tmp_path / "qfq.parquet"
    _bars(raw, "none")
    _bars(qfq, "qfq", instrument_id="XSHE.000001")
    factors = tmp_path / "factors.json"
    _write(
        factors,
        {
            "complete": True,
            "universe_count": 1,
            "request_end": "2026-08-14",
            "request_policy": {
                "concurrency": 1,
                "request_interval_ms": 50,
                "batch_size": 500,
                "batch_interval_seconds": 5,
            },
            "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
        },
    )
    runtime = tmp_path / "runtime.json"
    _write(runtime, {})

    report = audit_cn_operations(
        work_root=tmp_path / "missing",
        expected_sessions=[],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
    )

    assert "raw_qfq_key_coverage" in report["alerts"]
    assert "factor_instrument_coverage" in report["alerts"]
    assert report["key_coverage"] == {"raw_only_keys": 1, "qfq_only_keys": 1}


def test_cn_operations_audit_fails_closed(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    qfq = tmp_path / "qfq.parquet"
    _bars(raw, "none")
    _bars(qfq, "qfq")
    factors = tmp_path / "factors.json"
    _write(factors, {"complete": False, "requests": []})
    runtime = tmp_path / "runtime.json"
    _write(runtime, {"daily_timer": {"enabled": False, "active": False}})

    report = audit_cn_operations(
        work_root=tmp_path / "missing",
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
    )

    assert report["status"] == "failed"
    assert "missing_daily_summary:2026-08-14" in report["alerts"]
    assert "factor_coverage" in report["alerts"]
    assert "daily_timer" in report["alerts"]
    assert "operations_timer" in report["alerts"]
    assert "tailscale_service" in report["alerts"]
    assert "operator_linger" in report["alerts"]
    assert "weekly_reconciliation" in report["alerts"]


def test_runtime_helpers_report_real_kline_date_and_ignore_completed_tasks(
    tmp_path: Path,
) -> None:
    completed = tmp_path / "daily-20260814"
    _write(completed / "runner-summary.json", {"status": "completed"})

    assert _stale_daily_tasks(tmp_path, 0) == []
    assert _latest_price_date([{"date": "2026-08-13"}, {"time": "2026-08-14T00:00:00"}]) == (
        "2026-08-14"
    )


def test_runtime_capture_quotes_remote_ssh_commands(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "budget.json"
    _write(budget, {"blocked": None})
    remote_commands: list[str] = []

    def fake_run(command: list[str]) -> str:
        if command[0] == "systemctl":
            if "cat" in command:
                return "OnCalendar=Fri *-*-* 22:30:00 Asia/Shanghai"
            return "enabled" if "is-enabled" in command else "active"
        if command[0] == "loginctl":
            return "yes"
        remote = command[-1]
        remote_commands.append(remote)
        if remote.startswith("docker inspect"):
            return "/quant-stack-api healthy"
        if remote.startswith("cat "):
            return json.dumps(
                {
                    "first_import": "passed",
                    "idempotent_replay": "passed",
                    "readback": "passed",
                }
            )
        return json.dumps([{"date": "2026-08-14"}])

    monkeypatch.setattr(runtime_capture, "_run", fake_run)
    result = runtime_capture.capture(
        {
            "nas_host": "nas",
            "nas_containers": ["quant-stack-api"],
            "nas_isolated_evidence": "/data/evidence.json",
            "ss_kline_url": "http://127.0.0.1/history?period=1mo",
            "ss_kline_container": "backend",
            "provider_budget": str(budget),
            "work_root": str(tmp_path / "work"),
        }
    )

    assert result["ss_kline"]["last_date"] == "2026-08-14"  # type: ignore[index]
    assert result["operations_timer"] == {"enabled": True, "active": True}
    assert result["tailscale_service"] == {"enabled": True, "active": True}
    assert result["operator_linger"] is True
    assert result["weekly_reconciliation"] == {
        "timer": "quantstack-cn-operations-audit.timer",
        "scheduled": True,
    }
    assert len(remote_commands) == 3
    assert "'{{.Name}} {{if .State.Health}}" in remote_commands[0]
    assert remote_commands[1] == "cat /data/evidence.json"
    assert "'http://127.0.0.1/history?period=1mo'" in remote_commands[2]


def test_runtime_capture_local_mode_does_not_use_ssh(monkeypatch, tmp_path: Path) -> None:
    budget = tmp_path / "budget.json"
    _write(budget, {"blocked": None})
    observed: list[list[str]] = []

    def fake_run(command: list[str]) -> str:
        observed.append(command)
        if command[0] == "systemctl":
            if "cat" in command:
                return "OnCalendar=Fri *-*-* 22:30:00 Asia/Shanghai"
            return "enabled" if "is-enabled" in command else "active"
        if command[0] == "loginctl":
            return "yes"
        if command[:2] == ["docker", "inspect"]:
            return "/quant-stack-api healthy"
        if command[0] == "cat":
            return json.dumps({"first_import": "passed"})
        if command[:2] == ["docker", "exec"]:
            return json.dumps([{"date": "2026-08-21"}])
        raise AssertionError(command)

    monkeypatch.setattr(runtime_capture, "_run", fake_run)
    result = runtime_capture.capture(
        {
            "execution_mode": "local",
            "nas_host": "local",
            "nas_containers": ["quant-stack-api"],
            "nas_isolated_evidence": "/data/evidence.json",
            "ss_kline_url": "http://127.0.0.1/history",
            "ss_kline_container": "backend",
            "provider_budget": str(budget),
            "work_root": str(tmp_path / "work"),
            "tailscale_scope": "system",
            "tailscale_unit": "tailscaled.service",
        }
    )

    assert result["ss_kline"]["last_date"] == "2026-08-21"  # type: ignore[index]
    assert all(command[0] != "ssh" for command in observed)
    assert ["systemctl", "is-enabled", "tailscaled.service"] in observed


def test_runtime_capture_allows_local_scheduler_without_tailscale(
    monkeypatch, tmp_path: Path
) -> None:
    budget = tmp_path / "budget.json"
    _write(budget, {"blocked": None})

    def fake_run(command: list[str]) -> str:
        if command == ["crontab", "-l"]:
            return "\n".join(
                [
                    "# quantstack-daily-primary",
                    "0 18 * * 1-5 /usr/bin/flock -n /locks/daily /app/daily.sh",
                    "# quantstack-operations-audit",
                    "30 22 * * 5 /usr/bin/flock -n /locks/audit /app/audit.sh",
                ]
            )
        if command[0] == "systemctl":
            return "enabled" if "is-enabled" in command else "active"
        if command[:2] == ["docker", "inspect"]:
            return "/quant-stack-api healthy"
        if command[0] == "cat":
            return json.dumps({"first_import": "passed"})
        if command[:2] == ["docker", "exec"]:
            return json.dumps([{"date": "2026-08-21"}])
        raise AssertionError(command)

    monkeypatch.setattr(runtime_capture, "_run", fake_run)
    result = runtime_capture.capture(
        {
            "execution_mode": "local",
            "scheduler_backend": "user_cron",
            "nas_containers": ["quant-stack-api"],
            "nas_isolated_evidence": "/data/evidence.json",
            "ss_kline_url": "http://127.0.0.1/history",
            "ss_kline_container": "backend",
            "provider_budget": str(budget),
            "work_root": str(tmp_path / "work"),
            "tailscale_required": False,
            "daily_cron_schedule": "0 18 * * 1-5",
            "daily_cron_lock": "/locks/daily",
            "daily_cron_wrapper": "/app/daily.sh",
            "audit_cron_schedule": "30 22 * * 5",
            "audit_cron_lock": "/locks/audit",
            "audit_cron_wrapper": "/app/audit.sh",
        }
    )

    assert result["tailscale_required"] is False
    assert result["tailscale_service"] is None


def test_runtime_capture_supports_active_user_cron(monkeypatch, tmp_path: Path) -> None:
    budget = tmp_path / "budget.json"
    _write(budget, {"blocked": None})

    def fake_run(command: list[str]) -> str:
        if command == ["crontab", "-l"]:
            return "\n".join(
                [
                    "# quantstack-daily-primary",
                    "0 18 * * 1-5 /usr/bin/flock -n /locks/daily /app/daily.sh",
                    "# quantstack-operations-audit",
                    "30 22 * * 5 /usr/bin/flock -n /locks/audit /app/audit.sh",
                ]
            )
        if command[0] == "systemctl":
            return "enabled" if "is-enabled" in command else "active"
        if command[:2] == ["docker", "inspect"]:
            return "/quant-stack-api healthy"
        if command[0] == "cat":
            return json.dumps({"first_import": "passed"})
        if command[:2] == ["docker", "exec"]:
            return json.dumps([{"date": "2026-08-21"}])
        raise AssertionError(command)

    monkeypatch.setattr(runtime_capture, "_run", fake_run)
    result = runtime_capture.capture(
        {
            "execution_mode": "local",
            "scheduler_backend": "user_cron",
            "nas_containers": ["quant-stack-api"],
            "nas_isolated_evidence": "/data/evidence.json",
            "ss_kline_url": "http://127.0.0.1/history",
            "ss_kline_container": "backend",
            "provider_budget": str(budget),
            "work_root": str(tmp_path / "work"),
            "tailscale_scope": "system",
            "tailscale_unit": "tailscaled.service",
            "daily_cron_schedule": "0 18 * * 1-5",
            "daily_cron_lock": "/locks/daily",
            "daily_cron_wrapper": "/app/daily.sh",
            "audit_cron_schedule": "30 22 * * 5",
            "audit_cron_lock": "/locks/audit",
            "audit_cron_wrapper": "/app/audit.sh",
        }
    )

    assert result["scheduler_backend"] == "user_cron"
    assert result["scheduler_service"] == {"enabled": True, "active": True}
    assert result["daily_timer"] == {"enabled": True, "active": True}
    assert result["operations_timer"] == {"enabled": True, "active": True}
    assert result["operator_linger"] is None
    assert result["cron_entries"] == {
        "daily": {"present": True, "valid": True, "single_flight": True},
        "audit": {"present": True, "valid": True, "single_flight": True},
    }


def test_runtime_capture_rejects_marker_only_user_cron(monkeypatch, tmp_path: Path) -> None:
    budget = tmp_path / "budget.json"
    _write(budget, {"blocked": None})

    def fake_run(command: list[str]) -> str:
        if command == ["crontab", "-l"]:
            return "# quantstack-daily-primary\n# quantstack-operations-audit"
        if command[0] == "systemctl":
            return "enabled" if "is-enabled" in command else "active"
        if command[:2] == ["docker", "inspect"]:
            return "/quant-stack-api healthy"
        if command[0] == "cat":
            return json.dumps({"first_import": "passed"})
        if command[:2] == ["docker", "exec"]:
            return json.dumps([{"date": "2026-08-21"}])
        raise AssertionError(command)

    monkeypatch.setattr(runtime_capture, "_run", fake_run)
    result = runtime_capture.capture(
        {
            "execution_mode": "local",
            "scheduler_backend": "user_cron",
            "nas_containers": ["quant-stack-api"],
            "nas_isolated_evidence": "/data/evidence.json",
            "ss_kline_url": "http://127.0.0.1/history",
            "ss_kline_container": "backend",
            "provider_budget": str(budget),
            "work_root": str(tmp_path / "work"),
            "tailscale_required": False,
            "daily_cron_schedule": "0 18 * * 1-5",
            "daily_cron_lock": "/locks/daily",
            "daily_cron_wrapper": "/app/daily.sh",
            "audit_cron_schedule": "30 22 * * 5",
            "audit_cron_lock": "/locks/audit",
            "audit_cron_wrapper": "/app/audit.sh",
        }
    )

    assert result["daily_timer"] == {"enabled": False, "active": False}
    assert result["operations_timer"] == {"enabled": False, "active": False}
    assert result["cron_entries"] == {
        "daily": {"present": False, "valid": False, "single_flight": False},
        "audit": {"present": False, "valid": False, "single_flight": False},
    }


def test_operations_audit_accepts_user_cron_without_linger(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    qfq = tmp_path / "qfq.parquet"
    _bars(raw, "none")
    _bars(qfq, "qfq")
    factors = tmp_path / "factors.json"
    _write(factors, {"complete": False, "requests": []})
    runtime = tmp_path / "runtime.json"
    _write(
        runtime,
        {
            "scheduler_backend": "user_cron",
            "scheduler_service": {"enabled": True, "active": True},
            "operator_linger": None,
            "tailscale_required": False,
            "cron_entries": {
                "daily": {"valid": True, "single_flight": True},
                "audit": {"valid": True, "single_flight": True},
            },
        },
    )

    report = audit_cn_operations(
        work_root=tmp_path / "daily",
        expected_sessions=["2026-08-14"],
        raw_path=raw,
        qfq_path=qfq,
        factor_path=factors,
        runtime_evidence_path=runtime,
        through_date="2026-08-14",
    )

    assert "operator_linger" not in report["alerts"]
    assert "scheduler_service" not in report["alerts"]
    assert "tailscale_service" not in report["alerts"]
    assert "daily_cron_entry" not in report["alerts"]
    assert "audit_cron_entry" not in report["alerts"]
    assert "cron_single_flight" not in report["alerts"]


def test_cron_daily_wrapper_runs_promotion_after_daily_success() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "deploy/cron/quantstack-daily-chain.sh").read_text(encoding="utf-8")
    assert source.index("run_daily_incremental.py") < source.index("run_cn_qfq_session_daily.py")
    assert "set -euo pipefail" in source
