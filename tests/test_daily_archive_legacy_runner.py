import importlib.util
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/run_daily_archive_legacy.py"
_SPEC = importlib.util.spec_from_file_location("daily_incremental_runner_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def _config(tmp_path: Path):
    key = tmp_path / "worker.key"
    key.write_bytes(b"k" * 32)
    return _MODULE.DailySchedulerConfig(
        repo_root=_ROOT,
        python=Path("/usr/bin/python3"),
        work_root=tmp_path / "work",
        calendar_raw=_ROOT
        / "tests/fixtures/exchanges/cn/calendar/2026-official-window-raw-v1.json",
        calendar_normalized=_ROOT
        / "tests/fixtures/exchanges/cn/calendar/2026-official-window-normalized-v1.json",
        calendar_raw_uri="quant-data://evidence/calendar/2026-official-window-raw-v1.json",
        calendar_normalized_uri=(
            "quant-data://evidence/calendar/2026-official-window-normalized-v1.json"
        ),
        calendar_source=(
            "Shanghai Stock Exchange; Shenzhen Stock Exchange; Beijing Stock Exchange"
        ),
        calendar_version="cn-official-2026-window-v1",
        hmac_key_file=key,
        environment_id="test-daily-worker",
        remote_host="nas-test-alias",
        remote_staging_root="/isolated/daily",
        remote_repo_root="/opt/quantstack",
        remote_python="/opt/quantstack/.venv/bin/python",
        remote_data_root="/isolated/daily/imported",
        remote_hmac_key_file="/secure/worker.key",
    )


def _full_year_config(tmp_path: Path):
    return _config(tmp_path).model_copy(
        update={
            "calendar_raw": _ROOT / "config/cn_daily_calendar_2026_raw.json",
            "calendar_normalized": _ROOT / "config/cn_daily_calendar_2026_normalized.json",
            "calendar_raw_uri": "quant-data://evidence/calendar/cn-official-2026-v1-raw.json",
            "calendar_normalized_uri": (
                "quant-data://evidence/calendar/cn-official-2026-v1-normalized.json"
            ),
            "calendar_source": "Shanghai Stock Exchange; Shenzhen Stock Exchange",
            "calendar_version": "cn-official-2026-v1",
        }
    )


def test_daily_runner_skips_verified_non_session_before_provider_or_git(tmp_path: Path) -> None:
    result = _MODULE.run(_config(tmp_path), session_date=date(2026, 5, 2), dry_run=True)
    assert result == {
        "schema_version": "1.0.0",
        "status": "skipped",
        "session_date": "2026-05-02",
        "reason": "session date is not one verified complete trading session",
        "provider_requested": False,
        "nas_written": False,
    }


def test_daily_runner_operational_values_are_fixed() -> None:
    with pytest.raises(ValidationError):
        _MODULE.DailySchedulerConfig.model_validate(
            {
                "batch_size": 50,
                "min_batch_interval_seconds": 30,
            }
        )


def test_lightweight_nas_mode_keeps_readback_and_defers_full_replay(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"nas_verification_mode": "readback_only"})
    assert config.nas_verification_mode == "readback_only"
    assert _MODULE._nas_delivery_is_valid(
        first={
            "imported_archives": 2,
            "new_imports": 2,
            "idempotent_replays": 0,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
        replay=None,
        verification={
            "verified_receipts": 2,
            "persisted_bar_rows": 100,
            "duplicate_keys": 0,
            "source_commit": "a" * 40,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
        expected=2,
        expected_rows=100,
        source_commit="a" * 40,
        mode=config.nas_verification_mode,
    )


def test_full_replay_mode_still_requires_the_second_import(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.nas_verification_mode == "full_replay"
    assert not _MODULE._nas_delivery_is_valid(
        first={
            "imported_archives": 1,
            "new_imports": 1,
            "idempotent_replays": 0,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
        replay=None,
        verification={
            "verified_receipts": 1,
            "persisted_bar_rows": 50,
            "duplicate_keys": 0,
            "source_commit": "a" * 40,
            "catalog_published": False,
            "production_pointer_updated": False,
        },
        expected=1,
        expected_rows=50,
        source_commit="a" * 40,
        mode=config.nas_verification_mode,
    )


def test_calendar_uses_runtime_observation_after_static_evidence_fetch(tmp_path: Path) -> None:
    config = _full_year_config(tmp_path)
    cn = _MODULE._CN_TIMEZONE
    assert _MODULE._calendar_preflight(
        config,
        date(2026, 8, 11),
        now=datetime(2026, 8, 11, 18, 0, tzinfo=cn),
    ) == (True, "verified complete trading session")
    with pytest.raises(ValueError, match="before 18:00"):
        _MODULE._calendar_preflight(
            config,
            date(2026, 8, 11),
            now=datetime(2026, 8, 11, 17, 59, tzinfo=cn),
        )


def test_pending_sessions_are_oldest_first_and_skip_completed_and_weekends(
    tmp_path: Path,
) -> None:
    config = _full_year_config(tmp_path).model_copy(
        update={"catch_up_start_date": date(2026, 8, 7)}
    )
    completed = config.work_root / "daily-20260810" / "runner-summary.json"
    completed.parent.mkdir(parents=True)
    completed.write_text(json.dumps({"status": "completed"}), encoding="utf-8")

    sessions = _MODULE._pending_sessions(
        config,
        through_date=date(2026, 8, 12),
        now=datetime(2026, 8, 12, 18, 0, tzinfo=_MODULE._CN_TIMEZONE),
    )

    assert sessions == (date(2026, 8, 7), date(2026, 8, 11), date(2026, 8, 12))


def test_pending_sessions_before_release_skip_today_but_keep_closed_history(
    tmp_path: Path,
) -> None:
    config = _full_year_config(tmp_path).model_copy(
        update={"catch_up_start_date": date(2026, 8, 7)}
    )

    sessions = _MODULE._pending_sessions(
        config,
        through_date=date(2026, 8, 12),
        now=datetime(2026, 8, 12, 12, 30, tzinfo=_MODULE._CN_TIMEZONE),
    )

    assert sessions == (date(2026, 8, 7), date(2026, 8, 10), date(2026, 8, 11))


def test_catch_up_stops_at_first_failed_session(monkeypatch, tmp_path: Path, capsys) -> None:
    config = _config(tmp_path).model_copy(update={"catch_up_start_date": date(2026, 8, 7)})
    called: list[date] = []

    monkeypatch.setattr(_MODULE, "_load_config", lambda _: config)
    monkeypatch.setattr(
        _MODULE,
        "_pending_sessions",
        lambda *args, **kwargs: (date(2026, 8, 7), date(2026, 8, 10), date(2026, 8, 11)),
    )

    def fake_run(config, *, session_date, dry_run=False):
        called.append(session_date)
        if session_date == date(2026, 8, 10):
            raise RuntimeError("bounded failure")
        return {"status": "completed", "session_date": session_date.isoformat()}

    monkeypatch.setattr(_MODULE, "run", fake_run)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--config", str(tmp_path / "config.json")])

    assert _MODULE.main() == 2
    assert called == [date(2026, 8, 7), date(2026, 8, 10)]
    assert "bounded failure" in capsys.readouterr().err


def test_session_runs_share_provider_control_but_keep_data_isolated(
    monkeypatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    commands: list[tuple[list[str], dict[str, str] | None]] = []

    monkeypatch.setattr(_MODULE, "_calendar_preflight", lambda *_args: (True, "ready"))
    monkeypatch.setattr(_MODULE, "_source_commit", lambda _config: "a" * 40)
    monkeypatch.setattr(_MODULE, "_remote_preflight", lambda _config: None)

    def capture(command, **kwargs):
        commands.append((command, kwargs.get("env")))
        if "run-live-full-market" in command:
            raise RuntimeError("stop after command capture")
        return None

    monkeypatch.setattr(_MODULE, "_run", capture)
    for session_date in (date(2026, 8, 7), date(2026, 8, 10)):
        with pytest.raises(RuntimeError, match="command capture"):
            _MODULE.run(config, session_date=session_date)

    universe_commands = [item for item in commands if "build-live-universe" in item[0]]
    live_commands = [item for item in commands if "run-live-full-market" in item[0]]
    assert len(universe_commands) == 2
    assert len(live_commands) == 2
    control_roots = {
        command[command.index("--provider-control-root") + 1]
        for command, _env in [*universe_commands, *live_commands]
    }
    data_roots = {command[command.index("--data-root") + 1] for command, _env in live_commands}
    state_roots = {command[command.index("--state-root") + 1] for command, _env in live_commands}
    assert control_roots == {str(config.work_root / "control")}
    assert len(data_roots) == 2
    assert len(state_roots) == 2
    for _command, env in [*universe_commands, *live_commands]:
        assert env is not None
        assert env["QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS"] == "5"


def test_session_parquet_mode_does_not_request_archives_or_hmac(
    monkeypatch, tmp_path: Path
) -> None:
    config = _config(tmp_path).model_copy(
        update={"session_parquet_root": tmp_path / "session-parquet"}
    )
    commands: list[list[str]] = []
    monkeypatch.setattr(_MODULE, "_calendar_preflight", lambda *_args: (True, "ready"))
    monkeypatch.setattr(_MODULE, "_source_commit", lambda _config: "a" * 40)
    monkeypatch.setattr(_MODULE, "_remote_preflight", lambda _config: None)

    def capture(command, **_kwargs):
        commands.append(command)
        if "run-live-full-market" in command:
            raise RuntimeError("captured")
        return None

    monkeypatch.setattr(_MODULE, "_run", capture)
    with pytest.raises(RuntimeError, match="captured"):
        _MODULE.run(config, session_date=date(2026, 8, 17))

    live = next(command for command in commands if "run-live-full-market" in command)
    assert live[live.index("--session-parquet-root") + 1] == str(config.session_parquet_root)
    assert "--archive-root" not in live
    assert "--archive-manifest" not in live
    assert "--hmac-key-file" not in live


def test_completed_acquisition_resumes_after_provider_without_new_live_request(
    monkeypatch, tmp_path: Path
) -> None:
    run_root = tmp_path / "work/daily-20260817"
    archives = run_root / "archives"
    archives.mkdir(parents=True)
    archive = archives / "task-a.tar"
    archive.write_bytes(b"signed archive")
    manifest = {
        "schema_version": "1.0.0",
        "dataset_version": "daily-2026-08-17",
        "session_date": "2026-08-17",
        "universe_version": "2026-08-17.test",
        "generated_at": "2026-08-17T10:30:00Z",
        "entries": [
            {
                "task_id": "task-a",
                "requested_instruments": 1,
                "records_received": 1,
                "publication_bundle_uri": "quant-data://bundle/a",
                "worker_bundle_id": "bundle-a",
                "archive_relative_path": "archives/task-a.tar",
                "archive_sha256": "a" * 64,
                "archive_size_bytes": archive.stat().st_size,
            }
        ],
        "provider_snapshot_rows": 1,
        "active_universe_instruments": 1,
        "inactive_a_share_observations": 0,
        "planned_batches": 1,
        "succeeded_batches": 1,
        "failed_batches": 0,
        "requested_instruments": 1,
        "records_received": 1,
        "unresolved_instruments": 0,
        "duplicate_keys": 0,
        "duration_seconds": 1,
        "archive_size_bytes": archive.stat().st_size,
        "canonical_eligible": False,
        "production_pointer_updated": False,
    }
    (run_root / "archive-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (run_root / "run-report.json").write_text(
        json.dumps(
            {
                "session_date": "2026-08-17",
                "succeeded_batches": 1,
                "retryable_failed_batches": 0,
                "permanently_failed_batches": 0,
                "cancelled_batches": 0,
                "quarantined_batches": 0,
            }
        ),
        encoding="utf-8",
    )
    hmac_key = tmp_path / "worker.key"
    hmac_key.write_bytes(b"k" * 32)
    envelope = type("Envelope", (), {"source_commit": "b" * 40})()
    monkeypatch.setattr(_MODULE, "_read_verified_archive", lambda *_args: (envelope, {}))

    resumed = _MODULE._completed_acquisition(
        run_root=run_root,
        session_date=date(2026, 8, 17),
        manifest_path=run_root / "archive-manifest.json",
        report_path=run_root / "run-report.json",
        hmac_key_file=hmac_key,
    )

    assert resumed is not None
    manifest_model, source_commit = resumed
    assert manifest_model.session_date == date(2026, 8, 17)
    assert source_commit == "b" * 40


def test_incomplete_saved_acquisition_fails_closed(tmp_path: Path) -> None:
    run_root = tmp_path / "work/daily-20260817"
    run_root.mkdir(parents=True)
    (run_root / "archive-manifest.json").write_text("{}", encoding="utf-8")
    (run_root / "run-report.json").write_text("{}", encoding="utf-8")

    with pytest.raises((RuntimeError, ValidationError)):
        _MODULE._completed_acquisition(
            run_root=run_root,
            session_date=date(2026, 8, 17),
            manifest_path=run_root / "archive-manifest.json",
            report_path=run_root / "run-report.json",
            hmac_key_file=tmp_path / "worker.key",
        )


def test_systemd_timer_uses_cn_release_gate() -> None:
    timer = (_ROOT / "deploy/systemd/quantstack-daily.timer").read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 18:00:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer


def test_remote_preflight_is_before_the_provider_request() -> None:
    source = _SCRIPT.read_text(encoding="utf-8")
    run_body = source[source.index("def run(") : source.index("\ndef main()")]
    assert run_body.index("_remote_preflight(config)") < run_body.index('"build-live-universe"')
    assert "ConnectTimeout=20" in source
    assert "attempts=3" in source


def test_local_execution_uses_local_shell_and_atomic_staging(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"execution_mode": "local"})
    assert _MODULE._ssh_command(config, "true") == ["/bin/sh", "-c", "true"]
    command = _MODULE._transfer_command(
        config,
        archive=tmp_path / "archive.tar",
        report=tmp_path / "report.json",
    )
    assert "--local-staging-root" in command
    assert "--remote-host" not in command
    assert "ssh" not in command


def test_local_preflight_initializes_isolated_destinations(monkeypatch, tmp_path: Path) -> None:
    staging = tmp_path / "isolated/staging"
    imported = tmp_path / "isolated/imported"
    config = _config(tmp_path).model_copy(
        update={
            "execution_mode": "local",
            "remote_staging_root": str(staging),
            "remote_data_root": str(imported),
            "remote_repo_root": str(_ROOT),
            "remote_python": sys.executable,
            "remote_hmac_key_file": str(_config(tmp_path).hmac_key_file),
        }
    )
    monkeypatch.setattr(_MODULE, "_run", lambda *args, **kwargs: None)

    _MODULE._remote_preflight(config)

    assert staging.is_dir()
    assert imported.is_dir()


def test_remote_execution_remains_the_default(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.execution_mode == "remote_ssh"
    command = _MODULE._transfer_command(
        config,
        archive=tmp_path / "archive.tar",
        report=tmp_path / "report.json",
    )
    assert command[command.index("--remote-host") + 1] == "nas-test-alias"
    assert "--local-staging-root" not in command


def test_exact_source_check_does_not_ignore_untracked_files() -> None:
    source = _SCRIPT.read_text(encoding="utf-8")
    assert '"status", "--porcelain"' in source
    assert "--untracked-files=no" not in source


def test_immutable_deployment_uses_explicit_exact_source_commit(
    monkeypatch, tmp_path: Path
) -> None:
    config = _config(tmp_path).model_copy(update={"source_commit": "f" * 40})

    def unexpected_run(*_args, **_kwargs):
        raise AssertionError("explicit immutable source binding must not call git")

    monkeypatch.setattr(_MODULE, "_run", unexpected_run)
    assert _MODULE._source_commit(config) == "f" * 40
