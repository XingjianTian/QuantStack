import importlib.util
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_stack.data.session_parquet import install_session_package

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/run_daily_incremental.py"
_SPEC = importlib.util.spec_from_file_location("cn_session_daily_runner_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def _config(tmp_path: Path):
    return _MODULE.DailySchedulerConfig(
        schema_version="quantstack.cn-session-daily.v1",
        repo_root=_ROOT,
        python=Path("/usr/bin/python3"),
        work_root=tmp_path / "work",
        session_parquet_root=tmp_path / "sessions",
        calendar_raw=_ROOT
        / "tests/fixtures/exchanges/cn/calendar/2026-official-window-raw-v1.json",
        calendar_normalized=_ROOT
        / "tests/fixtures/exchanges/cn/calendar/2026-official-window-normalized-v1.json",
        calendar_raw_uri="quant-data://evidence/calendar/raw.json",
        calendar_normalized_uri="quant-data://evidence/calendar/normalized.json",
        calendar_source=(
            "Shanghai Stock Exchange; Shenzhen Stock Exchange; Beijing Stock Exchange"
        ),
        calendar_version="cn-official-2026-window-v1",
        environment_id="test-cn-session-daily",
        source_commit="a" * 40,
    )


def test_config_rejects_legacy_archive_and_hmac_fields(tmp_path: Path) -> None:
    payload = _config(tmp_path).model_dump(mode="json")
    payload["hmac_key_file"] = "/legacy/key"
    payload["remote_staging_root"] = "/legacy/staging"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        _MODULE.DailySchedulerConfig.model_validate(payload)


def test_config_keeps_fixed_baostock_batch_policy(tmp_path: Path) -> None:
    payload = _config(tmp_path).model_dump(mode="json")
    payload["batch_size"] = 100
    with pytest.raises(ValidationError):
        _MODULE.DailySchedulerConfig.model_validate(payload)


def test_non_session_skips_before_provider(tmp_path: Path) -> None:
    result = _MODULE.run(_config(tmp_path), session_date=date(2026, 5, 2), dry_run=True)
    assert result["status"] == "skipped"
    assert result["provider_requested"] is False


def test_completed_session_requires_current_summary_and_readable_package(tmp_path: Path) -> None:
    config = _config(tmp_path)
    session = date(2026, 5, 7)
    summary = config.work_root / "daily-20260507/runner-summary.json"
    summary.parent.mkdir(parents=True)

    summary.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "status": "completed",
                "session_date": session.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    assert _MODULE._completed_session(config, session) is False

    summary.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-session-daily-run.v1",
                "status": "completed",
                "session_date": session.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    assert _MODULE._completed_session(config, session) is False

    install_session_package(
        records=[
            {
                "instrument_id": "XSHG.600000",
                "trading_date": session,
                "market": "CN",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100.0,
            }
        ],
        output_root=config.session_parquet_root,
        market="CN",
        session=session,
        dataset_id="cn_equity_daily",
        version=f"{session.isoformat()}.session-v1",
        adjustment="raw",
    )
    assert _MODULE._completed_session(config, session) is True


def test_calendar_preflight_uses_runtime_close_gate(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(
        update={
            "calendar_raw": _ROOT / "config/cn_daily_calendar_2026_raw.json",
            "calendar_normalized": _ROOT / "config/cn_daily_calendar_2026_normalized.json",
            "calendar_source": "Shanghai Stock Exchange; Shenzhen Stock Exchange",
            "calendar_version": "cn-official-2026-v1",
        }
    )
    cn = _MODULE._CN_TIMEZONE
    assert _MODULE._calendar_preflight(
        config,
        date(2026, 8, 11),
        now=datetime(2026, 8, 11, 18, 0, tzinfo=cn),
    )[0]
    with pytest.raises(ValueError, match="before 18:00"):
        _MODULE._calendar_preflight(
            config,
            date(2026, 8, 11),
            now=datetime(2026, 8, 11, 17, 59, tzinfo=cn),
        )


def test_routine_command_has_session_cache_and_no_archive_or_hmac(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    commands: list[list[str]] = []
    environments: list[dict[str, str]] = []
    monkeypatch.setattr(_MODULE, "_calendar_preflight", lambda *_args: (True, "ready"))

    def capture(command, **_kwargs):
        commands.append(command)
        environments.append(_kwargs["env"])
        if "run-live-full-market" in command:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(_MODULE.subprocess, "run", capture)
    with pytest.raises(subprocess.CalledProcessError):
        _MODULE.run(config, session_date=date(2026, 8, 17))

    live = next(item for item in commands if "run-live-full-market" in item)
    assert "--session-parquet-root" in live
    assert "--session-cache-root" in live
    assert "--archive-root" not in live
    assert "--archive-manifest" not in live
    assert "--hmac-key-file" not in live
    assert environments[-1]["PYTHONPATH"] == f"{_ROOT / 'src'}:{_ROOT}"


def test_recovery_config_can_share_provider_control_without_sharing_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shared_control = tmp_path / "production-control"
    config = _config(tmp_path).model_copy(update={"provider_control_root": shared_control})
    commands: list[list[str]] = []
    monkeypatch.setattr(_MODULE, "_calendar_preflight", lambda *_args: (True, "ready"))

    def capture(command, **_kwargs):
        commands.append(command)
        if "run-live-full-market" in command:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(_MODULE.subprocess, "run", capture)
    with pytest.raises(subprocess.CalledProcessError):
        _MODULE.run(config, session_date=date(2026, 8, 17))

    provider_roots = [
        command[command.index("--provider-control-root") + 1]
        for command in commands
        if "--provider-control-root" in command
    ]
    assert provider_roots == [str(shared_control), str(shared_control)]
    live = next(item for item in commands if "run-live-full-market" in item)
    assert live[live.index("--session-parquet-root") + 1] == str(config.session_parquet_root)


def test_checked_in_entrypoints_use_the_session_only_runner() -> None:
    cron = (_ROOT / "deploy/cron/quantstack-daily-chain.sh").read_text(encoding="utf-8")
    service = (_ROOT / "deploy/systemd/quantstack-daily.service").read_text(encoding="utf-8")
    assert "run_daily_incremental.py" in cron
    assert "run_daily_incremental.py" in service
    assert "run_daily_archive_legacy.py" not in cron + service
    assert '-v "${QUANTSTACK_QFQ_REPO}:${QUANTSTACK_QFQ_REPO}:ro"' in cron
    assert '-e "PYTHONPATH=${QUANTSTACK_QFQ_REPO}/src:${QUANTSTACK_QFQ_REPO}"' in cron
    assert not list((_ROOT / "deploy/cron").glob("*core*"))


def test_example_configs_contain_no_daily_hash_or_archive_fields() -> None:
    for relative in (
        "config/daily_scheduler.example.json",
        "config/us_equity_daily_nas.example.json",
    ):
        payload = json.loads((_ROOT / relative).read_text(encoding="utf-8"))
        assert not {
            "hmac_key_file",
            "remote_hmac_key_file",
            "archive_root",
            "local_import_workers",
            "verification_mode",
            "nas_verification_mode",
        }.intersection(payload)
