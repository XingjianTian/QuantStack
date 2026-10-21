from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/run_us_equity_session_daily_nas.py"
_SPEC = importlib.util.spec_from_file_location("us_session_daily_config_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def _payload(tmp_path: Path) -> dict[str, object]:
    return {
        "schema_version": "quantstack.us-equity-session-daily.v1",
        "snapshot": str(tmp_path / "snapshot.json"),
        "state": str(tmp_path / "state.json"),
        "initial_watermark": "2026-08-28",
        "work_root": str(tmp_path / "work"),
        "session_parquet_root": str(tmp_path / "sessions"),
        "market_index_root": str(tmp_path / "market-indexes"),
        "yfinance_runtime_root": str(tmp_path / "runtime"),
        "yfinance_proxy_url": "http://127.0.0.1:7890",
        "session_readiness_symbol": "SPY",
        "session_readiness_retry_seconds": 3600,
        "session_readiness_max_attempts": 8,
        "provider_threads": 48,
        "provider_batch_size": 128,
        "provider_batch_start_interval_seconds": 45,
    }


def test_load_config_accepts_only_session_daily_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(_payload(tmp_path)), encoding="utf-8")
    config = _MODULE.load_config(path)
    assert config["schema_version"] == "quantstack.us-equity-session-daily.v1"
    assert config["provider_threads"] == 48
    assert config["provider_batch_start_interval_seconds"] == 45


@pytest.mark.parametrize(
    "legacy_field",
    ["hmac_key_file", "archive_root", "local_import_workers", "verification_mode"],
)
def test_load_config_rejects_legacy_fields(tmp_path: Path, legacy_field: str) -> None:
    payload = _payload(tmp_path)
    payload[legacy_field] = "unused"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="legacy fields"):
        _MODULE.load_config(path)


def test_routine_entrypoints_do_not_import_hash_or_archive_modules() -> None:
    for relative in (
        "scripts/run_daily_incremental.py",
        "scripts/run_us_equity_session_daily_nas.py",
    ):
        source = (_ROOT / relative).read_text(encoding="utf-8")
        assert "import hashlib" not in source
        assert "import hmac" not in source
        assert "worker_bundle" not in source
        assert "run_daily_archive_legacy" not in source
