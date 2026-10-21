from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.run_r12_us_missing_session_candidate import recover_missing_session

ROOT = Path(__file__).parents[1]
SNAPSHOT = (
    ROOT
    / "src/quant_stack/data/providers/yfinance/"
    "us_equity_xnas_xnys_2026_08_27_v1.json"
)


def _fixture(tmp_path: Path) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    authority = tmp_path / "authority"
    config = {
        "schema_version": "quantstack.us-equity-session-daily.v1",
        "snapshot": str(SNAPSHOT),
        "state": str(authority / "state.json"),
        "initial_watermark": "2026-08-26",
        "work_root": str(authority / "work"),
        "session_parquet_root": str(authority / "sessions"),
        "yfinance_runtime_root": str(authority / "runtime"),
        "yfinance_proxy_url": "http://127.0.0.1:7890",
        "session_readiness_symbol": "SPY",
        "session_readiness_retry_seconds": 3600,
        "session_readiness_max_attempts": 8,
        "provider_threads": 8,
        "provider_batch_size": 128,
        "provider_batch_start_interval_seconds": 45,
        "market_index_root": str(authority / "indexes"),
    }
    config_path = tmp_path / "production.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    route = authority / "runtime/routes/explicit-proxy-v1"
    route.mkdir(parents=True)
    (route / "request.lock").write_text("", encoding="utf-8")
    (route / "last_request_at").write_text("0\n", encoding="utf-8")
    source = tmp_path / "source"
    (source / "scripts").mkdir(parents=True)
    (source / "scripts/run_r12_us_missing_session_candidate.py").write_text(
        "# receipt fixture\n", encoding="utf-8"
    )
    archive = tmp_path / "source.tar.gz"
    archive.write_bytes(b"fixed source archive")
    return {
        "production_config_path": config_path,
        "candidate_root": tmp_path / "candidate",
        "source_root": source,
        "source_commit": "a" * 40,
        "source_archive": archive,
        "source_archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }


def test_dry_run_has_no_side_effects_and_is_bound_to_fixed_session(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    result = recover_missing_session(session="2026-09-22", **arguments)

    assert result["mode"] == "dry_run"
    assert result["provider_calls"] == 0
    assert result["authority_written"] is False
    assert not Path(arguments["candidate_root"]).exists()
    assert result["candidate_config"]["initial_watermark"] == "2026-09-21"

    with pytest.raises(ValueError, match="outside the fixed R12 scope"):
        recover_missing_session(session="2026-09-21", **arguments)


def test_execute_writes_only_candidate_and_persists_zero_retry_budget(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    observed: list[dict[str, object]] = []

    def runner(config: dict[str, object]) -> dict[str, object]:
        observed.append(config)
        return {"status": "completed", "session": "2026-09-22", "rows": 5_000}

    result = recover_missing_session(
        session="2026-09-22",
        execute=True,
        caller_holds_operation_lock=True,
        runner=runner,
        **arguments,
    )
    candidate = Path(arguments["candidate_root"])
    config = observed[0]
    assert result["status"] == "requires_review"
    assert Path(str(config["state"])).is_relative_to(candidate)
    assert Path(str(config["work_root"])).is_relative_to(candidate)
    assert Path(str(config["session_parquet_root"])).is_relative_to(candidate)
    assert Path(str(config["market_index_root"])).is_relative_to(candidate)
    assert config["yfinance_runtime_root"].endswith("authority/runtime")
    assert config["provider_threads"] == 1
    budget = json.loads((candidate / "control/session-budget.json").read_text())
    assert budget["reserved_requests"] == 49
    assert budget["sessions"]["2026-09-22"]["status"] == "completed_requires_review"

    with pytest.raises(ValueError, match="zero-retry policy"):
        recover_missing_session(
            session="2026-09-22",
            execute=True,
            caller_holds_operation_lock=True,
            runner=runner,
            **arguments,
        )


def test_candidate_and_authority_must_not_overlap(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    production = json.loads(Path(arguments["production_config_path"]).read_text())
    arguments["candidate_root"] = Path(production["work_root"]).parent

    with pytest.raises(ValueError, match="overlap"):
        recover_missing_session(session="2026-09-22", **arguments)


def test_candidate_must_not_overlap_source_or_symlink_receipts(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    arguments["candidate_root"] = arguments["source_root"]
    with pytest.raises(ValueError, match="overlap"):
        recover_missing_session(session="2026-09-22", **arguments)

    arguments = _fixture(tmp_path / "second")
    candidate = Path(arguments["candidate_root"])
    candidate.mkdir()
    (candidate / "receipts").symlink_to(tmp_path / "escaped", target_is_directory=True)
    with pytest.raises(ValueError, match="must not be a symlink"):
        recover_missing_session(session="2026-09-22", **arguments)


def test_session_must_match_candidate_watermark_before_provider(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    with pytest.raises(ValueError, match="next fixed candidate session 2026-09-22"):
        recover_missing_session(session="2026-09-23", **arguments)


def test_execute_requires_outer_operation_lock_assertion(tmp_path: Path) -> None:
    arguments = _fixture(tmp_path)
    with pytest.raises(ValueError, match="caller_holds_operation_lock"):
        recover_missing_session(session="2026-09-22", execute=True, **arguments)
