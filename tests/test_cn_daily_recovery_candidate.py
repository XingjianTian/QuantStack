import fcntl
import hashlib
import importlib.util
import json
import sys
import tarfile
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/run_cn_daily_recovery_candidate.py"
_SPEC = importlib.util.spec_from_file_location("r12_cn_candidate_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
_TEST_SOURCE_COMMIT = "b" * 40
_SOURCE_TREE_IGNORES = frozenset({".git", ".pytest_cache", ".ruff_cache", "__pycache__"})


def _production_config(tmp_path: Path) -> Path:
    path = tmp_path / "daily-worker.json"
    payload = {
        "schema_version": "quantstack.cn-session-daily.v1",
        "repo_root": str(_ROOT),
        "python": sys.executable,
        "work_root": str(tmp_path / "production-work"),
        "session_parquet_root": str(tmp_path / "production-sessions"),
        "market_index_root": str(tmp_path / "market-indexes"),
        "calendar_raw": str(_ROOT / "config/cn_daily_calendar_2026_raw.json"),
        "calendar_normalized": str(_ROOT / "config/cn_daily_calendar_2026_normalized.json"),
        "calendar_raw_uri": "quant-data://evidence/calendar/raw.json",
        "calendar_normalized_uri": "quant-data://evidence/calendar/normalized.json",
        "calendar_source": "Shanghai Stock Exchange; Shenzhen Stock Exchange",
        "calendar_version": "cn-official-2026-v1",
        "environment_id": "test-production",
        "source_commit": "a" * 40,
        "catch_up_start_date": "2026-09-11",
        "batch_size": 500,
        "min_batch_interval_seconds": 5,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    control = tmp_path / "production-work/control"
    control.mkdir(parents=True)
    (tmp_path / "production-work/cron.lock").touch()
    (control / "baostock-request-budget.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "daily_limit": 49000,
                "hard_daily_limit": 50000,
                "blocked": None,
                "days": {},
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_source_archive(repo_root: Path, path: Path, source_commit: str) -> str:
    with tarfile.open(
        path,
        "w:gz",
        format=tarfile.PAX_FORMAT,
        pax_headers={"comment": source_commit},
    ) as archive:
        for item in sorted(repo_root.rglob("*")):
            relative = item.relative_to(repo_root)
            if _MODULE._private_source_path(_MODULE.PurePosixPath(relative.as_posix())):
                continue
            archive.add(item, arcname=relative, recursive=False)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_archive(
    tmp_path: Path, *, source_commit: str = _TEST_SOURCE_COMMIT
) -> tuple[Path, str, Path, str]:
    repo_root = tmp_path / "candidate-repo"
    (repo_root / "src/quant_stack").mkdir(parents=True)
    (repo_root / "scripts").mkdir()
    (repo_root / "src/quant_stack/__init__.py").write_text("", encoding="utf-8")
    (repo_root / "scripts/run_daily_incremental.py").write_text(
        "# frozen test source\n", encoding="utf-8"
    )
    path = tmp_path / "source.tar.gz"
    source_sha256 = _write_source_archive(repo_root, path, source_commit)
    return path, source_sha256, repo_root, source_commit


def test_source_archive_rejects_a_private_env_even_with_a_valid_digest(tmp_path: Path) -> None:
    archive_path, _, repo_root, commit = _source_archive(tmp_path)
    private = repo_root / ".env"
    private.write_text("QUANT_API_KEY=synthetic-private-value\n")
    with tarfile.open(archive_path, "w:gz", pax_headers={"comment": commit}) as archive:
        archive.add(private, arcname=".env")
    with pytest.raises(ValueError, match="private configuration"):
        _MODULE._verify_source_archive(
            archive_path,
            expected_sha256=hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            source_commit=commit,
            candidate_repo_root=repo_root,
        )


def test_local_source_fixture_does_not_package_credentials_or_venv(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".env").write_text("PRIVATE=synthetic-value")
    (repo / ".venv").mkdir()
    (repo / ".venv/private.txt").write_text("synthetic-runtime-value")
    (repo / ".env.example").write_text("PRIVATE=")
    archive_path = tmp_path / "source.tar.gz"
    _write_source_archive(repo, archive_path, _TEST_SOURCE_COMMIT)
    with tarfile.open(archive_path) as archive:
        assert archive.getnames() == [".env.example"]


@pytest.fixture(scope="module")
def runtime_source_archive(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, str, Path, str]:
    archive_root = tmp_path_factory.mktemp("runtime-source")
    path = archive_root / "source.tar.gz"
    source_sha256 = _write_source_archive(_ROOT, path, _TEST_SOURCE_COMMIT)
    return path, source_sha256, _ROOT, _TEST_SOURCE_COMMIT


def test_dry_run_binds_candidate_to_shared_budget_without_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runtime_source_archive: tuple[Path, str, Path, str],
) -> None:
    config = _production_config(tmp_path)
    source_archive, source_sha256, repo_root, source_commit = runtime_source_archive
    candidate = tmp_path / "candidate"
    observed_config = {}

    def fake_run(cfg, *, session_date, dry_run=False):
        observed_config["value"] = cfg
        return {
            "status": "dry_run",
            "session_date": session_date.isoformat(),
            "provider_requested": False,
        }

    monkeypatch.setattr(
        _MODULE,
        "run",
        fake_run,
    )
    result = _MODULE.execute(
        production_config_path=config,
        candidate_root=candidate,
        candidate_repo_root=repo_root,
        source_commit=source_commit,
        source_archive=source_archive,
        source_archive_sha256=source_sha256,
        cron_lock=tmp_path / "production-work/cron.lock",
        session=date(2026, 9, 23),
        dry_run=True,
    )
    assert result["status"] == "dry_run"
    assert result["provider_control_root"].endswith("production-work/control")
    assert result["candidate_repo_root"] == str(repo_root)
    assert result["source_commit"] == source_commit
    assert result["source_receipt"]["archive_commit"] == source_commit
    assert result["source_receipt"]["source_file_count"] > 2
    assert observed_config["value"].reauthentication_replay is False
    assert result["authoritative_session_root"].endswith("production-sessions")
    assert result["authority_written"] is False
    assert not candidate.exists()
    assert not (tmp_path / "production-sessions").exists()


def test_candidate_root_cannot_enter_authoritative_roots(tmp_path: Path) -> None:
    config_path = _production_config(tmp_path)
    config = _MODULE._load_config(config_path)
    for candidate in (
        tmp_path / "production-work/recovery",
        tmp_path / "production-sessions/recovery",
        tmp_path,
    ):
        with pytest.raises(ValueError, match="outside production"):
            _MODULE._validate_candidate_root(candidate, config)


def test_candidate_rejects_preexisting_output_symlink(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    (candidate / "work").symlink_to(tmp_path / "production-work", target_is_directory=True)
    with pytest.raises(ValueError, match="must not be a symlink"):
        _MODULE._validate_candidate_layout(candidate.resolve(), date(2026, 9, 23))


def test_candidate_rejects_out_of_scope_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _production_config(tmp_path)
    source_archive, source_sha256, repo_root, source_commit = _source_archive(tmp_path)
    with pytest.raises(ValueError, match="outside the frozen"):
        _MODULE.execute(
            production_config_path=config,
            candidate_root=tmp_path / "candidate",
            candidate_repo_root=repo_root,
            source_commit=source_commit,
            source_archive=source_archive,
            source_archive_sha256=source_sha256,
            cron_lock=tmp_path / "cron.lock",
            session=date(2026, 9, 30),
            dry_run=True,
        )


@pytest.mark.parametrize("kind", ["wrong_path", "symlink"])
def test_candidate_rejects_nonproduction_cron_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kind: str,
    runtime_source_archive: tuple[Path, str, Path, str],
) -> None:
    config = _production_config(tmp_path)
    source_archive, source_sha256, repo_root, source_commit = runtime_source_archive
    wrong_lock = tmp_path / "wrong.lock"
    if kind == "wrong_path":
        wrong_lock.touch()
    else:
        wrong_lock.symlink_to(tmp_path / "production-work/cron.lock")
    monkeypatch.setattr(
        _MODULE,
        "run",
        lambda *_args, **_kwargs: pytest.fail("runner must not start"),
    )
    with pytest.raises(ValueError, match="existing shared CN daily lock"):
        _MODULE.execute(
            production_config_path=config,
            candidate_root=tmp_path / "candidate",
            candidate_repo_root=repo_root,
            source_commit=source_commit,
            source_archive=source_archive,
            source_archive_sha256=source_sha256,
            cron_lock=wrong_lock,
            session=date(2026, 9, 23),
            dry_run=True,
        )


def test_candidate_rejects_source_root_other_than_running_wrapper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _production_config(tmp_path)
    source_archive, source_sha256, repo_root, source_commit = _source_archive(tmp_path)
    monkeypatch.setattr(
        _MODULE,
        "run",
        lambda *_args, **_kwargs: pytest.fail("runner must not start"),
    )
    with pytest.raises(ValueError, match="running wrapper source root"):
        _MODULE.execute(
            production_config_path=config,
            candidate_root=tmp_path / "candidate",
            candidate_repo_root=repo_root,
            source_commit=source_commit,
            source_archive=source_archive,
            source_archive_sha256=source_sha256,
            cron_lock=tmp_path / "production-work/cron.lock",
            session=date(2026, 9, 23),
            dry_run=True,
        )


def test_source_archive_rejects_commit_mismatch(tmp_path: Path) -> None:
    source_archive, source_sha256, repo_root, _ = _source_archive(tmp_path)
    with pytest.raises(ValueError, match="archive commit does not match"):
        _MODULE._verify_source_archive(
            source_archive,
            expected_sha256=source_sha256,
            source_commit="c" * 40,
            candidate_repo_root=repo_root,
        )


@pytest.mark.parametrize("kind", ["content", "extra_file"])
def test_source_archive_rejects_candidate_tree_mismatch(
    tmp_path: Path, kind: str
) -> None:
    source_archive, source_sha256, repo_root, source_commit = _source_archive(tmp_path)
    if kind == "content":
        (repo_root / "scripts/run_daily_incremental.py").write_text(
            "# modified after archive\n", encoding="utf-8"
        )
        match = "content does not match"
    else:
        (repo_root / "untracked.py").write_text("", encoding="utf-8")
        match = "file set does not match"
    with pytest.raises(ValueError, match=match):
        _MODULE._verify_source_archive(
            source_archive,
            expected_sha256=source_sha256,
            source_commit=source_commit,
            candidate_repo_root=repo_root,
        )


def test_budget_preflight_stops_before_provider(tmp_path: Path) -> None:
    control = tmp_path / "control"
    control.mkdir()
    today = _MODULE.datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    (control / "baostock-request-budget.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "daily_limit": 49000,
                "hard_daily_limit": 50000,
                "blocked": None,
                "days": {today: {"used": 48000}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="budget preflight failed"):
        _MODULE._budget_preflight(control, 11000)


def test_shared_cron_lock_is_nonblocking(tmp_path: Path) -> None:
    lock = tmp_path / "cron.lock"
    lock.touch()
    with lock.open("a+") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="already held"):
            with _MODULE._shared_cron_lock(lock):
                pass


def test_task_budget_persists_reservations_and_does_not_reset_on_restart(
    tmp_path: Path,
) -> None:
    sessions = (
        date(2026, 9, 23),
        date(2026, 9, 24),
        date(2026, 9, 28),
        date(2026, 9, 29),
    )
    for index, session in enumerate(sessions, start=1):
        reserved = _MODULE._reserve_task_budget(
            tmp_path, session=session, planned_requests=11_000
        )
        assert reserved["allocated_after"] == index * 11_000
    with pytest.raises(RuntimeError, match="already reserved"):
        _MODULE._reserve_task_budget(
            tmp_path, session=sessions[0], planned_requests=11_000
        )
    with pytest.raises(RuntimeError, match="task budget exhausted"):
        _MODULE._reserve_task_budget(
            tmp_path, session=date(2026, 9, 30), planned_requests=1
        )
    payload = json.loads(_MODULE._task_budget_path(tmp_path).read_text(encoding="utf-8"))
    assert payload["allocated"] == 44_000
    assert set(payload["reservations"]) == {item.isoformat() for item in sessions}


def test_shared_ledger_delta_counts_a_cross_midnight_run() -> None:
    before = {"days": {"2026-09-29": {"used": 10}}}
    after = {
        "days": {
            "2026-09-29": {"used": 12},
            "2026-09-30": {"used": 4},
        }
    }
    assert _MODULE._ledger_delta(before, after) == 6


def test_object_partition_uses_frozen_u_and_keeps_omissions_unverified(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "run"
    package = tmp_path / "package"
    run_root.mkdir()
    package.mkdir()
    (run_root / "instruments.json").write_text(
        json.dumps(
            {
                "instruments": [
                    {"instrument_id": "XSHG.600000"},
                    {"instrument_id": "XSHE.000001"},
                ]
            }
        ),
        encoding="utf-8",
    )
    connection = _MODULE.duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 'XSHG.600000' AS instrument_id) TO ? (FORMAT PARQUET)",
            [str(package / "bars.parquet")],
        )
    finally:
        connection.close()
    (package / "manifest.json").write_text(
        json.dumps({"omissions": ["XSHE.000001"]}), encoding="utf-8"
    )
    partition = _MODULE._object_partition(run_root, package)
    assert partition["union_equals_u"] is True
    assert partition["omission_semantics"] == "unverified_omission"
    assert partition["raw_complete"] is False
