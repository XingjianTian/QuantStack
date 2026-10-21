import importlib.util
import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from quant_stack.daily_cli import _assert_daily_release_gate, _parser, main, report_exit_code
from quant_stack.data.daily import DailyRunPlan, DailyRunReport
from quant_stack.data.providers.cn.live import BaoStockRequestBudget
from quant_stack.data.providers.cn.replay import AKSHARE_REPLAY_RUNTIME_CONTRACT
from quant_stack.data.worker_bundle import import_worker_archive

_IMPORT_MANIFEST_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "import_daily_archive_manifest.py"
)
_IMPORT_MANIFEST_SPEC = importlib.util.spec_from_file_location(
    "import_daily_archive_manifest_test", _IMPORT_MANIFEST_SCRIPT
)
assert _IMPORT_MANIFEST_SPEC is not None and _IMPORT_MANIFEST_SPEC.loader is not None
_IMPORT_MANIFEST_MODULE = importlib.util.module_from_spec(_IMPORT_MANIFEST_SPEC)
_IMPORT_MANIFEST_SPEC.loader.exec_module(_IMPORT_MANIFEST_MODULE)
_VERIFY_IMPORT_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_daily_import.py"
_VERIFY_IMPORT_SPEC = importlib.util.spec_from_file_location(
    "verify_daily_import_test", _VERIFY_IMPORT_SCRIPT
)
assert _VERIFY_IMPORT_SPEC is not None and _VERIFY_IMPORT_SPEC.loader is not None
_VERIFY_IMPORT_MODULE = importlib.util.module_from_spec(_VERIFY_IMPORT_SPEC)
_VERIFY_IMPORT_SPEC.loader.exec_module(_VERIFY_IMPORT_MODULE)


def test_daily_release_gate_blocks_current_session_before_1800_cn() -> None:
    cn = ZoneInfo("Asia/Shanghai")
    with pytest.raises(ValueError, match="before 18:00"):
        _assert_daily_release_gate(
            date(2026, 8, 10),
            now=datetime(2026, 8, 10, 17, 59, 59, tzinfo=cn),
        )
    _assert_daily_release_gate(
        date(2026, 8, 10),
        now=datetime(2026, 8, 10, 18, 0, tzinfo=cn),
    )
    _assert_daily_release_gate(
        date(2026, 5, 7),
        now=datetime(2026, 8, 10, 9, 0, tzinfo=cn),
    )


def test_live_cli_reauth_replay_disable_flag_is_explicit_and_defaults_off() -> None:
    base = [
        "run-live-full-market",
        "plan.json",
        "--instruments",
        "instruments.json",
        "--universe-snapshot",
        "universe.json",
        "--calendar-raw",
        "calendar-raw.json",
        "--calendar-normalized",
        "calendar-normalized.json",
        "--calendar-raw-uri",
        "quant-data://calendar/raw.json",
        "--calendar-normalized-uri",
        "quant-data://calendar/normalized.json",
        "--data-root",
        "data",
        "--state-root",
        "state",
        "--report",
        "report.json",
        "--source-commit",
        "a" * 40,
        "--environment-id",
        "test",
    ]

    assert _parser().parse_args(base).disable_expired_session_reauth_replay is False
    assert (
        _parser()
        .parse_args([*base, "--disable-10001001-reauth-query-replay"])
        .disable_expired_session_reauth_replay
        is True
    )


def _spec(path: Path) -> None:
    contract = AKSHARE_REPLAY_RUNTIME_CONTRACT
    path.write_text(
        json.dumps(
            {
                "session": {
                    "session_date": "2026-07-28",
                    "calendar_source": "frozen-exchange-calendar",
                    "calendar_version": "fixture-v1",
                    "evidence_ref": "tests/fixtures/calendar/cn-session-2026-07-28.json",
                },
                "universe": {
                    "universe_id": "cn-daily-cli-fixture",
                    "version": "fixture-v1",
                    "instrument_ids": ["XSHG.600000", "XSHE.000001", "BJSE.430047"],
                    "evidence_ref": "src/quant_stack/data/providers/cn/replay/fixtures",
                },
                "provider_contract": {
                    "provider": contract.provider,
                    "provider_version": contract.provider_version,
                    "endpoint": contract.endpoint,
                    "adapter_version": contract.adapter_version,
                    "native_schema_version": contract.native_schema_version,
                    "batch_size": 3,
                    "max_attempts": 2,
                },
                "created_at": "2026-08-06T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )


def test_daily_cli_builds_and_replays_saved_plan_with_restart_dedup(tmp_path: Path, capsys) -> None:
    spec = tmp_path / "spec.json"
    plan_path = tmp_path / "plan.json"
    first_report_path = tmp_path / "first-report.json"
    second_report_path = tmp_path / "second-report.json"
    _spec(spec)

    assert main(["build-plan", str(spec), "--output", str(plan_path)]) == 0
    plan = DailyRunPlan.model_validate_json(plan_path.read_text(encoding="utf-8"))
    assert plan.dataset_version == "daily-2026-07-28-fixture-v1"
    assert len(plan.tasks) == 1

    common = [
        "run-replay",
        str(plan_path),
        "--data-root",
        str(tmp_path / "data"),
        "--state-root",
        str(tmp_path / "state"),
    ]
    assert main([*common, "--report", str(first_report_path)]) == 0
    assert main([*common, "--report", str(second_report_path)]) == 0

    first = DailyRunReport.model_validate_json(first_report_path.read_text(encoding="utf-8"))
    second = DailyRunReport.model_validate_json(second_report_path.read_text(encoding="utf-8"))
    assert first.succeeded_batches == second.succeeded_batches == 1
    assert first.deduplicated_batches == 0
    assert second.deduplicated_batches == 1
    assert second.publication_bundle_uris == first.publication_bundle_uris
    assert second.canonical_eligible is False
    assert second.production_pointer_updated is False
    assert capsys.readouterr().err == ""


def test_daily_cli_rejects_non_replay_plan_before_creating_roots(tmp_path: Path, capsys) -> None:
    spec = tmp_path / "spec.json"
    plan_path = tmp_path / "plan.json"
    _spec(spec)
    assert main(["build-plan", str(spec), "--output", str(plan_path)]) == 0
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["provider_contract"]["provider"] = "akshare_live"
    plan_path.write_text(json.dumps(payload), encoding="utf-8")

    data_root = tmp_path / "data"
    state_root = tmp_path / "state"
    assert (
        main(
            [
                "run-replay",
                str(plan_path),
                "--data-root",
                str(data_root),
                "--state-root",
                str(state_root),
                "--report",
                str(tmp_path / "report.json"),
            ]
        )
        == 2
    )
    assert not data_root.exists()
    assert not state_root.exists()
    assert "provider contract" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider", "akshare_live"),
        ("provider_version", "tampered-version"),
        ("endpoint", "tampered-endpoint"),
        ("adapter_version", "tampered-adapter"),
        ("native_schema_version", "tampered-schema"),
    ],
)
def test_daily_cli_rejects_task_provider_mismatch_without_writes(
    tmp_path: Path, capsys, field: str, value: str
) -> None:
    spec = tmp_path / "spec.json"
    plan_path = tmp_path / "plan.json"
    _spec(spec)
    assert main(["build-plan", str(spec), "--output", str(plan_path)]) == 0
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["tasks"][0][field] = value
    plan_path.write_text(json.dumps(payload), encoding="utf-8")

    data_root = tmp_path / "data"
    state_root = tmp_path / "state"
    report_path = tmp_path / "report.json"
    assert (
        main(
            [
                "run-replay",
                str(plan_path),
                "--data-root",
                str(data_root),
                "--state-root",
                str(state_root),
                "--report",
                str(report_path),
            ]
        )
        == 2
    )
    assert not data_root.exists()
    assert not state_root.exists()
    assert not report_path.exists()
    assert "provider contract" in capsys.readouterr().err


def test_daily_cli_exit_codes_are_stable() -> None:
    base = DailyRunReport.model_validate(
        {
            "dataset_version": "daily-2026-07-28-fixture-v1",
            "session_date": "2026-07-28",
            "universe_version": "fixture-v1",
            "started_at": "2026-08-06T00:00:00Z",
            "completed_at": "2026-08-06T00:00:01Z",
            "batches": [],
            "succeeded_batches": 0,
            "deduplicated_batches": 0,
            "retryable_failed_batches": 0,
            "permanently_failed_batches": 0,
            "quarantined_batches": 0,
            "cancelled_batches": 0,
            "publication_bundle_uris": [],
        }
    )
    assert report_exit_code(base) == 0
    assert report_exit_code(base.model_copy(update={"retryable_failed_batches": 1})) == 75
    assert report_exit_code(base.model_copy(update={"permanently_failed_batches": 1})) == 20
    assert report_exit_code(base.model_copy(update={"quarantined_batches": 1})) == 30
    assert report_exit_code(base.model_copy(update={"cancelled_batches": 1})) == 40


_ROOT = Path(__file__).parents[1]
_LIVE_SPEC = _ROOT / "config/cn_daily_live_sample_plan_v1.json"
_LIVE_INSTRUMENTS = _ROOT / "config/cn_daily_live_sample_v1.json"
_CALENDAR_RAW = _ROOT / "tests/fixtures/exchanges/cn/calendar/2026-official-window-raw-v1.json"
_CALENDAR_NORMALIZED = (
    _ROOT / "tests/fixtures/exchanges/cn/calendar/2026-official-window-normalized-v1.json"
)


def _live_args(
    tmp_path: Path,
    plan_path: Path,
    key_path: Path,
    *,
    instruments: Path = _LIVE_INSTRUMENTS,
) -> list[str]:
    return [
        "run-live",
        str(plan_path),
        "--instruments",
        str(instruments),
        "--calendar-raw",
        str(_CALENDAR_RAW),
        "--calendar-normalized",
        str(_CALENDAR_NORMALIZED),
        "--calendar-raw-uri",
        "quant-data://evidence/calendar/2026-official-window-raw-v1.json",
        "--calendar-normalized-uri",
        "quant-data://evidence/calendar/2026-official-window-normalized-v1.json",
        "--data-root",
        str(tmp_path / "data"),
        "--state-root",
        str(tmp_path / "state"),
        "--report",
        str(tmp_path / "report.json"),
        "--archive",
        str(tmp_path / "worker.tar"),
        "--hmac-key-file",
        str(key_path),
        "--source-commit",
        "a" * 40,
        "--environment-id",
        "test-macmini-movespeed-staging",
    ]


def _live_plan(tmp_path: Path) -> Path:
    path = tmp_path / "plan.json"
    assert main(["build-plan", str(_LIVE_SPEC), "--output", str(path)]) == 0
    return path


def test_daily_cli_live_rejects_contract_before_creating_output_roots(
    tmp_path: Path, capsys
) -> None:
    plan_path = _live_plan(tmp_path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["provider_contract"]["provider_version"] = "tampered"
    payload["tasks"][0]["provider_version"] = "tampered"
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    key_path = tmp_path / "key"
    key_path.write_bytes(b"k" * 32)

    assert main(_live_args(tmp_path, plan_path, key_path)) == 2
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "state").exists()
    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "worker.tar").exists()
    assert "BaoStock live provider contract" in capsys.readouterr().err


def test_daily_cli_live_rejects_short_key_before_creating_output_roots(
    tmp_path: Path, capsys
) -> None:
    plan_path = _live_plan(tmp_path)
    key_path = tmp_path / "key"
    key_path.write_bytes(b"too-short")

    assert main(_live_args(tmp_path, plan_path, key_path)) == 2
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "state").exists()
    assert not (tmp_path / "report.json").exists()
    assert "at least 32 bytes" in capsys.readouterr().err


def test_daily_cli_live_rejects_more_than_three_instruments_without_writes(
    tmp_path: Path, capsys
) -> None:
    plan_path = _live_plan(tmp_path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["universe"]["instrument_ids"].append("XSHE.000002")
    payload["tasks"][0]["instrument_scope"].append("XSHE.000002")
    payload["tasks"][0]["instrument_scope"].sort()
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    key_path = tmp_path / "key"
    key_path.write_bytes(b"k" * 32)

    assert main(_live_args(tmp_path, plan_path, key_path)) == 2
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "state").exists()
    assert "at most 3 instruments" in capsys.readouterr().err


def test_daily_cli_live_rejects_unapproved_session_without_writes(tmp_path: Path, capsys) -> None:
    plan_path = _live_plan(tmp_path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["session"]["session_date"] = "2026-05-06"
    payload["tasks"][0]["start_date"] = "2026-05-06"
    payload["tasks"][0]["end_date"] = "2026-05-06"
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    key_path = tmp_path / "key"
    key_path.write_bytes(b"k" * 32)

    assert main(_live_args(tmp_path, plan_path, key_path)) == 2
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "state").exists()
    assert "authorized 2026-05-07 session" in capsys.readouterr().err


def test_daily_cli_live_rejects_invalid_provider_symbol_without_writes(
    tmp_path: Path, capsys
) -> None:
    plan_path = _live_plan(tmp_path)
    instrument_path = tmp_path / "instruments.json"
    payload = json.loads(_LIVE_INSTRUMENTS.read_text(encoding="utf-8"))
    payload["instruments"][0]["provider_symbol"] = "sz.600000"
    instrument_path.write_text(json.dumps(payload), encoding="utf-8")
    key_path = tmp_path / "key"
    key_path.write_bytes(b"k" * 32)

    assert (
        main(
            _live_args(
                tmp_path,
                plan_path,
                key_path,
                instruments=instrument_path,
            )
        )
        == 2
    )
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "state").exists()
    assert "invalid venue or BaoStock symbol" in capsys.readouterr().err


def test_daily_cli_live_builds_authenticated_worker_archive(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    plan_path = _live_plan(tmp_path)
    key_path = tmp_path / "key"
    key = b"k" * 32
    key_path.write_bytes(key)

    class Result:
        error_code = "0"
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
        ]

        def __init__(self, code: str) -> None:
            self.code = code
            self.pending = True

        def next(self) -> bool:
            pending, self.pending = self.pending, False
            return pending

        def get_row_data(self) -> list[str]:
            return [
                date(2026, 5, 7).isoformat(),
                self.code,
                "10.0",
                "10.5",
                "9.8",
                "10.2",
                "10.0",
                "1000",
                "10200",
                "3",
            ]

    class Client:
        util = SimpleNamespace(socketutil=None)

        @staticmethod
        def login():
            return SimpleNamespace(error_code="0")

        @staticmethod
        def query_history_k_data_plus(code: str, *_args, **_kwargs):
            return Result(code)

        @staticmethod
        def logout() -> None:
            return None

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )

    assert main(_live_args(tmp_path, plan_path, key_path)) == 0
    report = DailyRunReport.model_validate_json(
        (tmp_path / "report.json").read_text(encoding="utf-8")
    )
    assert report.succeeded_batches == 1
    assert report.canonical_eligible is False
    assert report.production_pointer_updated is False
    archive_path = tmp_path / "worker.tar"
    assert archive_path.is_file()

    imported = import_worker_archive(
        archive_path=archive_path,
        data_root=tmp_path / "nas-isolated-candidate",
        hmac_key=key,
    )
    audit = imported.envelope.provider_transport_audit
    assert audit["completed_requests"] == 3
    assert audit["all_requests_succeeded"] is True
    assert all(audit["request_successful_source_fingerprints"])
    assert capsys.readouterr().err == ""


def test_daily_cli_builds_and_runs_full_market_with_restart_and_per_batch_archives(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    universe_rows = [
        ["sh.600000", "1", "浦发银行"],
        ["sh.600519", "1", "贵州茅台"],
        ["sz.000001", "1", "平安银行"],
        ["sz.000002", "1", "万科A"],
        ["sh.600001", "0", "停牌样本"],
        ["sh.000001", "1", "上证指数"],
    ]

    class UniverseResult:
        error_code = "0"
        fields = ["code", "tradeStatus", "code_name"]

        def __init__(self) -> None:
            self.rows = iter(universe_rows)
            self.current: list[str] | None = None

        def next(self) -> bool:
            self.current = next(self.rows, None)
            return self.current is not None

        def get_row_data(self) -> list[str]:
            assert self.current is not None
            return self.current

    class BarResult:
        error_code = "0"
        fields = [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
        ]

        def __init__(self, code: str) -> None:
            self.code = code
            self.pending = True

        def next(self) -> bool:
            pending, self.pending = self.pending, False
            return pending

        def get_row_data(self) -> list[str]:
            return [
                "2026-05-07",
                self.code,
                "10.0",
                "10.5",
                "9.8",
                "10.2",
                "10.0",
                "1000",
                "10200",
                "3",
            ]

    class Client:
        util = SimpleNamespace(socketutil=None)
        xshg_failures_remaining = 1

        @staticmethod
        def login():
            return SimpleNamespace(error_code="0")

        @staticmethod
        def query_all_stock(*_args, **_kwargs):
            return UniverseResult()

        @staticmethod
        def query_history_k_data_plus(code: str, *_args, **_kwargs):
            if code.startswith("sh.") and Client.xshg_failures_remaining:
                Client.xshg_failures_remaining -= 1
                raise ConnectionError("controlled full-market retry")
            return BarResult(code)

        @staticmethod
        def logout() -> None:
            return None

    monkeypatch.setattr("quant_stack.daily_cli._FULL_MARKET_MIN_INSTRUMENTS", 4)
    monkeypatch.setattr(
        "quant_stack.daily_cli.BaoStockRequestBudget.for_data_root",
        lambda _data_root: BaoStockRequestBudget(
            tmp_path / "control" / "baostock-request-budget.json",
            min_batch_interval_seconds=0,
        ),
    )
    monkeypatch.setattr("quant_stack.daily_cli.version", lambda _name: "0.9.3")
    monkeypatch.setattr("quant_stack.daily_cli.import_module", lambda _name: Client)
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers._require_runtime_version",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.providers.import_module",
        lambda _name: Client,
    )

    snapshot = tmp_path / "universe-snapshot.json"
    instruments = tmp_path / "instruments.json"
    spec = tmp_path / "spec.json"
    plan = tmp_path / "plan.json"
    assert (
        main(
            [
                "build-live-universe",
                "--session",
                "2026-05-07",
                "--snapshot",
                str(snapshot),
                "--instruments",
                str(instruments),
                "--spec",
                str(spec),
                "--calendar-source",
                "Shanghai Stock Exchange; Shenzhen Stock Exchange; Beijing Stock Exchange",
                "--calendar-version",
                "cn-official-2026-window-v1",
                "--calendar-raw",
                str(_CALENDAR_RAW),
                "--calendar-normalized",
                str(_CALENDAR_NORMALIZED),
                "--calendar-raw-uri",
                "quant-data://evidence/calendar/2026-official-window-raw-v1.json",
                "--calendar-evidence-ref",
                "quant-data://evidence/calendar/2026-official-window-normalized-v1.json",
                "--snapshot-evidence-ref",
                "quant-data://evidence/universe/2026-05-07.json",
                "--provider-control-root",
                str(tmp_path / "control"),
                "--batch-size",
                "2",
                "--retry-backoff-seconds",
                "0",
            ]
        )
        == 0
    )
    assert main(["build-plan", str(spec), "--output", str(plan)]) == 0
    saved = DailyRunPlan.model_validate_json(plan.read_text(encoding="utf-8"))
    assert len(saved.universe.instrument_ids) == 4
    assert len(saved.tasks) == 2
    universe_budget = BaoStockRequestBudget.for_control_root(tmp_path / "control")
    assert next(iter(universe_budget.snapshot()["days"].values()))["used"] == 2

    key = b"k" * 32
    key_path = tmp_path / "key"
    key_path.write_bytes(key)
    common = [
        "run-live-full-market",
        str(plan),
        "--instruments",
        str(instruments),
        "--universe-snapshot",
        str(snapshot),
        "--calendar-raw",
        str(_CALENDAR_RAW),
        "--calendar-normalized",
        str(_CALENDAR_NORMALIZED),
        "--calendar-raw-uri",
        "quant-data://evidence/calendar/2026-official-window-raw-v1.json",
        "--calendar-normalized-uri",
        "quant-data://evidence/calendar/2026-official-window-normalized-v1.json",
        "--data-root",
        str(tmp_path / "data"),
        "--state-root",
        str(tmp_path / "state"),
        "--archive-root",
        str(tmp_path / "archives"),
        "--hmac-key-file",
        str(key_path),
        "--source-commit",
        "a" * 40,
        "--environment-id",
        "test-macmini-movespeed-full-market",
    ]
    first_report = tmp_path / "first-report.json"
    first_manifest = tmp_path / "first-manifest.json"
    assert (
        main(
            [
                *common,
                "--report",
                str(first_report),
                "--archive-manifest",
                str(first_manifest),
            ]
        )
        == 75
    )
    first = DailyRunReport.model_validate_json(first_report.read_text(encoding="utf-8"))
    first_archive_manifest = json.loads(first_manifest.read_text(encoding="utf-8"))
    assert first.succeeded_batches == 1
    assert first.deduplicated_batches == 0
    assert first.retryable_failed_batches == 1
    assert sum(item.records_received for item in first.batches) == 2
    assert len(first_archive_manifest["entries"]) == 1
    assert first_archive_manifest["requested_instruments"] == 4
    assert first_archive_manifest["records_received"] == 2
    assert first_archive_manifest["unresolved_instruments"] == 2

    second_report = tmp_path / "second-report.json"
    second_manifest = tmp_path / "second-manifest.json"
    assert (
        main(
            [
                *common,
                "--report",
                str(second_report),
                "--archive-manifest",
                str(second_manifest),
            ]
        )
        == 0
    )
    second = DailyRunReport.model_validate_json(second_report.read_text(encoding="utf-8"))
    assert second.succeeded_batches == 2
    assert second.deduplicated_batches == 1
    assert second.retryable_failed_batches == 0
    assert set(first.publication_bundle_uris) < set(second.publication_bundle_uris)
    manifest = json.loads(second_manifest.read_text(encoding="utf-8"))
    assert len(manifest["entries"]) == 2
    assert manifest["provider_snapshot_rows"] == 6
    assert manifest["active_universe_instruments"] == 4
    assert manifest["inactive_a_share_observations"] == 1
    assert manifest["requested_instruments"] == 4
    assert manifest["records_received"] == 4
    assert manifest["unresolved_instruments"] == 0
    assert manifest["duplicate_keys"] == 0

    nas_root = tmp_path / "nas-isolated-candidate"
    import_args = SimpleNamespace(
        manifest=second_manifest,
        data_root=nas_root,
        hmac_key_file=key_path,
    )
    imported = _IMPORT_MANIFEST_MODULE.run(import_args)
    replayed = _IMPORT_MANIFEST_MODULE.run(import_args)
    assert imported["new_imports"] == 2
    assert imported["idempotent_replays"] == 0
    assert replayed["new_imports"] == 0
    assert replayed["idempotent_replays"] == 2
    verified = _VERIFY_IMPORT_MODULE.run(
        manifest_path=second_manifest,
        data_root=nas_root,
    )
    assert verified["manifest_archives"] == 2
    assert verified["verified_receipts"] == 2
    assert verified["persisted_bar_rows"] == 4
    assert verified["distinct_instruments"] == 4
    assert verified["duplicate_keys"] == 0
    assert verified["source_commit"] == "a" * 40
    assert verified["catalog_published"] is False
    assert verified["production_pointer_updated"] is False
    assert capsys.readouterr().err == ""

    session_common: list[str] = []
    skip = False
    for item in common:
        if skip:
            skip = False
            continue
        if item in {"--archive-root", "--hmac-key-file"}:
            skip = True
            continue
        session_common.append(item)
    session_report = tmp_path / "session-report.json"
    session_root = tmp_path / "session-parquet"
    session_cache = tmp_path / "session-cache"
    session_args = [
        *session_common,
        "--report",
        str(session_report),
        "--session-parquet-root",
        str(session_root),
        "--session-cache-root",
        str(session_cache),
    ]
    assert main(session_args) == 0
    session_payload = json.loads(session_report.read_text(encoding="utf-8"))
    assert session_payload["schema_version"] == "quantstack.cn-session-collection.v1"
    assert session_payload["bars"] == 4
    assert session_payload["instruments"] == 4
    assert session_payload["provider_retries"] == 0
    assert len(list(session_cache.glob("batch-*.json.gz"))) == 2
    package = session_root / "market=CN" / "session=2026-05-07"
    assert {item.name for item in package.iterdir()} == {"bars.parquet", "manifest.json"}
    assert main(session_args) == 0
