import hashlib
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import quant_stack.data.tasks.runtime as runtime_module
from quant_stack.data.ingestion import IngestionService
from quant_stack.data.models import DatasetManifest
from quant_stack.data.providers.cn import CNProvider, CNProviderDiscrepancyReport
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    BaoStockReplayProvider,
    normalize_akshare_replay_bars,
    normalize_baostock_replay_bars,
)
from quant_stack.data.publication import CatalogPublisher, load_publication_bundle
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.tasks import (
    FileTaskStore,
    Task,
    TaskAlreadyRunningError,
    TaskRuntime,
    TaskState,
)

ROOT = Path(__file__).parents[1]
DISCREPANCY_FIXTURE = ROOT / "tests/fixtures/providers/cn/replay/discrepancy_report.json"


def _task(**overrides: object) -> Task:
    values: dict[str, object] = {
        "dataset_id": "cn_equity_daily",
        "dataset_version": "2026-07-28.1",
        "provider": "akshare_replay",
        "provider_version": "1.18.81",
        "endpoint": "stock_zh_a_hist",
        "adapter_version": "contract-only",
        "native_schema_version": "fixture-required-v1",
        "market": "CN",
        "instrument_scope": ("XSHG.600000", "XSHE.000001", "BJSE.430047"),
        "start_date": date(2026, 7, 27),
        "end_date": date(2026, 7, 28),
        "created_at": datetime(2026, 7, 29, tzinfo=UTC),
    }
    values.update(overrides)
    return Task(**values)


def test_task_idempotency_uses_stable_business_identity() -> None:
    first = _task()
    reordered = _task(
        instrument_scope=("BJSE.430047", "XSHG.600000", "XSHE.000001"),
        created_at=datetime(2026, 7, 30, tzinfo=UTC),
    )
    changed_version = _task(dataset_version="2026-07-28.2")

    assert first.idempotency_key == reordered.idempotency_key
    assert first.task_id == reordered.task_id
    assert first.idempotency_key != changed_version.idempotency_key


def test_idempotency_separates_dataset_ids() -> None:
    assert _task().idempotency_key != _task(dataset_id="cn_index_daily").idempotency_key


def test_idempotency_separates_provider_versions() -> None:
    assert _task().idempotency_key != _task(provider_version="1.18.82").idempotency_key


def test_idempotency_separates_native_schema_versions() -> None:
    assert (
        _task().idempotency_key
        != _task(native_schema_version="fixture-required-v2").idempotency_key
    )


def test_idempotency_separates_frequencies() -> None:
    assert _task().idempotency_key != _task(frequency="1w").idempotency_key


def test_idempotency_separates_adapter_versions() -> None:
    assert _task().idempotency_key != _task(adapter_version="replay-v2").idempotency_key


def test_task_state_machine_allows_only_declared_transitions() -> None:
    assert {state.value for state in TaskState} == {
        "pending",
        "running",
        "succeeded",
        "retryable_failed",
        "permanently_failed",
        "quarantined",
        "cancelled",
    }
    runtime = TaskRuntime()
    task = runtime.submit(_task())
    assert runtime.transition(task.task_id, TaskState.RUNNING).state is TaskState.RUNNING
    quarantined = runtime.transition(task.task_id, TaskState.QUARANTINED)
    assert quarantined.state is TaskState.QUARANTINED
    with pytest.raises(ValueError, match="Invalid task transition"):
        runtime.transition(task.task_id, TaskState.RUNNING)

    cancelled_task = runtime.submit(_task(dataset_version="cancelled-v1"))
    assert (
        runtime.transition(cancelled_task.task_id, TaskState.CANCELLED).state is TaskState.CANCELLED
    )

    failed_task = runtime.submit(_task(dataset_version="permanent-failure-v1"))
    runtime.transition(failed_task.task_id, TaskState.RUNNING)
    assert (
        runtime.transition(failed_task.task_id, TaskState.PERMANENTLY_FAILED).state
        is TaskState.PERMANENTLY_FAILED
    )


def test_retry_attempt_inherits_checkpoint_and_cursor(tmp_path: Path) -> None:
    class FailOnceBaoStockReplayProvider(BaoStockReplayProvider):
        def __init__(self) -> None:
            self.calls = 0

        def fetch_bars(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("offline replay timeout")
            return super().fetch_bars(*args, **kwargs)

    provider = FailOnceBaoStockReplayProvider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={provider.name: normalize_baostock_replay_bars},
    )
    runtime = TaskRuntime()
    task = _task(
        provider=provider.name,
        provider_version="0.9.3",
        endpoint=provider.endpoint,
        instrument_scope=("XSHG.600000", "XSHE.000001"),
        max_attempts=2,
    )

    failed = runtime.execute(
        task,
        service,
        checkpoint={"next_instrument_index": 1},
        cursor="XSHE.000001",
    )
    assert failed.task.state is TaskState.RETRYABLE_FAILED
    assert failed.attempt is not None
    assert failed.attempt.retry.retryable is True
    assert failed.attempt.retry.next_retry_at is not None

    recovered = runtime.execute(task, service)
    attempts = runtime.attempts(task.task_id)
    assert recovered.task.state is TaskState.SUCCEEDED
    assert recovered.result is not None
    assert len(attempts) == 2
    assert attempts[1].checkpoint == {"next_instrument_index": 1}
    assert attempts[1].cursor == "XSHE.000001"


def test_retry_classification_checks_wrapped_connection_error(tmp_path: Path) -> None:
    class WrappedNetworkFailureProvider(BaoStockReplayProvider):
        def fetch_bars(self, *args, **kwargs):
            try:
                raise RuntimeError("transport-specific failure")
            except RuntimeError as exc:
                raise ConnectionError("sanitized provider failure") from exc

    provider = WrappedNetworkFailureProvider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={provider.name: normalize_baostock_replay_bars},
    )
    runtime = TaskRuntime()
    task = _task(
        provider=provider.name,
        provider_version="0.9.3",
        endpoint=provider.endpoint,
        instrument_scope=("XSHG.600000",),
        max_attempts=2,
    )

    failed = runtime.execute(task, service)

    assert failed.task.state is TaskState.RETRYABLE_FAILED
    assert failed.attempt is not None
    assert failed.attempt.retry.retryable is True
    assert failed.attempt.retry.error_type == "RuntimeError"


def test_same_task_concurrent_execution_is_locked(tmp_path: Path) -> None:
    class BlockingProvider(AKShareReplayProvider):
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.fetch_count = 0

        def fetch_bars(self, *args, **kwargs):
            self.fetch_count += 1
            if self.fetch_count == 1:
                self.started.set()
                assert self.release.wait(timeout=10)
            return super().fetch_bars(*args, **kwargs)

    provider = BlockingProvider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path / "data"),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    task = _task()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(TaskRuntime(store).execute, task, service)
        assert provider.started.wait(timeout=10)
        with pytest.raises(TaskAlreadyRunningError, match="already running"):
            TaskRuntime(store).execute(task, service)
        independent = TaskRuntime(store).execute(
            _task(dataset_version="2026-07-28.independent"), service
        )
        assert independent.task.state is TaskState.SUCCEEDED
        provider.release.set()
        assert first.result(timeout=10).task.state is TaskState.SUCCEEDED

    persisted = store.load(task.task_id)
    assert persisted is not None
    assert len(persisted.attempts) == 1


def test_nondurable_task_store_keeps_atomic_readable_state(tmp_path: Path) -> None:
    store = FileTaskStore(tmp_path / "state", durable=False)
    task = _task()

    store.save(task, ())

    persisted = store.load(task.task_id)
    assert persisted is not None
    assert persisted.task == task
    assert not list(store.root.glob("*.tmp"))


def test_task_lock_releases_after_process_exit(tmp_path: Path) -> None:
    store = FileTaskStore(tmp_path / "state")
    task_id = _task().task_id
    script = (
        "import sys,time\n"
        "from quant_stack.data.tasks import FileTaskStore\n"
        "store=FileTaskStore(sys.argv[1])\n"
        "with store.execution_lock(sys.argv[2]):\n"
        " print('LOCKED', flush=True)\n"
        " time.sleep(60)\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(store.root), task_id],
        cwd=Path(__file__).parents[1],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "LOCKED"
        with pytest.raises(TaskAlreadyRunningError):
            with store.execution_lock(task_id):
                pass
    finally:
        process.terminate()
        process.wait(timeout=10)

    with store.execution_lock(task_id):
        pass


def test_replay_providers_satisfy_cn_contract_and_cover_three_mics() -> None:
    baostock = BaoStockReplayProvider()
    akshare = AKShareReplayProvider()

    assert isinstance(baostock, CNProvider)
    assert isinstance(akshare, CNProvider)
    bao_instruments = list(baostock.list_instruments("CN"))
    ak_instruments = list(akshare.list_instruments("CN"))
    assert {item.venue for item in bao_instruments} == {"XSHG", "XSHE"}
    assert {item.venue for item in ak_instruments} == {"XSHG", "XSHE", "BJSE"}

    bao_records = list(
        baostock.fetch_bars(
            "CN", ["XSHG.600000", "XSHE.000001"], date(2026, 7, 27), date(2026, 7, 28)
        )
    )
    ak_records = list(
        akshare.fetch_bars(
            "CN",
            ["XSHG.600000", "XSHE.000001", "BJSE.430047"],
            date(2026, 7, 27),
            date(2026, 7, 28),
        )
    )
    assert len(bao_records) == 4
    assert len(ak_records) == 6
    assert {record["code"][:2] for record in bao_records} == {"sh", "sz"}
    assert {record["股票代码"] for record in ak_records} == {
        "600000",
        "000001",
        "430047",
    }


def test_task_runtime_runs_replay_through_ingestion_and_deduplicates_dataset(
    tmp_path: Path,
) -> None:
    provider = AKShareReplayProvider()
    storage = StagingFileSystemStorage(tmp_path)
    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    runtime = TaskRuntime()
    task = _task()

    execution = runtime.execute(task, service)

    assert execution.task.state is TaskState.SUCCEEDED
    assert execution.result is not None
    assert execution.result.raw_manifest.provider == "akshare_replay"
    assert execution.result.raw_manifest.version == task.dataset_version
    assert execution.result.bar_manifest.version == task.dataset_version
    assert execution.result.bar_manifest.row_count == 6
    assert execution.result.quality_report.quality_status == "passed"
    assert len(storage.list_manifests()) == 0
    assert not (tmp_path / "catalog/quant.duckdb").exists()
    raw_payload = json.loads(execution.result.raw_artifact.path.read_text(encoding="utf-8"))
    assert raw_payload["job"]["provider_endpoint"] == "stock_zh_a_hist"
    assert raw_payload["job"]["idempotency_key"] == task.idempotency_key
    assert raw_payload["job"]["provider_version"] == task.provider_version
    assert raw_payload["job"]["adapter_version"] == task.adapter_version
    assert raw_payload["job"]["native_schema_version"] == task.native_schema_version
    assert execution.result.raw_manifest.provider_version == task.provider_version
    normalized = storage.read_dataset(execution.result.bar_artifact.uri)
    assert {row["instrument_id"] for row in normalized} == {
        "XSHG.600000",
        "XSHE.000001",
        "BJSE.430047",
    }
    assert (
        max(row["volume"] for row in normalized if row["instrument_id"] == "BJSE.430047") == 350_000
    )

    assert execution.bundle is not None
    receipt = CatalogPublisher(tmp_path).publish(execution.bundle)
    assert not receipt.idempotent_replay
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    catalog_row = connection.execute(
        "SELECT provider, row_count, quality_status FROM datasets "
        "WHERE dataset_id = 'cn_equity_daily' AND version = '2026-07-28.1'"
    ).fetchone()
    connection.close()
    assert catalog_row == ("akshare_replay", 6, "passed")

    duplicate = runtime.execute(
        _task(created_at=datetime(2026, 7, 29, tzinfo=UTC) + timedelta(hours=1)),
        service,
    )
    assert duplicate.deduplicated is True
    assert duplicate.attempt is None
    assert duplicate.result == execution.result
    assert len(runtime.attempts(task.task_id)) == 1
    assert len(storage.list_manifests()) == 3

    restarted_runtime = TaskRuntime()
    replayed = restarted_runtime.execute(_task(), service)
    assert replayed.task.state is TaskState.SUCCEEDED
    assert replayed.result is not None
    assert replayed.result.bar_manifest == execution.result.bar_manifest
    assert len(storage.list_manifests()) == 3


def test_schema_failure_persists_raw_uri(tmp_path: Path) -> None:
    class DriftAKShareReplayProvider(AKShareReplayProvider):
        def fetch_bars(self, *args, **kwargs):
            records = [dict(record) for record in super().fetch_bars(*args, **kwargs)]
            records[0].pop("收盘")
            return records

    provider = DriftAKShareReplayProvider()
    data_root = tmp_path / "data"
    storage = StagingFileSystemStorage(data_root)
    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    runtime = TaskRuntime(store)

    execution = runtime.execute(_task(max_attempts=1), service)

    assert execution.task.state is TaskState.PERMANENTLY_FAILED
    assert execution.attempt is not None
    assert execution.attempt.retry.error_type == "ProviderSchemaDriftError"
    assert execution.attempt.failure_context is not None
    context = execution.attempt.failure_context
    assert context.failure_stage == "provider_schema_validation"
    assert context.raw_uri is not None
    assert context.raw_manifest_uri is not None
    raw_path = data_root / context.raw_uri.removeprefix("quant-data://")
    manifest_path = data_root / context.raw_manifest_uri.removeprefix("quant-data://")
    raw_manifest = DatasetManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    assert raw_path.is_file()
    assert hashlib.sha256(raw_path.read_bytes()).hexdigest() == raw_manifest.sha256
    restarted = FileTaskStore(tmp_path / "state").load(execution.task.task_id)
    assert restarted is not None
    assert restarted.attempts[-1].failure_context == context
    manifests = list((data_root / "staging/proposed_manifests").rglob("*.json"))
    assert len(manifests) == 1
    assert not (data_root / "normalized").exists()
    assert not (data_root / "staging/bundles").exists()


def test_quality_failure_persists_quality_report_uri(tmp_path: Path) -> None:
    class QualityFailureProvider(AKShareReplayProvider):
        def fetch_bars(self, *args, **kwargs):
            records = [dict(record) for record in super().fetch_bars(*args, **kwargs)]
            records[0]["最高"] = 0.01
            return records

    provider = QualityFailureProvider()
    data_root = tmp_path / "data"
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    execution = TaskRuntime(store).execute(_task(max_attempts=1), service)

    assert execution.task.state is TaskState.QUARANTINED
    assert execution.bundle is None
    assert execution.attempt is not None
    assert execution.attempt.retry.error_type == "DataQualityError"
    context = execution.attempt.failure_context
    assert context is not None
    assert context.failure_stage == "quality_gate"
    assert context.raw_uri is not None
    assert context.raw_manifest_uri is not None
    assert context.quality_report_uri is not None
    for uri in (context.raw_uri, context.raw_manifest_uri, context.quality_report_uri):
        path = data_root / uri.removeprefix("quant-data://")
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest()
    restarted = FileTaskStore(tmp_path / "state").load(execution.task.task_id)
    assert restarted is not None
    assert restarted.attempts[-1].failure_context == context
    assert not (data_root / "staging/bundles").exists()


def test_discrepancy_fixture_report_preserves_both_provider_lineages() -> None:
    report = CNProviderDiscrepancyReport.model_validate_json(
        DISCREPANCY_FIXTURE.read_text(encoding="utf-8")
    )
    record = report.records[0]

    assert report.fixture_only is True
    assert record.primary_provider == "baostock_replay"
    assert record.comparison_provider == "akshare_replay"
    assert record.differing_fields == ("volume",)
    assert record.tolerance_policy == "ohlc_abs<=0.01;volume_relative<=0.005"
    assert record.discrepancy_status == "open"
    assert record.primary_raw_uri != record.comparison_raw_uri


def test_restart_dedup_contract_matches_runtime(tmp_path: Path) -> None:
    provider = AKShareReplayProvider()
    storage = StagingFileSystemStorage(tmp_path / "data")
    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    first = TaskRuntime(store).execute(_task(), service)
    restarted = TaskRuntime(store).execute(_task(), service)

    assert first.task.state is TaskState.SUCCEEDED
    assert restarted.deduplicated is True
    assert restarted.result is None
    assert restarted.bundle is None
    persisted = store.load(first.task.task_id)
    assert persisted is not None
    assert persisted.task.state is TaskState.SUCCEEDED
    assert len(persisted.attempts) == 1
    assert persisted.publication_bundle_uri is not None


def test_running_attempt_is_recovered_after_process_crash(tmp_path: Path) -> None:
    class CrashOnceProvider(AKShareReplayProvider):
        crashed = False

        def fetch_bars(self, *args, **kwargs):
            if not self.crashed:
                self.crashed = True
                raise SystemExit("simulated process exit")
            return super().fetch_bars(*args, **kwargs)

    provider = CrashOnceProvider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path / "data"),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    task = _task(max_attempts=2)

    with pytest.raises(SystemExit, match="simulated process exit"):
        TaskRuntime(store).execute(task, service, checkpoint={"instrument_index": 2})

    crashed = store.load(task.task_id)
    assert crashed is not None
    assert crashed.task.state is TaskState.RUNNING
    assert crashed.attempts[-1].state is TaskState.RUNNING

    resumed = TaskRuntime(store).execute(task, service)
    persisted = store.load(task.task_id)
    assert resumed.task.state is TaskState.SUCCEEDED
    assert persisted is not None
    assert [attempt.state for attempt in persisted.attempts] == [
        TaskState.RETRYABLE_FAILED,
        TaskState.SUCCEEDED,
    ]
    assert persisted.attempts[0].retry.error_type == "InterruptedExecution"
    assert persisted.attempts[1].checkpoint == {"instrument_index": 2}


class FetchCountingAKShareReplayProvider(AKShareReplayProvider):
    def __init__(self) -> None:
        self.list_count = 0
        self.fetch_count = 0

    def list_instruments(self, *args, **kwargs):
        self.list_count += 1
        return super().list_instruments(*args, **kwargs)

    def fetch_bars(self, *args, **kwargs):
        self.fetch_count += 1
        return super().fetch_bars(*args, **kwargs)


def _contract_execution(tmp_path: Path, **task_overrides: object):
    provider = FetchCountingAKShareReplayProvider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    execution = TaskRuntime().execute(_task(max_attempts=1, **task_overrides), service)
    return execution, provider


def _assert_contract_rejected_before_fetch(tmp_path: Path, **overrides: object) -> None:
    execution, provider = _contract_execution(tmp_path, **overrides)
    assert execution.task.state is TaskState.PERMANENTLY_FAILED
    assert execution.attempt is not None
    assert execution.attempt.retry.error_type == "ProviderContractMismatchError"
    assert "mismatch" in (execution.attempt.retry.error_message or "").lower()
    assert execution.attempt.failure_context is not None
    assert execution.attempt.failure_context.failure_stage == "provider_contract_validation"
    assert execution.attempt.failure_context.raw_uri is None
    assert provider.list_count == 0
    assert provider.fetch_count == 0
    assert not (tmp_path / "raw").exists()


def test_provider_contract_accepts_exact_task_pin(tmp_path: Path) -> None:
    execution, provider = _contract_execution(tmp_path)
    assert execution.task.state is TaskState.SUCCEEDED
    assert provider.fetch_count == 1


def test_provider_contract_rejects_endpoint_mismatch_before_fetch(tmp_path: Path) -> None:
    _assert_contract_rejected_before_fetch(tmp_path, endpoint="wrong-endpoint")


def test_provider_contract_rejects_provider_name_mismatch(tmp_path: Path) -> None:
    provider = FetchCountingAKShareReplayProvider()
    service = IngestionService(
        providers={"aliased_replay": provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={"aliased_replay": normalize_akshare_replay_bars},
    )
    execution = TaskRuntime().execute(_task(provider="aliased_replay", max_attempts=1), service)
    assert execution.task.state is TaskState.PERMANENTLY_FAILED
    assert execution.attempt is not None
    assert execution.attempt.failure_context is not None
    assert execution.attempt.failure_context.failure_stage == "provider_contract_validation"
    assert provider.list_count == provider.fetch_count == 0


def test_provider_contract_rejects_provider_version_mismatch(tmp_path: Path) -> None:
    _assert_contract_rejected_before_fetch(tmp_path, provider_version="wrong-version")


def test_provider_contract_rejects_adapter_version_mismatch(tmp_path: Path) -> None:
    _assert_contract_rejected_before_fetch(tmp_path, adapter_version="wrong-adapter")


def test_provider_contract_rejects_native_schema_version_mismatch(tmp_path: Path) -> None:
    _assert_contract_rejected_before_fetch(tmp_path, native_schema_version="wrong-native-schema")


def test_provider_contract_rejects_frequency_mismatch(tmp_path: Path) -> None:
    _assert_contract_rejected_before_fetch(tmp_path, frequency="1w")


def test_manifest_and_bar_lineage_use_same_validated_contract(tmp_path: Path) -> None:
    execution, provider = _contract_execution(tmp_path)
    assert execution.result is not None
    contract = provider.runtime_contract
    expected = {
        "provider": contract.provider,
        "provider_version": contract.provider_version,
        "adapter_version": contract.adapter_version,
        "native_schema_version": contract.native_schema_version,
    }
    for manifest in (
        execution.result.raw_manifest,
        execution.result.instrument_manifest,
        execution.result.bar_manifest,
    ):
        assert {key: getattr(manifest, key) for key in expected} == expected
    raw_job = json.loads(execution.result.raw_artifact.path.read_text(encoding="utf-8"))["job"]
    assert raw_job["provider_endpoint"] == contract.endpoint
    assert {key: raw_job[key] for key in expected} == expected
    rows = StagingFileSystemStorage(tmp_path).read_dataset(execution.result.bar_artifact.uri)
    for row in rows:
        lineage = row["lineage"]
        assert lineage["endpoint"] == contract.endpoint
        assert {key: lineage[key] for key in expected} == expected


def test_resume_after_bundle_write_before_state_commit_uses_attempt_scoped_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = AKShareReplayProvider()
    data_root = tmp_path / "data"
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    store = FileTaskStore(tmp_path / "state")
    task = _task(max_attempts=2)
    real_write = runtime_module.write_publication_bundle

    def crash_after_write(storage, bundle):
        real_write(storage, bundle)
        raise SystemExit("simulated exit after bundle write")

    monkeypatch.setattr(runtime_module, "write_publication_bundle", crash_after_write)
    with pytest.raises(SystemExit, match="after bundle write"):
        TaskRuntime(store).execute(task, service)

    crashed = store.load(task.task_id)
    assert crashed is not None
    assert crashed.task.state is TaskState.RUNNING
    assert crashed.attempts[-1].state is TaskState.RUNNING
    bundle_paths = sorted((data_root / "catalog/publication_bundles").glob("*.json"))
    assert len(bundle_paths) == 1
    first_bundle = load_publication_bundle(bundle_paths[0])

    monkeypatch.setattr(runtime_module, "write_publication_bundle", real_write)
    resumed = TaskRuntime(store).execute(task, service)
    assert resumed.task.state is TaskState.SUCCEEDED
    assert resumed.bundle is not None
    assert resumed.bundle.attempt_id.endswith("-2")
    assert first_bundle.attempt_id.endswith("-1")
    assert resumed.bundle.bundle_id != first_bundle.bundle_id
    bundle_paths = sorted((data_root / "catalog/publication_bundles").glob("*.json"))
    assert len(bundle_paths) == 2
    assert {load_publication_bundle(path).bundle_id for path in bundle_paths} == {
        first_bundle.bundle_id,
        resumed.bundle.bundle_id,
    }
    publisher = CatalogPublisher(data_root)
    first_receipt = publisher.publish(resumed.bundle)
    replay_receipt = publisher.publish(resumed.bundle)
    assert first_receipt.bundle_id == resumed.bundle.bundle_id
    assert replay_receipt.idempotent_replay is True
