from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.daily import (
    DailyProviderContract,
    DailyUniverse,
    DailyWorker,
    VerifiedTradingSession,
    build_daily_run_plan,
)
from quant_stack.data.ingestion import IngestionService
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    normalize_akshare_replay_bars,
)
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.tasks import FileTaskStore, TaskRuntime, TaskState


def _session() -> VerifiedTradingSession:
    return VerifiedTradingSession(
        session_date=date(2026, 7, 28),
        calendar_source="frozen-exchange-calendar",
        calendar_version="fixture-v1",
        evidence_ref="tests/fixtures/calendar/cn-session-2026-07-28.json",
    )


def _universe(*instrument_ids: str) -> DailyUniverse:
    return DailyUniverse(
        universe_id="cn-daily-fixture",
        version="fixture-v1",
        instrument_ids=instrument_ids,
        evidence_ref="tests/fixtures/providers/cn/replay",
    )


def _contract(*, batch_size: int = 2, max_attempts: int = 2) -> DailyProviderContract:
    provider = AKShareReplayProvider()
    contract = provider.runtime_contract
    return DailyProviderContract(
        provider=contract.provider,
        provider_version=contract.provider_version,
        endpoint=contract.endpoint,
        adapter_version=contract.adapter_version,
        native_schema_version=contract.native_schema_version,
        batch_size=batch_size,
        max_attempts=max_attempts,
    )


def _service(root: Path, provider: AKShareReplayProvider) -> IngestionService:
    return IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(root),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )


def test_daily_plan_is_deterministic_and_covers_one_verified_session() -> None:
    instruments = ("XSHE.000001", "BJSE.430047", "XSHG.600000")
    first = build_daily_run_plan(
        session=_session(),
        universe=_universe(*instruments),
        provider_contract=_contract(batch_size=2),
        created_at=datetime(2026, 8, 6, tzinfo=UTC),
    )
    reordered = build_daily_run_plan(
        session=_session(),
        universe=_universe(*reversed(instruments)),
        provider_contract=_contract(batch_size=2),
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )

    assert tuple(task.task_id for task in first.tasks) == tuple(
        task.task_id for task in reordered.tasks
    )
    assert tuple(item for task in first.tasks for item in task.instrument_scope) == tuple(
        sorted(instruments)
    )
    assert all(task.start_date == task.end_date == _session().session_date for task in first.tasks)
    assert first.dataset_version == "daily-2026-07-28-fixture-v1"
    assert first.canonical_eligible is False
    assert first.production_pointer_updated is False


def test_daily_plan_scales_to_six_thousand_instruments_deterministically() -> None:
    instruments = tuple(f"XSHG.{600_000 + index:06d}" for index in range(3_000)) + tuple(
        f"XSHE.{index + 1:06d}" for index in range(3_000)
    )
    first = build_daily_run_plan(
        session=_session(),
        universe=DailyUniverse(
            universe_id="cn-daily-full-market-capacity",
            version="synthetic-6000-v1",
            instrument_ids=instruments,
            evidence_ref="synthetic://tests/full-market-capacity-6000",
        ),
        provider_contract=_contract(batch_size=500),
        created_at=datetime(2026, 8, 6, tzinfo=UTC),
    )
    reordered = build_daily_run_plan(
        session=_session(),
        universe=DailyUniverse(
            universe_id="cn-daily-full-market-capacity",
            version="synthetic-6000-v1",
            instrument_ids=tuple(reversed(instruments)),
            evidence_ref="synthetic://tests/full-market-capacity-6000",
        ),
        provider_contract=_contract(batch_size=500),
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )

    task_ids = tuple(task.task_id for task in first.tasks)
    flattened = tuple(item for task in first.tasks for item in task.instrument_scope)

    assert len(first.tasks) == 12
    assert all(len(task.instrument_scope) == 500 for task in first.tasks)
    assert len(task_ids) == len(set(task_ids))
    assert flattened == tuple(sorted(instruments))
    assert len(flattened) == len(set(flattened)) == 6_000
    assert task_ids == tuple(task.task_id for task in reordered.tasks)
    assert first.dataset_version == "daily-2026-07-28-synthetic-6000-v1"
    assert first.canonical_eligible is False
    assert first.production_pointer_updated is False


def test_daily_worker_persists_bundles_and_deduplicates_restart(tmp_path: Path) -> None:
    plan = build_daily_run_plan(
        session=_session(),
        universe=_universe("XSHG.600000", "XSHE.000001", "BJSE.430047"),
        provider_contract=_contract(batch_size=3),
        created_at=datetime(2026, 8, 6, tzinfo=UTC),
    )
    provider = AKShareReplayProvider()
    store = FileTaskStore(tmp_path / "state")
    first = DailyWorker(TaskRuntime(store), _service(tmp_path / "data", provider)).run(plan)
    restarted = DailyWorker(TaskRuntime(store), _service(tmp_path / "data", provider)).run(plan)

    assert first.succeeded_batches == 1
    assert first.deduplicated_batches == 0
    assert len(first.publication_bundle_uris) == 1
    assert restarted.succeeded_batches == 1
    assert restarted.deduplicated_batches == 1
    assert restarted.publication_bundle_uris == first.publication_bundle_uris


def test_daily_worker_isolates_a_failed_batch_and_keeps_successes(tmp_path: Path) -> None:
    class OneBatchFails(AKShareReplayProvider):
        def fetch_bars(self, market, symbols, start, end, frequency):
            if symbols == ["XSHG.600000"]:
                raise ValueError("frozen fixture rejects one batch")
            return super().fetch_bars(market, symbols, start, end, frequency)

    plan = build_daily_run_plan(
        session=_session(),
        universe=_universe("XSHG.600000", "XSHE.000001", "BJSE.430047"),
        provider_contract=_contract(batch_size=2, max_attempts=1),
        created_at=datetime(2026, 8, 6, tzinfo=UTC),
    )
    provider = OneBatchFails()
    report = DailyWorker(
        TaskRuntime(FileTaskStore(tmp_path / "state")),
        _service(tmp_path / "data", provider),
    ).run(plan)

    assert report.succeeded_batches == 1
    assert report.permanently_failed_batches == 1
    assert len(report.publication_bundle_uris) == 1
    failed = next(item for item in report.batches if item.state is TaskState.PERMANENTLY_FAILED)
    assert failed.instrument_ids == ("XSHG.600000",)
    assert failed.error_stage == "provider_fetch"
    assert report.canonical_eligible is False
    assert report.production_pointer_updated is False
