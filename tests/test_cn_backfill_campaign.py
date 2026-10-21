from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.backfill import (
    BackfillPilotRunner,
    BackfillUniverse,
    BackfillUniverseSource,
    build_backfill_campaign_plan,
    build_campaign_progress,
    build_full_historical_universe,
    finalize_and_publish_backfill,
    finalize_campaign,
    requeue_campaign_failures,
    run_campaign_wave,
)
from quant_stack.data.backfill.models import BackfillShard
from quant_stack.data.providers.cn.live import BaoStockDailyQuotaExceeded

_UNIVERSE = Path(__file__).parents[1] / "config/cn_backfill_universe_v1.json"


def _native(provider_symbol: str, observed: date, close: float = 10) -> dict[str, object]:
    return {
        "date": observed.isoformat(),
        "code": provider_symbol,
        "open": str(close - 0.1),
        "high": str(close + 0.2),
        "low": str(close - 0.2),
        "close": str(close),
        "volume": "123400",
        "amount": "1234000.50",
        "adjustflag": "3",
        "_retrieval_timestamp": datetime(2026, 8, 7, tzinfo=UTC).isoformat(),
    }


def _source(observed: date, rows: list[dict[str, str]]) -> BackfillUniverseSource:
    codes = [item["code"] for item in rows]
    return BackfillUniverseSource(
        snapshot_version=f"{observed}.test-v1",
        observed_date=observed,
        provider_version="0.9.3",
        native_schema_version="test-v1",
        raw_uri=f"quant-data://test/{observed}.json",
        raw_sha256="a" * 64,
        row_count=len(rows),
        xshg_count=sum(item.startswith("sh.") for item in codes),
        xshe_count=sum(item.startswith("sz.") for item in codes),
    )


def test_full_historical_universe_keeps_more_than_medium_sample_limit() -> None:
    first = [
        {"code": f"sh.{600000 + index:06d}", "tradeStatus": "1", "code_name": "S"}
        for index in range(300)
    ] + [{"code": f"sz.{index:06d}", "tradeStatus": "1", "code_name": "Z"} for index in range(300)]
    recent = first[10:290] + first[310:590]
    snapshots = [(date(2015, 1, 5), first), (date(2026, 8, 3), recent)]
    sources = [_source(observed, rows) for observed, rows in snapshots]

    universe = build_full_historical_universe(
        snapshots,
        sources,
        universe_id="cn-full-test",
        version="2015-2026.full-test-v1",
    )

    assert len(universe.instruments) == 600
    assert sum(item.venue == "XSHG" for item in universe.instruments) == 300
    assert sum(item.venue == "XSHE" for item in universe.instruments) == 300
    assert len({item.instrument_id for item in universe.instruments}) == 600
    assert (
        sum("historical_only_observed" in item.selection_bucket for item in universe.instruments)
        == 40
    )
    assert universe.selection_seed == 0
    assert "no sampling" in universe.selection_policy


def test_campaign_runs_bounded_waves_resumes_and_finalizes(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    created_at = datetime(2026, 8, 7, tzinfo=UTC)
    first = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-ten-year-test",
        dataset_version="campaign-test-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
        created_at=created_at,
    )
    second = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-ten-year-test",
        dataset_version="campaign-test-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
        created_at=created_at,
    )
    assert first.shards == second.shards
    assert first.content_sha256 == second.content_sha256
    assert len(first.shards) == 3
    assert (
        type(first).model_validate_json(first.model_dump_json(exclude_computed_fields=True)).shards
        == first.shards
    )

    aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}
    calls: Counter[tuple[str, ...]] = Counter()

    def fetch(ids, start, _end):
        calls[tuple(ids)] += 1
        return [_native(aliases[item], start, close=10 + index) for index, item in enumerate(ids)]

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=first.dataset_version,
        max_attempts=2,
    )
    wave_one = run_campaign_wave(runner, universe, first, max_shards=1)
    assert wave_one.selected_shards == 1
    assert wave_one.progress.succeeded_shards == 1
    assert wave_one.progress.pending_shards == 2
    assert sum(calls.values()) == 1
    with pytest.raises(ValueError, match="cannot finalize"):
        finalize_campaign(runner, universe, first)

    restarted = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=first.dataset_version,
        max_attempts=2,
    )
    wave_two = run_campaign_wave(restarted, universe, first, max_shards=1)
    assert wave_two.selected_shards == 1
    assert wave_two.progress.succeeded_shards == 2
    assert sum(calls.values()) == 2
    wave_three = run_campaign_wave(restarted, universe, first, max_shards=10)
    assert wave_three.selected_shards == 1
    assert wave_three.progress.finalizable is True
    assert wave_three.progress.total_rows == 60
    assert sum(calls.values()) == 3

    report = finalize_campaign(restarted, universe, first)
    assert report.replayed_shards == 3
    assert report.total_rows == 60
    assert report.failed_shards == 0
    assert report.query_proof["duplicate_instrument_date_keys"] == 0
    assert sum(calls.values()) == 3

    no_work = run_campaign_wave(restarted, universe, first, max_shards=10)
    assert no_work.selected_shards == 0
    assert no_work.progress.finalizable is True
    assert build_campaign_progress(restarted, universe, first).planned_shards == 3


def test_terminal_campaign_finalizes_and_registers_catalog_without_refetch(
    tmp_path: Path,
) -> None:
    import duckdb

    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-canonical-finalize-test",
        dataset_version="campaign-canonical-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}
    calls = 0

    def fetch(ids, start, _end):
        nonlocal calls
        calls += 1
        if calls == 1:
            return []
        return [_native(aliases[item], start, close=10 + index) for index, item in enumerate(ids)]

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    wave = run_campaign_wave(runner, universe, plan, max_shards=10)
    assert wave.progress.finalizable is True
    assert wave.progress.empty_shards == 1
    assert calls == 3
    plan_path = tmp_path / "control/plan.json"
    universe_path = tmp_path / "control/universe.json"
    progress_path = tmp_path / "control/progress.json"
    for path, value in (
        (plan_path, plan),
        (universe_path, universe),
        (progress_path, wave.progress),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            value.model_dump_json(indent=2, exclude_computed_fields=True) + "\n",
            encoding="utf-8",
        )
    progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
    empty = next(item for item in progress_payload["shards"] if item["status"] == "empty")
    empty["row_count"] = None
    progress_payload["finalizable"] = False
    progress_path.write_text(json.dumps(progress_payload), encoding="utf-8")

    receipt, report = finalize_and_publish_backfill(
        data_root=tmp_path,
        plan_path=plan_path,
        universe_path=universe_path,
        progress_path=progress_path,
        parquet_roots=(tmp_path / "pilot/normalized",),
        environment_id="pytest",
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )

    assert calls == 3
    assert report["row_count"] == 35
    assert report["duplicate_keys"] == 0
    assert report["ss_candidate"] is False
    assert report["receipt_replay_verified"] is True
    assert receipt.publication_state.candidate_eligible is False
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    try:
        registered = connection.execute(
            "SELECT dataset_id, version, row_count FROM datasets"
        ).fetchone()
    finally:
        connection.close()
    assert registered == ("cn_equity_daily", "campaign-canonical-v1", 35)


def test_canonical_finalize_fails_closed_when_a_succeeded_shard_is_missing(
    tmp_path: Path,
) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-canonical-missing-test",
        dataset_version="campaign-canonical-missing-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}

    def fetch(ids, start, _end):
        return [_native(aliases[item], start) for item in ids]

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    progress = run_campaign_wave(runner, universe, plan, max_shards=10).progress
    plan_path = tmp_path / "plan.json"
    universe_path = tmp_path / "universe.json"
    progress_path = tmp_path / "progress.json"
    plan_path.write_text(plan.model_dump_json(exclude_computed_fields=True), encoding="utf-8")
    universe_path.write_text(
        universe.model_dump_json(exclude_computed_fields=True), encoding="utf-8"
    )
    progress_path.write_text(
        progress.model_dump_json(exclude_computed_fields=True), encoding="utf-8"
    )
    next((tmp_path / "pilot/normalized").rglob("*.parquet")).unlink()

    with pytest.raises(ValueError, match="missing authoritative parquet shards"):
        finalize_and_publish_backfill(
            data_root=tmp_path / "catalog-root",
            plan_path=plan_path,
            universe_path=universe_path,
            progress_path=progress_path,
            parquet_roots=(tmp_path / "pilot/normalized",),
        )
    assert not (tmp_path / "catalog-root/catalog/quant.duckdb").exists()


def test_campaign_keeps_terminal_failure_explicit(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-failure-test",
        dataset_version="campaign-failure-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    calls = 0

    def fail(_ids, _start, _end):
        nonlocal calls
        calls += 1
        raise ValueError("deterministic contract failure")

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fail,
        dataset_version=plan.dataset_version,
        max_attempts=3,
    )
    first = run_campaign_wave(runner, universe, plan, max_shards=1)
    assert first.progress.failed_shards == 1
    assert calls == 1
    second = run_campaign_wave(runner, universe, plan, max_shards=1)
    assert second.selected_shards == 1
    assert second.progress.failed_shards == 2
    assert calls == 2
    with pytest.raises(ValueError, match="failed=2"):
        finalize_campaign(runner, universe, plan)


def test_campaign_stops_wave_at_provider_quota_without_fanout(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-quota-pause-test",
        dataset_version="campaign-quota-pause-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        raise BaoStockDailyQuotaExceeded(day="2026-08-07", used=49_000, requested=26, limit=49_000)

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    report = run_campaign_wave(runner, universe, plan, max_shards=3)
    assert report.selected_shards == 1
    assert report.progress.retryable_shards == 1
    assert report.progress.pending_shards == 2
    assert calls == 1


def test_campaign_wave_applies_serial_batch_and_request_limits(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-safe-wave-test",
        dataset_version="campaign-safe-wave-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        return []

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    report = run_campaign_wave(
        runner,
        universe,
        plan,
        max_shards=10,
        max_batches=2,
        max_requests=52,
    )
    assert report.selected_shards == 2
    assert report.estimated_batch_count == 2
    assert report.estimated_request_count == 52
    assert report.safety_limits["min_batch_interval_seconds"] == 600.0
    assert report.safety_limits["max_batches_per_day"] == 144
    assert report.safety_limits["wave_max_batches"] == 2
    assert calls == 2


def test_campaign_wave_reports_effective_pacing_from_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS", "30")
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-effective-pacing-test",
        dataset_version="campaign-effective-pacing-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )

    def fetch(_ids, _start, _end):
        return []

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    report = run_campaign_wave(
        runner,
        universe,
        plan,
        max_shards=1,
        max_batches=1,
        max_requests=52,
    )
    assert report.safety_limits["min_batch_interval_seconds"] == 30.0
    assert report.safety_limits["max_batches_per_day"] == 2880
    assert report.safety_limits["wave_max_batches"] == 1


def test_campaign_requeues_only_explicit_connection_failures(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-requeue-test",
        dataset_version="campaign-requeue-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        if calls <= 3:
            raise ConnectionError("controlled outage")
        return []

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=3,
    )
    first = run_campaign_wave(runner, universe, plan, max_shards=1)
    assert first.progress.failed_shards == 1
    report = requeue_campaign_failures(
        runner,
        universe,
        plan,
        allowed_error_classes=("ConnectionError",),
        additional_attempts=3,
    )
    assert report.requeued_shards == 1
    assert report.entries[0].previous_max_attempts == 3
    assert report.entries[0].new_max_attempts == 6
    resumed = run_campaign_wave(runner, universe, plan, max_shards=1)
    assert resumed.progress.empty_shards == 1
    assert calls == 4


def test_campaign_requeue_does_not_reopen_unapproved_failure(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-requeue-deny-test",
        dataset_version="campaign-requeue-deny-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )

    def fail(_ids, _start, _end):
        raise ValueError("deterministic failure")

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fail,
        dataset_version=plan.dataset_version,
    )
    run_campaign_wave(runner, universe, plan, max_shards=1)
    report = requeue_campaign_failures(
        runner,
        universe,
        plan,
        allowed_error_classes=("ConnectionError",),
    )
    assert report.requeued_shards == 0
    assert report.skipped_failed_shards == 1


@pytest.mark.parametrize("tamper", ["omitted", "reordered", "modified", "overlapping"])
def test_campaign_rejects_tampered_plan_before_provider_or_output(
    tmp_path: Path,
    tamper: str,
) -> None:
    universe = BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="cn-tamper-test",
        dataset_version="campaign-tamper-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    shards = list(plan.shards)
    if tamper == "omitted":
        shards.pop()
    elif tamper == "reordered":
        shards[0], shards[1] = shards[1], shards[0]
    elif tamper == "modified":
        shards[0] = shards[0].model_copy(
            update={"instrument_ids": tuple(reversed(shards[0].instrument_ids))}
        )
    else:
        shards[1] = BackfillShard(
            **{
                **shards[1].model_dump(),
                "instrument_ids": shards[0].instrument_ids,
            }
        )
    tampered = plan.model_copy(update={"shards": tuple(shards)})
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        return []

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
    )
    with pytest.raises(ValueError, match="complete deterministic plan"):
        run_campaign_wave(runner, universe, tampered, max_shards=1)
    with pytest.raises(ValueError, match="complete deterministic plan"):
        finalize_campaign(runner, universe, tampered)

    assert calls == 0
    assert not (tmp_path / "run-inventory.json").exists()
    assert not (tmp_path / "backfill-research.duckdb").exists()
