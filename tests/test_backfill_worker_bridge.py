from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.backfill import (
    BackfillPilotRunner,
    BackfillUniverse,
    build_backfill_campaign_plan,
    build_backfill_worker_archive,
    requeue_campaign_failures,
    run_campaign_wave,
)
from quant_stack.data.worker_bundle import import_worker_archive

_UNIVERSE = Path(__file__).parents[1] / "config/cn_backfill_universe_v1.json"


def _universe() -> BackfillUniverse:
    return BackfillUniverse.model_validate_json(_UNIVERSE.read_text(encoding="utf-8"))


def _native(provider_symbol: str, observed: date) -> dict[str, object]:
    return {
        "date": observed.isoformat(),
        "code": provider_symbol,
        "open": "10",
        "high": "11",
        "low": "9",
        "close": "10",
        "volume": "100",
        "amount": "1000",
        "adjustflag": "3",
        "_retrieval_timestamp": datetime(2026, 8, 7, tzinfo=UTC).isoformat(),
    }


def test_explicit_selector_requeues_and_runs_only_one_shard(tmp_path: Path) -> None:
    universe = _universe()
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="explicit-selector-test",
        dataset_version="explicit-selector-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise ConnectionError("controlled outage")
        return []

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    first = run_campaign_wave(runner, universe, plan, max_shards=2)
    assert first.selected_shards == 2
    first_task = runner.task_for_shard(plan.shards[0])
    first_state = runner.store.load(first_task.task_id)
    assert first_state is not None
    assert first_state.task.state.value == "permanently_failed"

    selected = plan.shards[1].shard_id
    report = requeue_campaign_failures(
        runner,
        universe,
        plan,
        allowed_error_classes=("ConnectionError",),
        additional_attempts=1,
        shard_id=selected,
    )
    assert report.requeued_shards == 1
    assert report.entries[0].shard_id == selected
    resumed = run_campaign_wave(
        runner,
        universe,
        plan,
        max_shards=1,
        shard_id=selected,
    )
    assert resumed.selected_shards == 1
    assert resumed.results[0].shard_id == selected
    assert resumed.results[0].status == "empty"
    unchanged = runner.store.load(first_task.task_id)
    assert unchanged is not None
    assert unchanged.task.state.value == "permanently_failed"
    assert len(unchanged.attempts) == len(first_state.attempts)
    assert calls == 3

    with pytest.raises(ValueError, match="not in campaign plan"):
        run_campaign_wave(runner, universe, plan, max_shards=1, shard_id="missing-shard")


def test_backfill_worker_bridge_import_is_signed_idempotent_and_staging_only(
    tmp_path: Path,
) -> None:
    universe = _universe()
    plan = build_backfill_campaign_plan(
        universe,
        campaign_id="worker-bridge-test",
        dataset_version="worker-bridge-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )

    def fetch(ids, start, _end):
        aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}
        return [_native(aliases[item], start) for item in ids]

    producer = tmp_path / "producer"
    runner = BackfillPilotRunner(
        producer,
        universe,
        fetch,
        dataset_version=plan.dataset_version,
        max_attempts=1,
    )
    result = run_campaign_wave(runner, universe, plan, max_shards=1).results[0]
    audit = {
        "successful_source_fingerprint": "a" * 64,
        "completed_requests": len(result.instrument_ids),
        "request_successful_source_fingerprints": ["a" * 64] * len(result.instrument_ids),
    }
    archive = build_backfill_worker_archive(
        data_root=producer,
        shard=plan.shards[0],
        result=result,
        universe=universe,
        source_commit="b" * 40,
        environment_id="bridge-test",
        provider_transport_audit=audit,
        hmac_key=b"k" * 32,
        output_path=tmp_path / "worker.tar",
    )

    nas = tmp_path / "nas"
    imported = import_worker_archive(archive_path=archive.path, data_root=nas, hmac_key=b"k" * 32)
    replayed = import_worker_archive(archive_path=archive.path, data_root=nas, hmac_key=b"k" * 32)
    assert imported.idempotent_replay is False
    assert replayed.idempotent_replay is True
    assert imported.envelope.publication_authority == "nas-catalog-publisher-only"
    assert not (nas / "catalog/quant.duckdb").exists()
    assert not (nas / "catalog/current-manifest-set.json").exists()
