from __future__ import annotations

import json
import shutil
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.backfill import (
    BACKFILL_DATASET_ID,
    BackfillPilotRunner,
    BackfillRunInventory,
    BackfillUniverse,
    build_annual_batch_shards,
    rebuild_query_index,
)

ROOT = Path(__file__).parents[1]
UNIVERSE = ROOT / "config/cn_backfill_universe_v1.json"


def _universe() -> BackfillUniverse:
    return BackfillUniverse.model_validate_json(UNIVERSE.read_text(encoding="utf-8"))


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
        "_retrieval_timestamp": datetime(2026, 8, 4, tzinfo=UTC).isoformat(),
    }


def test_versioned_universe_and_shards_are_deterministic() -> None:
    universe = _universe()
    assert len(universe.instruments) == 60
    assert {item.venue for item in universe.instruments} == {"XSHG", "XSHE"}
    assert len(universe.content_sha256) == 64

    first = build_annual_batch_shards(
        universe,
        dataset_version="2015-2026.pilot-v1",
        start=date(2015, 1, 1),
        end=date(2026, 8, 4),
        batch_size=10,
    )
    second = build_annual_batch_shards(
        universe,
        dataset_version="2015-2026.pilot-v1",
        start=date(2015, 1, 1),
        end=date(2026, 8, 4),
        batch_size=10,
    )
    assert first == second
    assert len(first) == 72
    assert len({item.shard_id for item in first}) == 72
    assert all(item.dataset_id == BACKFILL_DATASET_ID for item in first)


def test_runner_retries_isolates_shards_and_replays_without_provider_io(tmp_path: Path) -> None:
    universe = _universe()
    shards = build_annual_batch_shards(
        universe,
        dataset_version="offline-pilot-v1",
        start=date(2015, 1, 1),
        end=date(2016, 12, 31),
        batch_size=25,
    )
    calls: Counter[tuple[date, tuple[str, ...]]] = Counter()
    aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}

    def fetch(ids, start, end):
        key = (start, tuple(ids))
        calls[key] += 1
        if start.year == 2015 and ids[0] == universe.instruments[0].instrument_id:
            if calls[key] == 1:
                raise TimeoutError("bounded offline retry")
        if start.year == 2016 and ids[0] == universe.instruments[25].instrument_id:
            raise ConnectionError("isolated permanent failure")
        return [_native(aliases[item], start, close=10 + index) for index, item in enumerate(ids)]

    runner = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version="offline-pilot-v1",
        max_attempts=2,
    )
    report = runner.run(shards)
    assert report.failed_shards == 1
    assert report.succeeded_shards == 5
    assert report.retries == 2
    assert report.total_rows == 95
    assert report.production_pointer_updated is False
    assert report.canonical_eligible is False
    assert report.ss_candidate is False
    assert not (tmp_path / "catalog/current-manifest-set.json").exists()
    assert report.query_proof["repeat_query_equal"] is True
    assert report.query_proof["ss_database_accessed"] is False

    calls_before = calls.copy()
    replayed = runner.run([item for item in shards if item.start_date.year == 2015])
    assert replayed.replayed_shards == 3
    assert calls == calls_before

    connection = duckdb.connect(
        str(
            tmp_path
            / "pilot/query/version=offline-pilot-v1/backfill-research.duckdb"
        ),
        read_only=True,
    )
    try:
        assert connection.execute("SELECT count(*) FROM pilot_bars").fetchone()[0] == 95
    finally:
        connection.close()


def test_duplicate_rows_fail_quality_but_preserve_raw(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="duplicate-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    provider_symbol = universe.instruments[0].provider_symbol
    duplicate = _native(provider_symbol, date(2015, 1, 5))

    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [duplicate, dict(duplicate)],
        dataset_version="duplicate-v1",
        max_attempts=1,
    ).run((shard,))

    assert report.failed_shards == 1
    assert report.total_rows == 0
    assert len(list((tmp_path / "pilot/raw").rglob("*.json"))) == 1
    state = json.loads(next((tmp_path / "pilot/state").rglob("*.json")).read_text())
    assert state["task"]["state"] == "permanently_failed"
    assert state["attempts"][0]["failure_context"]["failure_stage"] == "quality_gate"


def test_provider_empty_no_trade_row_is_reported_and_skipped(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="empty-row-v1",
        start=date(2021, 1, 1),
        end=date(2021, 12, 31),
        batch_size=25,
    )[0]
    empty = _native(universe.instruments[0].provider_symbol, date(2021, 2, 1))
    empty.update({"open": "", "high": "", "low": "", "close": "", "volume": "0", "amount": ""})
    valid = _native(universe.instruments[0].provider_symbol, date(2021, 2, 2))

    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [empty, valid],
        dataset_version="empty-row-v1",
        max_attempts=1,
    ).run((shard,))

    assert report.succeeded_shards == 1
    assert report.total_rows == 1
    assert report.anomaly_count == 1
    quality_path = next((tmp_path / "pilot/quality").rglob("*.json"))
    quality = json.loads(quality_path.read_text())
    assert quality["status"] == "warning"
    assert quality["provider_empty_observation_count"] == 1


def test_provider_suspension_row_is_reported_and_skipped(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="suspension-v1",
        start=date(2021, 1, 1),
        end=date(2021, 12, 31),
        batch_size=25,
    )[0]
    suspended = _native(universe.instruments[0].provider_symbol, date(2021, 11, 24), 38.88)
    suspended.update(
        {
            "open": "38.8800",
            "high": "38.8800",
            "low": "38.8800",
            "close": "38.8800",
            "volume": "",
            "amount": "",
        }
    )
    valid = _native(universe.instruments[0].provider_symbol, date(2021, 11, 23), 38.88)

    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [valid, suspended],
        dataset_version="suspension-v1",
        max_attempts=1,
    ).run((shard,))

    assert report.succeeded_shards == 1
    assert report.total_rows == 1
    assert report.anomaly_count == 1
    quality = json.loads(next((tmp_path / "pilot/quality").rglob("*.json")).read_text())
    assert quality["provider_empty_observations"][0]["classification"] == (
        "provider_suspended_no_trade_observation"
    )


def test_interrupted_attempt_is_persisted_and_recovered(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="interrupted-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]

    interrupted = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: (_ for _ in ()).throw(KeyboardInterrupt()),
        dataset_version="interrupted-v1",
        max_attempts=2,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.run((shard,))

    state_path = next((tmp_path / "pilot/state").rglob("*.json"))
    persisted = json.loads(state_path.read_text())
    assert persisted["task"]["state"] == "running"
    assert persisted["attempts"][0]["state"] == "running"

    provider_symbol = universe.instruments[0].provider_symbol
    recovered = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [_native(provider_symbol, date(2015, 1, 5))],
        dataset_version="interrupted-v1",
        max_attempts=2,
    ).run((shard,))
    assert recovered.succeeded_shards == 1
    assert recovered.retries == 1
    final = json.loads(state_path.read_text())
    assert [item["state"] for item in final["attempts"]] == [
        "retryable_failed",
        "succeeded",
    ]
    assert final["attempts"][0]["retry"]["error_type"] == "InterruptedExecution"


def test_deterministic_validation_failure_is_not_retried(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="deterministic-failure-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    calls = 0

    def fetch(_ids, _start, _end):
        nonlocal calls
        calls += 1
        raise ValueError("deterministic schema or unit failure")

    report = BackfillPilotRunner(
        tmp_path,
        universe,
        fetch,
        dataset_version="deterministic-failure-v1",
        max_attempts=3,
    ).run((shard,))

    assert report.failed_shards == 1
    assert report.retries == 0
    assert calls == 1
    state = json.loads(next((tmp_path / "pilot/state").rglob("*.json")).read_text())
    assert len(state["attempts"]) == 1
    assert state["attempts"][0]["retry"]["retryable"] is False


def test_missing_classification_never_infers_listing_boundaries(tmp_path: Path) -> None:
    universe = _universe()
    instruments = list(universe.instruments)
    instruments[0] = instruments[0].model_copy(
        update={"listing_date": date(2020, 1, 1), "boundary_evidence": "verified"}
    )
    instruments[1] = instruments[1].model_copy(
        update={
            "listing_date": date(2000, 1, 1),
            "delisting_date": date(2010, 12, 31),
            "boundary_evidence": "verified",
        }
    )
    universe = universe.model_copy(update={"instruments": tuple(instruments)})
    shard = build_annual_batch_shards(
        universe,
        dataset_version="missing-classification-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    empty = _native(universe.instruments[2].provider_symbol, date(2015, 2, 2))
    empty.update({"open": "", "high": "", "low": "", "close": "", "volume": "0", "amount": ""})

    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [empty],
        dataset_version="missing-classification-v1",
        max_attempts=1,
    ).run((shard,))

    classifications = report.shard_results[0].missing_classifications
    assert classifications[universe.instruments[0].instrument_id] == "verified_pre_listing"
    assert classifications[universe.instruments[1].instrument_id] == "verified_post_delisting"
    assert classifications[universe.instruments[2].instrument_id] == "provider_empty_no_trade"
    assert classifications[universe.instruments[3].instrument_id] == "unknown_missing"
    quality = json.loads(next((tmp_path / "pilot/quality").rglob("*.json")).read_text())
    assert all(
        item["listing_or_delisting_inferred"] is False
        for item in quality["missing_classifications"].values()
    )


def test_versioned_index_coexists_and_rebuilds_after_root_move(tmp_path: Path) -> None:
    universe = _universe()
    aliases = {item.instrument_id: item.provider_symbol for item in universe.instruments}

    def fetch(ids, start, _end):
        return [_native(aliases[item], start) for item in ids]

    reports = []
    for version in ("coexist-a", "coexist-b"):
        shard = build_annual_batch_shards(
            universe,
            dataset_version=version,
            start=date(2015, 1, 1),
            end=date(2015, 12, 31),
            batch_size=25,
        )[0]
        reports.append(
            BackfillPilotRunner(
                tmp_path,
                universe,
                fetch,
                dataset_version=version,
                max_attempts=1,
            ).run((shard,))
        )

    indexes = sorted((tmp_path / "pilot/query").rglob("*.duckdb"))
    assert len(indexes) == 2
    moved = tmp_path.parent / f"{tmp_path.name}-moved"
    shutil.copytree(tmp_path, moved)
    moved_index = moved / reports[0].query_index_uri.removeprefix("quant-data://")
    moved_index.unlink()
    _, proof = rebuild_query_index(moved, reports[0].inventory_uri)
    assert proof["rows"] == 25
    assert proof["source_paths_embedded"] is False
    connection = duckdb.connect(str(moved_index), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM pilot_bars").fetchone()[0] == 25
        assert connection.execute(
            "SELECT value FROM backfill_index_metadata WHERE key='table_materialization'"
        ).fetchone()[0] == "physical_from_authoritative_parquet"
    finally:
        connection.close()


def test_inventory_rejects_mixed_dataset_version_uri(tmp_path: Path) -> None:
    universe = _universe()
    shard = build_annual_batch_shards(
        universe,
        dataset_version="single-version-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    provider_symbol = universe.instruments[0].provider_symbol
    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [_native(provider_symbol, date(2015, 1, 5))],
        dataset_version="single-version-v1",
        max_attempts=1,
    ).run((shard,))
    inventory_path = tmp_path / report.inventory_uri.removeprefix("quant-data://")
    payload = json.loads(inventory_path.read_text())
    payload["shards"][0]["task_state_artifact"]["uri"] = (
        "quant-data://pilot/state/version=other-version/tasks/task.json"
    )
    with pytest.raises(ValueError, match="bind the dataset version"):
        BackfillRunInventory.model_validate(payload)
