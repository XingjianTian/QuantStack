from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import scripts.manage_cn_backfill_research_mirror as mirror_module
from quant_stack.data.backfill import (
    BackfillPilotRunner,
    BackfillUniverse,
    build_annual_batch_shards,
)
from scripts.manage_cn_backfill_research_mirror import (
    build_manifest,
    rebuild_index,
    verify_mirror,
)

ROOT = Path(__file__).parents[1]


def test_research_mirror_manifest_verifies_and_rebuilds_index(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(
        (ROOT / "config/cn_backfill_universe_v1.json").read_text()
    )
    shard = build_annual_batch_shards(
        universe,
        dataset_version="mirror-test-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    provider_symbol = universe.instruments[0].provider_symbol
    native = {
        "date": "2015-01-05",
        "code": provider_symbol,
        "open": "9.9",
        "high": "10.2",
        "low": "9.8",
        "close": "10.0",
        "volume": "123400",
        "amount": "1234000.50",
        "adjustflag": "3",
        "_retrieval_timestamp": datetime(2026, 8, 4, tzinfo=UTC).isoformat(),
    }
    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [native],
        dataset_version="mirror-test-v1",
        max_attempts=1,
    ).run((shard,))

    manifest = build_manifest(
        tmp_path,
        "mirror-test-v1",
        "quant-research://backfill/test/version=mirror-test-v1/",
    )
    assert manifest["canonical_eligible"] is False
    verification = verify_mirror(tmp_path)
    assert verification["inventory_rows"] == 1
    index = tmp_path / report.query_index_uri.removeprefix("quant-data://")
    index.unlink()
    proof = rebuild_index(tmp_path)
    assert proof["rows"] == 1
    assert proof["duplicate_instrument_date_keys"] == 0
    assert proof["repeat_query_equal"] is True
    assert proof["mac_worker_required_for_query"] is False
    assert (
        tmp_path
        / "pilot/query/version=mirror-test-v1/nas-query-proof.json"
    ).is_file()
    assert verify_mirror(tmp_path)["status"] == "verified"


def test_research_mirror_rejects_unlisted_data_file(tmp_path: Path) -> None:
    universe = BackfillUniverse.model_validate_json(
        (ROOT / "config/cn_backfill_universe_v1.json").read_text()
    )
    shard = build_annual_batch_shards(
        universe,
        dataset_version="mirror-extra-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    native = {
        "date": "2015-01-05",
        "code": universe.instruments[0].provider_symbol,
        "open": "9.9",
        "high": "10.2",
        "low": "9.8",
        "close": "10.0",
        "volume": "123400",
        "amount": "1234000.50",
        "adjustflag": "3",
        "_retrieval_timestamp": datetime(2026, 8, 4, tzinfo=UTC).isoformat(),
    }
    BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [native],
        dataset_version="mirror-extra-v1",
        max_attempts=1,
    ).run((shard,))
    build_manifest(
        tmp_path,
        "mirror-extra-v1",
        "quant-research://backfill/test/version=mirror-extra-v1/",
    )
    unexpected = tmp_path / "pilot/normalized/version=mirror-extra-v1/year=2015/stale.parquet"
    unexpected.parent.mkdir(parents=True, exist_ok=True)
    unexpected.write_bytes(b"stale")
    with pytest.raises(ValueError, match="unlisted files"):
        verify_mirror(tmp_path)


def test_index_validation_failure_keeps_existing_index(tmp_path: Path, monkeypatch) -> None:
    universe = BackfillUniverse.model_validate_json(
        (ROOT / "config/cn_backfill_universe_v1.json").read_text()
    )
    shard = build_annual_batch_shards(
        universe,
        dataset_version="mirror-atomic-v1",
        start=date(2015, 1, 1),
        end=date(2015, 12, 31),
        batch_size=25,
    )[0]
    native = {
        "date": "2015-01-05",
        "code": universe.instruments[0].provider_symbol,
        "open": "9.9",
        "high": "10.2",
        "low": "9.8",
        "close": "10.0",
        "volume": "123400",
        "amount": "1234000.50",
        "adjustflag": "3",
        "_retrieval_timestamp": datetime(2026, 8, 4, tzinfo=UTC).isoformat(),
    }
    report = BackfillPilotRunner(
        tmp_path,
        universe,
        lambda _ids, _start, _end: [native],
        dataset_version="mirror-atomic-v1",
        max_attempts=1,
    ).run((shard,))
    build_manifest(
        tmp_path,
        "mirror-atomic-v1",
        "quant-research://backfill/test/version=mirror-atomic-v1/",
    )
    rebuild_index(tmp_path)
    index = tmp_path / report.query_index_uri.removeprefix("quant-data://")
    original = index.read_bytes()

    def fail_validation(*_args, **_kwargs):
        raise ValueError("controlled index metric mismatch")

    monkeypatch.setattr(mirror_module, "_validate_materialized_index", fail_validation)
    with pytest.raises(ValueError, match="controlled index metric mismatch"):
        rebuild_index(tmp_path)
    assert index.read_bytes() == original
