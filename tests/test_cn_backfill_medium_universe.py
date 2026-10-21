from __future__ import annotations

from datetime import date
from pathlib import Path

from quant_stack.data.backfill.models import BackfillUniverse, BackfillUniverseSource
from quant_stack.data.backfill.universe import build_historical_universe

ROOT = Path(__file__).parents[1]


def test_committed_medium_universe_binds_sources_seed_and_declared_hash() -> None:
    universe = BackfillUniverse.model_validate_json(
        (ROOT / "config/cn_backfill_medium_universe_v1.json").read_text()
    )
    assert len(universe.instruments) == 500
    assert universe.selection_seed == 2303
    assert len(universe.source_refs) == 7
    assert universe.source_refs[0].observed_date.year == 2015
    assert universe.source_refs[-1].observed_date.year == 2026
    assert universe.declared_content_sha256 == universe.content_sha256
    assert sum(item.venue == "XSHG" for item in universe.instruments) == 250
    assert sum(item.venue == "XSHE" for item in universe.instruments) == 250
    assert any(
        "historical_only_observed" in item.selection_bucket
        for item in universe.instruments
    )


def _row(code: str) -> dict[str, str]:
    return {"code": code, "tradeStatus": "1", "code_name": code}


def _source(observed: date, suffix: str) -> BackfillUniverseSource:
    return BackfillUniverseSource(
        snapshot_version=f"{observed}.test-v1",
        observed_date=observed,
        provider_version="0.9.3",
        native_schema_version="test-v1",
        raw_uri=f"quant-data://pilot/universe-snapshots/version=test/{suffix}.json",
        raw_sha256=suffix * 64,
        row_count=600,
        xshg_count=300,
        xshe_count=300,
    )


def test_historical_universe_is_balanced_deterministic_and_keeps_membership_limited() -> None:
    early_date = date(2015, 1, 5)
    recent_date = date(2025, 1, 2)
    xshg = [_row(f"sh.600{number:03d}") for number in range(200)] + [
        _row(f"sh.688{number:03d}") for number in range(100)
    ]
    xshe = [_row(f"sz.000{number:03d}") for number in range(150)] + [
        _row(f"sz.300{number:03d}") for number in range(150)
    ]
    early = xshg + xshe
    recent = xshg[25:] + xshe[25:]
    snapshots = ((early_date, early), (recent_date, recent))
    sources = (_source(early_date, "a"), _source(recent_date, "b"))

    first = build_historical_universe(
        snapshots,
        sources,
        universe_id="test-medium",
        version="test-v1",
        selection_seed=2303,
        target_count=300,
    )
    second = build_historical_universe(
        snapshots,
        sources,
        universe_id="test-medium",
        version="test-v1",
        selection_seed=2303,
        target_count=300,
    )

    assert first == second
    assert first.declared_content_sha256 == first.content_sha256
    assert sum(item.venue == "XSHG" for item in first.instruments) == 150
    assert sum(item.venue == "XSHE" for item in first.instruments) == 150
    assert any(
        "historical_only_observed" in item.selection_bucket
        for item in first.instruments
    )
    assert all(item.boundary_evidence == "unavailable" for item in first.instruments)
    assert all(item.membership_evidence == "provider_observed" for item in first.instruments)
