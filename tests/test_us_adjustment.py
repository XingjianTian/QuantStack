import gzip
import json
from copy import deepcopy
from datetime import date

import pytest

from quant_stack.data import us_corporate_actions
from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchInstrumentQuery,
    CanonicalBatchRepository,
)
from quant_stack.data.publication import ProducerEnvironment
from quant_stack.data.us_adjustment import (
    compare_us_adjustment_shadow,
    derive_us_adjustment_shadow,
    materialize_us_adjustment_shadow,
    publish_us_adjustment_shadow,
)


def _row(day, close, adjusted, *, split=0.0, dividend=0.0, volume=100.0):
    return {
        "ticker": "TEST",
        "Date": day,
        "Open": close,
        "High": close,
        "Low": close,
        "Close": close,
        "Adj Close": adjusted,
        "Volume": volume,
        "Stock Splits": split,
        "Dividends": dividend,
    }


def test_split_shadow_preserves_yahoo_latest_share_basis() -> None:
    raw = [
        _row("2026-01-02", 25, 25, volume=400),
        _row("2026-01-05", 25, 25, split=4, volume=400),
        _row("2026-01-06", 26, 26, volume=420),
    ]
    original = deepcopy(raw)

    rows = derive_us_adjustment_shadow(raw)

    assert rows[0]["split_adjusted"]["close"] == 25
    assert rows[0]["split_adjusted"]["volume"] == 400
    assert rows[1]["split_adjusted"]["close"] == 25
    assert rows[0]["total_return_action_derived"]["close"] == 25
    assert compare_us_adjustment_shadow(rows)["provider_alignment_passed"] is True
    assert raw == original


def test_cash_dividend_shadow_matches_provider_total_return_anchor() -> None:
    rows = derive_us_adjustment_shadow(
        [
            _row("2026-02-02", 100, 99),
            _row("2026-02-03", 99, 99, dividend=1),
            _row("2026-02-04", 100, 100),
        ]
    )

    assert rows[0]["total_return_action_derived"]["close"] == 99
    comparison = compare_us_adjustment_shadow(rows)
    assert comparison["corporate_action_rows"] == 1
    assert comparison["maximum_action_vs_provider_difference_bps"] == pytest.approx(0)
    assert comparison["provider_alignment_passed"] is True


def test_no_action_shadow_preserves_unadjusted_prices() -> None:
    rows = derive_us_adjustment_shadow(
        [_row("2026-03-02", 10, 10), _row("2026-03-03", 11, 11)]
    )

    assert [item["split_adjusted"]["close"] for item in rows] == [10, 11]
    assert [item["total_return_action_derived"]["close"] for item in rows] == [10, 11]
    assert compare_us_adjustment_shadow(rows)["provider_alignment_passed"] is True


def test_missing_action_evidence_cannot_silently_match_provider_adjustment() -> None:
    rows = derive_us_adjustment_shadow(
        [_row("2026-04-02", 100, 90), _row("2026-04-03", 101, 101)]
    )

    comparison = compare_us_adjustment_shadow(rows)
    assert comparison["provider_alignment_passed"] is False
    assert comparison["usable_for_indicators_or_scans"] is False


def test_first_row_dividend_is_retained_without_adjusting_outside_window() -> None:
    rows = derive_us_adjustment_shadow(
        [
            _row("2026-05-04", 100, 100, dividend=1),
            _row("2026-05-05", 101, 101),
        ]
    )

    assert rows[0]["corporate_actions"]["cash_dividend"] == 1
    assert rows[0]["total_return_action_derived"]["close"] == 100
    assert compare_us_adjustment_shadow(rows)["provider_alignment_passed"] is True


def test_materialization_writes_isolated_layers_and_stays_blocked_for_consumers(
    tmp_path,
) -> None:
    report = materialize_us_adjustment_shadow(
        {
            "XNAS.SPLIT": [
                _row("2026-01-02", 25, 25),
                _row("2026-01-05", 25, 25, split=4),
            ],
            "XNYS.NONE": [
                _row("2026-01-02", 10, 10),
                _row("2026-01-05", 11, 11),
            ],
        },
        tmp_path,
    )

    assert report["status"] == "passed"
    assert report["provider_aligned_instruments"] == 2
    assert report["raw_or_normalized_overwritten"] is False
    assert report["catalog_published"] is False
    assert report["usable_for_indicators_or_scans"] is False
    assert {path.name for path in tmp_path.glob("*.parquet")} == {
        "split-adjusted.parquet",
        "total-return-action-derived.parquet",
        "total-return-provider-shadow.parquet",
    }


def test_materialization_blocks_when_historical_raw_lacks_action_fields(tmp_path) -> None:
    records = [_row("2026-01-02", 10, 9), _row("2026-01-05", 11, 11)]
    for item in records:
        item.pop("Dividends")
        item.pop("Stock Splits")

    report = materialize_us_adjustment_shadow({"XNAS.MISSING": records}, tmp_path)

    assert report["status"] == "blocked"
    assert report["action_evidence_complete"] is False
    assert report["usable_for_indicators_or_scans"] is False


def test_passed_shadow_publishes_as_explicit_batch_research_version(
    tmp_path, monkeypatch,
) -> None:
    shadow_root = tmp_path / "shadow"
    data_root = tmp_path / "catalog"
    materialize_us_adjustment_shadow(
        {
            "XNAS.TEST": [
                _row("2026-01-02", 25, 25),
                _row("2026-01-05", 25, 25, split=4),
            ]
        },
        shadow_root,
    )

    receipt, proof = publish_us_adjustment_shadow(
        shadow_root=shadow_root,
        data_root=data_root,
        dataset_id="us_equity_split_adjusted_shadow",
        dataset_version="2026-08-25.v1",
        environment_id="test",
    )
    monkeypatch.setattr(
        ProducerEnvironment,
        "current",
        classmethod(lambda cls, environment_id="local": pytest.fail("must reuse producer")),
    )
    replay_receipt, replay_proof = publish_us_adjustment_shadow(
        shadow_root=shadow_root,
        data_root=data_root,
        dataset_id="us_equity_split_adjusted_shadow",
        dataset_version="2026-08-25.v1",
        environment_id="test",
    )

    assert receipt.publication_state.shadow_eligible is True
    assert receipt.publication_state.candidate_eligible is False
    assert proof["receipt_replay_verified"] is True
    assert proof["production_path_changed"] is False
    assert replay_receipt.receipt_id == receipt.receipt_id
    assert replay_proof["created"] is False
    repository = CanonicalBatchRepository(data_root, consumer_access="shadow")
    query = BatchBarsQuery(
        instrument_ids=("XNAS.TEST",),
        start=date.fromisoformat("2026-01-02"),
        end=date.fromisoformat("2026-01-05"),
        market="US",
        dataset_id="us_equity_split_adjusted_shadow",
        dataset_version="2026-08-25.v1",
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.connection.close()
    assert len(response.items) == 2
    assert {item.adjustment_semantics for item in response.items} == {"split_adjusted"}
    instrument_query = BatchInstrumentQuery(
        symbols=("TEST",),
        market="US",
        dataset_id="instruments_us",
        dataset_version="2026-08-25.v1",
    )
    instrument_plan = repository.prepare_instruments(instrument_query)
    try:
        instrument_response = repository.execute_instruments(
            instrument_plan, instrument_query
        )
    finally:
        instrument_plan.connection.close()
    assert [item.instrument_id for item in instrument_response.items] == ["XNAS.TEST"]


def test_blocked_shadow_cannot_be_published(tmp_path) -> None:
    shadow_root = tmp_path / "shadow"
    records = [_row("2026-01-02", 10, 9), _row("2026-01-05", 11, 11)]
    for item in records:
        item.pop("Dividends")
        item.pop("Stock Splits")
    materialize_us_adjustment_shadow({"XNAS.TEST": records}, shadow_root)

    with pytest.raises(ValueError, match="complete action evidence"):
        publish_us_adjustment_shadow(
            shadow_root=shadow_root,
            data_root=tmp_path / "catalog",
            dataset_id="us_equity_split_adjusted_shadow",
            dataset_version="2026-08-25.v1",
        )


def test_passed_adjustment_can_publish_as_explicit_ss_candidate(tmp_path) -> None:
    shadow_root = tmp_path / "shadow"
    data_root = tmp_path / "catalog"
    materialize_us_adjustment_shadow(
        {
            "XNAS.TEST": [
                _row("2026-01-02", 25, 24),
                _row("2026-01-05", 25, 25, dividend=1),
            ]
        },
        shadow_root,
    )

    receipt, proof = publish_us_adjustment_shadow(
        shadow_root=shadow_root,
        data_root=data_root,
        dataset_id="us_equity_total_return",
        dataset_version="2026-08-27.ss-v1",
        environment_id="test",
    )

    assert receipt.publication_state.candidate_eligible is True
    assert receipt.publication_state.cutover_eligible is False
    assert proof["production_path_changed"] is True
    repository = CanonicalBatchRepository(data_root, consumer_access="candidate")
    query = BatchBarsQuery(
        instrument_ids=("XNAS.TEST",),
        start=date.fromisoformat("2026-01-02"),
        end=date.fromisoformat("2026-01-05"),
        market="US",
        dataset_id="us_equity_total_return",
        dataset_version="2026-08-27.ss-v1",
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.connection.close()
    assert {item.adjustment_semantics for item in response.items} == {"total_return"}


def test_streamed_full_market_materialization_quarantines_invalid_series(
    tmp_path, monkeypatch
) -> None:
    rows = {
        "GOOD": [_row("2026-01-02", 10, 10), _row("2026-01-05", 11, 11)],
        "BAD": [
            _row("2026-01-02", 10, 10),
            _row("2026-01-05", 11, 11, dividend=20),
        ],
        "DRIFT": [_row("2026-01-02", 10, 9), _row("2026-01-05", 11, 11)],
    }
    monkeypatch.setattr(
        us_corporate_actions,
        "iter_us_raw_series",
        lambda *_args, **_kwargs: iter(rows.items()),
    )
    entries = []
    for symbol, records in rows.items():
        entries.append(
            {
                "symbol": symbol,
                "instrument_id": f"XNAS.{symbol}",
                "observed_dates": [item["Date"] for item in records],
                "actions": [
                    {
                        "Date": item["Date"],
                        "Dividends": item["Dividends"],
                        "Stock Splits": item["Stock Splits"],
                    }
                    for item in records
                    if item["Dividends"] or item["Stock Splits"] not in (0, 1)
                ],
            }
        )
    package = {
        "schema_version": "quantstack.us-corporate-actions-evidence.v1",
        "strictly_serial": True,
        "provider_retries": 0,
        "instrument_count": len(entries),
        "entries": entries,
    }
    package_path = tmp_path / "actions.json.gz"
    package_path.write_bytes(gzip.compress(json.dumps(package).encode(), mtime=0))

    report = us_corporate_actions.materialize_us_adjustment_from_action_evidence(
        data_root=tmp_path / "isolated",
        dataset_version_prefix="fixture",
        action_package=package_path,
        output_root=tmp_path / "shadow",
    )

    assert report["status"] == "passed"
    assert report["source_instruments"] == 3
    assert report["source_rows"] == 6
    assert report["instruments"] == 1
    assert report["rows"] == 2
    assert report["provider_aligned_instruments"] == 1
    assert report["provider_misaligned_instruments"] == 1
    assert report["adjustment_quarantined_instruments"] == 2
    assert {item["reason_code"] for item in report["quarantines"]} == {
        "invalid_action_semantics",
        "provider_alignment_failed",
    }
