import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.batch import BatchBarsQuery, CanonicalBatchRepository
from quant_stack.data.us_adjustment import materialize_us_adjustment_shadow
from quant_stack.data.us_adjustment_incremental import (
    materialize_us_total_return_incremental,
    publish_us_total_return_incremental,
)
from quant_stack.data.us_total_return_dataset import us_total_return_relation_sql


def _raw(day: str, close: float) -> dict[str, object]:
    return {
        "Date": day,
        "Open": close - 1,
        "High": close + 1,
        "Low": close - 2,
        "Close": close,
        "Adj Close": close,
        "Volume": 100.0,
        "Dividends": 0.0,
        "Stock Splits": 0.0,
    }


def _daily(instrument_id: str, close: float, **actions: float) -> dict[str, object]:
    return {
        **_raw("2026-08-26", close),
        "instrument_id": instrument_id,
        **actions,
    }


def _base(tmp_path: Path) -> Path:
    root = tmp_path / "base"
    materialize_us_adjustment_shadow(
        {
            "XNAS.AAA": [_raw("2026-08-25", 10.0)],
            "XNAS.BBB": [_raw("2026-08-25", 20.0)],
            "XNYS.CCC": [_raw("2026-08-25", 30.0)],
        },
        root,
    )
    return root / "total-return-action-derived.parquet"


def test_incremental_adjusts_only_event_series_and_appends_session(tmp_path: Path) -> None:
    output = tmp_path / "candidate" / "bars.parquet"
    report = tmp_path / "candidate" / "evidence.json"
    records = [
        _daily("XNAS.AAA", 11.0),
        _daily("XNAS.BBB", 21.0, Dividends=2.0),
        _daily("XNYS.CCC", 16.0, **{"Stock Splits": 2.0}),
    ]

    result = materialize_us_total_return_incremental(
        base_bars_path=_base(tmp_path),
        daily_records=records,
        session=date(2026, 8, 26),
        output_path=output,
        report_path=report,
        base_dataset_version="base-v1",
    )

    assert result.created is True
    assert result.row_count == 6
    assert result.action_instruments == 2
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT instrument_id, trading_date, close, volume FROM "
            f"{us_total_return_relation_sql(output)} "
            "ORDER BY instrument_id, trading_date"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        ("XNAS.AAA", date(2026, 8, 25), 10.0, 100.0),
        ("XNAS.AAA", date(2026, 8, 26), 11.0, 100.0),
        ("XNAS.BBB", date(2026, 8, 25), 18.0, 100.0),
        ("XNAS.BBB", date(2026, 8, 26), 21.0, 100.0),
        ("XNYS.CCC", date(2026, 8, 25), 15.0, 200.0),
        ("XNYS.CCC", date(2026, 8, 26), 16.0, 100.0),
    ]
    descriptor = json.loads(output.read_text(encoding="utf-8"))
    assert descriptor["schema_version"] == "quantstack.us-total-return-dataset.v1"
    assert len(descriptor["bars"]) == 2
    assert len(descriptor["events"]) == 1
    assert json.loads(report.read_text())["candidate_projection_rewritten_locally"] is False

    replay = materialize_us_total_return_incremental(
        base_bars_path=_base(tmp_path),
        daily_records=records,
        session=date(2026, 8, 26),
        output_path=output,
        report_path=report,
        base_dataset_version="base-v1",
    )
    assert replay.created is False


def test_incremental_requires_complete_action_evidence(tmp_path: Path) -> None:
    row = _daily("XNAS.AAA", 11.0)
    row.pop("Dividends")
    with pytest.raises(ValueError, match="action evidence is incomplete"):
        materialize_us_total_return_incremental(
            base_bars_path=_base(tmp_path),
            daily_records=[row],
            session=date(2026, 8, 26),
            output_path=tmp_path / "bars.parquet",
            report_path=tmp_path / "report.json",
            base_dataset_version="base-v1",
        )


def test_incremental_flattens_descriptors_and_compounds_later_events(tmp_path: Path) -> None:
    first = tmp_path / "first" / "bars.parquet"
    materialize_us_total_return_incremental(
        base_bars_path=_base(tmp_path),
        daily_records=[_daily("XNAS.AAA", 11.0, Dividends=1.0)],
        session=date(2026, 8, 26),
        output_path=first,
        report_path=tmp_path / "first" / "report.json",
        base_dataset_version="base-v1",
    )
    second_record = _daily("XNAS.AAA", 6.0, **{"Stock Splits": 2.0})
    second_record["Date"] = "2026-08-27"
    second = tmp_path / "second" / "bars.parquet"
    materialize_us_total_return_incremental(
        base_bars_path=first,
        daily_records=[second_record],
        session=date(2026, 8, 27),
        output_path=second,
        report_path=tmp_path / "second" / "report.json",
        base_dataset_version="first-v1",
    )
    descriptor = json.loads(second.read_text(encoding="utf-8"))
    assert len(descriptor["bars"]) == 3
    assert len(descriptor["events"]) == 2
    connection = duckdb.connect()
    try:
        closes = connection.execute(
            "SELECT trading_date,close FROM "
            f"{us_total_return_relation_sql(second)} WHERE instrument_id='XNAS.AAA' "
            "ORDER BY trading_date"
        ).fetchall()
    finally:
        connection.close()
    assert closes == [
        (date(2026, 8, 25), 4.5),
        (date(2026, 8, 26), 5.5),
        (date(2026, 8, 27), 6.0),
    ]


def test_incremental_starts_new_instrument_without_rewriting_history(
    tmp_path: Path,
) -> None:
    output = tmp_path / "candidate" / "bars.parquet"
    report = tmp_path / "candidate" / "evidence.json"

    result = materialize_us_total_return_incremental(
        base_bars_path=_base(tmp_path),
        daily_records=[_daily("XNAS.NEW", 15.0)],
        session=date(2026, 8, 26),
        output_path=output,
        report_path=report,
        base_dataset_version="base-v1",
    )

    assert result.row_count == 4
    assert result.instrument_count == 4
    assert json.loads(report.read_text())["new_instruments"] == ["XNAS.NEW"]


def test_incremental_rejects_new_instrument_with_unanchored_action(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="cannot begin with a corporate action"):
        materialize_us_total_return_incremental(
            base_bars_path=_base(tmp_path),
            daily_records=[_daily("XNAS.NEW", 15.0, Dividends=1.0)],
            session=date(2026, 8, 26),
            output_path=tmp_path / "bars.parquet",
            report_path=tmp_path / "report.json",
            base_dataset_version="base-v1",
        )


def test_incremental_candidate_publishes_without_a_second_parquet_copy(
    tmp_path: Path,
) -> None:
    result, receipt, proof = publish_us_total_return_incremental(
        base_bars_path=_base(tmp_path),
        daily_records=[
            _daily("XNAS.AAA", 11.0),
            _daily("XNAS.BBB", 21.0, Dividends=2.0),
            _daily("XNYS.CCC", 16.0, **{"Stock Splits": 2.0}),
        ],
        session=date(2026, 8, 26),
        work_root=tmp_path / "work",
        data_root=tmp_path / "catalog",
        base_dataset_version="base-v1",
        dataset_version="2026-08-26.daily-v1",
        environment_id="test",
    )

    assert receipt.publication_state.candidate_eligible is True
    assert proof["last_date"] == "2026-08-26"
    published = (
        tmp_path
        / "catalog/normalized/bars_1d/market=US"
        / "version=2026-08-26.daily-v1/bars.parquet"
    )
    assert published.stat().st_ino == result.bars_path.stat().st_ino
    repository = CanonicalBatchRepository(tmp_path / "catalog", consumer_access="candidate")
    query = BatchBarsQuery(
        instrument_ids=("XNAS.BBB",),
        start=date(2026, 8, 25),
        end=date(2026, 8, 26),
        market="US",
        dataset_id="us_equity_total_return",
        dataset_version="2026-08-26.daily-v1",
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.connection.close()
    assert [item.close for item in response.items] == [18.0, 21.0]
