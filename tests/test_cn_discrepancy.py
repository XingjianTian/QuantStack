from copy import deepcopy
from datetime import UTC, datetime

from quant_stack.data.providers.cn.replay import compare_replay_bars

NOW = datetime(2026, 7, 29, tzinfo=UTC)
RAW_A = "quant-data://raw/baostock.json"
RAW_B = "quant-data://raw/akshare.json"


def _row(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument_id": "XSHG.600000",
        "ts_event": "2026-07-28T07:00:00+00:00",
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "volume": 1_000_000,
        "amount": 10_050_000,
        "lineage": {"provider": "baostock_replay", "endpoint": "daily"},
    }
    row.update(updates)
    return row


def _compare(primary, comparison):
    return compare_replay_bars(
        primary,
        comparison,
        primary_provider="baostock_replay",
        comparison_provider="akshare_replay",
        primary_raw_uri=RAW_A,
        comparison_raw_uri=RAW_B,
        generated_at=NOW,
    )


def test_exact_tolerance_and_converted_volume_pass() -> None:
    primary = _row()
    comparison = deepcopy(primary)
    comparison["open"] = 10.009
    comparison["volume"] = 1_004_000
    comparison["amount"] = 10_090_000
    comparison["lineage"] = {"provider": "akshare_replay", "endpoint": "daily"}

    report = _compare([primary], [comparison])

    assert report.status == "passed"
    assert report.records == ()


def test_ohlc_missing_date_endpoint_drift_duplicate_and_suspension_semantics() -> None:
    primary = _row(volume=0, amount=0)
    comparison = _row(
        high=10.3,
        volume=100,
        amount=1000,
        lineage={"provider": "unexpected", "endpoint": ""},
    )
    missing = _row(
        instrument_id="XSHE.000001",
        ts_event="2026-07-27T07:00:00+00:00",
        lineage={"provider": "baostock_replay", "endpoint": "daily"},
    )

    report = _compare([primary, primary, missing], [comparison])
    fields = {field for record in report.records for field in record.differing_fields}

    assert report.status == "quarantined"
    assert {
        "duplicate",
        "high",
        "volume",
        "amount",
        "missing_date",
        "suspension_semantics",
    } <= fields
    assert "endpoint_source_drift" in fields
    assert report.canonical_selection_reason
    assert all(record.primary_raw_uri == RAW_A for record in report.records)
    assert all(record.comparison_raw_uri == RAW_B for record in report.records)
