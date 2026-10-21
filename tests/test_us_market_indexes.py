from __future__ import annotations

from datetime import date

from quant_stack.data.session_parquet import install_session_package
from quant_stack.data.us_market_indexes import (
    US_MARKET_INDEXES,
    install_index_history,
    normalize_index_records,
    read_index_snapshot,
)


def _record(symbol: str, day: str, close: float) -> dict[str, object]:
    return {
        "ticker": symbol,
        "Date": day,
        "Open": close - 1,
        "High": close + 1,
        "Low": close - 2,
        "Close": close,
        "Volume": 1000,
    }


def test_index_history_and_daily_session_build_snapshot_without_hashes(tmp_path) -> None:
    history = [
        _record(symbol, day, 100 + offset)
        for symbol in US_MARKET_INDEXES
        for offset, day in enumerate(("2026-08-27", "2026-08-28"))
    ]
    install_index_history(history, tmp_path)
    daily = normalize_index_records(
        [_record(symbol, "2026-08-31", 103) for symbol in US_MARKET_INDEXES]
    )
    install_session_package(
        records=daily,
        output_root=tmp_path / "sessions",
        market="US",
        session=date(2026, 8, 31),
        dataset_id="us_market_indexes_daily",
        version="2026-08-31.session-v1",
        adjustment="raw",
        requested_instruments=4,
    )

    snapshot = read_index_snapshot(tmp_path, as_of_date=date(2026, 8, 31))

    assert [entry["symbol"] for entry in snapshot] == list(US_MARKET_INDEXES)
    assert all(entry["latest_date"] == "2026-08-31" for entry in snapshot)
    assert all(len(entry["history"]) == 3 for entry in snapshot)
    assert not list(tmp_path.rglob("*.sha256"))
