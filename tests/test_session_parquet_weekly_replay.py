from __future__ import annotations

from datetime import date
from pathlib import Path

from quant_stack.data.session_parquet import install_session_package
from scripts.run_session_parquet_weekly_replay import run


def test_weekly_replay_reads_latest_session_without_second_business_file(tmp_path: Path) -> None:
    install_session_package(
        records=[
            {
                "instrument_id": "XNAS.AAPL",
                "symbol": "AAPL",
                "venue": "XNAS",
                "trading_date": date(2026, 8, 28),
                "ts_event": "2026-08-28T20:00:00Z",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
                "amount": None,
                "currency": "USD",
            }
        ],
        output_root=tmp_path,
        market="US",
        session=date(2026, 8, 28),
        dataset_id="us_equity_daily",
        version="20260828",
        adjustment="raw",
    )
    report = run(tmp_path, "US")
    assert report["equivalent"] is True
    assert report["provider_requested"] is False
    assert report["business_files_written"] == 0
