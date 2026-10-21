import gzip
import json
from datetime import date

import pytest

from quant_stack.data.providers.yfinance import ReplayYFinanceTransport
from quant_stack.data.us_corporate_actions import run_us_corporate_action_campaign


def _snapshot(path):
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "us-actions-test-v1",
                "as_of": "2026-08-25T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "instruments": [
                    {
                        "instrument_id": "XNAS.AAPL",
                        "symbol": "AAPL",
                        "canonical_venue": "XNAS",
                        "provider_symbol": "AAPL",
                        "status": "listed",
                        "currency": "USD",
                        "timezone": "America/New_York",
                    },
                    {
                        "instrument_id": "XNYS.IBM",
                        "symbol": "IBM",
                        "canonical_venue": "XNYS",
                        "provider_symbol": "IBM",
                        "status": "listed",
                        "currency": "USD",
                        "timezone": "America/New_York",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def _fixture(path):
    rows = []
    for symbol in ("AAPL", "IBM"):
        for day in (2, 3):
            rows.append(
                {
                    "ticker": symbol,
                    "Date": f"2026-01-{day:02d}",
                    "Open": 100.0,
                    "High": 101.0,
                    "Low": 99.0,
                    "Close": 100.0,
                    "Adj Close": 100.0,
                    "Volume": 1000.0,
                    "Dividends": 1.0 if symbol == "IBM" and day == 3 else 0.0,
                    "Stock Splits": 0.0,
                }
            )
    path.write_text(json.dumps({"provider": "yfinance", "records": rows}), encoding="utf-8")


def test_corporate_action_campaign_keeps_only_actions_plus_observed_dates(tmp_path) -> None:
    snapshot = tmp_path / "snapshot.json"
    fixture = tmp_path / "fixture.json"
    _snapshot(snapshot)
    _fixture(fixture)
    args = type(
        "Args",
        (),
        {
            "snapshot": snapshot,
            "state_root": tmp_path / "state",
            "package": tmp_path / "actions.json.gz",
            "report": tmp_path / "report.json",
            "start": date(2026, 1, 2),
            "end": date(2026, 1, 3),
            "request_interval_seconds": 0.0,
            "yfinance_runtime_root": None,
            "yfinance_proxy_url": None,
            "fixture": fixture,
        },
    )()

    first = run_us_corporate_action_campaign(args)
    second = run_us_corporate_action_campaign(
        args,
        transport_factory=lambda _: pytest.fail("resumed campaign must not fetch"),
    )
    package = json.loads(gzip.decompress(args.package.read_bytes()))

    assert first["actual_requests"] == 2
    assert first["corporate_action_rows"] == 1
    assert first["aggregate_packages"] == 1
    assert second["actual_requests"] == 0
    assert second["resumed_instruments"] == 2
    assert package["instrument_count"] == 2
    assert package["entries"][0]["observed_dates"] == ["2026-01-02", "2026-01-03"]
    assert package["entries"][0]["actions"] == []
    assert package["entries"][1]["actions"] == [
        {"Date": "2026-01-03", "Dividends": 1.0, "Stock Splits": 0.0}
    ]


def test_live_campaign_rejects_sub_fifty_millisecond_interval(tmp_path) -> None:
    snapshot = tmp_path / "snapshot.json"
    _snapshot(snapshot)
    args = type(
        "Args",
        (),
        {
            "snapshot": snapshot,
            "state_root": tmp_path / "state",
            "package": tmp_path / "actions.json.gz",
            "report": tmp_path / "report.json",
            "start": date(2026, 1, 2),
            "end": date(2026, 1, 3),
            "request_interval_seconds": 0.049,
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": "http://proxy.example:7890",
            "fixture": None,
        },
    )()

    with pytest.raises(ValueError, match="at least a 0.05 second interval"):
        run_us_corporate_action_campaign(args)


def test_replay_transport_zero_fills_missing_action_columns(tmp_path) -> None:
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "provider": "yfinance",
                "records": [
                    {
                        "ticker": "AAPL",
                        "Date": "2026-01-02",
                        "Open": 1,
                        "High": 1,
                        "Low": 1,
                        "Close": 1,
                        "Adj Close": 1,
                        "Volume": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    rows = ReplayYFinanceTransport(fixture).fetch_daily_bars_with_actions(
        ["AAPL"], date(2026, 1, 2), date(2026, 1, 2)
    )

    assert rows[0]["Dividends"] == 0
    assert rows[0]["Stock Splits"] == 0
