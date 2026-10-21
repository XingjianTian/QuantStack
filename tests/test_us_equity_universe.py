import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.build_us_equity_universe import build_snapshot, classify_security

ROOT = Path(__file__).parents[1]
FULL_MARKET_SNAPSHOT = (
    ROOT
    / "src/quant_stack/data/providers/yfinance/us_equity_xnas_xnys_2026_08_27_v1.json"
)
FULL_MARKET_REPORT = FULL_MARKET_SNAPSHOT.with_name(
    "us_equity_xnas_xnys_2026_08_27_v1.classification.json"
)


def _sources() -> tuple[str, str]:
    nasdaq = (
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|"
        "Round Lot Size|ETF|NextShares\n"
        "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N\n"
        "ADRX|Example PLC - American Depositary Shares|Q|N|N|100|N|N\n"
        "BADF|Bad Filing Corp - Common Stock|Q|N|D|100|N|N\n"
        "TEST|Test Company - Common Stock|Q|Y|N|100|N|N\n"
        "QQQ|Invesco QQQ Trust|Q|N|N|100|Y|N\n"
        "AMB|Ambiguous Holdings|Q|N|N|100|N|N\n"
        "File Creation Time: 2026082718:00|||||||\n"
    )
    other = (
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
        "BRK.B|Berkshire Hathaway Inc. Class B Common Stock|N|BRK.B|N|1|N|BRK.B\n"
        "PREF|Issuer Depositary Shares Each Representing Preferred Stock|N|PREF|N|100|N|PREF\n"
        "UNIT|Acquisition Corp Units|N|UNIT|N|100|N|UNIT\n"
        "ARCX|NYSE Arca Common Stock|P|ARCX|N|100|N|ARCX\n"
        "File Creation Time: 2026082718:00|||||||\n"
    )
    return nasdaq, other


def test_full_market_snapshot_classification_is_closed_and_deterministic() -> None:
    nasdaq, other = _sources()
    snapshot, report = build_snapshot(
        nasdaq_listed=nasdaq,
        other_listed=other,
        snapshot_id="us-equity-xnas-xnys-2026-08-27.v1",
        observed_at=datetime(2026, 8, 27, tzinfo=UTC),
    )

    assert report["input_rows"] == 10
    assert report["included_count"] == 3
    assert report["quarantined_count"] == 1
    assert report["excluded_count"] == 6
    assert report["counts_closed"] is True
    assert [item.provider_symbol for item in snapshot.instruments] == ["AAPL", "ADRX", "BRK-B"]
    assert snapshot.membership_basis == "current_xnas_xnys_listings_not_point_in_time"
    assert snapshot.instruments[-1].source_symbol == "BRK.B"
    assert snapshot.instruments[-1].instrument_id == "XNYS.BRK-B"
    assert report["quarantined"][0]["source_symbol"] == "AMB"
    assert len(snapshot.excluded_source_symbols) == 7


@pytest.mark.parametrize(
    ("name", "etf", "venue", "disposition", "reason"),
    [
        ("Issuer Ordinary Shares", "N", "XNAS", "included", "common_or_depositary_equity"),
        ("Issuer ADR", "N", "XNAS", "included", "common_or_depositary_equity"),
        ("Issuer Common Stock Warrant", "N", "XNYS", "excluded", "warrant"),
        ("Issuer Common Stock", "Y", "XNYS", "excluded", "etf_flag"),
        ("Issuer Holdings", "N", "XNAS", "quarantined", "ambiguous_security_name"),
        ("Issuer Holdings", "N", "XNYS", "included", "plain_xnys_equity_name"),
    ],
)
def test_security_name_classification(
    name: str, etf: str, venue: str, disposition: str, reason: str
) -> None:
    result = classify_security(name=name, etf=etf, venue=venue)
    assert (result.disposition, result.reason) == (disposition, reason)


def test_full_market_snapshot_rejects_duplicate_source_symbols() -> None:
    nasdaq, other = _sources()
    other = other.replace("BRK.B|", "AAPL|")
    with pytest.raises(ValueError, match="duplicate Nasdaq Trader symbols: AAPL"):
        build_snapshot(
            nasdaq_listed=nasdaq,
            other_listed=other,
            snapshot_id="duplicate.v1",
            observed_at=datetime(2026, 8, 27, tzinfo=UTC),
        )


def test_full_market_snapshot_requires_timezone_aware_observation() -> None:
    nasdaq, other = _sources()
    with pytest.raises(ValueError, match="timezone-aware"):
        build_snapshot(
            nasdaq_listed=nasdaq,
            other_listed=other,
            snapshot_id="naive.v1",
            observed_at=datetime(2026, 8, 27),
        )


def test_versioned_full_market_snapshot_matches_closed_classification_report() -> None:
    snapshot = json.loads(FULL_MARKET_SNAPSHOT.read_text(encoding="utf-8"))
    report = json.loads(FULL_MARKET_REPORT.read_text(encoding="utf-8"))
    schema = json.loads(
        (ROOT / "schemas/data/us_instrument_snapshot.schema.json").read_text(
            encoding="utf-8"
        )
    )

    Draft202012Validator(
        schema, format_checker=Draft202012Validator.FORMAT_CHECKER
    ).validate(snapshot)

    assert snapshot["snapshot_id"] == report["snapshot_id"]
    assert len(snapshot["instruments"]) == report["included_count"] == 5079
    assert report["input_rows"] == 13184
    assert report["quarantined_count"] == 3
    assert report["excluded_count"] == 8102
    assert report["counts_closed"] is True
    assert {item["source_symbol"] for item in report["quarantined"]} == {
        "CDZIP",
        "MNSBP",
        "WAFDP",
    }
    provider_symbols = [item["provider_symbol"] for item in snapshot["instruments"]]
    assert len(provider_symbols) == len(set(provider_symbols))
