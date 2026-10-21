import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.providers.yfinance import ReplayYFinanceTransport
from scripts.build_us_core_universe import (
    build_exchange_index,
    build_snapshot,
    parse_sp500_constituents,
)
from scripts.import_us_equity_core_shards import run as run_import
from scripts.run_us_equity_core_backfill import common_sessions, create_transfer_shards
from scripts.run_us_equity_core_backfill import run as run_backfill
from scripts.run_us_equity_isolated_daily import run as run_daily


def _html(count: int = 500) -> str:
    rows = [
        '<tr><th>Symbol</th><th>Security</th><th>GICS Sector</th></tr>',
        '<tr><td>BRK.B</td><td>Berkshire Hathaway</td><td>Financials</td></tr>',
    ]
    rows.extend(
        f'<tr><td>T{index:03d}</td><td>Test {index}</td><td>Test</td></tr>'
        for index in range(1, count)
    )
    return '<html><table id="constituents">' + "".join(rows) + "</table></html>"


def _listed(count: int = 500) -> tuple[str, str]:
    nasdaq = (
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|"
        "Round Lot Size|ETF|NextShares\n"
    )
    other = (
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|"
        "Test Issue|NASDAQ Symbol\n"
    )
    other += "BRK.B|Berkshire Hathaway|N|BRK.B|N|40|N|BRK.B\n"
    for index in range(1, count):
        nasdaq += f"T{index:03d}|Test {index}|Q|N|N|100|N|N\n"
    return nasdaq, other


def test_constituent_parser_and_exchange_index_are_deterministic() -> None:
    constituents = parse_sp500_constituents(_html())
    nasdaq, other = _listed()
    venues = build_exchange_index(nasdaq, other)

    assert len(constituents) == 500
    assert constituents[0]["Symbol"] == "BRK.B"
    assert venues["BRK.B"] == "XNYS"
    assert venues["T001"] == "XNAS"


def test_core_snapshot_preserves_source_symbol_and_yahoo_mapping() -> None:
    nasdaq, other = _listed()
    snapshot = build_snapshot(
        constituents_html=_html(),
        nasdaq_listed=nasdaq,
        other_listed=other,
        snapshot_id="us-core-500-2026-08-25.v1",
        observed_at=datetime(2026, 8, 25, tzinfo=UTC),
    )

    assert len(snapshot.instruments) == 500
    assert snapshot.membership_basis == "current_sp500_constituents_not_point_in_time"
    assert snapshot.instruments[0].source_symbol == "BRK.B"
    assert snapshot.instruments[0].provider_symbol == "BRK-B"
    assert snapshot.instruments[0].instrument_id == "XNYS.BRK-B"
    assert len(snapshot.source_refs) == 3
    assert snapshot.excluded_source_symbols == ()


def test_core_snapshot_records_unsupported_exchange_exclusion() -> None:
    nasdaq, other = _listed()
    other = other.replace(
        "BRK.B|Berkshire Hathaway|N|BRK.B|N|40|N|BRK.B",
        "BRK.B|Berkshire Hathaway|Z|BRK.B|N|40|N|BRK.B",
    )
    snapshot = build_snapshot(
        constituents_html=_html(count=501),
        nasdaq_listed=nasdaq + "T500|Test 500|Q|N|N|100|N|N\n",
        other_listed=other,
        snapshot_id="us-core-500-2026-08-25.v1",
        observed_at=datetime(2026, 8, 25, tzinfo=UTC),
    )

    assert len(snapshot.instruments) == 500
    assert snapshot.excluded_source_symbols == ("BRK.B",)


def test_core_snapshot_fails_closed_on_missing_exchange_mapping() -> None:
    nasdaq, other = _listed()
    with pytest.raises(ValueError, match="T499"):
        build_snapshot(
            constituents_html=_html(),
            nasdaq_listed=nasdaq.replace("T499|Test 499|Q|N|N|100|N|N\n", ""),
            other_listed=other,
            snapshot_id="us-core-500-2026-08-25.v1",
            observed_at=datetime(2026, 8, 25, tzinfo=UTC),
        )


def _core_fixture(tmp_path: Path) -> tuple[Path, Path, list[str]]:
    symbols = ["AAA", "BBB", "CCC"]
    snapshot = {
        "schema_version": "1.0.0",
        "snapshot_id": "us-core-test.v1",
        "as_of": "2026-08-25T00:00:00Z",
        "source": "curated_research_snapshot",
        "update_policy": "new_version",
        "membership_basis": "current_sp500_constituents_not_point_in_time",
        "source_refs": ["https://example.test/constituents"],
        "instruments": [
            {
                "instrument_id": f"{'XNAS' if index < 2 else 'XNYS'}.{symbol}",
                "symbol": symbol,
                "canonical_venue": "XNAS" if index < 2 else "XNYS",
                "provider_symbol": symbol,
                "source_symbol": symbol,
                "name": symbol,
                "asset_class": "equity",
                "listed_from": None,
                "listed_to": None,
                "status": "listed",
                "currency": "USD",
                "timezone": "America/New_York",
            }
            for index, symbol in enumerate(symbols)
        ],
    }
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    sessions = common_sessions(date(2026, 8, 17), date(2026, 8, 21))
    records = []
    for symbol_index, symbol in enumerate(symbols):
        for session_index, trading_date in enumerate(sessions):
            close = 100.0 + symbol_index + session_index / 10
            records.append(
                {
                    "ticker": symbol,
                    "Date": trading_date.isoformat(),
                    "Open": close - 0.2,
                    "High": close + 0.5,
                    "Low": close - 0.6,
                    "Close": close,
                    "Adj Close": close - 0.1,
                    "Volume": 1000 + session_index,
                }
            )
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(
        json.dumps({"provider": "yfinance", "records": records}), encoding="utf-8"
    )
    return snapshot_path, fixture_path, symbols


def _backfill_args(tmp_path: Path, snapshot: Path, fixture: Path):
    key = tmp_path / "worker.key"
    key.write_bytes(b"c" * 32)
    return type(
        "Args",
        (),
        {
            "snapshot": snapshot,
            "staging_root": tmp_path / "producer",
            "task_state_root": tmp_path / "tasks",
            "archive_root": tmp_path / "archives",
            "transfer_shard_root": tmp_path / "shards",
            "report": tmp_path / "backfill.json",
            "hmac_key_file": key,
            "commit_sha": "a" * 40,
            "environment_id": "core-test",
            "dataset_version": "2016-2026.core-test-v1",
            "start": date(2026, 8, 17),
            "end": date(2026, 8, 21),
            "execution_time": datetime(2026, 8, 25, tzinfo=UTC),
            "request_interval_seconds": 0.25,
            "minimum_instruments": 3,
            "maximum_instruments": 3,
            "max_archives_per_shard": 2,
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": None,
            "fixture": fixture,
        },
    )()


def test_core_backfill_is_serial_recoverable_sharded_and_isolated(tmp_path: Path) -> None:
    snapshot, fixture, symbols = _core_fixture(tmp_path)
    args = _backfill_args(tmp_path, snapshot, fixture)
    sleeps: list[float] = []

    report = run_backfill(args, sleeper=sleeps.append)

    assert report["instrument_count"] == 3
    assert report["actual_requests"] == 3
    assert report["records_received"] == 15
    assert report["parallel_requests"] is False
    assert report["provider_retries"] == 0
    assert sleeps == [0.25, 0.25]
    assert [item["symbol"] for item in report["entries"]] == symbols
    assert [item["archive_count"] for item in report["transfer_shards"]] == [2, 1]

    import_args = type(
        "ImportArgs",
        (),
        {
            "shard_root": args.transfer_shard_root,
            "scratch_parent": tmp_path / "scratch",
            "isolated_root": tmp_path / "isolated",
            "hmac_key_file": args.hmac_key_file,
            "report": tmp_path / "import.json",
        },
    )()
    imported = run_import(import_args)
    assert imported["archives"] == 3
    assert imported["new_imports"] == 3
    assert imported["replay_imports"] == 3
    assert imported["rows"] == 15
    assert imported["instruments"] == 3
    assert imported["sessions"] == 5
    assert imported["duplicate_keys"] == 0

    repeated = run_backfill(args, sleeper=lambda _: None)
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == 3


def test_core_backfill_does_not_refetch_a_retryable_failure(tmp_path: Path) -> None:
    snapshot, fixture, symbols = _core_fixture(tmp_path)
    args = _backfill_args(tmp_path, snapshot, fixture)
    failed_symbol = symbols[1]
    should_fail = True
    fetched: list[str] = []

    class TrackingTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            nonlocal should_fail
            fetched.append(self.symbol)
            if self.symbol == failed_symbol and should_fail:
                should_fail = False
                raise ConnectionError("injected provider failure")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match=failed_symbol):
        run_backfill(args, transport_factory=TrackingTransport, sleeper=lambda _: None)
    assert fetched == symbols[:2]
    assert len(list(args.archive_root.glob("*.tar"))) == 1

    fetched.clear()
    with pytest.raises(ValueError, match="permanently_failed"):
        run_backfill(args, transport_factory=TrackingTransport, sleeper=lambda _: None)
    assert fetched == []


def test_core_transfer_shards_reject_more_than_one_hundred_archives(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="between 1 and 100"):
        create_transfer_shards([], tmp_path / "shards", max_archives=101)


def test_live_core_backfill_rejects_sub_fifty_millisecond_interval(tmp_path: Path) -> None:
    snapshot, fixture, _ = _core_fixture(tmp_path)
    args = _backfill_args(tmp_path, snapshot, fixture)
    args.fixture = None
    args.yfinance_proxy_url = "http://proxy.example:7890"
    args.request_interval_seconds = 0.049

    with pytest.raises(ValueError, match="at least a 0.05 second interval"):
        run_backfill(args)


def test_core_backfill_audits_quality_quarantine_without_refetch(tmp_path: Path) -> None:
    snapshot, fixture, symbols = _core_fixture(tmp_path)
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    bad = next(row for row in payload["records"] if row["ticker"] == symbols[1])
    bad["Low"] = bad["High"] + 1.0
    fixture.write_text(json.dumps(payload), encoding="utf-8")
    args = _backfill_args(tmp_path, snapshot, fixture)

    report = run_backfill(args, sleeper=lambda _: None)

    assert report["instrument_count"] == 3
    assert report["successful_instruments"] == 2
    assert report["quarantined_instruments"] == 1
    assert report["actual_requests"] == 3
    assert [item["symbol"] for item in report["entries"]] == [symbols[0], symbols[2]]
    assert report["quarantines"][0]["symbol"] == symbols[1]
    assert report["quarantines"][0]["failed_checks"] == ["ohlc_valid"]
    assert len(list(args.archive_root.glob("*.tar"))) == 2

    class NoFetchTransport(ReplayYFinanceTransport):
        def fetch_daily_bars(self, requested, start, end):
            raise AssertionError("terminal tasks must not refetch")

    repeated = run_backfill(
        args,
        transport_factory=lambda _: NoFetchTransport(fixture),
        sleeper=lambda _: None,
    )
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == 2
    assert repeated["quarantined_instruments"] == 1


def test_daily_runner_uses_the_versioned_core_snapshot(tmp_path: Path) -> None:
    snapshot, _, symbols = _core_fixture(tmp_path)
    trading_date = date(2026, 8, 25)
    records = []
    for index, symbol in enumerate(symbols):
        close = 100.0 + index
        records.append(
            {
                "ticker": symbol,
                "Date": trading_date.isoformat(),
                "Open": close - 0.2,
                "High": close + 0.5,
                "Low": close - 0.6,
                "Close": close,
                "Adj Close": close - 0.1,
                "Volume": 1000 + index,
            }
        )
    fixture = tmp_path / "daily-fixture.json"
    fixture.write_text(
        json.dumps({"provider": "yfinance", "records": records}), encoding="utf-8"
    )
    key = tmp_path / "daily.key"
    key.write_bytes(b"d" * 32)
    args = type(
        "DailyArgs",
        (),
        {
            "snapshot": snapshot,
            "state": tmp_path / "daily-state.json",
            "initial_watermark": date(2026, 8, 24),
            "staging_root": tmp_path / "daily-producer",
            "task_state_root": tmp_path / "daily-tasks",
            "archive_root": tmp_path / "daily-archives",
            "isolated_root": tmp_path / "daily-isolated",
            "report": tmp_path / "daily-report.json",
            "hmac_key_file": key,
            "commit_sha": "b" * 40,
            "environment_id": "core-daily-test",
            "request_interval_seconds": 0.25,
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": None,
            "fixture": fixture,
            "now": datetime(2026, 8, 26, 16, tzinfo=UTC),
        },
    )()

    report = run_daily(args, sleeper=lambda _: None)

    assert report["symbols"] == symbols
    assert report["rows"] == 3
    assert report["instruments"] == 3
    assert report["archives"] == 3
    assert report["receipts"] == 3
    assert report["duplicate_keys"] == 0
