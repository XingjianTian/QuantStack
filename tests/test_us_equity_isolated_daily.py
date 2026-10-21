import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import scripts.run_us_equity_isolated_daily as daily_module
from quant_stack.data.providers.yfinance import ReplayYFinanceTransport, load_us_instrument_snapshot
from scripts.run_us_equity_isolated_daily import next_completed_session, run
from scripts.run_us_equity_isolated_scale import SNAPSHOT_PATH


def _fixture(path: Path, trading_date: date = date(2026, 8, 25)) -> list[str]:
    snapshot = load_us_instrument_snapshot(SNAPSHOT_PATH)
    symbols = [item.provider_symbol for item in snapshot.instruments]
    records = []
    for index, symbol in enumerate(symbols, start=1):
        close = 100.0 + index
        records.append(
            {
                "ticker": symbol,
                "Date": trading_date.isoformat(),
                "Open": close - 0.4,
                "High": close + 0.8,
                "Low": close - 0.9,
                "Close": close,
                "Adj Close": close - 0.1,
                "Volume": 1_000_000 + index,
            }
        )
    path.write_text(json.dumps({"provider": "yfinance", "records": records}), encoding="utf-8")
    return symbols


def _args(tmp_path: Path, fixture: Path):
    key_path = tmp_path / "worker.key"
    key_path.write_bytes(b"d" * 32)
    return type(
        "Args",
        (),
        {
            "state": tmp_path / "daily-state.json",
            "initial_watermark": date(2026, 8, 24),
            "staging_root": tmp_path / "producer",
            "task_state_root": tmp_path / "tasks",
            "archive_root": tmp_path / "archives",
            "isolated_root": tmp_path / "isolated",
            "report": tmp_path / "report.json",
            "hmac_key_file": key_path,
            "commit_sha": "f" * 40,
            "environment_id": "us-daily-test",
            "request_interval_seconds": 0.25,
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": None,
            "fixture": fixture,
            "now": datetime(2026, 8, 26, 16, tzinfo=UTC),
        },
    )()


def test_completed_session_gate_uses_oldest_gap_and_release_delay() -> None:
    watermark = date(2026, 8, 24)

    assert next_completed_session(watermark, datetime(2026, 8, 25, 20, tzinfo=UTC)) is None
    assert next_completed_session(watermark, datetime(2026, 8, 26, 16, tzinfo=UTC)) == date(
        2026, 8, 25
    )
    assert next_completed_session(
        date(2026, 11, 26),
        datetime(2026, 11, 27, 19, tzinfo=UTC),
        release_delay=timedelta(hours=1),
    ) == date(2026, 11, 27)


def test_daily_run_is_serial_imported_twice_and_advances_only_after_readback(
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "fixture.json"
    symbols = _fixture(fixture)
    args = _args(tmp_path, fixture)
    sleeps: list[float] = []

    report = run(args, sleeper=sleeps.append)

    assert report["status"] == "completed"
    assert report["session_date"] == "2026-08-25"
    assert report["symbols"] == symbols
    assert report["actual_requests"] == 20
    assert report["deduplicated_tasks"] == 0
    assert report["archives"] == 20
    assert report["first_imports"] == 20
    assert report["new_imports"] == 20
    assert report["replay_imports"] == 20
    assert report["receipts"] == 20
    assert report["rows"] == 20
    assert report["instruments"] == 20
    assert report["parallel_requests"] is False
    assert report["retries"] == 0
    assert report["duplicate_keys"] == 0
    assert sleeps == [0.25] * 19
    assert json.loads(args.state.read_text(encoding="utf-8"))["watermark"] == "2026-08-25"
    assert not (args.isolated_root / "catalog/quant.duckdb").exists()
    assert not (args.isolated_root / "catalog/current-manifest-set.json").exists()

    repeated_sleeps: list[float] = []
    repeated = run(args, sleeper=repeated_sleeps.append)
    assert repeated["status"] == "no_completed_session"
    assert repeated["actual_requests"] == 0
    assert repeated_sleeps == []
    assert len(list(args.archive_root.rglob("*.tar"))) == 20


def test_daily_run_does_not_refetch_failed_task_or_advance_watermark(
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "fixture.json"
    symbols = _fixture(fixture)
    args = _args(tmp_path, fixture)
    failed_symbol = symbols[2]
    should_fail = True
    fetches: list[str] = []

    class TrackingReplayTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            nonlocal should_fail
            fetches.append(self.symbol)
            if self.symbol == failed_symbol and should_fail:
                should_fail = False
                raise ConnectionError("injected provider failure")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match=failed_symbol):
        run(args, transport_factory=TrackingReplayTransport, sleeper=lambda _: None)
    assert fetches == symbols[:3]
    assert not args.state.exists()
    assert len(list(args.archive_root.rglob("*.tar"))) == 2

    fetches.clear()
    with pytest.raises(ValueError, match="permanently_failed"):
        run(args, transport_factory=TrackingReplayTransport, sleeper=lambda _: None)
    assert fetches == []
    assert not args.state.exists()


def test_live_daily_requires_explicit_proxy(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    _fixture(fixture)
    args = _args(tmp_path, fixture)
    args.fixture = None

    with pytest.raises(ValueError, match="requires --yfinance-proxy-url"):
        run(args)


def test_live_daily_rejects_sub_fifty_millisecond_interval(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    _fixture(fixture)
    args = _args(tmp_path, fixture)
    args.fixture = None
    args.yfinance_proxy_url = "http://proxy.example:7890"
    args.request_interval_seconds = 0.049

    with pytest.raises(ValueError, match="at least a 0.05 second interval"):
        run(args)


def test_daily_state_is_bound_to_the_instrument_snapshot(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    _fixture(fixture)
    args = _args(tmp_path, fixture)
    args.state.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "dataset_id": "yfinance_us_equity_daily",
                "instrument_snapshot_id": "another-snapshot.v1",
                "watermark": "2026-08-24",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported US daily incremental state"):
        run(args, sleeper=lambda _: None)


def test_import_failure_keeps_watermark_for_recovery(tmp_path: Path, monkeypatch) -> None:
    fixture = tmp_path / "fixture.json"
    _fixture(fixture)
    args = _args(tmp_path, fixture)
    original = daily_module.import_worker_archive
    calls = 0

    def fail_first_import(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected isolated write failure")
        return original(**kwargs)

    monkeypatch.setattr(daily_module, "import_worker_archive", fail_first_import)
    with pytest.raises(OSError, match="isolated write failure"):
        run(args, sleeper=lambda _: None)

    assert not args.state.exists()
    assert len(list(args.archive_root.rglob("*.tar"))) == 20

    recovered = run(args, sleeper=lambda _: None)
    assert recovered["actual_requests"] == 0
    assert recovered["new_imports"] == 18
    assert recovered["receipts"] == 20
    assert json.loads(args.state.read_text(encoding="utf-8"))["watermark"] == "2026-08-25"
