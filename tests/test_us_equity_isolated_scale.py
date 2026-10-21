import json
from datetime import date
from pathlib import Path

import pytest

from quant_stack.data.providers.yfinance import ReplayYFinanceTransport, load_us_instrument_snapshot
from quant_stack.data.worker_bundle import import_worker_archive
from scripts.run_us_equity_isolated_scale import SNAPSHOT_PATH, completed_sessions, run


def _fixture(path: Path, end: date = date(2026, 8, 24)) -> tuple[list[str], list[str]]:
    snapshot = load_us_instrument_snapshot(SNAPSHOT_PATH)
    symbols = [item.provider_symbol for item in snapshot.instruments]
    sessions = [item.isoformat() for item in completed_sessions(end)]
    records = []
    for symbol_index, symbol in enumerate(symbols, start=1):
        for session_index, trading_date in enumerate(sessions, start=1):
            close = 100.0 + symbol_index + session_index / 10
            records.append(
                {
                    "ticker": symbol,
                    "Date": trading_date,
                    "Open": close - 0.4,
                    "High": close + 0.8,
                    "Low": close - 0.9,
                    "Close": close,
                    "Adj Close": close - 0.1,
                    "Volume": 1_000_000 + session_index,
                }
            )
    path.write_text(json.dumps({"provider": "yfinance", "records": records}), encoding="utf-8")
    return symbols, sessions


def _args(tmp_path: Path, fixture: Path):
    key_path = tmp_path / "worker.key"
    key_path.write_bytes(b"s" * 32)
    return type(
        "Args",
        (),
        {
            "staging_root": tmp_path / "producer",
            "task_state_root": tmp_path / "state",
            "archive_root": tmp_path / "archives",
            "report": tmp_path / "report.json",
            "hmac_key_file": key_path,
            "commit_sha": "e" * 40,
            "environment_id": "us-scale-test",
            "dataset_version": "2026-08-24.isolated-scale-v1",
            "end": date(2026, 8, 24),
            "request_interval_seconds": 0.25,
            "yfinance_runtime_root": tmp_path / "yfinance-runtime",
            "yfinance_proxy_url": None,
            "canary_only": False,
            "fixture": fixture,
        },
    )()


def test_scale_snapshot_and_window_are_fixed_20_by_20() -> None:
    snapshot = load_us_instrument_snapshot(SNAPSHOT_PATH)

    assert len(snapshot.instruments) == 20
    assert sum(item.canonical_venue == "XNAS" for item in snapshot.instruments) == 10
    assert sum(item.canonical_venue == "XNYS" for item in snapshot.instruments) == 10
    sessions = completed_sessions(date(2026, 8, 24))
    assert len(sessions) == 20
    assert sessions[-1] == date(2026, 8, 24)


def test_scale_live_runner_requires_explicit_proxy(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    _fixture(fixture)
    args = _args(tmp_path, fixture)
    args.fixture = None

    with pytest.raises(ValueError, match="requires --yfinance-proxy-url"):
        run(args)


def test_scale_runner_is_serial_recoverable_and_nas_import_only(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    symbols, sessions = _fixture(fixture)
    args = _args(tmp_path, fixture)
    sleeps: list[float] = []

    report = run(args, sleeper=sleeps.append)

    assert report["symbols"] == symbols
    assert report["session_dates"] == sessions
    assert report["venue_counts"] == {"XNAS": 10, "XNYS": 10}
    assert report["actual_requests"] == 20
    assert report["deduplicated_tasks"] == 0
    assert report["records_received"] == 400
    assert report["parallel_requests"] is False
    assert sleeps == [0.25] * 19
    assert all(item["completed_requests"] == 1 for item in report["entries"])
    assert all(item["parallel_requests"] is False for item in report["entries"])

    key = args.hmac_key_file.read_bytes()
    nas_root = tmp_path / "nas"
    archives = sorted(args.archive_root.glob("*.tar"))
    assert len(archives) == 20
    first = [
        import_worker_archive(archive_path=archive, data_root=nas_root, hmac_key=key)
        for archive in archives
    ]
    replay = [
        import_worker_archive(archive_path=archive, data_root=nas_root, hmac_key=key)
        for archive in archives
    ]
    assert all(not item.idempotent_replay for item in first)
    assert all(item.idempotent_replay for item in replay)
    assert len(list((nas_root / "staging/worker_import_receipts").glob("*.json"))) == 20
    assert not (nas_root / "catalog/quant.duckdb").exists()
    assert not (nas_root / "catalog/current-manifest-set.json").exists()

    repeated_sleeps: list[float] = []
    repeated = run(args, sleeper=repeated_sleeps.append)
    assert repeated["actual_requests"] == 0
    assert repeated["deduplicated_tasks"] == 20
    assert repeated_sleeps == []


def test_scale_runner_resumes_after_a_symbol_request_failure(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    symbols, _ = _fixture(fixture)
    args = _args(tmp_path, fixture)
    fetches: list[str] = []
    fail_symbol = symbols[2]
    should_fail = True

    class TrackingReplayTransport(ReplayYFinanceTransport):
        def __init__(self, symbol: str) -> None:
            super().__init__(fixture)
            self.symbol = symbol

        def fetch_daily_bars(self, requested, start, end):
            nonlocal should_fail
            fetches.append(self.symbol)
            if self.symbol == fail_symbol and should_fail:
                should_fail = False
                raise ConnectionError("injected provider failure")
            return super().fetch_daily_bars(requested, start, end)

    with pytest.raises(RuntimeError, match=fail_symbol):
        run(args, transport_factory=TrackingReplayTransport, sleeper=lambda _: None)
    assert fetches == symbols[:3]
    assert len(list(args.archive_root.glob("*.tar"))) == 2

    fetches.clear()
    report = run(args, transport_factory=TrackingReplayTransport, sleeper=lambda _: None)
    assert fetches == symbols[2:]
    assert report["actual_requests"] == 18
    assert report["deduplicated_tasks"] == 2
    assert len(list(args.archive_root.glob("*.tar"))) == 20


def test_scale_runner_canary_stops_after_first_symbol_and_is_resumable(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    symbols, _ = _fixture(fixture)
    args = _args(tmp_path, fixture)
    args.canary_only = True
    sleeps: list[float] = []

    canary = run(args, sleeper=sleeps.append)

    assert canary["run_scope"] == "canary"
    assert canary["symbols"] == symbols[:1]
    assert canary["actual_requests"] == 1
    assert canary["all_succeeded"] is True
    assert canary["full_campaign_complete"] is False
    assert sleeps == []
    assert len(list(args.archive_root.glob("*.tar"))) == 1

    args.canary_only = False
    full = run(args, sleeper=lambda _: None)
    assert full["actual_requests"] == 19
    assert full["deduplicated_tasks"] == 1
    assert full["full_campaign_complete"] is True
