from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from scripts.acquire_ss_legacy_fixture import (
    EXPECTED_YFINANCE_VERSION,
    FROZEN_CORPUS,
    acquire,
)

REAL_FIXTURE_ROOT = (
    Path(__file__).parent / "fixtures/stock_screener/legacy_yfinance_cn_t110"
)


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class _FakeTicker:
    calls: list[tuple[str, str, str]] = []

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def history(self, *, period: str, interval: str) -> pd.DataFrame:
        self.calls.append((self.symbol, period, interval))
        index = pd.date_range(
            "2021-07-01", "2026-08-10", freq="B", tz="Asia/Shanghai", name="Date"
        )
        return pd.DataFrame(
            {
                "Open": range(1, len(index) + 1),
                "High": range(2, len(index) + 2),
                "Low": range(0, len(index)),
                "Close": range(1, len(index) + 1),
                "Volume": [1000] * len(index),
                "Dividends": [0.0] * len(index),
                "Stock Splits": [0.0] * len(index),
            },
            index=index,
        )


class _FakeYFinance:
    __version__ = EXPECTED_YFINANCE_VERSION
    Ticker = _FakeTicker


def test_acquire_writes_exact_eight_symbol_fixture(tmp_path: Path) -> None:
    _FakeTicker.calls = []
    clock = _Clock()
    output = tmp_path / "fixture"

    manifest = acquire(
        output,
        yfinance_module=_FakeYFinance,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert manifest["symbols"] == list(FROZEN_CORPUS)
    assert manifest["artifact_count"] == 8
    assert _FakeTicker.calls == [(symbol, "5y", "1d") for symbol in FROZEN_CORPUS]
    assert len(clock.sleeps) == 7
    assert all(seconds == 2.0 for seconds in clock.sleeps)

    ledger = [
        json.loads(line)
        for line in (output / "acquisition-ledger.jsonl").read_text().splitlines()
    ]
    assert [row["logical_call"] for row in ledger] == list(range(1, 9))
    assert [row["symbol"] for row in ledger] == list(FROZEN_CORPUS)
    assert all(row["status"] == "success" for row in ledger)

    verification = json.loads((output / "window-verification.json").read_text())
    assert len(verification["symbols"]) == 8
    assert all(
        set(item["windows"]) == {"6mo", "1y", "2y", "5y"}
        for item in verification["symbols"]
    )
    for artifact in manifest["artifacts"]:
        frame = pd.read_parquet(output / artifact["artifact"])
        assert len(frame) == artifact["rows"]
        assert str(frame.index.tz) == "Asia/Shanghai"


def test_acquire_rejects_wrong_yfinance_version_before_calls(tmp_path: Path) -> None:
    class WrongVersion:
        __version__ = "1.5.2"
        Ticker = _FakeTicker

    _FakeTicker.calls = []
    with pytest.raises(RuntimeError, match="expected yfinance"):
        acquire(tmp_path / "fixture", yfinance_module=WrongVersion)
    assert _FakeTicker.calls == []


def test_acquire_fails_atomically_on_incomplete_frame(tmp_path: Path) -> None:
    class BrokenTicker(_FakeTicker):
        def history(self, *, period: str, interval: str) -> pd.DataFrame:
            frame = super().history(period=period, interval=interval)
            if self.symbol == FROZEN_CORPUS[2]:
                return frame.drop(columns=["Volume"])
            return frame

    class BrokenYFinance:
        __version__ = EXPECTED_YFINANCE_VERSION
        Ticker = BrokenTicker

    output = tmp_path / "fixture"
    with pytest.raises(RuntimeError, match="missing required columns"):
        acquire(output, yfinance_module=BrokenYFinance, sleep=lambda _: None)
    assert not output.exists()
    assert not (tmp_path / ".fixture.acquiring").exists()


def test_acquire_rejects_existing_output_root(tmp_path: Path) -> None:
    output = tmp_path / "fixture"
    output.mkdir()
    with pytest.raises(RuntimeError, match="must not already exist"):
        acquire(output, yfinance_module=_FakeYFinance)


def test_committed_fixture_matches_declared_acquisition_contract() -> None:
    manifest = json.loads((REAL_FIXTURE_ROOT / "manifest.json").read_text())
    ledger = [
        json.loads(line)
        for line in (REAL_FIXTURE_ROOT / "acquisition-ledger.jsonl")
        .read_text()
        .splitlines()
    ]
    verification = json.loads(
        (REAL_FIXTURE_ROOT / "window-verification.json").read_text()
    )

    assert manifest["symbols"] == list(FROZEN_CORPUS)
    assert manifest["artifact_count"] == len(FROZEN_CORPUS)
    assert [row["logical_call"] for row in ledger] == list(range(1, 9))
    assert [row["symbol"] for row in ledger] == list(FROZEN_CORPUS)
    assert all(row["status"] == "success" for row in ledger)

    if manifest.get("synthetic") is True:
        provenance = json.loads((REAL_FIXTURE_ROOT / "source-provenance.json").read_text())
        assert manifest["provider_calls"] == 0
        assert provenance["synthetic"] is True and provenance["provider_calls"] == 0
        assert all(row["synthetic"] is True and row["provider_calls"] == 0 for row in ledger)
        assert all("started_at" not in row and "finished_at" not in row for row in ledger)
    else:
        starts = [datetime.fromisoformat(row["started_at"]) for row in ledger]
        assert all(
            (current - previous).total_seconds() >= 2.0
            for previous, current in zip(starts, starts[1:], strict=False)
        )

    artifacts = {item["symbol"]: item for item in manifest["artifacts"]}
    windows = {item["symbol"]: item["windows"] for item in verification["symbols"]}
    assert set(artifacts) == set(FROZEN_CORPUS)
    assert set(windows) == set(FROZEN_CORPUS)
    for symbol in FROZEN_CORPUS:
        artifact = artifacts[symbol]
        frame = pd.read_parquet(REAL_FIXTURE_ROOT / artifact["artifact"])
        assert len(frame) == artifact["rows"] == 1211
        assert list(frame.columns) == [
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        ]
        assert frame.index.is_monotonic_increasing
        assert not frame.index.has_duplicates
        assert str(frame.index.tz) == "Asia/Shanghai"
        assert set(windows[symbol]) == {"6mo", "1y", "2y", "5y"}
        assert all(window["rows"] > 0 for window in windows[symbol].values())
