from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from quant_stack.data.providers.yfinance import (
    LiveYFinanceTransport,
    YFinanceEmptyResponseError,
    YFinanceRateLimitError,
    load_calibrated_interval,
    resolve_yfinance_interval,
    write_pacing_policy,
)
from scripts.calibrate_yfinance_pacing import calibrate


def _snapshot(path: Path, count: int = 6) -> None:
    instruments = []
    for index in range(count):
        symbol = f"T{index:03d}"
        instruments.append(
            {
                "instrument_id": f"XNAS.{symbol}",
                "symbol": symbol,
                "canonical_venue": "XNAS",
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
        )
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "pacing-test.v1",
                "as_of": "2026-08-27T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "membership_basis": "curated_research",
                "source_refs": ["https://example.test"],
                "instruments": instruments,
            }
        ),
        encoding="utf-8",
    )


def _args(tmp_path: Path):
    snapshot = tmp_path / "snapshot.json"
    _snapshot(snapshot)
    return type(
        "Args",
        (),
        {
            "snapshot": snapshot,
            "start": date(2026, 8, 24),
            "end": date(2026, 8, 26),
            "yfinance_runtime_root": tmp_path / "runtime",
            "yfinance_proxy_url": "http://proxy.example:7890",
            "report": tmp_path / "report.json",
            "stage_size": 2,
            "symbol_offset": 0,
            "intervals": (1.0, 0.5, 0.25),
            "fallback_interval_seconds": 10.0,
            "direct_250ms": False,
            "direct_100ms": False,
            "direct_50ms": False,
        },
    )()


class _SuccessfulTransport:
    def fetch_daily_bars(self, symbols, start, end):
        return [{"ticker": symbols[0], "Date": start.isoformat()}]


def test_default_and_persisted_interval(tmp_path: Path) -> None:
    assert resolve_yfinance_interval(tmp_path, None) == 1.0
    write_pacing_policy(
        tmp_path,
        selected_interval_seconds=0.5,
        status="calibrated",
        stages=[],
    )
    assert load_calibrated_interval(tmp_path) == 0.5
    assert resolve_yfinance_interval(tmp_path, None) == 0.5
    assert resolve_yfinance_interval(tmp_path, 1.0) == 1.0


def test_live_transport_owns_minimum_interval_invariant(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least a 0.05 second interval"):
        LiveYFinanceTransport(runtime_root=tmp_path, minimum_interval_seconds=0.049)

    assert (
        LiveYFinanceTransport(
            runtime_root=tmp_path, minimum_interval_seconds=0.05
        ).minimum_interval_seconds
        == 0.05
    )
    assert LiveYFinanceTransport(runtime_root=tmp_path).minimum_interval_seconds == 1.0

    write_pacing_policy(
        tmp_path,
        selected_interval_seconds=0.5,
        status="calibrated",
        stages=[],
    )
    assert LiveYFinanceTransport(runtime_root=tmp_path).minimum_interval_seconds == 0.5


def test_calibration_rejects_stage_above_100(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.stage_size = 101
    args.intervals = (0.25,)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.5,
        status="partial",
        stages=[],
    )
    args.fallback_interval_seconds = 0.5

    with pytest.raises(ValueError, match="limited to 100"):
        calibrate(args, transport_factory=lambda interval, symbol: _SuccessfulTransport())


def test_calibration_rejects_total_budget_above_300(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.stage_size = 101

    with pytest.raises(ValueError, match="limited to 300"):
        calibrate(args, transport_factory=lambda interval, symbol: _SuccessfulTransport())


def test_calibration_rejects_unapproved_interval(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.intervals = (1.0, 0.75, 0.5, 0.25)

    with pytest.raises(ValueError, match="authorized ordered suffix"):
        calibrate(args, transport_factory=lambda interval, symbol: _SuccessfulTransport())


def test_replacement_calibration_rejects_unsafe_fallback(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.intervals = (0.25,)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.5,
        status="partial",
        stages=[],
    )
    args.fallback_interval_seconds = 0.25

    with pytest.raises(ValueError, match="persisted safe interval"):
        calibrate(args, transport_factory=lambda interval, symbol: _SuccessfulTransport())


def test_replacement_calibration_uses_next_authorized_suffix(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.intervals = (0.25,)
    args.fallback_interval_seconds = 0.5
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.5,
        status="partial",
        stages=[],
    )

    report = calibrate(
        args, transport_factory=lambda interval, symbol: _SuccessfulTransport()
    )

    assert report["selected_interval_seconds"] == 0.25
    assert [stage["interval_seconds"] for stage in report["stages"]] == [0.25]


def test_clean_calibration_selects_fastest_stage(tmp_path: Path) -> None:
    args = _args(tmp_path)
    calls: list[tuple[float, str]] = []

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        return _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"

    assert report["status"] == "calibrated"
    assert report["selected_interval_seconds"] == 0.25
    assert [item[0] for item in calls] == [1.0, 1.0, 0.5, 0.5, 0.25, 0.25]
    assert load_calibrated_interval(route_root) == 0.25


def test_rate_limit_stops_and_keeps_last_clean_stage(tmp_path: Path) -> None:
    args = _args(tmp_path)
    calls: list[tuple[float, str]] = []

    class RateLimitedTransport(_SuccessfulTransport):
        def fetch_daily_bars(self, symbols, start, end):
            raise YFinanceRateLimitError("test limit")

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        return RateLimitedTransport() if interval == 0.5 else _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"

    assert report["status"] == "rate_limited"
    assert report["selected_interval_seconds"] == 1.0
    assert [item[0] for item in calls] == [1.0, 1.0, 0.5]
    assert load_calibrated_interval(route_root) == 1.0


def test_empty_response_is_counted_without_aborting_stage(tmp_path: Path) -> None:
    args = _args(tmp_path)
    calls: list[tuple[float, str]] = []

    class EmptyTransport(_SuccessfulTransport):
        def fetch_daily_bars(self, symbols, start, end):
            raise YFinanceEmptyResponseError("no bar for bounded session")

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        if interval == 0.25 and symbol == "T004":
            return EmptyTransport()
        return _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"

    assert report["status"] == "calibrated"
    assert report["selected_interval_seconds"] == 0.25
    assert report["stages"][-1] == {
        "interval_seconds": 0.25,
        "stage_size": 2,
        "attempted_requests": 2,
        "successful_requests": 1,
        "empty_responses": 1,
        "outcome": "clean",
    }
    assert len(calls) == 6
    assert load_calibrated_interval(route_root) == 0.25


def test_direct_250ms_stage_persists_only_after_all_requests_succeed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_250ms = True
    calls: list[tuple[float, str]] = []

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        return _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"

    assert report["status"] == "calibrated"
    assert report["direct_250ms"] is True
    assert report["selected_interval_seconds"] == 0.25
    assert calls == [(0.25, "T000"), (0.25, "T001")]
    assert load_calibrated_interval(route_root) == 0.25


def test_direct_250ms_stage_stops_on_first_empty_without_policy(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_250ms = True

    class EmptyTransport(_SuccessfulTransport):
        def fetch_daily_bars(self, symbols, start, end):
            raise YFinanceEmptyResponseError("empty")

    report = calibrate(args, transport_factory=lambda interval, symbol: EmptyTransport())
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"

    assert report["status"] == "empty_response"
    assert report["selected_interval_seconds"] is None
    assert report["policy_path"] is None
    assert report["stages"][0]["attempted_requests"] == 1
    assert not (route_root / "pacing-policy.json").exists()


def test_direct_100ms_stage_persists_only_after_all_requests_succeed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_100ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.25,
        status="calibrated",
        stages=[],
    )
    calls: list[tuple[float, str]] = []

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        return _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)
    assert report["status"] == "calibrated"
    assert report["direct_100ms"] is True
    assert report["direct_interval_seconds"] == 0.1
    assert report["selected_interval_seconds"] == 0.1
    assert calls == [(0.1, "T000"), (0.1, "T001")]
    assert load_calibrated_interval(route_root) == 0.1


@pytest.mark.parametrize("persisted_interval", [None, 0.5, 0.1])
def test_direct_100ms_rejects_unqualified_policy_before_transport(
    tmp_path: Path, persisted_interval: float | None
) -> None:
    args = _args(tmp_path)
    args.direct_100ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    if persisted_interval is not None:
        write_pacing_policy(
            route_root,
            selected_interval_seconds=persisted_interval,
            status="calibrated",
            stages=[],
        )
    calls = 0

    def factory(interval: float, symbol: str):
        nonlocal calls
        calls += 1
        return _SuccessfulTransport()

    with pytest.raises(
        ValueError, match="requires an existing calibrated 0.25 second policy"
    ):
        calibrate(args, transport_factory=factory)

    assert calls == 0


def test_direct_100ms_failure_preserves_existing_policy(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_100ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.25,
        status="calibrated",
        stages=[],
    )

    class EmptyTransport(_SuccessfulTransport):
        def fetch_daily_bars(self, symbols, start, end):
            raise YFinanceEmptyResponseError("empty")

    report = calibrate(args, transport_factory=lambda interval, symbol: EmptyTransport())

    assert report["status"] == "empty_response"
    assert report["selected_interval_seconds"] == 0.25
    assert report["policy_path"] is None
    assert report["stages"][0]["attempted_requests"] == 1
    assert load_calibrated_interval(route_root) == 0.25


def test_direct_50ms_stage_persists_only_after_all_requests_succeed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_50ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.1,
        status="calibrated",
        stages=[],
    )
    calls: list[tuple[float, str]] = []

    def factory(interval: float, symbol: str):
        calls.append((interval, symbol))
        return _SuccessfulTransport()

    report = calibrate(args, transport_factory=factory)

    assert report["status"] == "calibrated"
    assert report["direct_50ms"] is True
    assert report["direct_interval_seconds"] == 0.05
    assert report["selected_interval_seconds"] == 0.05
    assert calls == [(0.05, "T000"), (0.05, "T001")]
    assert load_calibrated_interval(route_root) == 0.05


@pytest.mark.parametrize("persisted_interval", [None, 0.25, 0.05])
def test_direct_50ms_rejects_unqualified_policy_before_transport(
    tmp_path: Path, persisted_interval: float | None
) -> None:
    args = _args(tmp_path)
    args.direct_50ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    if persisted_interval is not None:
        write_pacing_policy(
            route_root,
            selected_interval_seconds=persisted_interval,
            status="calibrated",
            stages=[],
        )
    calls = 0

    def factory(interval: float, symbol: str):
        nonlocal calls
        calls += 1
        return _SuccessfulTransport()

    with pytest.raises(
        ValueError, match="requires an existing calibrated 0.1 second policy"
    ):
        calibrate(args, transport_factory=factory)

    assert calls == 0


def test_direct_50ms_failure_preserves_existing_policy(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.direct_50ms = True
    route_root = args.yfinance_runtime_root / "routes/explicit-proxy-v1"
    write_pacing_policy(
        route_root,
        selected_interval_seconds=0.1,
        status="calibrated",
        stages=[],
    )

    class EmptyTransport(_SuccessfulTransport):
        def fetch_daily_bars(self, symbols, start, end):
            raise YFinanceEmptyResponseError("empty")

    report = calibrate(args, transport_factory=lambda interval, symbol: EmptyTransport())

    assert report["status"] == "empty_response"
    assert report["selected_interval_seconds"] == 0.1
    assert report["policy_path"] is None
    assert report["stages"][0]["attempted_requests"] == 1
    assert load_calibrated_interval(route_root) == 0.1
