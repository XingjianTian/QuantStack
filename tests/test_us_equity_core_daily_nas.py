from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from scripts.run_us_equity_core_daily_nas import (
    FULL_MARKET_CONFIG_SCHEMA,
    load_config,
    run_daily,
    run_weekly,
    wait_for_session_ready,
)


def _config(tmp_path: Path) -> dict[str, object]:
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "snapshot_id": "nas-core-test.v1",
                "as_of": "2026-08-25T00:00:00Z",
                "source": "curated_research_snapshot",
                "update_policy": "new_version",
                "membership_basis": "curated_research",
                "source_refs": ["https://example.test"],
                "instruments": [
                    {
                        "instrument_id": "XNAS.AAA",
                        "symbol": "AAA",
                        "canonical_venue": "XNAS",
                        "provider_symbol": "AAA",
                        "source_symbol": "AAA",
                        "name": "AAA",
                        "asset_class": "equity",
                        "listed_from": None,
                        "listed_to": None,
                        "status": "listed",
                        "currency": "USD",
                        "timezone": "America/New_York",
                    }
                ],
            }
        )
    )
    return {
        "schema_version": "quantstack.us-core-daily-nas.v1",
        "repo_root": str(Path(__file__).parents[1]),
        "snapshot": str(snapshot),
        "state": str(tmp_path / "state.json"),
        "initial_watermark": "2026-08-25",
        "work_root": str(tmp_path / "work"),
        "isolated_root": str(tmp_path / "isolated"),
        "hmac_key_file": str(tmp_path / "worker.key"),
        "yfinance_runtime_root": str(tmp_path / "yfinance"),
        "yfinance_proxy_url": "http://127.0.0.1:7890",
        "verification_mode": "readback_only",
    }


def _full_market_config(tmp_path: Path) -> dict[str, object]:
    payload = _config(tmp_path)
    payload.update(
        {
            "schema_version": FULL_MARKET_CONFIG_SCHEMA,
            "action_evidence_root": str(tmp_path / "actions"),
            "action_initial_watermark": "2026-08-25",
            "session_readiness_retry_seconds": 60,
            "session_readiness_max_attempts": 2,
        }
    )
    return payload


def test_config_requires_local_mihomo_and_readback_only(tmp_path: Path) -> None:
    payload = _config(tmp_path)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload))
    assert load_config(path)["verification_mode"] == "readback_only"

    payload["yfinance_proxy_url"] = "http://proxy.example:7890"
    path.write_text(json.dumps(payload))
    try:
        load_config(path)
    except ValueError as exc:
        assert "local Mihomo" in str(exc)
    else:
        raise AssertionError("non-local proxy was accepted")


def test_daily_processes_at_most_one_completed_session(tmp_path: Path) -> None:
    config = _config(tmp_path)
    calls: list[object] = []

    def collect(args):
        calls.append(args.session)
        return {
            "actual_requests": 1,
            "request_interval_seconds": 0.25,
            "aggregate_transfers": 1,
        }

    def accept(args):
        calls.append(args.verification_mode)
        return {
            "rows": 1,
            "instruments": 1,
            "duplicate_keys": 0,
            "verification_mode": "readback_only",
            "watermark_after": "2026-08-26",
        }

    result = run_daily(
        config,
        now=datetime(2026, 8, 27, 12, tzinfo=UTC),
        collect_fn=collect,
        accept_fn=accept,
    )

    assert calls == [date(2026, 8, 26), "readback_only"]
    assert result["session_date"] == "2026-08-26"
    assert result["provider_retries"] == 0


def test_daily_current_watermark_is_noop(tmp_path: Path) -> None:
    config = _config(tmp_path)
    Path(str(config["state"])).write_text(
        json.dumps(
            {
                "schema_version": "1.1.0",
                "dataset_id": "yfinance_us_equity_daily",
                "instrument_snapshot_id": "nas-core-test.v1",
                "dataset_version_prefix": "core-daily-v2",
                "watermark": "2026-08-26",
            }
        )
    )
    result = run_daily(config, now=datetime(2026, 8, 27, 5, tzinfo=UTC))
    assert result["status"] == "noop"
    assert result["provider_requested"] is False


def test_readiness_waits_then_records_first_available_time(tmp_path: Path) -> None:
    config = _full_market_config(tmp_path)
    responses = iter(
        [
            [{"ticker": "SPY", "Date": "2026-08-27T00:00:00-04:00"}],
            [{"ticker": "SPY", "Date": "2026-08-28T00:00:00-04:00"}],
        ]
    )

    class Transport:
        def fetch_daily_bars_with_actions(self, symbols, start, end):
            assert symbols == ["SPY"]
            assert start == end == date(2026, 8, 28)
            return next(responses)

    clocks = iter(
        [
            datetime(2026, 8, 29, 2, tzinfo=UTC),
            datetime(2026, 8, 29, 3, tzinfo=UTC),
        ]
    )
    sleeps: list[float] = []
    result = wait_for_session_ready(
        config,
        session=date(2026, 8, 28),
        sleeper=sleeps.append,
        clock=lambda: next(clocks),
        transport_factory=Transport,
    )

    assert sleeps == [60]
    assert result["status"] == "ready"
    assert result["provider_requests"] == 2
    assert result["first_ready_observed_at"] == "2026-08-29T03:00:00+00:00"
    assert result["first_ready_observed_at_asia_shanghai"] == ("2026-08-29T11:00:00+08:00")
    saved = json.loads((tmp_path / "work/reports/readiness-2026-08-28.json").read_text())
    assert [item["status"] for item in saved["attempts"]] == ["not_ready", "ready"]

    class NoSecondProviderCall:
        def fetch_daily_bars_with_actions(self, symbols, start, end):
            raise AssertionError("ready report must prevent a second Provider call")

    reused = wait_for_session_ready(
        config,
        session=date(2026, 8, 28),
        transport_factory=NoSecondProviderCall,
    )
    assert reused["reused_ready_report"] is True
    assert reused["provider_requests_current_run"] == 0


def test_readiness_exhaustion_does_not_start_full_market_collection(
    tmp_path: Path,
) -> None:
    config = _full_market_config(tmp_path)

    class Transport:
        def fetch_daily_bars_with_actions(self, symbols, start, end):
            return [{"ticker": "SPY", "Date": "2026-08-27T00:00:00-04:00"}]

    sleeps: list[float] = []
    with pytest.raises(RuntimeError, match="was not ready after 2 checks"):
        wait_for_session_ready(
            config,
            session=date(2026, 8, 28),
            sleeper=sleeps.append,
            clock=lambda: datetime(2026, 8, 29, 2, tzinfo=UTC),
            transport_factory=Transport,
        )

    assert sleeps == [60]
    report = json.loads((tmp_path / "work/reports/readiness-2026-08-28.json").read_text())
    assert report["status"] == "exhausted"

    collect_called = False

    def collect(_args):
        nonlocal collect_called
        collect_called = True
        raise AssertionError("full-market collection must remain gated")

    def not_ready(*_args, **_kwargs):
        raise RuntimeError("not ready")

    with pytest.raises(RuntimeError, match="not ready"):
        run_daily(
            config,
            now=datetime(2026, 8, 29, 12, tzinfo=UTC),
            collect_fn=collect,
            readiness_fn=not_ready,
        )
    assert collect_called is False


def test_weekly_replays_only_current_completed_session(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config["local_import_workers"] = 4
    Path(str(config["state"])).write_text(
        json.dumps(
            {
                "schema_version": "1.1.0",
                "dataset_id": "yfinance_us_equity_daily",
                "instrument_snapshot_id": "nas-core-test.v1",
                "dataset_version_prefix": "core-daily-v2",
                "watermark": "2026-08-26",
            }
        )
    )
    observed = []

    def replay(args):
        observed.append((args.session, args.local_import_workers))
        return {"status": "completed", "provider_requested": False}

    result = run_weekly(config, replay_fn=replay)
    assert observed == [(date(2026, 8, 26), 4)]
    assert result["provider_requested"] is False
