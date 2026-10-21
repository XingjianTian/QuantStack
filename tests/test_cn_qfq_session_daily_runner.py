from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_stack.data.session_parquet import install_session_package
from scripts.run_cn_qfq_session_daily import (
    _factor_delta_is_complete,
    _validate_provider_request_budget,
    _validate_provider_request_cap,
    run,
)


def test_complete_factor_delta_skips_provider_only_for_exact_plan(tmp_path: Path) -> None:
    path = tmp_path / "factor-delta.json"
    plan = {"requests": [{"instrument_id": "XSHG.600000"}]}
    path.write_text(
        json.dumps(
            {
                "complete": True,
                "universe_count": 1,
                "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
            }
        ),
        encoding="utf-8",
    )

    assert _factor_delta_is_complete(plan, path) is True
    assert (
        _factor_delta_is_complete({"requests": [{"instrument_id": "XSHE.000001"}]}, path) is False
    )


def test_factor_request_budget_blocks_anomalous_fanout_before_provider() -> None:
    with pytest.raises(RuntimeError, match="before Provider access"):
        _validate_provider_request_budget(
            {
                "universe_count": 5_465,
                "rotation_request_count": 1_093,
                "event_candidate_count": 5_206,
                "request_count": 5_247,
            }
        )


def test_factor_request_budget_accepts_normal_rotation_and_events() -> None:
    _validate_provider_request_budget(
        {
            "universe_count": 5_465,
            "rotation_request_count": 1_093,
            "event_candidate_count": 4,
            "request_count": 1_095,
        }
    )


def test_caller_request_cap_blocks_before_provider() -> None:
    plan = {"request_count": 12}
    with pytest.raises(RuntimeError, match="caller reservation before Provider access"):
        _validate_provider_request_cap(plan, 11)
    _validate_provider_request_cap(plan, 12)
    _validate_provider_request_cap(plan, None)


def test_cn_qfq_session_daily_uses_one_session_and_no_archive_or_replay(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    sessions = tmp_path / "sessions"
    base = tmp_path / "base.parquet"
    instruments = tmp_path / "instruments.parquet"
    ledger = tmp_path / "ledger.json"
    config = tmp_path / "config.json"
    state = tmp_path / "state.json"
    bar = {
        "instrument_id": "XSHG.600000",
        "symbol": "600000",
        "venue": "XSHG",
        "trading_date": date(2026, 8, 27),
        "ts_event": "2026-08-27T07:00:00Z",
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.0,
        "volume": 100.0,
        "amount": 1000.0,
        "currency": "CNY",
    }
    pq.write_table(pa.Table.from_pylist([bar]), base)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": instrument_id,
                    "symbol": symbol,
                    "venue": "XSHG",
                    "currency": "CNY",
                    "timezone": "Asia/Shanghai",
                    "active": True,
                }
                for instrument_id, symbol in (
                    ("XSHG.600000", "600000"),
                    ("XSHG.600001", "600001"),
                )
            ]
        ),
        instruments,
    )
    ledger.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-qfq-factor-ledger.v2",
                "watermark": "2026-08-27",
                "last_checked": {"XSHG.600000": "2026-08-27"},
                "factors": [
                    {
                        "instrument_id": "XSHG.600000",
                        "effective_date": "0001-01-01",
                        "factor": 1.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    install_session_package(
        records=[
            {
                **bar,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "trading_date": date(2026, 8, 28),
                "ts_event": "2026-08-28T07:00:00Z",
                "provider_preclose": 10.0,
            }
            for instrument_id, symbol in (
                ("XSHG.600000", "600000"),
                ("XSHG.600001", "600001"),
            )
        ],
        output_root=sessions,
        market="CN",
        session=date(2026, 8, 28),
        dataset_id="cn_equity_daily_raw",
        version="20260828",
        adjustment="raw",
    )
    config.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-qfq-session-daily.v1",
                "repo_root": str(tmp_path),
                "python": "python",
                "data_root": str(data_root),
                "session_root": str(sessions),
                "work_root": str(tmp_path / "work"),
                "provider_control_root": str(tmp_path / "control"),
                "instruments_path": str(instruments),
                "ss_base_url": "http://ss.test",
                "ss_report_root": str(tmp_path / "ss-reports"),
            }
        ),
        encoding="utf-8",
    )
    state.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-qfq-session-state.v1",
                "watermark": "2026-08-27",
                "dataset_version": "baseline",
                "dataset_path": str(base),
                "factor_ledger_path": str(ledger),
                "row_count": 1,
                "instrument_count": 1,
                "first_date": "2026-08-27",
                "latest_raw_closes": {"XSHG.600000": 10.0},
            }
        ),
        encoding="utf-8",
    )

    def fake_run(command: list[str], **_kwargs: object) -> None:
        plan_path = Path(command[command.index("--plan") + 1])
        output = Path(command[command.index("--output") + 1])
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        output.write_text(
            json.dumps(
                {
                    "universe_count": len(plan["requests"]),
                    "request_start": "2026-08-28",
                    "request_end": "2026-08-28",
                    "requests": [
                        {
                            **item,
                            "status": "ok_no_adjustments",
                            "rows": [],
                            "empty_confirmations": 1,
                        }
                        for item in plan["requests"]
                    ],
                }
            ),
            encoding="utf-8",
        )

    snapshot_calls = []

    def fake_sync_daily_snapshot(**kwargs: object) -> dict[str, object]:
        snapshot_calls.append(kwargs)
        return {
            "status": "published",
            "watermark": "2026-08-28",
            "scan_id": "scan-20260828",
        }

    monkeypatch.setattr("scripts.run_cn_qfq_session_daily.subprocess.run", fake_run)
    monkeypatch.setattr(
        "scripts.run_cn_qfq_session_daily.sync_daily_snapshot",
        fake_sync_daily_snapshot,
    )
    report = run(config, state)
    assert report["status"] == "completed"
    assert report["history_rewritten"] is False
    assert (tmp_path / "work/session=2026-08-28/factor-delta-routine.json").is_file()
    assert "large_payload_hashes" not in report
    assert not list(tmp_path.rglob("*.tar"))
    assert not list(tmp_path.rglob("*.hmac"))
    promoted_state = json.loads(state.read_text(encoding="utf-8"))
    assert promoted_state["instrument_count"] == 2
    assert snapshot_calls == [
        {
            "ss_base_url": "http://ss.test",
            "dataset_version": promoted_state["dataset_version"],
            "expected_watermark": date(2026, 8, 28),
        }
    ]
    assert (
        json.loads(
            (tmp_path / "ss-reports/cn-daily-snapshot-20260828.json").read_text(encoding="utf-8")
        )["scan_id"]
        == "scan-20260828"
    )

    # A qfq no-op must still converge the scan-backed homepage. This makes a
    # transient SS publication failure recoverable without repeating Provider
    # work or rewriting the promoted dataset.
    snapshot_calls.clear()
    noop_report = run(config, state)
    assert noop_report["status"] == "noop"
    assert len(snapshot_calls) == 1
    assert snapshot_calls[0]["expected_watermark"] == date(2026, 8, 28)
