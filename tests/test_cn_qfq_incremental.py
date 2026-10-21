from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.cn_qfq_incremental import (
    append_stable_session,
    build_factor_ledger,
    build_factor_query_plan,
    build_stable_base,
    load_daily_session_records,
    merge_factor_delta,
    project_qfq,
)
from quant_stack.data.qfq_derivative import build_qfq_derivative


def _write_bars(path: Path, *, dates: list[str]) -> None:
    ids = ["XSHG.600000", "XSHE.000001"]
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id,split_part(instrument_id,'.',2) symbol,"
            "split_part(instrument_id,'.',1) venue,CAST(trading_date AS DATE) trading_date,"
            "CAST(trading_date AS TIMESTAMP) AT TIME ZONE 'Asia/Shanghai' ts_event,"
            "10.0+day_index AS open,12.0+day_index AS high,9.0+day_index AS low,"
            "11.0+day_index AS close,100.0 AS volume,1100.0 AS amount,'CNY' AS currency,"
            "struct_pack(adjustment := 'none') lineage FROM (SELECT unnest(?) instrument_id) i "
            "CROSS JOIN (SELECT unnest(?) trading_date,unnest(?) day_index) d) "
            f"TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)",
            [ids, dates, list(range(len(dates)))],
        )
    finally:
        connection.close()


def _full_factors(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "requests": [
                    {
                        "instrument_id": "XSHG.600000",
                        "status": "ok",
                        "rows": [
                            {"dividOperateDate": "2026-01-02", "backAdjustFactor": "2"},
                        ],
                    },
                    {
                        "instrument_id": "XSHE.000001",
                        "status": "ok_no_adjustments",
                        "rows": [],
                        "empty_confirmations": 1,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def test_factor_plan_spreads_full_universe_across_five_trading_sessions() -> None:
    universe = [
        {"instrument_id": f"XSHG.{index:06d}", "provider_symbol": f"sh.{index:06d}"}
        for index in range(100)
    ]
    native = {
        item["instrument_id"]: {"code": item["provider_symbol"], "preclose": "10"}
        for item in universe
    }
    previous = {item["instrument_id"]: 10.0 for item in universe}
    covered: set[str] = set()

    for ordinal in range(5):
        plan = build_factor_query_plan(
            session=date(2026, 8, 24 + ordinal),
            session_ordinal=ordinal,
            universe=universe,
            daily_native_records=native,
            previous_closes=previous,
        )
        assert plan["request_count"] == 20
        assert plan["request_reduction_ratio"] == 0.8
        covered.update(item["instrument_id"] for item in plan["requests"])

    assert covered == set(previous)


def test_factor_plan_adds_preclose_event_and_new_listing_outside_rotation() -> None:
    universe = [
        {"instrument_id": f"XSHG.{index:06d}", "provider_symbol": f"sh.{index:06d}"}
        for index in range(10)
    ]
    native = {
        item["instrument_id"]: {"code": item["provider_symbol"], "preclose": "10"}
        for item in universe
    }
    native["XSHG.000001"]["preclose"] = "8"
    previous = {item["instrument_id"]: 10.0 for item in universe[:-1]}

    plan = build_factor_query_plan(
        session=date(2026, 8, 27),
        session_ordinal=0,
        universe=universe,
        daily_native_records=native,
        previous_closes=previous,
        last_factor_checks={item["instrument_id"]: date(2026, 8, 22) for item in universe},
    )
    by_id = {item["instrument_id"]: item["reasons"] for item in plan["requests"]}

    assert "preclose_changed_from_previous_raw_close" in by_id["XSHG.000001"]
    assert "new_listing_or_first_observation" in by_id["XSHG.000009"]
    assert plan["event_candidate_count"] == 2
    assert {item["start_date"] for item in plan["requests"]} == {"2026-08-23"}


def test_factor_plan_keeps_suspended_missing_daily_rows_in_rotation() -> None:
    universe = [
        {"instrument_id": f"XSHG.{index:06d}", "provider_symbol": f"sh.{index:06d}"}
        for index in range(10)
    ]
    native = {
        item["instrument_id"]: {"code": item["provider_symbol"], "preclose": "10"}
        for item in universe
        if item["instrument_id"] != "XSHG.000000"
    }
    previous = {item["instrument_id"]: 10.0 for item in universe}

    plan = build_factor_query_plan(
        session=date(2026, 8, 27),
        session_ordinal=0,
        universe=universe,
        daily_native_records=native,
        previous_closes=previous,
    )

    assert "XSHG.000000" in {item["instrument_id"] for item in plan["requests"]}
    assert plan["request_count"] == 2


def test_legacy_session_without_preclose_is_blocked_before_factor_planning(
    tmp_path: Path,
) -> None:
    daily = tmp_path / "legacy-session.parquet"
    _write_bars(daily, dates=["2026-08-28"])

    with pytest.raises(ValueError, match="routine factor Provider requests are blocked"):
        load_daily_session_records(daily)


def test_stable_numerator_append_and_new_factor_do_not_rewrite_history(tmp_path: Path) -> None:
    raw = tmp_path / "raw.parquet"
    daily = tmp_path / "daily.parquet"
    factors = tmp_path / "factors.json"
    ledger = tmp_path / "ledger.json"
    next_ledger = tmp_path / "ledger-next.json"
    stable_base = tmp_path / "stable-base.parquet"
    stable_daily = tmp_path / "stable-daily.parquet"
    projected = tmp_path / "qfq.parquet"
    full_current_factors = tmp_path / "full-current-factors.json"
    legacy_projected = tmp_path / "qfq-legacy.parquet"
    _write_bars(raw, dates=["2026-01-01", "2026-01-02"])
    _write_bars(daily, dates=["2026-01-03"])
    _full_factors(factors)
    build_factor_ledger(factor_path=factors, output_path=ledger, watermark=date(2026, 1, 2))
    base = build_stable_base(
        raw_bars_path=raw,
        factor_ledger_path=ledger,
        output_path=stable_base,
    )
    before = stable_base.read_bytes()

    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.cn-qfq-factor-plan.v2",
                "session": "2026-01-03",
                "rotation_slices": 5,
                "rotation_slot": 0,
                "requests": [
                    {"instrument_id": "XSHG.600000", "provider_symbol": "sh.600000"},
                    {"instrument_id": "XSHE.000001", "provider_symbol": "sz.000001"},
                ],
            }
        ),
        encoding="utf-8",
    )
    delta = tmp_path / "delta.json"
    delta.write_text(
        json.dumps(
            {
                "requests": [
                    {
                        "instrument_id": "XSHG.600000",
                        "status": "ok",
                        "rows": [{"dividOperateDate": "2026-01-03", "backAdjustFactor": "4"}],
                    },
                    {
                        "instrument_id": "XSHE.000001",
                        "status": "ok_no_adjustments",
                        "rows": [],
                        "empty_confirmations": 1,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    merge_factor_delta(
        current_ledger_path=ledger,
        factor_delta_path=delta,
        factor_plan_path=plan,
        output_path=next_ledger,
    )
    appended = append_stable_session(
        daily_raw_path=daily,
        factor_ledger_path=next_ledger,
        output_path=stable_daily,
    )
    projection = project_qfq(
        stable_paths=[stable_base, stable_daily],
        factor_ledger_path=next_ledger,
        output_path=projected,
    )
    full_current_factors.write_text(
        json.dumps(
            {
                "requests": [
                    {
                        "instrument_id": "XSHG.600000",
                        "status": "ok",
                        "rows": [
                            {"dividOperateDate": "2026-01-02", "backAdjustFactor": "2"},
                            {"dividOperateDate": "2026-01-03", "backAdjustFactor": "4"},
                        ],
                    },
                    {
                        "instrument_id": "XSHE.000001",
                        "status": "ok_no_adjustments",
                        "rows": [],
                        "empty_confirmations": 1,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    combined_raw = tmp_path / "combined-raw.parquet"
    connection = duckdb.connect()
    try:
        destination = str(combined_raw).replace("'", "''")
        connection.execute(
            f"COPY (SELECT * FROM read_parquet(?)) TO '{destination}' (FORMAT PARQUET)",
            [[str(raw), str(daily)]],
        )
    finally:
        connection.close()
    build_qfq_derivative(
        raw_bars_path=combined_raw,
        factor_path=full_current_factors,
        corpus_path=None,
        output_path=legacy_projected,
    )

    assert base["row_count"] == 4
    assert appended["row_count"] == 2
    assert stable_base.read_bytes() == before
    assert projection["row_count"] == 6
    connection = duckdb.connect()
    try:
        closes = connection.execute(
            "SELECT trading_date,close FROM read_parquet(?) "
            "WHERE instrument_id='XSHG.600000' ORDER BY trading_date",
            [str(projected)],
        ).fetchall()
        differences = connection.execute(
            "SELECT count(*) FROM ((SELECT * FROM read_parquet(?) EXCEPT ALL "
            "SELECT * FROM read_parquet(?)) UNION ALL (SELECT * FROM read_parquet(?) "
            "EXCEPT ALL SELECT * FROM read_parquet(?)))",
            [
                str(projected),
                str(legacy_projected),
                str(legacy_projected),
                str(projected),
            ],
        ).fetchone()[0]
    finally:
        connection.close()
    assert [round(row[1], 6) for row in closes] == [11.0, 6.0, 11.0]
    assert differences == 0
