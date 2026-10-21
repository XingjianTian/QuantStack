from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

_ROOT = Path(__file__).parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "plan_daily_qfq_promotion", _ROOT / "scripts/plan_daily_qfq_promotion.py"
)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

_SESSIONS = [date(2026, 8, 7), date(2026, 8, 10)]


def _bars(path: Path, *, session: str, adjustment: str, ids=("XSHG.600000",)) -> None:
    connection = duckdb.connect()
    try:
        connection.register("fixture_ids", pa.table({"instrument_id": list(ids)}))
        output = "'" + str(path).replace("'", "''") + "'"
        connection.execute(
            "COPY (SELECT instrument_id, split_part(instrument_id, '.', 2) symbol, "
            "'XSHG' venue, CAST(? AS DATE) trading_date, "
            "CAST(? AS TIMESTAMP) AT TIME ZONE 'Asia/Shanghai' ts_event, "
            "10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS close, 100.0 AS volume, "
            "1050.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := ?) AS lineage "
            f"FROM fixture_ids) TO {output} (FORMAT PARQUET)",
            [session, session, adjustment],
        )
    finally:
        connection.close()


def _isolated_bars(
    path: Path, *, session: str, ids=("XSHG.600000",), adjustment: str = "none"
) -> None:
    connection = duckdb.connect()
    try:
        connection.register("fixture_ids", pa.table({"instrument_id": list(ids)}))
        output = "'" + str(path).replace("'", "''") + "'"
        connection.execute(
            "COPY (SELECT instrument_id, "
            "CAST(? AS TIMESTAMP) AT TIME ZONE 'Asia/Shanghai' ts_event, "
            "10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS close, 100.0 AS volume, "
            "1050.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := ?) AS lineage FROM fixture_ids) "
            f"TO {output} (FORMAT PARQUET)",
            [session + " 15:00:00", adjustment],
        )
    finally:
        connection.close()


def _factor(path: Path, *, request_end="2026-08-11", ids=("XSHG.600000",)) -> None:
    path.write_text(
        json.dumps(
            {
                "request_end": request_end,
                "requests": [
                    {"instrument_id": value, "status": "ok", "rows": [{"x": "1"}]} for value in ids
                ],
            }
        ),
        encoding="utf-8",
    )


def _inputs(tmp_path: Path):
    current = tmp_path / "current.parquet"
    day1 = tmp_path / "day1.parquet"
    day2 = tmp_path / "day2.parquet"
    factor = tmp_path / "factor.json"
    _bars(current, session="2026-08-06", adjustment="qfq")
    _bars(day1, session="2026-08-07", adjustment="none")
    _bars(day2, session="2026-08-10", adjustment="none")
    _factor(factor)
    return current, day1, day2, factor


def test_plan_requires_full_rebuild_and_has_no_publication(tmp_path: Path) -> None:
    current, day1, day2, factor = _inputs(tmp_path)
    report = _MODULE.plan_daily_qfq_promotion(
        current_qfq_path=current,
        daily_raw_paths=[day1, day2],
        factor_path=factor,
        expected_sessions=_SESSIONS,
    )
    assert report["status"] == "ready"
    assert report["strategy"] == "full_rebuild_required"
    assert report["target_watermark"] == "2026-08-10"
    assert report["daily_raw"]["row_count"] == 2
    assert report["raw_inputs_unchanged"] is True
    assert report["qfq_written"] is False
    assert report["catalog_published"] is False
    assert report["production_pointer_updated"] is False


def test_plan_accepts_isolated_batches_without_trading_date_column(tmp_path: Path) -> None:
    current = tmp_path / "current.parquet"
    batch1 = tmp_path / "batch1.parquet"
    batch2 = tmp_path / "batch2.parquet"
    factor = tmp_path / "factor.json"
    _bars(current, session="2026-08-06", adjustment="qfq", ids=("XSHG.600000",))
    _isolated_bars(batch1, session="2026-08-07", ids=("XSHG.600000",))
    _isolated_bars(batch2, session="2026-08-07", ids=("XSHE.000001",))
    _factor(factor, request_end="2026-08-07", ids=("XSHG.600000", "XSHE.000001"))

    report = _MODULE.plan_daily_qfq_promotion(
        current_qfq_path=current,
        daily_raw_paths=[batch1, batch2],
        factor_path=factor,
        expected_sessions=[date(2026, 8, 7)],
    )

    assert report["daily_raw"]["sessions"] == ["2026-08-07"]
    assert report["daily_raw"]["row_count"] == 2
    assert report["target_instrument_count"] == 2


def test_plan_rejects_stale_factors(tmp_path: Path) -> None:
    current, day1, day2, factor = _inputs(tmp_path)
    _factor(factor, request_end="2026-08-07")
    with pytest.raises(ValueError, match="factor artifact is stale"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1, day2],
            factor_path=factor,
            expected_sessions=_SESSIONS,
        )


def test_plan_rejects_overlap_and_wrong_adjustment(tmp_path: Path) -> None:
    current, day1, _day2, factor = _inputs(tmp_path)
    overlap = tmp_path / "overlap.parquet"
    _bars(overlap, session="2026-08-06", adjustment="none")
    with pytest.raises(ValueError, match="overlap"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[overlap],
            factor_path=factor,
            expected_sessions=[date(2026, 8, 6)],
        )
    wrong = tmp_path / "wrong.parquet"
    _bars(wrong, session="2026-08-11", adjustment="qfq")
    with pytest.raises(ValueError, match="adjustment must be none"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1, wrong],
            factor_path=factor,
            expected_sessions=[date(2026, 8, 7), date(2026, 8, 11)],
        )


def test_plan_rejects_duplicate_daily_keys_and_factor_universe_mismatch(
    tmp_path: Path,
) -> None:
    current, day1, _day2, factor = _inputs(tmp_path)
    duplicate = tmp_path / "duplicate.parquet"
    _bars(duplicate, session="2026-08-07", adjustment="none")
    with pytest.raises(ValueError, match="strictly increasing|duplicate"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1, duplicate],
            factor_path=factor,
            expected_sessions=[date(2026, 8, 7), date(2026, 8, 7)],
        )
    _factor(factor, ids=("XSHG.600000", "XSHE.000001"))
    with pytest.raises(ValueError, match="exactly cover"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1],
            factor_path=factor,
            expected_sessions=[date(2026, 8, 7)],
        )


def test_plan_rejects_missing_verified_session(tmp_path: Path) -> None:
    current, day1, _day2, factor = _inputs(tmp_path)
    with pytest.raises(ValueError, match="verified expected sessions"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1],
            factor_path=factor,
            expected_sessions=_SESSIONS,
        )


@pytest.mark.parametrize("input_name", ["current", "day1", "factor"])
def test_cli_rejects_output_overlapping_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, input_name: str
) -> None:
    current, day1, _day2, factor = _inputs(tmp_path)
    paths = {"current": current, "day1": day1, "factor": factor}
    monkeypatch.setattr(
        "sys.argv",
        [
            "plan",
            "--current-qfq",
            str(current),
            "--daily-raw",
            str(day1),
            "--factor",
            str(factor),
            "--expected-session",
            "2026-08-07",
            "--output",
            str(paths[input_name]),
        ],
    )
    with pytest.raises(ValueError, match="must not overlap"):
        _MODULE.main()


@pytest.mark.parametrize("target", ["current", "daily"])
def test_plan_rejects_null_adjustment(tmp_path: Path, target: str) -> None:
    current, day1, day2, factor = _inputs(tmp_path)
    path = current if target == "current" else day1
    expected = "qfq" if target == "current" else "none"
    _bars(
        path,
        session="2026-08-06" if target == "current" else "2026-08-07",
        adjustment=expected,
        ids=("XSHG.600000", "XSHE.000001"),
    )
    connection = duckdb.connect()
    try:
        replacement = tmp_path / "replacement.parquet"
        output = "'" + str(replacement).replace("'", "''") + "'"
        connection.execute(
            "COPY (SELECT * REPLACE (struct_pack(adjustment := CASE WHEN "
            "instrument_id='XSHE.000001' THEN NULL ELSE ? END) AS lineage) "
            f"FROM read_parquet(?)) TO {output} (FORMAT PARQUET)",
            [expected, str(path)],
        )
    finally:
        connection.close()
    replacement.replace(path)
    _factor(factor, ids=("XSHG.600000", "XSHE.000001"))
    with pytest.raises(ValueError, match="adjustment must be"):
        _MODULE.plan_daily_qfq_promotion(
            current_qfq_path=current,
            daily_raw_paths=[day1, day2],
            factor_path=factor,
            expected_sessions=_SESSIONS,
        )
