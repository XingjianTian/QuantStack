from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts/build_cn_raw_union.py"
_SPEC = importlib.util.spec_from_file_location("build_cn_raw_union_test", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def _historical(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 'XSHG.600000' instrument_id, '600000' symbol, "
            "'XSHG' venue, DATE '2026-08-06' trading_date, "
            "TIMESTAMPTZ '2026-08-06 15:00:00 Asia/Shanghai' ts_event, "
            "10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS \"close\", "
            "100.0 AS volume, 1050.0 AS amount, 'CNY' AS currency, "
            "'baostock' AS provider, 'sh.600000' AS provider_symbol, "
            "'none' AS adjustment) TO ? (FORMAT PARQUET)",
            [str(path)],
        )
    finally:
        connection.close()


def _daily(path: Path, session: str) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            f"COPY (SELECT 'XSHG.600000' instrument_id, "
            f"TIMESTAMP '{session} 15:00:00' "
            "AT TIME ZONE 'Asia/Shanghai' ts_event, 10.0 AS open, 11.0 AS high, "
            "9.0 AS low, 10.5 AS \"close\", 100.0 AS volume, 1050.0 AS amount, 'CNY' currency, "
            "struct_pack(adjustment := 'none') lineage) "
            f"TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def test_builds_unsorted_exact_union_and_is_idempotent(tmp_path: Path) -> None:
    historical = tmp_path / "historical.parquet"
    daily_12 = tmp_path / "daily-12.parquet"
    daily_13 = tmp_path / "daily-13.parquet"
    output = tmp_path / "union.parquet"
    _historical(historical)
    _daily(daily_12, "2026-08-12")
    _daily(daily_13, "2026-08-13")

    kwargs = {
        "historical_path": historical,
        "daily_paths": [daily_12, daily_13],
        "expected_sessions": [date(2026, 8, 12), date(2026, 8, 13)],
        "output_path": output,
    }
    first = _MODULE.build_raw_union(**kwargs)
    second = _MODULE.build_raw_union(**kwargs)

    assert first["created"] is True
    assert second["created"] is False
    assert first["row_count"] == 3
    assert first["duplicate_keys"] == 0
    assert first["end_date"] == "2026-08-13"


def test_rejects_session_gap_and_input_overwrite(tmp_path: Path) -> None:
    historical = tmp_path / "historical.parquet"
    daily = tmp_path / "daily.parquet"
    _historical(historical)
    _daily(daily, "2026-08-12")
    with pytest.raises(ValueError, match="expected sessions"):
        _MODULE.build_raw_union(
            historical_path=historical,
            daily_paths=[daily],
            expected_sessions=[date(2026, 8, 13)],
            output_path=tmp_path / "union.parquet",
        )
    with pytest.raises(ValueError, match="must not overwrite"):
        _MODULE.build_raw_union(
            historical_path=historical,
            daily_paths=[daily],
            expected_sessions=[date(2026, 8, 12)],
            output_path=daily,
        )
