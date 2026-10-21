#!/usr/bin/env python3
"""Plan a fail-closed full qfq rebuild from isolated daily raw inputs."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import duckdb

from quant_stack.data.storage.atomic import atomic_replace_bytes


@contextmanager
def _bounded_connection(scratch_parent: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with tempfile.TemporaryDirectory(
        dir=scratch_parent,
        prefix=".qfq-plan-duckdb-",
    ) as temporary_directory:
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit='384MB'")
            connection.execute("SET threads=1")
            connection.execute("SET preserve_insertion_order=false")
            connection.execute("SET temp_directory=?", [temporary_directory])
            yield connection
        finally:
            connection.close()


def _trading_date_expression(connection: duckdb.DuckDBPyConnection, path: Path) -> str:
    columns = {
        str(row[0])
        for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path.resolve())]
        ).fetchall()
    }
    if "trading_date" in columns:
        return "trading_date"
    if "ts_event" in columns:
        return "CAST(timezone('Asia/Shanghai', ts_event) AS DATE)"
    raise ValueError(f"Parquet input has neither trading_date nor ts_event: {path}")


def _parquet_profile(
    path: Path, *, expected_adjustment: str, scratch_parent: Path | None = None
) -> dict[str, object]:
    with _bounded_connection(scratch_parent or path.resolve().parent) as connection:
        trading_date = _trading_date_expression(connection, path)
        row = connection.execute(
            f"SELECT count(*), count(DISTINCT (instrument_id, {trading_date})), "
            f"count(DISTINCT instrument_id), min({trading_date}), max({trading_date}), "
            "count(*) FILTER (WHERE lineage.adjustment IS NULL "
            "OR lineage.adjustment != ?) "
            "FROM read_parquet(?)",
            [expected_adjustment, str(path.resolve())],
        ).fetchone()
        instruments = {
            str(item[0])
            for item in connection.execute(
                "SELECT DISTINCT instrument_id FROM read_parquet(?)",
                [str(path.resolve())],
            ).fetchall()
        }
    if row is None or row[0] < 1:
        raise ValueError(f"Parquet input is empty: {path}")
    if row[0] != row[1]:
        raise ValueError(f"Parquet input contains duplicate instrument-date keys: {path}")
    if row[5] != 0:
        raise ValueError(f"Parquet adjustment must be {expected_adjustment}: {path}")
    return {
        "path": str(path.resolve()),
        "row_count": int(row[0]),
        "instrument_count": int(row[2]),
        "first_date": row[3],
        "last_date": row[4],
        "instruments": instruments,
    }


def _parquet_keys(
    path: Path, *, scratch_parent: Path | None = None
) -> list[tuple[str, date]]:
    with _bounded_connection(scratch_parent or path.resolve().parent) as connection:
        trading_date = _trading_date_expression(connection, path)
        return [
            (str(instrument_id), session)
            for instrument_id, session in connection.execute(
                f"SELECT instrument_id, {trading_date} FROM read_parquet(?)",
                [str(path.resolve())],
            ).fetchall()
        ]


def _factor_profile(path: Path, expected_ids: set[str], target_date: date) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    requests = payload.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("qfq factor artifact has no request list")
    request_end = date.fromisoformat(str(payload.get("request_end", "")))
    if request_end < target_date:
        raise ValueError(
            f"qfq factor artifact is stale: request_end={request_end}, target={target_date}"
        )
    ids = [str(item.get("instrument_id")) for item in requests]
    if len(ids) != len(set(ids)) or set(ids) != expected_ids:
        raise ValueError("qfq factor artifact does not exactly cover the raw union")
    invalid = [
        instrument_id
        for instrument_id, item in zip(ids, requests, strict=True)
        if item.get("status") not in {"ok", "ok_no_adjustments"}
    ]
    if invalid:
        raise ValueError("qfq factor artifact contains unavailable requests")
    return {
        "path": str(path.resolve()),
        "request_end": request_end.isoformat(),
        "instrument_count": len(ids),
    }


def plan_daily_qfq_promotion(
    *,
    current_qfq_path: Path,
    daily_raw_paths: list[Path],
    factor_path: Path,
    expected_sessions: list[date],
    scratch_parent: Path | None = None,
) -> dict[str, object]:
    if not daily_raw_paths:
        raise ValueError("at least one isolated daily raw Parquet is required")
    current = _parquet_profile(
        current_qfq_path,
        expected_adjustment="qfq",
        scratch_parent=scratch_parent,
    )
    daily = [
        _parquet_profile(
            path,
            expected_adjustment="none",
            scratch_parent=scratch_parent,
        )
        for path in daily_raw_paths
    ]
    input_sessions: list[date] = []
    keys: set[tuple[str, date]] = set()
    daily_ids: set[str] = set()
    for item in daily:
        if item["first_date"] != item["last_date"]:
            raise ValueError("each isolated daily Parquet must contain exactly one session")
        session = item["first_date"]
        assert isinstance(session, date)
        input_sessions.append(session)
        daily_ids.update(item["instruments"])
        for key in _parquet_keys(
            Path(str(item["path"])), scratch_parent=scratch_parent
        ):
            if key in keys:
                raise ValueError("daily inputs contain duplicate instrument-date keys")
            keys.add(key)
    if input_sessions != sorted(input_sessions):
        raise ValueError("daily inputs must be supplied in increasing session order")
    sessions = sorted(set(input_sessions))
    current_last = current["last_date"]
    assert isinstance(current_last, date)
    if sessions[0] <= current_last:
        raise ValueError("daily inputs overlap the current qfq date range")
    if sessions != expected_sessions:
        raise ValueError("daily inputs do not exactly cover the verified expected sessions")
    union_ids = set(current["instruments"]) | daily_ids
    factor = _factor_profile(factor_path, union_ids, sessions[-1])
    return {
        "schema_version": "quantstack.daily-qfq-promotion-plan.v1",
        "status": "ready",
        "strategy": "full_rebuild_required",
        "reason": "latest-factor anchoring can change historical qfq prices",
        "formula": "raw_price * factor_t / factor_latest",
        "current_qfq": {
            key: current[key]
            for key in ("path", "row_count", "instrument_count", "first_date", "last_date")
        },
        "daily_raw": {
            "paths": [item["path"] for item in daily],
            "sessions": [value.isoformat() for value in sessions],
            "row_count": sum(int(item["row_count"]) for item in daily),
            "instrument_count": len(daily_ids),
        },
        "verified_expected_sessions": [value.isoformat() for value in expected_sessions],
        "factor": factor,
        "target_watermark": sessions[-1].isoformat(),
        "target_instrument_count": len(union_ids),
        "raw_inputs_unchanged": True,
        "qfq_written": False,
        "catalog_published": False,
        "production_pointer_updated": False,
        "deployed": False,
        "cutover": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-qfq", type=Path, required=True)
    parser.add_argument("--daily-raw", type=Path, action="append", required=True)
    parser.add_argument("--factor", type=Path, required=True)
    parser.add_argument(
        "--expected-session",
        type=date.fromisoformat,
        action="append",
        required=True,
        help="Verified trading session; repeat in strict order",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inputs = [args.current_qfq, *args.daily_raw, args.factor]
    if args.output.resolve() in {path.resolve() for path in inputs}:
        raise ValueError("output path must not overlap any input path")
    report = plan_daily_qfq_promotion(
        current_qfq_path=args.current_qfq,
        daily_raw_paths=args.daily_raw,
        factor_path=args.factor,
        expected_sessions=args.expected_session,
        scratch_parent=args.output.resolve().parent,
    )
    atomic_replace_bytes(
        args.output,
        (json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n").encode(),
    )
    print(json.dumps(report, ensure_ascii=False, default=str, sort_keys=True))


if __name__ == "__main__":
    main()
