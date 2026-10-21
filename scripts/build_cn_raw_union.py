#!/usr/bin/env python3
"""Build and verify an immutable historical + isolated-daily CN raw union."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import duckdb

from quant_stack.data.storage.atomic import atomic_write_immutable_file


@contextmanager
def _bounded_connection(scratch_parent: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Keep full-history validation inside the Daily Worker memory envelope."""

    with tempfile.TemporaryDirectory(
        dir=scratch_parent,
        prefix=".raw-union-duckdb-",
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


def _sql_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _equivalent(left: Path, right: Path) -> bool:
    with _bounded_connection(left.parent) as connection:
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?) EXCEPT ALL "
            "SELECT * FROM read_parquet(?)) UNION ALL "
            "(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)))",
            [str(left), str(right), str(right), str(left)],
        ).fetchone()
    return changed == (False,)


def build_raw_union(
    *,
    historical_path: Path,
    daily_paths: list[Path],
    expected_sessions: list[date],
    output_path: Path,
) -> dict[str, object]:
    historical_path = historical_path.expanduser().resolve()
    daily_paths = [path.expanduser().resolve() for path in daily_paths]
    output_path = output_path.expanduser().resolve()
    if not daily_paths or len(daily_paths) != len(set(daily_paths)):
        raise ValueError("daily raw inputs must be non-empty and unique")
    if expected_sessions != sorted(set(expected_sessions)):
        raise ValueError("expected sessions must be sorted and unique")
    if output_path in {historical_path, *daily_paths}:
        raise ValueError("raw union output must not overwrite an input")

    proof: dict[str, object] = {}

    historical_select = (
        f"SELECT instrument_id, symbol, venue, trading_date, ts_event, open, high, low, "
        f"close, volume, amount, currency, struct_pack(adjustment := 'none') lineage "
        f"FROM read_parquet({_sql_path(historical_path)})"
    )
    daily_selects = [
        "SELECT instrument_id, split_part(instrument_id, '.', 2) symbol, "
        "split_part(instrument_id, '.', 1) venue, "
        "CAST(timezone('Asia/Shanghai', ts_event) AS DATE) trading_date, "
        "ts_event, open, high, low, close, volume, amount, currency, "
        "struct_pack(adjustment := 'none') lineage "
        f"FROM read_parquet({_sql_path(path)})"
        for path in daily_paths
    ]
    union_sql = " UNION ALL ".join([historical_select, *daily_selects])

    def write(candidate: Path) -> None:
        with _bounded_connection(candidate.parent) as connection:
            connection.execute(
                f"COPY ({union_sql}) TO {_sql_path(candidate)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )

    def validate(candidate: Path) -> None:
        with _bounded_connection(candidate.parent) as connection:
            actual = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
                "count(*) FILTER (WHERE lineage.adjustment <> 'none' "
                "OR lineage.adjustment IS NULL) FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
            historical_count = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(historical_path)]
            ).fetchone()[0]
            daily_counts = [
                connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[
                    0
                ]
                for path in daily_paths
            ]
            sessions = [
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT trading_date FROM read_parquet(?) "
                    "WHERE trading_date > (SELECT max(trading_date) FROM read_parquet(?)) "
                    "ORDER BY trading_date",
                    [str(candidate), str(historical_path)],
                ).fetchall()
            ]
        if actual is None:
            raise ValueError("raw union validation returned no proof")
        expected_rows = int(historical_count) + sum(int(value) for value in daily_counts)
        if actual[0] != expected_rows or actual[1] != expected_rows or actual[5] != 0:
            raise ValueError(f"raw union row/key/adjustment validation failed: {actual}")
        if sessions != expected_sessions:
            raise ValueError(f"raw union sessions do not match expected sessions: {sessions}")
        if actual[4] != expected_sessions[-1]:
            raise ValueError("raw union watermark does not match the final expected session")
        proof.update(
            row_count=int(actual[0]),
            instrument_count=int(actual[2]),
            start_date=actual[3].isoformat(),
            end_date=actual[4].isoformat(),
            duplicate_keys=0,
            adjustment="none",
            daily_sessions=[value.isoformat() for value in sessions],
            historical_rows=int(historical_count),
            daily_rows=sum(int(value) for value in daily_counts),
        )

    if output_path.is_file():
        validate(output_path)
        return {**proof, "created": False, "inputs_unchanged": True}

    created = atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=_equivalent,
        artifact_kind="CN raw union Parquet",
    )
    if not proof:
        validate(output_path)
    return {**proof, "created": created, "inputs_unchanged": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--historical", type=Path, required=True)
    parser.add_argument("--daily", type=Path, action="append", required=True)
    parser.add_argument(
        "--expected-session", type=date.fromisoformat, action="append", required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build_raw_union(
        historical_path=args.historical,
        daily_paths=args.daily,
        expected_sessions=args.expected_session,
        output_path=args.output,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
