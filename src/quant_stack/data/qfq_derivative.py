from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from .storage.atomic import atomic_write_immutable_file


@contextmanager
def _bounded_connection(scratch_parent: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    with tempfile.TemporaryDirectory(
        dir=scratch_parent,
        prefix=".qfq-derivative-duckdb-",
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


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _instrument_ids(raw_bars_path: Path, corpus_path: Path | None) -> list[str]:
    if corpus_path is not None:
        symbols = json.loads(corpus_path.read_text(encoding="utf-8"))["symbols"]
        ids = [
            ("XSHG." if str(symbol).endswith(".SS") else "XSHE.")
            + str(symbol).split(".", maxsplit=1)[0]
            for symbol in symbols
        ]
    else:
        with _bounded_connection(raw_bars_path.resolve().parent) as connection:
            ids = [
                str(row[0])
                for row in connection.execute(
                    "SELECT DISTINCT instrument_id FROM read_parquet(?) ORDER BY instrument_id",
                    [str(raw_bars_path)],
                ).fetchall()
            ]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("qfq universe must contain unique CN instruments")
    if any(not value.startswith(("XSHG.", "XSHE.")) for value in ids):
        raise ValueError("qfq universe contains an unsupported instrument")
    return ids


def _factor_rows(factor_path: Path, expected_ids: set[str]) -> list[dict[str, Any]]:
    payload = json.loads(factor_path.read_text(encoding="utf-8"))
    requests = payload.get("requests")
    if not isinstance(requests, list):
        raise ValueError("qfq factor artifact has no request list")
    by_id = {str(item.get("instrument_id")): item for item in requests}
    if set(by_id) != expected_ids:
        raise ValueError("qfq factor artifact does not exactly cover the corpus")
    rows: list[dict[str, Any]] = []
    for instrument_id in sorted(expected_ids):
        request = by_id[instrument_id]
        if request.get("status") == "ok_no_adjustments":
            if request.get("rows") or int(request.get("empty_confirmations", 0)) < 1:
                raise ValueError(f"qfq empty-factor evidence is invalid for {instrument_id}")
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_date": "0001-01-01",
                    "factor": 1.0,
                }
            )
            continue
        if request.get("status") != "ok" or not request.get("rows"):
            raise ValueError(f"qfq factors are unavailable for {instrument_id}")
        for native in request["rows"]:
            factor = float(native["backAdjustFactor"])
            if factor <= 0:
                raise ValueError("qfq factors must be positive")
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_date": native["dividOperateDate"],
                    "factor": factor,
                }
            )
    return rows


def _equivalent(left: Path, right: Path) -> bool:
    with _bounded_connection(left.resolve().parent) as connection:
        changed = connection.execute(
            "SELECT EXISTS((SELECT * FROM read_parquet(?) EXCEPT ALL "
            "SELECT * FROM read_parquet(?)) UNION ALL "
            "(SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)))",
            [str(left), str(right), str(right), str(left)],
        ).fetchone()
    return changed == (False,)


def build_qfq_derivative(
    *,
    raw_bars_path: Path,
    factor_path: Path,
    corpus_path: Path | None,
    output_path: Path,
) -> dict[str, object]:
    """Build an immutable qfq Parquet for the declared raw-bar universe."""

    import pyarrow as pa

    raw_bars_path = raw_bars_path.expanduser().resolve()
    factor_path = factor_path.expanduser().resolve()
    if corpus_path is not None:
        corpus_path = corpus_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    ids = _instrument_ids(raw_bars_path, corpus_path)
    factors = _factor_rows(factor_path, set(ids))

    proof: dict[str, object] = {}

    def write(candidate: Path) -> None:
        with _bounded_connection(candidate.parent) as connection:
            connection.register("qfq_factors", pa.Table.from_pylist(factors))
            connection.execute(
                "COPY (WITH selected AS ("
                "SELECT * FROM read_parquet(?) WHERE instrument_id IN (SELECT unnest(?))"
                "), joined AS ("
                "SELECT b.*, f.factor FROM selected b ASOF LEFT JOIN qfq_factors f "
                "ON b.instrument_id = f.instrument_id "
                "AND b.trading_date >= CAST(f.effective_date AS DATE)"
                "), latest_raw AS ("
                "SELECT instrument_id, max(trading_date) latest_date "
                "FROM selected GROUP BY instrument_id"
                "), anchors AS ("
                "SELECT f.instrument_id, "
                "arg_max(f.factor, CAST(f.effective_date AS DATE)) anchor "
                "FROM qfq_factors f JOIN latest_raw r ON f.instrument_id = r.instrument_id "
                "AND CAST(f.effective_date AS DATE) <= r.latest_date GROUP BY f.instrument_id"
                ") SELECT j.instrument_id, j.symbol, j.venue, j.trading_date, j.ts_event, "
                "j.open * coalesce(j.factor, a.anchor) / a.anchor AS open, "
                "j.high * coalesce(j.factor, a.anchor) / a.anchor AS high, "
                "j.low * coalesce(j.factor, a.anchor) / a.anchor AS low, "
                "j.close * coalesce(j.factor, a.anchor) / a.anchor AS close, "
                "j.volume, j.amount, j.currency, "
                "struct_pack(adjustment := 'qfq') AS lineage "
                "FROM joined j JOIN anchors a USING (instrument_id)) "
                f"TO {_sql_string(str(candidate))} (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(raw_bars_path), ids],
            )

    def validate(candidate: Path) -> None:
        with _bounded_connection(candidate.parent) as connection:
            row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), count(*) FILTER (WHERE open IS NULL), "
                "count(*) FILTER (WHERE NOT (high >= greatest(open, close) "
                "AND low <= least(open, close) AND low <= high)), "
                "count(DISTINCT lineage.adjustment) "
                "FROM read_parquet(?)",
                [str(candidate)],
            ).fetchone()
            raw = connection.execute(
                "SELECT count(*), count(DISTINCT instrument_id) FROM read_parquet(?) "
                "WHERE instrument_id IN (SELECT unnest(?))",
                [str(raw_bars_path), ids],
            ).fetchone()
            anchor_drift = connection.execute(
                "WITH q AS (SELECT instrument_id, arg_max(close, trading_date) latest_close "
                "FROM read_parquet(?) GROUP BY instrument_id), "
                "r AS (SELECT instrument_id, arg_max(close, trading_date) latest_close "
                "FROM read_parquet(?) WHERE instrument_id IN (SELECT unnest(?)) "
                "GROUP BY instrument_id) "
                "SELECT max(abs(q.latest_close-r.latest_close)) "
                "FROM q JOIN r USING (instrument_id)",
                [str(candidate), str(raw_bars_path), ids],
            ).fetchone()[0]
        if row is None or raw is None:
            raise ValueError("qfq validation returned no proof")
        if row[:3] != (raw[0], raw[0], len(ids)) or row[3:] != (0, 0, 1):
            raise ValueError(
                "qfq row coverage or OHLC validation failed: "
                f"qfq={row}, raw={raw}, expected_instruments={len(ids)}"
            )
        if float(anchor_drift or 0) > 1e-9:
            raise ValueError("latest qfq close is not anchored to raw")
        proof.update(
            row_count=int(row[0]),
            instrument_count=int(row[2]),
            latest_anchor_max_close_drift=float(anchor_drift or 0),
        )

    created = atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=_equivalent,
        artifact_kind="qfq derivative Parquet",
    )
    if not proof:
        validate(output_path)
    return {
        **proof,
        "created": created,
        "adjustment": "qfq",
        "formula": "raw_price * back_adjust_factor_t / latest_back_adjust_factor",
        "raw_bars_unchanged": True,
    }
