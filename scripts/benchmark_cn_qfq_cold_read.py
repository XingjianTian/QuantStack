#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from quant_stack.data.cn_qfq_dataset import (
    cn_qfq_relation_sql,
    load_cn_qfq_dataset,
    read_materialized_cn_qfq_rows,
    select_cn_qfq_bars,
)

SCHEMA_VERSION = "quantstack.cn-qfq-cold-read-benchmark.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_group_hits(descriptor: Path, instrument_id: str) -> dict[str, object]:
    dataset = load_cn_qfq_dataset(descriptor, instrument_ids=(instrument_id,))
    if dataset is None:
        raise ValueError("row-group input is not a CN qfq descriptor")
    sources = select_cn_qfq_bars(dataset, instrument_ids=(instrument_id,))
    hits = 0
    total = 0
    inspected: list[dict[str, object]] = []
    for source in sources:
        parquet = pq.ParquetFile(source)
        column_index = parquet.schema_arrow.names.index("instrument_id")
        source_hits = 0
        for index in range(parquet.metadata.num_row_groups):
            total += 1
            statistics = parquet.metadata.row_group(index).column(column_index).statistics
            if statistics is None or not statistics.has_min_max:
                raise RuntimeError(f"instrument row-group statistics are missing: {source}")
            minimum = statistics.min
            maximum = statistics.max
            if isinstance(minimum, bytes):
                minimum = minimum.decode()
            if isinstance(maximum, bytes):
                maximum = maximum.decode()
            if str(minimum) <= instrument_id <= str(maximum):
                source_hits += 1
        hits += source_hits
        inspected.append(
            {
                "path": str(source),
                "row_groups": parquet.metadata.num_row_groups,
                "hits": source_hits,
            }
        )
    return {
        "instrument_id": instrument_id,
        "row_group_hits": hits,
        "row_groups_inspected": total,
        "sources": inspected,
    }


def _process_read_bytes() -> int | None:
    path = Path("/proc/self/io")
    if not path.is_file():
        return None
    values = dict(line.split(":", 1) for line in path.read_text(encoding="utf-8").splitlines())
    return int(values["read_bytes"])


def _evict(path: Path) -> None:
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("targeted POSIX_FADV_DONTNEED is unavailable")
    descriptor = load_cn_qfq_dataset(path)
    if descriptor is None:
        raise ValueError("benchmark input is not a CN qfq descriptor")
    for source in (path, *descriptor.bars, *descriptor.events):
        descriptor_fd = os.open(source, os.O_RDONLY)
        try:
            os.posix_fadvise(descriptor_fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(descriptor_fd)


def _execute_once(
    descriptor: Path,
    instrument_id: str,
    start: date,
    end: date,
    *,
    engine: str,
) -> dict[str, object]:
    before = _process_read_bytes()
    started = time.perf_counter()
    if engine == "duckdb":
        relation = cn_qfq_relation_sql(descriptor, instrument_ids=(instrument_id,))
        if relation is None:
            raise ValueError("benchmark input is not a CN qfq descriptor")
    else:
        relation = None
    relation_ready = time.perf_counter()
    start_ts = datetime.combine(start, datetime_time.min, tzinfo=UTC)
    end_ts = datetime.combine(end, datetime_time.max, tzinfo=UTC)
    connection = duckdb.connect() if engine == "duckdb" else None
    connection_ready = time.perf_counter()
    if connection is not None:
        try:
            rows = connection.execute(
                f"SELECT instrument_id,ts_event,open,high,low,close,volume,amount,currency,"
                f"coalesce(lineage.adjustment,'unknown') FROM {relation} "
                "WHERE instrument_id=? AND ts_event>=? AND ts_event<=? ORDER BY ts_event",
                [instrument_id, start_ts, end_ts],
            ).fetchall()
        finally:
            connection.close()
    else:
        rows = read_materialized_cn_qfq_rows(
            descriptor,
            instrument_ids=(instrument_id,),
            start_ts=start_ts,
            end_ts=end_ts,
            after=None,
            limit=100_000,
        )
        if rows is None:
            raise ValueError("pyarrow benchmark requires materialized CN qfq partitions")
    completed = time.perf_counter()
    elapsed_ms = (completed - started) * 1000
    after = _process_read_bytes()
    return {
        "instrument_id": instrument_id,
        "engine": engine,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "row_count": len(rows),
        "query_ms": round(elapsed_ms, 3),
        "relation_ms": round((relation_ready - started) * 1000, 3),
        "connection_ms": round((connection_ready - relation_ready) * 1000, 3),
        "execute_ms": round((completed - connection_ready) * 1000, 3),
        "block_read_bytes": None if before is None or after is None else after - before,
    }


def _warm_parquet_engine(engine: str) -> None:
    with tempfile.TemporaryDirectory(prefix="quantstack-cold-read-warmup-") as temporary:
        fixture = Path(temporary) / "fixture.parquet"
        connection = duckdb.connect()
        try:
            sql_path = "'" + str(fixture).replace("'", "''") + "'"
            connection.execute(f"COPY (SELECT 1 AS value) TO {sql_path} (FORMAT PARQUET)")
            if engine == "duckdb":
                connection.execute("SELECT * FROM read_parquet(?)", [str(fixture)]).fetchall()
        finally:
            connection.close()
        if engine == "pyarrow":
            pq.read_table(fixture).to_pylist()


def _worker(args: argparse.Namespace) -> int:
    _warm_parquet_engine(args.engine)
    print(
        json.dumps(
            _execute_once(
                args.descriptor,
                args.instrument_id,
                args.start,
                args.end,
                engine=args.engine,
            ),
            sort_keys=True,
        )
    )
    return 0


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _sample_instruments(descriptor: Path, count: int) -> tuple[list[str], date]:
    dataset = load_cn_qfq_dataset(descriptor)
    if dataset is None:
        raise ValueError("benchmark input is not a CN qfq descriptor")
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT instrument_id,max(trading_date) last_date FROM read_parquet(?,"
            "union_by_name=true,hive_partitioning=false) GROUP BY instrument_id "
            "ORDER BY instrument_id",
            [[str(path) for path in dataset.bars]],
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < count:
        raise ValueError("candidate has fewer instruments than requested samples")
    indexes = [round(index * (len(rows) - 1) / (count - 1)) for index in range(count)]
    return [str(rows[index][0]) for index in indexes], max(row[1] for row in rows)


def benchmark(
    descriptor: Path,
    *,
    samples: int,
    rounds: int,
    require_physical_read: bool,
    engine: str,
    representative_instrument: str,
    baseline_descriptor: Path | None,
    max_row_group_hits: int,
) -> dict[str, object]:
    descriptor = descriptor.expanduser().resolve()
    instruments, end = _sample_instruments(descriptor, samples)
    results: dict[str, list[dict[str, object]]] = {"400d": [], "10y": []}
    script = Path(__file__).resolve()
    for sample_round in range(1, rounds + 1):
        for label, start in (
            ("400d", end - timedelta(days=400)),
            ("10y", end - timedelta(days=3653)),
        ):
            for instrument_id in instruments:
                _evict(descriptor)
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(script),
                        "--worker",
                        "--descriptor",
                        str(descriptor),
                        "--instrument-id",
                        instrument_id,
                        "--start",
                        start.isoformat(),
                        "--end",
                        end.isoformat(),
                        "--engine",
                        engine,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                payload = json.loads(completed.stdout)
                payload["sample_round"] = sample_round
                if require_physical_read and not payload.get("block_read_bytes"):
                    raise RuntimeError(
                        f"cold-read proof failed for {instrument_id}: no block-read bytes"
                    )
                results[label].append(payload)
    summary: dict[str, object] = {}
    for label, rows in results.items():
        query_times = [float(row["query_ms"]) for row in rows]
        summary[label] = {
            "samples": len(rows),
            "query_p50_ms": round(statistics.median(query_times), 3),
            "query_p95_ms": round(_percentile(query_times, 0.95), 3),
            "total_block_read_bytes": sum(int(row.get("block_read_bytes") or 0) for row in rows),
            "min_block_read_bytes": min(int(row.get("block_read_bytes") or 0) for row in rows),
            "max_block_read_bytes": max(int(row.get("block_read_bytes") or 0) for row in rows),
            "zero_block_read_samples": sum(
                1 for row in rows if not int(row.get("block_read_bytes") or 0)
            ),
        }
    candidate_layout = _row_group_hits(descriptor, representative_instrument)
    if int(candidate_layout["row_group_hits"]) > max_row_group_hits:
        raise RuntimeError(
            f"candidate row-group proof failed for {representative_instrument}: "
            f"{candidate_layout['row_group_hits']} > {max_row_group_hits}"
        )
    baseline_layout = None
    if baseline_descriptor is not None:
        baseline_descriptor = baseline_descriptor.expanduser().resolve()
        baseline_layout = _row_group_hits(baseline_descriptor, representative_instrument)
    return {
        "schema_version": SCHEMA_VERSION,
        "cache_policy": (
            "fresh_process_engine_warmup_then_targeted_posix_fadv_dontneed_"
            "before_candidate_first_read"
        ),
        "require_physical_read": require_physical_read,
        "descriptor": str(descriptor),
        "descriptor_sha256": _sha256(descriptor),
        "engine": engine,
        "instruments": instruments,
        "rounds": rounds,
        "summary": summary,
        "row_group_acceptance": {
            "representative_instrument": representative_instrument,
            "maximum_candidate_hits": max_row_group_hits,
            "candidate": candidate_layout,
            "baseline": baseline_layout,
            "passed": True,
        },
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure isolated CN qfq cold reads.")
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--require-physical-read", action="store_true")
    parser.add_argument("--engine", choices=("duckdb", "pyarrow"), default="duckdb")
    parser.add_argument("--representative-instrument", default="XSHE.002963")
    parser.add_argument("--baseline-descriptor", type=Path)
    parser.add_argument("--max-row-group-hits", type=int, default=2)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--instrument-id", help=argparse.SUPPRESS)
    parser.add_argument("--start", type=date.fromisoformat, help=argparse.SUPPRESS)
    parser.add_argument("--end", type=date.fromisoformat, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        if not args.instrument_id or args.start is None or args.end is None:
            parser.error("worker requires instrument, start, and end")
        return _worker(args)
    if args.samples < 2:
        parser.error("--samples must be at least 2")
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.max_row_group_hits < 1:
        parser.error("--max-row-group-hits must be positive")
    print(
        json.dumps(
            benchmark(
                args.descriptor,
                samples=args.samples,
                rounds=args.rounds,
                require_physical_read=args.require_physical_read,
                engine=args.engine,
                representative_instrument=args.representative_instrument,
                baseline_descriptor=args.baseline_descriptor,
                max_row_group_hits=args.max_row_group_hits,
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
