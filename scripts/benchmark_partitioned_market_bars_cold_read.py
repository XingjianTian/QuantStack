#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchQueryMetrics,
    CanonicalBatchRepository,
    CanonicalDatasetSnapshot,
    PreparedBatchQuery,
)
from quant_stack.data.cn_qfq_dataset import load_cn_qfq_dataset
from quant_stack.data.models import DatasetKind, DatasetLayer, DatasetManifest
from quant_stack.data.partitioned_bars_dataset import (
    PARTITIONED_MARKET_BARS_SCHEMA_VERSION,
)
from quant_stack.data.us_total_return_dataset import (
    load_us_total_return_dataset,
)

SCHEMA_VERSION = "quantstack.partitioned-market-bars-cold-read-benchmark.v1"


class _NoPayloadDuckDB:
    def execute(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("partitioned single-instrument Batch path used DuckDB payload SQL")

    def close(self) -> None:
        pass


def _descriptor_payload(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != PARTITIONED_MARKET_BARS_SCHEMA_VERSION:
        raise ValueError("benchmark requires the unified partitioned market-bars descriptor")
    if payload.get("market") not in {"CN", "US"}:
        raise ValueError("partitioned market-bars descriptor has an unsupported market")
    return payload


def _load_selected(path: Path, instrument_id: str):
    payload = _descriptor_payload(path)
    if payload["market"] == "CN":
        return payload, load_cn_qfq_dataset(path, instrument_ids=(instrument_id,))
    return payload, load_us_total_return_dataset(path, instrument_ids=(instrument_id,))


def _selected_sources(path: Path, instrument_id: str) -> tuple[Path, ...]:
    _, dataset = _load_selected(path, instrument_id)
    values = [path, *dataset.bars, *dataset.events]
    if dataset.latest is not None:
        values.append(dataset.latest)
    return tuple(dict.fromkeys(values))


def _evict(path: Path, instrument_id: str) -> None:
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("targeted POSIX_FADV_DONTNEED is unavailable")
    for source in _selected_sources(path, instrument_id):
        descriptor_fd = os.open(source, os.O_RDONLY)
        try:
            os.posix_fadvise(descriptor_fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(descriptor_fd)


def _process_read_bytes() -> int | None:
    path = Path("/proc/self/io")
    if not path.is_file():
        return None
    values = dict(line.split(":", 1) for line in path.read_text().splitlines())
    return int(values["read_bytes"])


def _warm_arrow() -> None:
    import pyarrow as pa

    with tempfile.TemporaryDirectory(prefix="quantstack-arrow-warmup-") as temporary:
        fixture = Path(temporary) / "fixture.parquet"
        pq.write_table(pa.table({"value": [1]}), fixture)
        pq.read_table(fixture).to_pylist()


def _execute_once(
    descriptor: Path, instrument_id: str, start: date, end: date
) -> dict[str, object]:
    payload = _descriptor_payload(descriptor)
    before = _process_read_bytes()
    started = time.perf_counter()
    market = str(payload["market"])
    manifest = DatasetManifest(
        dataset_id=("cn_equity_daily" if market == "CN" else "us_equity_total_return"),
        version="cold-read-candidate",
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market=market,
        provider="isolated-candidate",
        frequency="1d",
        schema_ref="quant-stack://schemas/partitioned-market-bars-v1",
        storage_uri="quant-data://normalized/isolated/bars.json",
        available_at_policy="isolated-cold-read",
        revision_policy="immutable",
        row_count=int(payload.get("row_count", 0)),
        sha256="0" * 64,
        quality_status="passed",
        license_scope="research",
    )
    plan = PreparedBatchQuery(
        operation="batch_bars",
        snapshot=CanonicalDatasetSnapshot(manifest, "0" * 64),
        snapshot_token="isolated-cold-read",
        etag='"isolated-cold-read"',
        query_hash="isolated-cold-read",
        after=None,
        payload_path=descriptor,
        connection=_NoPayloadDuckDB(),
        metrics=BatchQueryMetrics(),
        publication_receipt=None,  # type: ignore[arg-type]
    )
    query = BatchBarsQuery(
        instrument_ids=(instrument_id,),
        market=market,
        start=start,
        end=end,
        page_size=100_000,
    )
    response = CanonicalBatchRepository(descriptor.parent).execute_bars(plan, query)
    rows = response.items
    elapsed_ms = (time.perf_counter() - started) * 1000
    after = _process_read_bytes()
    if plan.metrics.payload_queries != 1 or plan.metrics.parquet_scans != 1:
        raise RuntimeError("candidate did not use exactly one materialized partition Batch scan")
    return {
        "instrument_id": instrument_id,
        "market": payload["market"],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "row_count": len(rows),
        "query_ms": round(elapsed_ms, 3),
        "payload_ms": round(plan.metrics.payload_latency_ms, 3),
        "payload_queries": plan.metrics.payload_queries,
        "parquet_scans": plan.metrics.parquet_scans,
        "duckdb_connections": plan.metrics.duckdb_connections,
        "block_read_bytes": None if before is None or after is None else after - before,
    }


def _worker(args: argparse.Namespace) -> int:
    _warm_arrow()
    print(
        json.dumps(
            _execute_once(args.descriptor, args.instrument_id, args.start, args.end),
            sort_keys=True,
        )
    )
    return 0


def _sample_instruments(payload: dict[str, object], count: int) -> list[str]:
    partitions = payload.get("partitions")
    if not isinstance(partitions, list) or len(partitions) < count:
        raise ValueError("candidate has fewer partitions than requested samples")
    indexes = [round(index * (len(partitions) - 1) / (count - 1)) for index in range(count)]
    return [str(partitions[index]["first_instrument_id"]) for index in indexes]


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _row_group_hits(descriptor: Path, instrument_id: str) -> dict[str, object]:
    sources = _selected_sources(descriptor, instrument_id)[1:]
    inspected: list[dict[str, object]] = []
    hits = 0
    for source in sources:
        parquet = pq.ParquetFile(source)
        if "instrument_id" not in parquet.schema_arrow.names:
            continue
        column_index = parquet.schema_arrow.names.index("instrument_id")
        source_hits = 0
        for index in range(parquet.metadata.num_row_groups):
            row_statistics = parquet.metadata.row_group(index).column(column_index).statistics
            if row_statistics is None or not row_statistics.has_min_max:
                raise RuntimeError(f"instrument row-group statistics are missing: {source}")
            minimum, maximum = row_statistics.min, row_statistics.max
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
    return {"instrument_id": instrument_id, "row_group_hits": hits, "sources": inspected}


def benchmark(
    descriptor: Path,
    *,
    samples: int,
    rounds: int,
    require_physical_read: bool,
    instruments_override: tuple[str, ...] = (),
) -> dict[str, object]:
    descriptor = descriptor.expanduser().resolve()
    payload = _descriptor_payload(descriptor)
    instruments = (
        list(dict.fromkeys(instruments_override))
        if instruments_override
        else _sample_instruments(payload, samples)
    )
    end = date.fromisoformat(str(payload["watermark"]))
    periods = {"2y": end - timedelta(days=731), "10y": end - timedelta(days=3653)}
    results: dict[str, list[dict[str, object]]] = {label: [] for label in periods}
    script = Path(__file__).resolve()
    for sample_round in range(1, rounds + 1):
        for label, start in periods.items():
            for instrument_id in instruments:
                _evict(descriptor, instrument_id)
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
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                row = json.loads(completed.stdout)
                row["sample_round"] = sample_round
                if require_physical_read and not row.get("block_read_bytes"):
                    raise RuntimeError(f"cold-read proof failed for {instrument_id}")
                results[label].append(row)
    summary = {}
    for label, rows in results.items():
        times = [float(row["query_ms"]) for row in rows]
        summary[label] = {
            "samples": len(rows),
            "query_p50_ms": round(statistics.median(times), 3),
            "query_p95_ms": round(_percentile(times, 0.95), 3),
            "query_max_ms": round(max(times), 3),
            "min_block_read_bytes": min(int(row.get("block_read_bytes") or 0) for row in rows),
            "zero_block_read_samples": sum(
                not int(row.get("block_read_bytes") or 0) for row in rows
            ),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "cache_policy": "fresh_process_arrow_warmup_then_selected_files_posix_fadv_dontneed",
        "require_physical_read": require_physical_read,
        "descriptor": str(descriptor),
        "market": payload["market"],
        "instruments": instruments,
        "rounds": rounds,
        "summary": summary,
        "row_group_layout": _row_group_hits(descriptor, instruments[len(instruments) // 2]),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure unified partitioned market-bar cold reads."
    )
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--instrument", action="append", default=[])
    parser.add_argument("--require-physical-read", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--instrument-id", help=argparse.SUPPRESS)
    parser.add_argument("--start", type=date.fromisoformat, help=argparse.SUPPRESS)
    parser.add_argument("--end", type=date.fromisoformat, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        if not args.instrument_id or args.start is None or args.end is None:
            parser.error("worker requires instrument, start, and end")
        return _worker(args)
    if args.samples < 2 or args.rounds < 1:
        parser.error("samples must be at least 2 and rounds must be positive")
    print(
        json.dumps(
            benchmark(
                args.descriptor,
                samples=args.samples,
                rounds=args.rounds,
                require_physical_read=args.require_physical_read,
                instruments_override=tuple(args.instrument),
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
