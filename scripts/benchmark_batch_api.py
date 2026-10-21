from __future__ import annotations

import argparse
import json
import os
import resource
import tempfile
import time
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.api import create_app
from quant_stack.data.ingestion import (
    DataJob,
    IngestionService,
    ProviderRuntimeContract,
    RetryPolicy,
)
from quant_stack.data.models import Instrument, Market
from quant_stack.data.providers import SyntheticProvider
from quant_stack.data.publication import CatalogPublisher, build_publication_bundle
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.platform import PlatformRepository


class CanonicalCNSyntheticProvider(SyntheticProvider):
    """Offline benchmark provider using the same ISO MIC IDs as the SS adapter."""

    runtime_contract = ProviderRuntimeContract(
        provider="synthetic",
        endpoint="synthetic_batch_benchmark_v1",
        provider_version="benchmark-1.0.0",
        adapter_version="benchmark-1.0.0",
        native_schema_version="benchmark-1.0.0",
        supported_markets=("CN",),
        supported_frequencies=("1d",),
    )

    def list_instruments(self, market: Market, symbols=None):
        if market != "CN":
            yield from super().list_instruments(market, symbols)
            return
        for symbol in symbols or ["600000"]:
            yield Instrument(
                instrument_id=f"XSHG.{symbol}",
                symbol=symbol,
                market="CN",
                venue="XSHG",
                currency="CNY",
                timezone="Asia/Shanghai",
                name=f"Synthetic {symbol}",
            )

    def fetch_bars(self, market, symbols, start, end, frequency="1d"):
        for row in super().fetch_bars(market, symbols, start, end, frequency):
            if market == "CN":
                row = {
                    **row,
                    "instrument_id": f"XSHG.{row['symbol']}",
                    "venue": "XSHG",
                }
            yield row


def _peak_rss_kib() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value // 1024 if __import__("sys").platform == "darwin" else value


def _response_metrics(response) -> dict[str, float | int]:
    timings: dict[str, float] = {}
    for item in response.headers["server-timing"].split(","):
        name, duration = item.strip().split(";dur=", maxsplit=1)
        timings[name] = float(duration)
    return {
        "catalog_queries": int(response.headers["x-quant-catalog-queries"]),
        "payload_queries": int(response.headers["x-quant-payload-queries"]),
        "duckdb_connections": int(response.headers["x-quant-duckdb-connections"]),
        "parquet_scans": int(response.headers["x-quant-parquet-scans"]),
        "prepare_ms": timings["prepare"],
        "payload_ms": timings["payload"],
        "total_ms": timings["total"],
        "response_bytes": len(response.content),
    }


def run_benchmark(
    data_root: Path,
    *,
    instrument_count: int,
    trading_days: int,
    prepare_only: bool = False,
) -> dict:
    if trading_days != 20:
        raise ValueError("deterministic benchmark currently requires exactly 20 trading days")
    symbols = [f"{600_000 + index:06d}" for index in range(instrument_count)]
    storage = StagingFileSystemStorage(data_root)
    fixture_started = time.perf_counter()
    result = IngestionService(
        providers={"synthetic": CanonicalCNSyntheticProvider()}, storage=storage
    ).run(
        DataJob(
            dataset="synthetic_cn_batch_benchmark",
            dataset_version="5500x20-v1",
            provider="synthetic",
            provider_version=CanonicalCNSyntheticProvider.runtime_contract.provider_version,
            provider_endpoint=CanonicalCNSyntheticProvider.runtime_contract.endpoint,
            adapter_version=CanonicalCNSyntheticProvider.runtime_contract.adapter_version,
            native_schema_version=(
                CanonicalCNSyntheticProvider.runtime_contract.native_schema_version
            ),
            market="CN",
            frequency="1d",
            execution_time=datetime(2026, 8, 1, tzinfo=UTC),
            retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=symbols,
            start=date(2026, 7, 6),
            end=date(2026, 7, 31),
        )
    )
    bundle = build_publication_bundle(
        result=result,
        task_id="batch-benchmark-5500x20",
        attempt_id="deterministic-v1",
        endpoint=CanonicalCNSyntheticProvider.runtime_contract.endpoint,
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
        environment_id="benchmark",
    )
    CatalogPublisher(data_root).publish(bundle)
    fixture_seconds = time.perf_counter() - fixture_started
    if prepare_only:
        return {
            "benchmark_contract": "5500x20-v1",
            "fixture_prepared": True,
            "instrument_count": instrument_count,
            "trading_days": trading_days,
            "response_rows": instrument_count * trading_days,
            "fixture_seconds": round(fixture_seconds, 3),
        }

    from fastapi.testclient import TestClient

    repository = PlatformRepository(Path(__file__).parents[1], data_root / "experiments", data_root)
    os.environ.setdefault("QUANT_API_KEY", "batch-benchmark-local-only")
    auth_headers = {"X-Quant-API-Key": os.environ["QUANT_API_KEY"]}
    client = TestClient(create_app(repository, data_storage=storage))
    query = {
        "instrument_ids": [f"XSHG.{symbol}" for symbol in symbols],
        "market": "CN",
        "start": "2026-07-06",
        "end": "2026-07-31",
        "frequency": "1d",
        "dataset_id": "synthetic_cn_batch_benchmark",
        "dataset_version": "5500x20-v1",
        "page_size": 60_000,
    }
    request_bytes = len(json.dumps(query, separators=(",", ":")).encode())
    wall_started = time.perf_counter()
    first = client.post("/market/batch/bars", json=query, headers=auth_headers)
    first.raise_for_status()
    first_wall_ms = (time.perf_counter() - wall_started) * 1000
    first_payload = first.json()
    if "metrics" in first_payload:
        raise RuntimeError("benchmark response body contains volatile metrics")
    rows = list(first_payload["items"])
    metrics = [_response_metrics(first)]
    cursors = 1
    cursor = first_payload["next_cursor"]
    while cursor:
        page_query = {**query, "cursor": cursor}
        page = client.post("/market/batch/bars", json=page_query, headers=auth_headers)
        page.raise_for_status()
        payload = page.json()
        if "metrics" in payload:
            raise RuntimeError("benchmark response body contains volatile metrics")
        rows.extend(payload["items"])
        metrics.append(_response_metrics(page))
        cursor = payload["next_cursor"]
        cursors += 1
    cached_started = time.perf_counter()
    cached = client.post(
        "/market/batch/bars",
        json=query,
        headers={**auth_headers, "If-None-Match": first.headers["etag"]},
    )
    cached_latency_ms = (time.perf_counter() - cached_started) * 1000
    if cached.status_code != 304:
        raise RuntimeError("benchmark ETag request did not return 304")
    cached_metrics = _response_metrics(cached)
    if cached_metrics["payload_queries"] or cached_metrics["parquet_scans"]:
        raise RuntimeError("benchmark ETag request scanned the payload")
    keys = [(item["instrument_id"], item["ts_event"]) for item in rows]
    expected_rows = instrument_count * trading_days
    if len(rows) != expected_rows or keys != sorted(keys):
        raise RuntimeError("benchmark rows or deterministic ordering did not match")
    return {
        "benchmark_contract": "5500x20-v1",
        "instrument_count": instrument_count,
        "trading_days": trading_days,
        "request_rows": instrument_count,
        "response_rows": len(rows),
        "request_bytes": request_bytes,
        "response_bytes": sum(item["response_bytes"] for item in metrics),
        "first_page_wall_ms": round(first_wall_ms, 3),
        "prepare_ms": round(sum(item["prepare_ms"] for item in metrics), 3),
        "payload_ms": round(sum(item["payload_ms"] for item in metrics), 3),
        "total_service_ms": round(sum(item["total_ms"] for item in metrics), 3),
        "serialization_and_transport_ms": round(
            max(0.0, first_wall_ms - metrics[0]["total_ms"]), 3
        ),
        "fixture_seconds": round(fixture_seconds, 3),
        "peak_rss_kib": _peak_rss_kib(),
        "catalog_queries": sum(item["catalog_queries"] for item in metrics),
        "payload_queries": sum(item["payload_queries"] for item in metrics),
        "duckdb_connections": sum(item["duckdb_connections"] for item in metrics),
        "parquet_scans": sum(item["parquet_scans"] for item in metrics),
        "http_pages": cursors,
        "per_symbol_http_requests": 0,
        "etag_cache_hit": True,
        "etag_304_latency_ms": round(cached_latency_ms, 3),
        "etag_304_payload_queries": cached_metrics["payload_queries"],
        "etag_304_parquet_scans": cached_metrics["parquet_scans"],
        "stable_ordering": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--instrument-count", type=int, default=5_500)
    parser.add_argument("--trading-days", type=int, default=20)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if args.data_root:
        args.data_root.mkdir(parents=True, exist_ok=True)
        result = run_benchmark(
            args.data_root,
            instrument_count=args.instrument_count,
            trading_days=args.trading_days,
            prepare_only=args.prepare_only,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="quantstack-batch-benchmark-") as temporary:
            result = run_benchmark(
                Path(temporary),
                instrument_count=args.instrument_count,
                trading_days=args.trading_days,
                prepare_only=args.prepare_only,
            )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
