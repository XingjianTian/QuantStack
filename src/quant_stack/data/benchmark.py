from __future__ import annotations

import hashlib
import json
import os
import platform
import resource
import shutil
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field

from .models import DataModel, DatasetManifest, DatasetQualityMetadata
from .publication import (
    CatalogPublisher,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    _atomic_write,
    _sha256,
    publication_state_for,
)

WorkloadName = Literal["daily", "annual", "stress"]


class BenchmarkParameters(DataModel):
    workload: WorkloadName
    instruments: int = Field(ge=1)
    trading_days: int = Field(ge=1)
    total_rows: int | None = Field(default=None, ge=1)
    shard_rows: int = Field(default=100_000, ge=1)
    environment_id: str = Field(default="local", pattern=r"^[A-Za-z0-9._-]+$")
    storage_path_class: Literal["local", "network_staging"] = "local"
    container_execution: bool = False

    @property
    def rows(self) -> int:
        return self.total_rows or self.instruments * self.trading_days


class BenchmarkResult(DataModel):
    schema_version: Literal["1.1.0"] = "1.1.0"
    environment: ProducerEnvironment
    cpu_model: str
    core_count: int
    total_memory_bytes: int | None
    available_memory_bytes: int | None
    storage_path_class: str
    container_execution: bool
    workload: WorkloadName
    parameters: dict[str, int | str | bool | None]
    total_rows: int
    wall_clock_seconds: float
    rows_per_second: float
    raw_write_seconds: float
    normalization_quality_seconds: float
    parquet_write_seconds: float
    manifest_seconds: float
    duckdb_publication_seconds: float
    baseline_catalog_query_latency_ms: float
    catalog_query_latency_during_ingest_p95_ms: float
    catalog_query_sample_count: int = Field(ge=1)
    peak_rss_bytes: int
    swap_used_before_bytes: int | None
    swap_used_after_bytes: int | None
    input_bytes: int
    output_bytes: int
    file_count: int
    final_parquet_size_bytes: int
    idempotent_replay_seconds: float
    recovery_test_result: Literal["passed"] = "passed"
    bundle_id: str


def _memory_info() -> tuple[int | None, int | None, int | None]:
    if Path("/proc/meminfo").exists():
        values: dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", maxsplit=1)
            values[key] = int(raw.strip().split()[0]) * 1024
        total = values.get("MemTotal")
        available = values.get("MemAvailable")
        swap_used = (
            values.get("SwapTotal", 0) - values.get("SwapFree", 0)
            if "SwapTotal" in values
            else None
        )
        return total, available, swap_used
    if platform.system() == "Darwin":
        try:
            total = int(
                subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
            )
            stats = subprocess.run(
                ["vm_stat"], check=True, capture_output=True, text=True
            ).stdout.splitlines()
            page_size = int(stats[0].split()[-2])
            pages = {
                line.split(":", maxsplit=1)[0]: int(
                    line.split(":", maxsplit=1)[1].strip().rstrip(".")
                )
                for line in stats[1:]
                if ":" in line
            }
            available = page_size * sum(
                pages.get(key, 0) for key in ("Pages free", "Pages inactive", "Pages speculative")
            )
            return total, available, None
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
    try:
        total = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        available = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):
        total = available = None
    return total, available, None


def _cpu_model() -> str:
    if platform.system() == "Darwin":
        try:
            return subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", maxsplit=1)[1].strip()
    return platform.processor() or platform.machine()


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if platform.system() == "Darwin" else value * 1024)


def _row(index: int, instruments: int) -> dict[str, object]:
    instrument_index = index % instruments
    day_index = index // instruments
    symbol = f"{instrument_index + 1:06d}"
    venue = ("XSHG", "XSHE", "BJSE")[instrument_index % 3]
    base = 5.0 + (instrument_index % 1000) / 100 + (day_index % 17) / 100
    trading_day = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=day_index)
    return {
        "instrument_id": f"{venue}.{symbol}",
        "ts_event": trading_day,
        "open": base,
        "high": base + 0.2,
        "low": max(0.0, base - 0.2),
        "close": base + 0.05,
        "volume": float(1000 + index % 100_000),
        "amount": float((1000 + index % 100_000) * (base + 0.05)),
        "vwap": None,
        "currency": "CNY",
        "lineage": {
            "provider": "deterministic_benchmark",
            "endpoint": "fixture_generator_v1",
            "adjustment": "none",
        },
    }


def _query_latency_ms(catalog_path: Path) -> float:
    import duckdb

    started = time.perf_counter()
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        connection.execute("SELECT count(*) FROM datasets").fetchone()
    finally:
        connection.close()
    return (time.perf_counter() - started) * 1000


def run_benchmark(parameters: BenchmarkParameters, output_root: str | Path) -> BenchmarkResult:
    root = Path(output_root).expanduser().resolve()
    run_root = (root / parameters.environment_id / parameters.workload).resolve()
    if root not in run_root.parents:
        raise ValueError("benchmark run path escapes output root")
    data_root = run_root / "data"
    if run_root.exists():
        shutil.rmtree(run_root)
    data_root.mkdir(parents=True)
    total_memory, available_memory, swap_before = _memory_info()
    overall_started = time.perf_counter()
    publisher = CatalogPublisher(data_root)
    publisher.ensure_catalog()
    catalog_path = data_root / "catalog" / "quant.duckdb"
    baseline_latency = _query_latency_ms(catalog_path)
    latency_samples: list[float] = []
    sampling_stop = threading.Event()

    def sample_read_only_api() -> None:
        while not sampling_stop.is_set():
            latency_samples.append(_query_latency_ms(catalog_path))
            sampling_stop.wait(0.05)

    sampler = threading.Thread(target=sample_read_only_api, daemon=True)
    sampler.start()
    raw_seconds = normalized_seconds = parquet_seconds = 0.0
    input_bytes = output_bytes = parquet_size = 0
    artifacts: list[PublicationArtifact] = []
    manifests: list[DatasetManifest] = []
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    instrument_rows = [
        {
            "instrument_id": f"{('XSHG', 'XSHE', 'BJSE')[index % 3]}.{index + 1:06d}",
            "symbol": f"{index + 1:06d}",
            "market": "CN",
            "asset_class": "equity",
            "venue": ("XSHG", "XSHE", "BJSE")[index % 3],
            "currency": "CNY",
            "timezone": "Asia/Shanghai",
            "active": True,
        }
        for index in range(parameters.instruments)
    ]
    instrument_path = data_root / "normalized" / "instruments.parquet"
    instrument_path.parent.mkdir(parents=True, exist_ok=True)
    instrument_started = time.perf_counter()
    pq.write_table(pa.Table.from_pylist(instrument_rows), instrument_path, compression="zstd")
    parquet_seconds += time.perf_counter() - instrument_started
    instrument_size = instrument_path.stat().st_size
    output_bytes += instrument_size
    artifacts.append(
        PublicationArtifact(
            uri="quant-data://normalized/instruments.parquet",
            kind="instruments",
            size_bytes=instrument_size,
            sha256=_sha256(instrument_path),
        )
    )
    manifests.append(
        DatasetManifest(
            dataset_id=f"benchmark_{parameters.workload}_instruments",
            version="1.0.0",
            layer="normalized",
            kind="instruments",
            market="CN",
            provider="deterministic_benchmark",
            frequency="1d",
            storage_uri="quant-data://normalized/instruments.parquet",
            schema_ref="schemas/data/instrument.schema.json",
            created_at=created_at,
            available_at_policy="fixture_only",
            revision_policy="immutable",
            row_count=parameters.instruments,
            sha256=_sha256(instrument_path),
            quality_status="passed",
            quality=DatasetQualityMetadata(
                status="passed", checks=["deterministic_instruments_valid"]
            ),
            license_scope="offline_fixture_only",
            source_url="fixture://benchmark/v1",
            available_at=created_at,
        )
    )
    for shard_index, start in enumerate(range(0, parameters.rows, parameters.shard_rows)):
        end = min(start + parameters.shard_rows, parameters.rows)
        raw_path = data_root / "raw" / f"shard-{shard_index:05d}.jsonl"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_started = time.perf_counter()
        with raw_path.open("wb") as stream:
            for index in range(start, end):
                payload = (
                    json.dumps(_row(index, parameters.instruments), default=str) + "\n"
                ).encode()
                stream.write(payload)
                input_bytes += len(payload)
        raw_seconds += time.perf_counter() - raw_started

        normalize_started = time.perf_counter()
        rows = [_row(index, parameters.instruments) for index in range(start, end)]
        if any(row["high"] < row["close"] or row["volume"] < 0 for row in rows):
            raise ValueError("deterministic benchmark quality gate failed")
        normalized_seconds += time.perf_counter() - normalize_started

        parquet_path = data_root / "normalized" / f"shard-{shard_index:05d}.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_started = time.perf_counter()
        pq.write_table(pa.Table.from_pylist(rows), parquet_path, compression="zstd")
        parquet_seconds += time.perf_counter() - parquet_started
        parquet_size += parquet_path.stat().st_size
        output_bytes += parquet_path.stat().st_size
        for path, kind in ((raw_path, "raw"), (parquet_path, "bars")):
            artifacts.append(
                PublicationArtifact(
                    uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
                    kind=kind,
                    size_bytes=path.stat().st_size,
                    sha256=_sha256(path),
                )
            )
        manifests.append(
            DatasetManifest(
                dataset_id=f"benchmark_{parameters.workload}_{shard_index:05d}",
                version="1.0.0",
                layer="normalized",
                kind="market_bars",
                market="CN",
                provider="deterministic_benchmark",
                frequency="1d",
                storage_uri=f"quant-data://{parquet_path.relative_to(data_root).as_posix()}",
                schema_ref="schemas/data/bar.schema.json",
                created_at=created_at,
                available_at_policy="fixture_only",
                revision_policy="immutable",
                row_count=end - start,
                sha256=_sha256(parquet_path),
                quality_status="passed",
                quality=DatasetQualityMetadata(
                    status="passed",
                    checks=["deterministic_ohlcv_valid"],
                    report_uri="quant-data://quality/benchmark-quality.json",
                ),
                license_scope="offline_fixture_only",
                source_url="fixture://benchmark/v1",
                available_at=created_at,
            )
        )
        del rows

    sampling_stop.set()
    sampler.join()
    ordered_latencies = sorted(latency_samples)
    during_latency = (
        ordered_latencies[min(len(ordered_latencies) - 1, int(len(ordered_latencies) * 0.95))]
        if ordered_latencies
        else _query_latency_ms(catalog_path)
    )
    quality_path = data_root / "quality" / "benchmark-quality.json"
    _atomic_write(
        quality_path,
        (
            json.dumps(
                {
                    "schema_version": "1.0.0",
                    "status": "passed",
                    "checked_rows": parameters.rows,
                    "checks": ["deterministic_ohlcv_valid"],
                },
                indent=2,
            )
            + "\n"
        ).encode(),
    )
    artifacts.append(
        PublicationArtifact(
            uri="quant-data://quality/benchmark-quality.json",
            kind="quality",
            size_bytes=quality_path.stat().st_size,
            sha256=_sha256(quality_path),
        )
    )
    manifest_started = time.perf_counter()
    proposed_root = data_root / "staging" / "proposed_manifests"
    for manifest in manifests:
        manifest_path = proposed_root / f"{manifest.dataset_id}@{manifest.version}.json"
        _atomic_write(
            manifest_path,
            (json.dumps(manifest.model_dump(mode="json"), indent=2) + "\n").encode(),
        )
        artifacts.append(
            PublicationArtifact(
                uri=f"quant-data://{manifest_path.relative_to(data_root).as_posix()}",
                kind="proposed_manifest",
                size_bytes=manifest_path.stat().st_size,
                sha256=_sha256(manifest_path),
            )
        )
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"benchmark-{parameters.workload}",
        attempt_id="attempt-1",
        dataset_id=f"benchmark_{parameters.workload}",
        dataset_version="1.0.0",
        provider="deterministic_benchmark",
        endpoint="fixture_generator_v1",
        artifacts=tuple(artifacts),
        proposed_manifests=tuple(manifests),
        created_at=created_at,
        producer_environment=ProducerEnvironment.current(parameters.environment_id),
        canonical_eligible=True,
        publication_state=publication_state_for(
            "eligible",
            quality_passed=True,
            comparison_status="not_required",
        ),
        canonical_source_provider="deterministic_benchmark",
        canonical_source_endpoint="fixture_generator_v1",
        canonical_selection_reason="deterministic offline benchmark fixture",
        selection_rule_id="deterministic-benchmark-v1",
    )
    manifest_seconds = time.perf_counter() - manifest_started
    publish_started = time.perf_counter()
    publisher.publish(bundle)
    publication_seconds = time.perf_counter() - publish_started
    replay_started = time.perf_counter()
    replay = publisher.publish(bundle)
    replay_seconds = time.perf_counter() - replay_started
    if not replay.idempotent_replay:
        raise AssertionError("benchmark idempotent replay failed")
    checkpoint_path = run_root / "recovery-checkpoint.json"
    _atomic_write(
        checkpoint_path,
        json.dumps({"next_shard": len(manifests), "bundle_id": bundle.bundle_id}).encode(),
    )
    recovered = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if recovered != {"next_shard": len(manifests), "bundle_id": bundle.bundle_id}:
        raise AssertionError("benchmark checkpoint recovery failed")
    checkpoint_path.unlink()
    _, _, swap_after = _memory_info()
    wall = time.perf_counter() - overall_started
    result = BenchmarkResult(
        environment=bundle.producer_environment,
        cpu_model=_cpu_model(),
        core_count=os.cpu_count() or 1,
        total_memory_bytes=total_memory,
        available_memory_bytes=available_memory,
        storage_path_class=parameters.storage_path_class,
        container_execution=parameters.container_execution,
        workload=parameters.workload,
        parameters=parameters.model_dump(),
        total_rows=parameters.rows,
        wall_clock_seconds=wall,
        rows_per_second=parameters.rows / wall,
        raw_write_seconds=raw_seconds,
        normalization_quality_seconds=normalized_seconds,
        parquet_write_seconds=parquet_seconds,
        manifest_seconds=manifest_seconds,
        duckdb_publication_seconds=publication_seconds,
        baseline_catalog_query_latency_ms=baseline_latency,
        catalog_query_latency_during_ingest_p95_ms=during_latency,
        catalog_query_sample_count=max(1, len(latency_samples)),
        peak_rss_bytes=_peak_rss_bytes(),
        swap_used_before_bytes=swap_before,
        swap_used_after_bytes=swap_after,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        file_count=sum(1 for path in data_root.rglob("*") if path.is_file()),
        final_parquet_size_bytes=parquet_size + instrument_size,
        idempotent_replay_seconds=replay_seconds,
        bundle_id=bundle.bundle_id,
    )
    summary_path = run_root / "benchmark-result.json"
    _atomic_write(
        summary_path,
        (json.dumps(result.model_dump(mode="json"), indent=2) + "\n").encode(),
    )
    return result


def deterministic_fixture_hash(parameters: BenchmarkParameters, sample_rows: int = 1000) -> str:
    digest = hashlib.sha256()
    for index in range(min(parameters.rows, sample_rows)):
        digest.update(
            json.dumps(_row(index, parameters.instruments), default=str, sort_keys=True).encode()
        )
    return digest.hexdigest()
