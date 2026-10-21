from pathlib import Path

from quant_stack.data.benchmark import (
    BenchmarkParameters,
    deterministic_fixture_hash,
    run_benchmark,
)


def test_benchmark_fixture_is_deterministic() -> None:
    parameters = BenchmarkParameters(
        workload="daily", instruments=30, trading_days=3, shard_rows=25
    )
    assert deterministic_fixture_hash(parameters) == deterministic_fixture_hash(parameters)
    changed = parameters.model_copy(update={"trading_days": 4})
    assert deterministic_fixture_hash(parameters) != deterministic_fixture_hash(changed)


def test_benchmark_streams_shards_and_emits_valid_summary(tmp_path: Path) -> None:
    result = run_benchmark(
        BenchmarkParameters(
            workload="annual",
            instruments=40,
            trading_days=5,
            shard_rows=37,
            environment_id="test-machine",
        ),
        tmp_path,
    )
    assert result.total_rows == 200
    assert result.rows_per_second > 0
    assert result.file_count > 5
    assert result.final_parquet_size_bytes > 0
    assert result.idempotent_replay_seconds >= 0
    assert result.recovery_test_result == "passed"
    assert (tmp_path / "test-machine/annual/benchmark-result.json").is_file()


def test_benchmark_reports_catalog_latency_not_api_latency(tmp_path: Path) -> None:
    result = run_benchmark(
        BenchmarkParameters(
            workload="daily",
            instruments=10,
            trading_days=2,
            shard_rows=10,
            environment_id="metric-contract",
        ),
        tmp_path,
    )
    payload = result.model_dump(mode="json")
    assert payload["baseline_catalog_query_latency_ms"] >= 0
    assert payload["catalog_query_latency_during_ingest_p95_ms"] >= 0
    assert payload["catalog_query_sample_count"] >= 1
    assert not any("api_query_latency" in key for key in payload)
