from pathlib import Path

from quant_stack.nautilus_runner import engine_info, run_synthetic_backtest

ROOT = Path(__file__).parents[1]


def test_nautilus_engine_runs_deterministic_backtest(tmp_path: Path) -> None:
    info = engine_info()
    assert info.available
    result = run_synthetic_backtest(
        strategy_path=ROOT / "strategies/examples/sma_cross/strategy_spec.yaml",
        data_manifest_path=ROOT / "datasets/catalog/us_equity_daily_sample.yaml",
        experiment_root=tmp_path,
        repo_root=ROOT,
        upstream_versions={"nautilus_trader": "test"},
    )
    assert result["status"] == "completed"
    assert result["engine"]["version"] == "1.215.0"
    assert result["bars_processed"] == 60
    assert result["metrics"]["trade_count"] == 2
    assert result["metrics"]["final_equity"] > result["metrics"]["initial_equity"]
    assert (tmp_path / result["experiment_id"] / "report.md").is_file()
