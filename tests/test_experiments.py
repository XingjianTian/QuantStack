from pathlib import Path

import yaml

from quant_stack.experiments import create_experiment

ROOT = Path(__file__).parents[1]


def test_experiment_bundle_contains_reproducibility_evidence(tmp_path: Path) -> None:
    result = create_experiment(
        ROOT / "strategies/examples/sma_cross/strategy_spec.yaml",
        [ROOT / "datasets/catalog/us_equity_daily_sample.yaml"],
        tmp_path,
        ROOT,
        {"nautilus_trader": "abc123", "vibe_trading": "def456"},
    )
    manifest = yaml.safe_load((result / "experiment_manifest.yaml").read_text(encoding="utf-8"))
    assert manifest["strategy_version"] == "0.1.0"
    assert manifest["data_versions"][0]["version"] == "2026-07-27.1"
    assert (result / "backtest_result.json").is_file()
    assert (result / "report.md").is_file()


def test_experiment_records_uncommitted_when_git_is_unavailable(
    monkeypatch, tmp_path: Path
) -> None:
    def missing_git(*args, **kwargs):
        raise FileNotFoundError("git")

    monkeypatch.setattr("quant_stack.experiments.subprocess.run", missing_git)
    result = create_experiment(
        ROOT / "strategies/examples/sma_cross/strategy_spec.yaml",
        [ROOT / "datasets/catalog/us_equity_daily_sample.yaml"],
        tmp_path,
        ROOT,
    )
    manifest = yaml.safe_load((result / "experiment_manifest.yaml").read_text(encoding="utf-8"))
    assert manifest["code_commit"] == "uncommitted"
