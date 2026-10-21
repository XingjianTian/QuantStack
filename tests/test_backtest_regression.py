import json
from pathlib import Path

from quant_stack.regression import compare_metrics


def _write(path: Path, metrics: dict[str, float]) -> None:
    path.write_text(json.dumps({"metrics": metrics}), encoding="utf-8")


def test_backtest_regression_accepts_metrics_within_tolerance(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "candidate.json"
    _write(baseline, {"total_return": 0.12, "max_drawdown": 0.08, "trade_count": 10})
    _write(candidate, {"total_return": 0.12001, "max_drawdown": 0.08, "trade_count": 10})
    failures = compare_metrics(baseline, candidate, {"total_return": 0.001, "trade_count": 0})
    assert failures == []


def test_backtest_regression_reports_drift(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "candidate.json"
    _write(baseline, {"total_return": 0.12})
    _write(candidate, {"total_return": 0.10})
    assert compare_metrics(baseline, candidate, {"total_return": 0.001})
