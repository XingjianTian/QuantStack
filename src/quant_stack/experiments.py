from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import yaml

from .models import ExperimentManifest, load_dataset_manifest, load_strategy_spec


def _git_commit(repo: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True, capture_output=True, check=False
        )
    except OSError:
        return "uncommitted"
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


def create_experiment(
    strategy_path: str | Path,
    data_manifest_paths: list[str | Path],
    output_root: str | Path,
    repo_root: str | Path,
    upstream_versions: dict[str, str] | None = None,
) -> Path:
    strategy_path = Path(strategy_path)
    spec = load_strategy_spec(strategy_path)
    datasets = [load_dataset_manifest(path) for path in data_manifest_paths]
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    short_hash = hashlib.sha256(strategy_path.read_bytes()).hexdigest()[:8]
    experiment_id = f"{timestamp}-{spec.strategy_id}-{short_hash}"
    destination = Path(output_root) / experiment_id
    destination.mkdir(parents=True, exist_ok=False)

    shutil.copy2(strategy_path, destination / "strategy_spec.yaml")
    (destination / "data_manifests").mkdir()
    for path in data_manifest_paths:
        shutil.copy2(path, destination / "data_manifests" / Path(path).name)

    parameters = {
        "strategy_id": spec.strategy_id,
        "strategy_version": spec.version,
        "backtest": spec.backtest.model_dump(mode="json"),
    }
    (destination / "parameters.yaml").write_text(
        yaml.safe_dump(parameters, sort_keys=False), encoding="utf-8"
    )
    (destination / "backtest_result.json").write_text(
        json.dumps({"status": "not_run", "metrics": {}, "trades": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    report = (
        f"# Experiment {experiment_id}\n\n"
        f"- Strategy: `{spec.strategy_id}` `{spec.version}`\n"
        f"- Status: created; backtest not run\n"
        "- Conclusion: pending\n"
    )
    (destination / "report.md").write_text(report, encoding="utf-8")

    manifest = ExperimentManifest(
        experiment_id=experiment_id,
        strategy_id=spec.strategy_id,
        strategy_version=spec.version,
        strategy_spec_sha256=hashlib.sha256(strategy_path.read_bytes()).hexdigest(),
        code_commit=_git_commit(Path(repo_root)),
        upstream_versions=upstream_versions or {},
        data_versions=[
            {"dataset_id": item.dataset_id, "version": item.version} for item in datasets
        ],
        parameter_file="parameters.yaml",
        result_file="backtest_result.json",
        report_file="report.md",
    )
    (destination / "experiment_manifest.yaml").write_text(
        yaml.safe_dump(manifest.model_dump(mode="json"), sort_keys=False), encoding="utf-8"
    )
    return destination
