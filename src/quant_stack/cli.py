from __future__ import annotations

import argparse
import json
from pathlib import Path

from .catalog import verify_dataset
from .experiments import create_experiment
from .models import load_dataset_manifest, load_strategy_spec
from .regression import compare_metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-stack")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-strategy")
    validate.add_argument("path", type=Path)

    dataset = commands.add_parser("verify-dataset")
    dataset.add_argument("manifest", type=Path)
    dataset.add_argument("--data-root", type=Path)

    experiment = commands.add_parser("create-experiment")
    experiment.add_argument("--strategy", required=True, type=Path)
    experiment.add_argument("--data-manifest", action="append", default=[], type=Path)
    experiment.add_argument("--output-root", required=True, type=Path)

    regression = commands.add_parser("compare-backtest")
    regression.add_argument("baseline", type=Path)
    regression.add_argument("candidate", type=Path)
    regression.add_argument("--tolerances", required=True, type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "validate-strategy":
        spec = load_strategy_spec(args.path)
        print(f"valid {spec.strategy_id}@{spec.version} status={spec.status.value}")
        return 0
    if args.command == "verify-dataset":
        manifest = load_dataset_manifest(args.manifest)
        print(verify_dataset(manifest, args.data_root))
        return 0
    if args.command == "create-experiment":
        path = create_experiment(
            args.strategy, args.data_manifest, args.output_root, Path.cwd()
        )
        print(path)
        return 0
    if args.command == "compare-backtest":
        tolerances = json.loads(args.tolerances.read_text(encoding="utf-8"))
        failures = compare_metrics(args.baseline, args.candidate, tolerances)
        if failures:
            print("\n".join(failures))
            return 1
        print("backtest metrics within tolerance")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
