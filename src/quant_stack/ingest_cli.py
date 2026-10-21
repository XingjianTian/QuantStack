from __future__ import annotations

import argparse
import json
from pathlib import Path

from .data.benchmark import BenchmarkParameters, run_benchmark
from .data.ingestion import IngestionService
from .data.providers.cn.replay import (
    AKShareReplayProvider,
    BaoStockReplayProvider,
    normalize_akshare_replay_bars,
    normalize_baostock_replay_bars,
)
from .data.publication import CatalogPublisher, load_publication_bundle
from .data.storage import StagingFileSystemStorage
from .data.tasks import (
    FileTaskStore,
    Task,
    TaskAlreadyRunningError,
    TaskExecution,
    TaskRuntime,
    TaskState,
)


def _runtime(data_root: Path, state_root: Path) -> tuple[TaskRuntime, IngestionService]:
    providers = (AKShareReplayProvider(), BaoStockReplayProvider())
    service = IngestionService(
        providers={provider.name: provider for provider in providers},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={
            "akshare_replay": normalize_akshare_replay_bars,
            "baostock_replay": normalize_baostock_replay_bars,
        },
    )
    return TaskRuntime(FileTaskStore(state_root)), service


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-stack-ingest")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run-task")
    run.add_argument("spec", type=Path)
    run.add_argument("--data-root", required=True, type=Path)
    run.add_argument("--state-root", required=True, type=Path)

    resume = commands.add_parser("resume-task")
    resume.add_argument("task_id")
    resume.add_argument("--data-root", required=True, type=Path)
    resume.add_argument("--state-root", required=True, type=Path)

    inspect = commands.add_parser("inspect-task")
    inspect.add_argument("task_id")
    inspect.add_argument("--state-root", required=True, type=Path)

    publish = commands.add_parser("publish-bundle")
    publish.add_argument("bundle", type=Path)
    publish.add_argument("--data-root", required=True, type=Path)

    benchmark = commands.add_parser("benchmark")
    benchmark.add_argument("--workload", choices=("daily", "annual", "stress"), required=True)
    benchmark.add_argument("--instruments", type=int, default=5_500)
    benchmark.add_argument("--trading-days", type=int, default=10)
    benchmark.add_argument("--total-rows", type=int)
    benchmark.add_argument("--shard-rows", type=int, default=100_000)
    benchmark.add_argument("--environment-id", default="local")
    benchmark.add_argument(
        "--storage-path-class", choices=("local", "network_staging"), default="local"
    )
    benchmark.add_argument("--container", action="store_true")
    benchmark.add_argument("--output-root", required=True, type=Path)
    return parser


def _execution_exit_code(state: TaskState) -> int:
    return {
        TaskState.SUCCEEDED: 0,
        TaskState.RETRYABLE_FAILED: 75,
        TaskState.PERMANENTLY_FAILED: 20,
        TaskState.QUARANTINED: 30,
        TaskState.CANCELLED: 40,
    }.get(state, 2)


def _print_execution(execution: TaskExecution, store: FileTaskStore) -> None:
    persisted = store.load(execution.task.task_id)
    print(
        json.dumps(
            {
                "state": execution.task.state,
                "task": execution.task.model_dump(mode="json"),
                "attempt": (
                    execution.attempt.model_dump(mode="json")
                    if execution.attempt is not None
                    else None
                ),
                "deduplicated": execution.deduplicated,
                "publication_bundle_uri": (
                    persisted.publication_bundle_uri if persisted is not None else None
                ),
                "result_loaded": execution.result is not None,
            },
            indent=2,
        )
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run-task":
        task = Task.model_validate_json(args.spec.read_text(encoding="utf-8"))
        runtime, service = _runtime(args.data_root, args.state_root)
        try:
            execution = runtime.execute(task, service)
        except TaskAlreadyRunningError as exc:
            print(str(exc))
            return 75
        assert runtime.store is not None
        _print_execution(execution, runtime.store)
        return _execution_exit_code(execution.task.state)
    if args.command == "resume-task":
        store = FileTaskStore(args.state_root)
        state = store.load(args.task_id)
        if state is None:
            print(f"unknown task: {args.task_id}")
            return 2
        runtime, service = _runtime(args.data_root, args.state_root)
        try:
            execution = runtime.execute(state.task, service)
        except TaskAlreadyRunningError as exc:
            print(str(exc))
            return 75
        assert runtime.store is not None
        _print_execution(execution, runtime.store)
        return _execution_exit_code(execution.task.state)
    if args.command == "inspect-task":
        state = FileTaskStore(args.state_root).load(args.task_id)
        if state is None:
            print(f"unknown task: {args.task_id}")
            return 2
        print(json.dumps(state.model_dump(mode="json"), indent=2))
        return 0
    if args.command == "publish-bundle":
        bundle = load_publication_bundle(args.bundle)
        receipt = CatalogPublisher(args.data_root).publish(bundle)
        print(json.dumps(receipt.model_dump(mode="json"), indent=2))
        return 0
    if args.command == "benchmark":
        parameters = BenchmarkParameters(
            workload=args.workload,
            instruments=args.instruments,
            trading_days=args.trading_days,
            total_rows=args.total_rows,
            shard_rows=args.shard_rows,
            environment_id=args.environment_id,
            storage_path_class=args.storage_path_class,
            container_execution=args.container,
        )
        result = run_benchmark(parameters, args.output_root)
        print(json.dumps(result.model_dump(mode="json"), indent=2))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
