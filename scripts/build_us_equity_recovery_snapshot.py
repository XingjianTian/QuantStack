"""Build a recovery snapshot without refetching completed historical tasks."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.providers.yfinance import load_us_instrument_snapshot
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.tasks import TaskState, TaskStateFile


def build(args: argparse.Namespace) -> dict[str, object]:
    snapshot = load_us_instrument_snapshot(args.snapshot)
    roots = [args.task_state_root, *getattr(args, "prior_attempt_root", [])]
    observed: list[dict[str, TaskState]] = []
    for root in roots:
        states: dict[str, TaskState] = {}
        for path in root.glob("task-*.json"):
            state = TaskStateFile.model_validate_json(path.read_text(encoding="utf-8"))
            task = state.task
            if (
                task.dataset_id == "yfinance_us_equity_daily"
                and task.dataset_version.startswith(f"{args.dataset_version_prefix}.")
                and task.start_date == args.start
                and task.end_date == args.end
                and len(task.instrument_scope) == 1
            ):
                states[task.instrument_scope[0]] = task.state
        observed.append(states)

    completed_states = {TaskState.SUCCEEDED, TaskState.QUARANTINED}
    baseline = observed[0]
    later_attempted = set().union(*(set(item) for item in observed[1:]))
    selection = getattr(args, "selection", "all_incomplete")
    if selection == "pending_terminal":
        recovery = [
            item
            for item in snapshot.instruments
            if item.provider_symbol in baseline
            and baseline[item.provider_symbol] not in completed_states
            and item.provider_symbol not in later_attempted
        ]
    elif selection == "not_requested":
        recovery = [
            item for item in snapshot.instruments if item.provider_symbol not in baseline
        ]
    else:
        completed = {
            symbol
            for states in observed
            for symbol, state in states.items()
            if state in completed_states
        }
        recovery = [
            item for item in snapshot.instruments if item.provider_symbol not in completed
        ]
    payload = snapshot.model_dump(mode="json")
    payload["snapshot_id"] = args.recovery_snapshot_id
    payload["source_refs"] = [*snapshot.source_refs, f"recovery-of:{snapshot.snapshot_id}"]
    payload["membership_basis"] = "recovery_subset_of_versioned_snapshot"
    payload["instruments"] = [item.model_dump(mode="json") for item in recovery]
    atomic_replace_bytes(
        args.output,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
    )
    report = {
        "schema_version": "quantstack.us-equity-recovery-snapshot.v1",
        "source_snapshot_id": snapshot.snapshot_id,
        "recovery_snapshot_id": args.recovery_snapshot_id,
        "source_instruments": len(snapshot.instruments),
        "selection": selection,
        "recovery_instruments": len(recovery),
        "excluded_succeeded": sum(
            state is TaskState.SUCCEEDED for states in observed for state in states.values()
        ),
        "excluded_quarantined": sum(
            state is TaskState.QUARANTINED for states in observed for state in states.values()
        ),
        "retryable_terminal_or_interrupted": sum(
            state not in completed_states for state in baseline.values()
        ),
        "not_previously_requested": sum(
            item.provider_symbol not in baseline for item in snapshot.instruments
        ),
        "excluded_later_attempted": len(later_attempted),
        "data_deleted": False,
    }
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--task-state-root", type=Path, required=True)
    parser.add_argument("--prior-attempt-root", type=Path, action="append", default=[])
    parser.add_argument("--dataset-version-prefix", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--recovery-snapshot-id", required=True)
    parser.add_argument(
        "--selection",
        choices=("all_incomplete", "pending_terminal", "not_requested"),
        default="all_incomplete",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args), sort_keys=True))


if __name__ == "__main__":
    main()
