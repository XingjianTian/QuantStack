#!/usr/bin/env python3
"""Run one explicitly authorized US omission recovery into an isolated candidate."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.r12_us_recovery import (
    MAX_BATCH_SIZE,
    MAX_THREADS,
    MAX_TIMEOUT_SECONDS,
    run_targeted_recovery,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--object-id", action="append", default=[])
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--budget-ledger", type=Path, required=True)
    parser.add_argument("--maximum-objects", type=int, default=515)
    parser.add_argument("--shared-runtime-root", type=Path, required=True)
    parser.add_argument("--proxy-url", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-size", type=int, default=MAX_BATCH_SIZE)
    parser.add_argument("--threads", type=int, choices=[1], default=MAX_THREADS)
    parser.add_argument("--timeout-seconds", type=int, default=MAX_TIMEOUT_SECONDS)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--caller-holds-operation-lock", action="store_true")
    args = parser.parse_args()

    result = run_targeted_recovery(
        queue_path=args.queue,
        session=args.session,
        candidate_root=args.candidate_root,
        budget_ledger=args.budget_ledger,
        runtime_root=args.shared_runtime_root,
        maximum_objects=args.maximum_objects,
        run_id=args.run_id,
        execute=args.execute,
        caller_holds_operation_lock=args.caller_holds_operation_lock,
        object_ids=args.object_id,
        batch_size=args.batch_size,
        threads=args.threads,
        timeout_seconds=args.timeout_seconds,
        proxy_url=args.proxy_url,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
