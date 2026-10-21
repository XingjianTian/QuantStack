#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from quant_stack.data.cn_operations_audit import audit_cn_operations
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.run_daily_incremental import _calendar_preflight, _load_config


def _recent_sessions(config_path: Path, lookback_days: int) -> tuple[list[str], str]:
    config = _load_config(config_path)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    sessions: list[str] = []
    for offset in range(lookback_days - 1, -1, -1):
        candidate = now.date() - timedelta(days=offset)
        if candidate == now.date() and now.hour < 18:
            continue
        eligible, _ = _calendar_preflight(config, candidate, now=now)
        if eligible:
            sessions.append(candidate.isoformat())
    if not sessions:
        raise ValueError("no verified complete session is available in the audit window")
    return sessions, sessions[-1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--expected-session", action="append")
    parser.add_argument("--scheduler-config", type=Path)
    parser.add_argument("--lookback-calendar-days", type=int, default=7)
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--qfq", type=Path)
    parser.add_argument("--factors", type=Path)
    parser.add_argument("--promotion-state", type=Path)
    parser.add_argument("--promotion-work-root", type=Path)
    parser.add_argument("--runtime-evidence", type=Path, required=True)
    parser.add_argument("--through")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--history-root", type=Path)
    args = parser.parse_args()
    expected_sessions = args.expected_session
    through_date = args.through
    if args.scheduler_config is not None:
        if expected_sessions or through_date:
            parser.error("scheduler-derived sessions cannot be combined with explicit sessions")
        expected_sessions, through_date = _recent_sessions(
            args.scheduler_config.expanduser().resolve(), args.lookback_calendar_days
        )
    if not expected_sessions or not through_date:
        parser.error("provide explicit sessions and through, or --scheduler-config")
    raw_path = args.raw
    qfq_path = args.qfq
    factor_path = args.factors
    artifact_evidence = None
    if args.promotion_state is not None:
        if args.promotion_work_root is None:
            parser.error("--promotion-state requires --promotion-work-root")
        if any(path is not None for path in (args.raw, args.qfq, args.factors)):
            parser.error("promotion state cannot be combined with explicit artifact paths")
        state = json.loads(args.promotion_state.expanduser().resolve().read_text(encoding="utf-8"))
        if state.get("watermark") != through_date:
            parser.error("promotion state watermark does not match the audit through date")
        if state.get("materialization_mode") == "nas_local":
            summary_path = (
                args.promotion_work_root
                / f"qfq-{through_date.replace('-', '')}"
                / "summary.json"
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            materialization = summary.get("materialization", {})
            artifact_evidence = {
                "raw": summary.get("raw", {}),
                "qfq": summary.get("qfq", {}),
                "factors": summary.get("factors", {}),
                "exact_key_coverage": (
                    materialization.get("mode") == "nas_local"
                    and materialization.get("exact_key_coverage") is True
                ),
            }
            raw_path = qfq_path = factor_path = None
        else:
            raw_path = Path(state["raw_path"])
            qfq_path = Path(state["qfq_path"])
            factor_value = state.get("factor_path")
            if not factor_value:
                parser.error("promotion state has no factor_path")
            factor_path = Path(factor_value)
    if artifact_evidence is None and (
        raw_path is None or qfq_path is None or factor_path is None
    ):
        parser.error("provide --promotion-state or all of --raw, --qfq and --factors")
    report = audit_cn_operations(
        work_root=args.work_root.expanduser().resolve(),
        expected_sessions=expected_sessions,
        raw_path=(raw_path.expanduser().resolve() if raw_path is not None else None),
        qfq_path=(qfq_path.expanduser().resolve() if qfq_path is not None else None),
        factor_path=(factor_path.expanduser().resolve() if factor_path is not None else None),
        runtime_evidence_path=args.runtime_evidence.expanduser().resolve(),
        through_date=through_date,
        promotion_work_root=(
            args.promotion_work_root.expanduser().resolve()
            if args.promotion_work_root is not None
            else None
        ),
        artifact_evidence=artifact_evidence,
    )
    atomic_replace_bytes(
        args.output.expanduser().resolve(),
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    if args.history_root is not None:
        history = args.history_root.expanduser().resolve()
        atomic_replace_bytes(
            history / f"operations-audit-through-{through_date.replace('-', '')}.json",
            (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
        )
    print(json.dumps({"status": report["status"], "alerts": report["alerts"]}))
    raise SystemExit(0 if report["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
