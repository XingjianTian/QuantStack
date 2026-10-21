#!/usr/bin/env python3
"""Advance one CN session through lightweight qfq and Catalog publication."""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb

from quant_stack.data.cn_qfq_incremental import (
    build_factor_query_plan,
    factor_last_checked,
    load_daily_session_records,
    merge_factor_delta,
)
from quant_stack.data.cn_qfq_publication import publish_cn_qfq_session_dataset
from quant_stack.data.cn_qfq_session import build_cn_qfq_session_release
from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes
from scripts.sync_cn_ss_daily_snapshot import sync_daily_snapshot, write_report

MAX_EVENT_CANDIDATE_RATIO = 0.05
MIN_EVENT_CANDIDATE_BUDGET = 25


def _read(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload: dict[str, object]) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def _factor_delta_is_complete(plan: dict[str, object], path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        payload = _read(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    planned = plan.get("requests")
    completed = payload.get("requests")
    if not isinstance(planned, list) or not isinstance(completed, list):
        return False
    planned_ids = [str(item["instrument_id"]) for item in planned]
    completed_ids = [str(item.get("instrument_id")) for item in completed]
    return (
        payload.get("complete") is True
        and payload.get("universe_count") == len(planned_ids)
        and completed_ids == planned_ids
    )


def _validate_provider_request_budget(plan: dict[str, object]) -> None:
    """Stop anomalous factor fan-out before the Provider subprocess starts."""

    universe_count = int(plan["universe_count"])
    rotation_count = int(plan["rotation_request_count"])
    event_count = int(plan["event_candidate_count"])
    request_count = int(plan["request_count"])
    event_budget = max(
        MIN_EVENT_CANDIDATE_BUDGET,
        math.ceil(universe_count * MAX_EVENT_CANDIDATE_RATIO),
    )
    if event_count > event_budget or request_count > rotation_count + event_budget:
        raise RuntimeError(
            "CN factor request plan exceeds the routine daily budget before Provider access: "
            f"requests={request_count}, rotation={rotation_count}, "
            f"events={event_count}, event_budget={event_budget}"
        )


def _validate_provider_request_cap(
    plan: dict[str, object], max_provider_requests: int | None
) -> None:
    """Apply an optional caller-owned cap before starting the factor subprocess."""

    if max_provider_requests is None:
        return
    if max_provider_requests < 1:
        raise ValueError("max Provider requests must be positive")
    request_count = int(plan["request_count"])
    if request_count > max_provider_requests:
        raise RuntimeError(
            "CN factor request plan exceeds the caller reservation before Provider access: "
            f"requests={request_count} reserved={max_provider_requests}"
        )


def _universe(path: Path) -> list[dict[str, str]]:
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT instrument_id FROM read_parquet(?) ORDER BY instrument_id", [str(path)]
        ).fetchall()
    finally:
        connection.close()
    prefixes = {"XSHG": "sh", "XSHE": "sz"}
    return [
        {
            "instrument_id": str(row[0]),
            "provider_symbol": (
                f"{prefixes[str(row[0]).split('.', maxsplit=1)[0]]}."
                f"{str(row[0]).split('.', maxsplit=1)[1]}"
            ),
        }
        for row in rows
    ]


def _session_closes(path: Path) -> dict[str, float]:
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT instrument_id,close FROM read_parquet(?)", [str(path)]
        ).fetchall()
    finally:
        connection.close()
    return {str(instrument_id): float(close) for instrument_id, close in rows}


def _next_package(root: Path, watermark: date) -> tuple[date, Path] | None:
    candidates: list[tuple[date, Path]] = []
    for path in (root / "market=CN").glob("session=*"):
        session = date.fromisoformat(path.name.removeprefix("session="))
        if session > watermark:
            candidates.append((session, path))
    return min(candidates) if candidates else None


def _run(
    config_path: Path,
    state_path: Path,
    *,
    max_provider_requests: int | None = None,
) -> dict[str, object]:
    config = _read(config_path)
    state = _read(state_path)
    if config.get("schema_version") != "quantstack.cn-qfq-session-daily.v1":
        raise ValueError("unsupported CN qfq session daily config")
    if state.get("schema_version") != "quantstack.cn-qfq-session-state.v1":
        raise ValueError("unsupported CN qfq session state")
    watermark = date.fromisoformat(str(state["watermark"]))
    next_package = _next_package(Path(str(config["session_root"])), watermark)
    if next_package is None:
        return {"status": "noop", "watermark": watermark.isoformat()}
    session, package = next_package
    inspected = inspect_session_package(package)
    raw_path = inspected.path / "bars.parquet"
    native = load_daily_session_records(raw_path)
    universe = _universe(Path(str(config["instruments_path"])))
    plan = build_factor_query_plan(
        session=session,
        session_ordinal=session.toordinal(),
        universe=universe,
        daily_native_records=native,
        previous_closes={
            str(key): float(value) for key, value in dict(state["latest_raw_closes"]).items()
        },
        last_factor_checks=factor_last_checked(Path(str(state["factor_ledger_path"]))),
    )
    _validate_provider_request_budget(plan)
    _validate_provider_request_cap(plan, max_provider_requests)
    run_root = Path(str(config["work_root"])) / f"session={session.isoformat()}"
    run_root.mkdir(parents=True, exist_ok=True)
    plan_path = run_root / "factor-plan.json"
    # Keep the routine checkpoint separate from retained migration evidence.
    # A pre-lightweight full-universe delta must never control a later daily run.
    delta_path = run_root / "factor-delta-routine.json"
    next_ledger = run_root / "factor-ledger.json"
    _write(plan_path, plan)
    repo = Path(str(config["repo_root"])).resolve()
    provider_skipped = _factor_delta_is_complete(plan, delta_path)
    if not provider_skipped:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = f"{repo / 'src'}:{repo}"
        subprocess.run(
            [
                str(config.get("python", sys.executable)),
                str(repo / "scripts/acquire_cn_qfq_factors.py"),
                "--plan",
                str(plan_path),
                "--output",
                str(delta_path),
                "--start",
                session.isoformat(),
                "--end",
                session.isoformat(),
                "--provider-control-root",
                str(config["provider_control_root"]),
            ],
            check=True,
            cwd=repo,
            env=environment,
        )
    merge_factor_delta(
        current_ledger_path=Path(str(state["factor_ledger_path"])),
        factor_delta_path=delta_path,
        factor_plan_path=plan_path,
        output_path=next_ledger,
    )
    dataset_version = f"daily-through-{session:%Y%m%d}.qfq-session-v1"
    target = (
        Path(str(config["data_root"]))
        / "normalized/bars_1d/market=CN"
        / f"version={dataset_version}"
    )
    release = build_cn_qfq_session_release(
        session=session,
        raw_session_path=raw_path,
        current_dataset_path=Path(str(state["dataset_path"])),
        current_ledger_path=Path(str(state["factor_ledger_path"])),
        next_ledger_path=next_ledger,
        output_root=target,
    )
    latest_closes = {
        str(key): float(value) for key, value in dict(state["latest_raw_closes"]).items()
    }
    latest_closes.update(_session_closes(raw_path))
    row_count = int(state["row_count"]) + inspected.row_count
    receipt, proof = publish_cn_qfq_session_dataset(
        data_root=Path(str(config["data_root"])),
        dataset_version=dataset_version,
        descriptor_path=Path(str(release["descriptor_path"])),
        session_manifest_path=package / "manifest.json",
        instruments_source_path=Path(str(config["instruments_path"])),
        row_count=row_count,
        instrument_count=len(universe),
        first_date=date.fromisoformat(str(state["first_date"])),
        last_date=session,
    )
    next_state = {
        **state,
        "watermark": session.isoformat(),
        "dataset_version": dataset_version,
        "dataset_path": str(release["descriptor_path"]),
        "factor_ledger_path": str(next_ledger),
        "row_count": row_count,
        "instrument_count": len(universe),
        "latest_raw_closes": latest_closes,
        "last_receipt_id": receipt.receipt_id,
    }
    _write(state_path, next_state)
    return {
        "status": "completed",
        "session": session.isoformat(),
        "request_count": int(plan["request_count"]),
        "provider_skipped_from_complete_checkpoint": provider_skipped,
        "row_count": row_count,
        "history_rewritten": False,
        "publication": proof,
    }


def run(
    config_path: Path,
    state_path: Path,
    *,
    max_provider_requests: int | None = None,
) -> dict[str, object]:
    config = _read(config_path)
    lock_path = Path(str(config["work_root"])) / "control/session-daily.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another CN qfq session daily process is running") from exc
        result = _run(
            config_path,
            state_path,
            max_provider_requests=max_provider_requests,
        )
        ss_base_url = str(config.get("ss_base_url") or "").strip()
        if ss_base_url:
            current_state = _read(state_path)
            watermark = date.fromisoformat(str(current_state["watermark"]))
            snapshot = sync_daily_snapshot(
                ss_base_url=ss_base_url,
                dataset_version=str(current_state["dataset_version"]),
                expected_watermark=watermark,
            )
            report_root = Path(str(config["ss_report_root"]))
            report_path = report_root / f"cn-daily-snapshot-{watermark:%Y%m%d}.json"
            write_report(
                report_path,
                snapshot,
                gid=(None if config.get("ss_report_gid") is None else int(config["ss_report_gid"])),
            )
            result["ss_daily_snapshot"] = snapshot
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--max-provider-requests", type=int)
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                args.config.resolve(),
                args.state.resolve(),
                max_provider_requests=args.max_provider_requests,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
