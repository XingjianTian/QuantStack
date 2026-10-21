"""Run the routine CN daily directly into one session Parquet package."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from datetime import time as wall_time
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from quant_stack.daily_cli import (
    _assert_daily_release_gate,
    _assert_verified_session,
    _verified_calendar,
)
from quant_stack.data.session_parquet import inspect_session_package
from quant_stack.data.storage.atomic import atomic_replace_bytes

_CN_TIMEZONE = ZoneInfo("Asia/Shanghai")
_COMMIT = re.compile(r"^[a-f0-9]{40}$")


class DailySchedulerConfig(BaseModel):
    """Only fields used by the routine session-Parquet worker."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["quantstack.cn-session-daily.v1"]
    repo_root: Path
    python: Path
    work_root: Path
    session_parquet_root: Path
    provider_control_root: Path | None = None
    market_index_root: Path | None = None
    reauthentication_replay: bool = True
    calendar_raw: Path
    calendar_normalized: Path
    calendar_raw_uri: str
    calendar_normalized_uri: str
    calendar_source: str
    calendar_version: str
    environment_id: str
    source_commit: str | None = Field(default=None, pattern=r"^[a-f0-9]{40}$")
    catch_up_start_date: date | None = None
    batch_size: Literal[500] = 500
    min_batch_interval_seconds: Literal[5] = 5


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def _load_config(path: Path) -> DailySchedulerConfig:
    config = DailySchedulerConfig.model_validate_json(path.read_text(encoding="utf-8"))
    for required in (
        config.repo_root,
        config.python,
        config.calendar_raw,
        config.calendar_normalized,
    ):
        if not required.exists():
            raise FileNotFoundError(f"required scheduler path is unavailable: {required}")
    return config


def _source_commit(config: DailySchedulerConfig) -> str:
    if config.source_commit is not None:
        return config.source_commit
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=config.repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if dirty:
        raise RuntimeError("daily scheduler requires a clean source checkout")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=config.repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not _COMMIT.fullmatch(commit):
        raise RuntimeError("daily scheduler could not resolve an exact source commit")
    return commit


def _provider_control_root(config: DailySchedulerConfig) -> Path:
    """Resolve the shared BaoStock budget and single-flight domain.

    Routine production configs keep the historical ``work_root/control``
    default. Recovery candidates may place every data artifact in a fresh
    isolated root while still sharing the production Provider budget and
    session lock.
    """

    return config.provider_control_root or config.work_root / "control"


def _calendar_preflight(
    config: DailySchedulerConfig,
    session_date: date,
    *,
    now: datetime | None = None,
) -> tuple[bool, str]:
    _assert_daily_release_gate(session_date, now=now)
    calendar, evidence = _verified_calendar(
        raw_path=config.calendar_raw,
        normalized_path=config.calendar_normalized,
        raw_uri=config.calendar_raw_uri,
        normalized_uri=config.calendar_normalized_uri,
        observed_at=now,
    )
    if evidence.source != config.calendar_source or evidence.version != config.calendar_version:
        raise ValueError("scheduler calendar identity does not match configured evidence")
    try:
        _assert_verified_session(session_date=session_date, calendar=calendar)
    except ValueError as exc:
        return False, str(exc)
    return True, "verified complete trading session"


def _completed_session(config: DailySchedulerConfig, session_date: date) -> bool:
    path = config.work_root / f"daily-{session_date:%Y%m%d}" / "runner-summary.json"
    if not path.is_file():
        return False
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
        if (
            summary.get("schema_version") != "quantstack.cn-session-daily-run.v1"
            or summary.get("status") != "completed"
            or summary.get("session_date") != session_date.isoformat()
        ):
            return False
        package = inspect_session_package(
            config.session_parquet_root
            / "market=CN"
            / f"session={session_date.isoformat()}"
        )
        return package.session == session_date and package.market == "CN"
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        return False


def _pending_sessions(
    config: DailySchedulerConfig,
    *,
    through_date: date,
    now: datetime,
) -> tuple[date, ...]:
    start = config.catch_up_start_date
    if start is None:
        return (through_date,)
    pending: list[date] = []
    current = start
    while current <= through_date:
        if current == now.date() and now.timetz().replace(tzinfo=None) < wall_time(18, 0):
            current += timedelta(days=1)
            continue
        eligible, _ = _calendar_preflight(config, current, now=now)
        if eligible and not _completed_session(config, current):
            pending.append(current)
        current += timedelta(days=1)
    return tuple(pending)


def run(
    config: DailySchedulerConfig,
    *,
    session_date: date,
    dry_run: bool = False,
) -> dict[str, object]:
    eligible, reason = _calendar_preflight(config, session_date)
    run_root = config.work_root / f"daily-{session_date:%Y%m%d}"
    summary_path = run_root / "runner-summary.json"
    if not eligible:
        result = {
            "schema_version": "quantstack.cn-session-daily-run.v1",
            "status": "skipped",
            "session_date": session_date.isoformat(),
            "reason": reason,
            "provider_requested": False,
        }
        if not dry_run:
            _write_json(summary_path, result)
        return result
    if summary_path.is_file():
        previous = json.loads(summary_path.read_text(encoding="utf-8"))
        if previous.get("status") == "completed":
            return {**previous, "scheduler_noop": True}
    commit = _source_commit(config)
    if dry_run:
        return {
            "schema_version": "quantstack.cn-session-daily-run.v1",
            "status": "dry_run",
            "session_date": session_date.isoformat(),
            "source_commit": commit,
            "batch_size": config.batch_size,
            "min_batch_interval_seconds": config.min_batch_interval_seconds,
            "persistence": "single_session_parquet",
        }

    run_root.mkdir(parents=True, exist_ok=True)
    config.session_parquet_root.mkdir(parents=True, exist_ok=True)
    snapshot = run_root / "universe-snapshot.json"
    instruments = run_root / "instruments.json"
    spec = run_root / "spec.json"
    plan = run_root / "plan.json"
    report = run_root / "run-report.json"
    base = [str(config.python), "-m", "quant_stack.daily_cli"]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{config.repo_root / 'src'}:{config.repo_root}"
    env["QUANTSTACK_BAOSTOCK_MIN_BATCH_INTERVAL_SECONDS"] = str(config.min_batch_interval_seconds)

    def execute(command: list[str]) -> None:
        subprocess.run(command, cwd=config.repo_root, env=env, check=True)

    execute(
        [
            *base,
            "build-live-universe",
            "--session",
            session_date.isoformat(),
            "--snapshot",
            str(snapshot),
            "--instruments",
            str(instruments),
            "--spec",
            str(spec),
            "--calendar-source",
            config.calendar_source,
            "--calendar-version",
            config.calendar_version,
            "--calendar-raw",
            str(config.calendar_raw),
            "--calendar-normalized",
            str(config.calendar_normalized),
            "--calendar-raw-uri",
            config.calendar_raw_uri,
            "--calendar-evidence-ref",
            config.calendar_normalized_uri,
            "--snapshot-evidence-ref",
            f"quant-data://evidence/universe/{session_date.isoformat()}.json",
            "--provider-control-root",
            str(_provider_control_root(config)),
            "--batch-size",
            str(config.batch_size),
        ]
    )
    execute([*base, "build-plan", str(spec), "--output", str(plan)])
    live_command = [
        *base,
        "run-live-full-market",
        str(plan),
            "--instruments",
            str(instruments),
            "--universe-snapshot",
            str(snapshot),
            "--calendar-raw",
            str(config.calendar_raw),
            "--calendar-normalized",
            str(config.calendar_normalized),
            "--calendar-raw-uri",
            config.calendar_raw_uri,
            "--calendar-normalized-uri",
            config.calendar_normalized_uri,
            "--data-root",
            str(run_root / "temporary-data"),
            "--provider-control-root",
            str(_provider_control_root(config)),
            "--state-root",
            str(run_root / "state"),
            "--report",
            str(report),
            "--session-parquet-root",
            str(config.session_parquet_root),
            "--session-cache-root",
            str(run_root / "provider-cache"),
            "--source-commit",
            commit,
            "--environment-id",
            config.environment_id,
    ]
    if not config.reauthentication_replay:
        live_command.append("--disable-expired-session-reauth-replay")
    execute(live_command)
    package = inspect_session_package(
        config.session_parquet_root / "market=CN" / f"session={session_date.isoformat()}"
    )
    result = {
        "schema_version": "quantstack.cn-session-daily-run.v1",
        "status": "completed",
        "session_date": session_date.isoformat(),
        "source_commit": commit,
        "records_received": package.row_count,
        "distinct_instruments": package.instrument_count,
        "omissions": package.omission_count,
        "persistence": "single_session_parquet",
        "provider_retries": 0,
        "catalog_published": False,
        "production_pointer_updated": False,
    }
    _write_json(summary_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        config = _load_config(args.config)
        if args.session is not None:
            results = [run(config, session_date=args.session, dry_run=args.dry_run)]
            planned = [args.session]
        else:
            now = datetime.now(_CN_TIMEZONE)
            planned = list(_pending_sessions(config, through_date=now.date(), now=now))
            results = [run(config, session_date=item, dry_run=args.dry_run) for item in planned]
        print(
            json.dumps(
                {
                    "schema_version": "quantstack.cn-session-daily-chain.v1",
                    "status": "completed",
                    "sessions_planned": [item.isoformat() for item in planned],
                    "sessions_completed": len(results),
                    "results": results,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps({"status": "failed", "error_type": type(exc).__name__, "error": str(exc)}),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
