#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from quant_stack.data.storage.atomic import atomic_replace_bytes


def _run(command: list[str]) -> str:
    return subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=60
    ).stdout.strip()


def _stale_daily_tasks(work_root: Path, stale_hours: int) -> list[str]:
    cutoff = datetime.now(UTC) - timedelta(hours=stale_hours)
    stale: list[str] = []
    for directory in sorted(work_root.glob("daily-*")):
        summary = directory / "runner-summary.json"
        if summary.is_file():
            try:
                if json.loads(summary.read_text(encoding="utf-8")).get("status") in {
                    "completed",
                    "skipped",
                }:
                    continue
            except json.JSONDecodeError:
                pass
        modified = datetime.fromtimestamp(directory.stat().st_mtime, UTC)
        if modified < cutoff:
            stale.append(directory.name)
    return stale


def _latest_price_date(payload: object) -> str | None:
    if not isinstance(payload, list):
        return None
    dates = [
        str(item.get("date") or item.get("time"))[:10]
        for item in payload
        if isinstance(item, dict) and (item.get("date") or item.get("time"))
    ]
    return max(dates, default=None)


def _unit_state(unit: str, *, user: bool = True) -> dict[str, bool]:
    prefix = ["systemctl", "--user"] if user else ["systemctl"]
    return {
        "enabled": _run([*prefix, "is-enabled", unit]) == "enabled",
        "active": _run([*prefix, "is-active", unit]) == "active",
    }


def _host_command(config: dict[str, object], command: list[str]) -> list[str]:
    if config.get("execution_mode", "remote_ssh") == "local":
        return command
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        str(config["nas_host"]),
        shlex.join(command),
    ]


def _cron_entry_state(
    crontab: str,
    *,
    marker: str,
    schedule: str,
    lock_path: str,
    wrapper_path: str,
) -> dict[str, bool]:
    lines = crontab.splitlines()
    try:
        marker_index = lines.index(marker)
    except ValueError:
        return {"present": False, "valid": False, "single_flight": False}
    entry = ""
    for line in lines[marker_index + 1 :]:
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.startswith("#"):
            break
        entry = candidate
        break
    parts = entry.split(maxsplit=5)
    if len(parts) != 6:
        return {
            "present": bool(entry),
            "valid": False,
            "single_flight": False,
        }
    schedule_matches = " ".join(parts[:5]) == schedule
    command = shlex.split(parts[5])
    required_prefix = ["/usr/bin/flock", "-n", lock_path, wrapper_path]
    single_flight = command[:4] == required_prefix
    return {
        "present": True,
        "valid": schedule_matches and single_flight,
        "single_flight": single_flight,
    }


def capture(config: dict[str, object]) -> dict[str, object]:
    containers = [str(item) for item in config["nas_containers"]]  # type: ignore[index]
    inspect = _run(
        _host_command(
            config,
            [
                "docker",
                "inspect",
                "--format",
                (
                    "{{.Name}} {{if .State.Health}}{{.State.Health.Status}}"
                    "{{else}}{{.State.Status}}{{end}}"
                ),
                *containers,
            ],
        )
    )
    container_status = {
        line.split(maxsplit=1)[0].removeprefix("/"): line.split(maxsplit=1)[1]
        for line in inspect.splitlines()
    }
    isolated_raw = _run(
        _host_command(config, ["cat", str(config["nas_isolated_evidence"])])
    )
    ss_url = str(config["ss_kline_url"])
    price_history = json.loads(
        _run(
            _host_command(
                config,
                [
                    "docker",
                    "exec",
                    str(config["ss_kline_container"]),
                    "python",
                    "-c",
                    (
                        "import json,urllib.request;"
                        "print(json.dumps(json.load(urllib.request.urlopen("
                        f"{ss_url!r},timeout=30))))"
                    ),
                ],
            )
        )
    )
    budget = json.loads(Path(str(config["provider_budget"])).read_text(encoding="utf-8"))
    scheduler_backend = str(config.get("scheduler_backend", "systemd_user"))
    operations_timer_unit = "quantstack-cn-operations-audit.timer"
    if scheduler_backend == "user_cron":
        crontab = _run(["crontab", "-l"])
        cron_service = _unit_state(str(config.get("cron_unit", "cron.service")), user=False)
        daily_marker = str(config.get("daily_cron_marker", "# quantstack-daily-primary"))
        audit_marker = str(config.get("audit_cron_marker", "# quantstack-operations-audit"))
        daily_cron_entry = _cron_entry_state(
            crontab,
            marker=daily_marker,
            schedule=str(config["daily_cron_schedule"]),
            lock_path=str(config["daily_cron_lock"]),
            wrapper_path=str(config["daily_cron_wrapper"]),
        )
        audit_cron_entry = _cron_entry_state(
            crontab,
            marker=audit_marker,
            schedule=str(config["audit_cron_schedule"]),
            lock_path=str(config["audit_cron_lock"]),
            wrapper_path=str(config["audit_cron_wrapper"]),
        )
        daily_timer = {
            "enabled": daily_cron_entry["valid"],
            "active": cron_service["active"] and daily_cron_entry["valid"],
        }
        operations_timer = {
            "enabled": audit_cron_entry["valid"],
            "active": cron_service["active"] and audit_cron_entry["valid"],
        }
        operator_linger: bool | None = None
        weekly_reconciliation = {
            "scheduler": "user_cron",
            "scheduled": audit_cron_entry["valid"],
        }
    elif scheduler_backend == "systemd_user":
        operations_timer_text = _run(
            ["systemctl", "--user", "cat", operations_timer_unit]
        )
        cron_service = None
        daily_timer = _unit_state("quantstack-daily.timer")
        operations_timer = _unit_state(operations_timer_unit)
        operator_linger = _run(
            [
                "loginctl",
                "show-user",
                str(config.get("operator_user", "root")),
                "-p",
                "Linger",
                "--value",
            ]
        ) == "yes"
        weekly_reconciliation = {
            "timer": operations_timer_unit,
            "scheduled": (
                "OnCalendar=Fri *-*-* 22:30:00 Asia/Shanghai"
                in operations_timer_text
            ),
        }
    else:
        raise ValueError(f"unsupported scheduler backend: {scheduler_backend}")
    tailscale_required = bool(config.get("tailscale_required", True))
    return {
        "captured_at": datetime.now(UTC).isoformat(),
        "scheduler_backend": scheduler_backend,
        "scheduler_service": cron_service,
        "daily_timer": daily_timer,
        "operations_timer": operations_timer,
        "cron_entries": (
            {"daily": daily_cron_entry, "audit": audit_cron_entry}
            if scheduler_backend == "user_cron"
            else None
        ),
        "tailscale_required": tailscale_required,
        "tailscale_service": (
            _unit_state(
                str(config.get("tailscale_unit", "tailscaled-userspace.service")),
                user=config.get("tailscale_scope", "user") == "user",
            )
            if tailscale_required
            else None
        ),
        "operator_linger": operator_linger,
        "weekly_reconciliation": weekly_reconciliation,
        "provider_budget": budget,
        "nas_containers": container_status,
        "stale_tasks": _stale_daily_tasks(
            Path(str(config["work_root"])), int(config.get("stale_hours", 6))
        ),
        "nas_isolated": json.loads(isolated_raw),
        "ss_kline": {
            "url": str(config["ss_kline_url"]),
            "last_date": _latest_price_date(price_history),
            "points": len(price_history) if isinstance(price_history, list) else 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    payload = capture(config)
    atomic_replace_bytes(
        args.output,
        (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(json.dumps({"status": "captured", "ss_last_date": payload["ss_kline"]["last_date"]}))  # type: ignore[index]


if __name__ == "__main__":
    main()
