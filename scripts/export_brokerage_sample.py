"""Export fictional CLI results for Quant Center's static account preview.

Run with both repository source roots on PYTHONPATH. All broker responses come
from the checked-in fixtures; socket connections are disabled during export.
Runtime databases exist only in temporary directories and are removed on exit.
"""

from __future__ import annotations

import argparse
import io
import json
import socket
import tempfile
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from quant_stack.brokerage.sync import PrivateSnapshotStore
from quant_stack.brokerage_cli import DEFAULT_ACCOUNT_ID, DEFAULT_FIXTURE_ROOT, main

SAMPLE_TIME = "2026-10-16T00:00:00Z"
STALE_TIME = "2026-10-16T01:00:00Z"
REPOSITORY = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPOSITORY / "apps/quant-center/src/brokerageSample.generated.json"


def _without_attempt_ids(value: Any) -> Any:
    """Omit random operational identifiers, preserving all financial facts."""
    if isinstance(value, dict):
        return {
            key: _without_attempt_ids(item)
            for key, item in value.items()
            if key not in {"attempt_id", "last_attempt_id"}
        }
    if isinstance(value, list):
        return [_without_attempt_ids(item) for item in value]
    return value


def _offline_only(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("The fictional preview exporter forbids network connections")


def _profile(profile: str, runtime: Path) -> dict[str, Any]:
    base = [
        "--profile",
        profile,
        "--fixture-root",
        str(DEFAULT_FIXTURE_ROOT),
        "--state-root",
        str(runtime),
    ]

    def run(command: str, at: str, expected: int) -> dict[str, Any]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main([*base, command, "--at", at])
        if code != expected:
            raise RuntimeError(f"{profile}/{command} failed: {stdout.getvalue()}")
        return json.loads(stdout.getvalue())

    sync = run("sync-sample", SAMPLE_TIME, 0)
    snapshot = PrivateSnapshotStore(runtime, source_root=REPOSITORY).latest(DEFAULT_ACCOUNT_ID)
    if snapshot is None:
        raise RuntimeError(f"{profile} did not publish a fictional snapshot")
    proposal = run("propose-sample", SAMPLE_TIME, 0 if profile == "ready" else 2)
    state = run("show-state", SAMPLE_TIME, 0)
    stale_state = run("show-state", STALE_TIME, 0)
    return _without_attempt_ids(
        {
            "snapshot": snapshot.model_dump(mode="json"),
            "sync": sync,
            "proposal": proposal,
            "state": state,
            "staleState": stale_state,
        }
    )


def export_sample() -> dict[str, Any]:
    with (
        tempfile.TemporaryDirectory(prefix="quantstack-center-fixture-") as temporary,
        patch.object(socket.socket, "connect", _offline_only),
        patch.object(socket.socket, "connect_ex", _offline_only),
    ):
        root = Path(temporary)
        return {
            "schemaVersion": 1,
            # This is the fixed replay time, deliberately not the wall clock.
            "generatedAt": SAMPLE_TIME,
            "sampleTime": SAMPLE_TIME,
            "staleTime": STALE_TIME,
            "fictional": True,
            "nonExecutable": True,
            "ready": _profile("ready", root / "ready"),
            "blocked": _profile("blocked", root / "blocked"),
        }


def export_main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    data = export_sample()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Exported fictional account preview; no broker network access or executable orders")


if __name__ == "__main__":
    export_main()
