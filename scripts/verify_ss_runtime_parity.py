#!/usr/bin/env python3
"""Fail closed unless the SS API and both scan workers run one immutable image."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

REQUIRED_ROLES = (
    "backend",
    "celery-general",
    "celery-userscans-cn",
    "celery-userscans-us",
)


def evaluate_runtime_parity(
    records: list[dict[str, Any]], *, expected_image: str | None = None
) -> dict[str, Any]:
    by_role: dict[str, dict[str, Any]] = {}
    for record in records:
        labels = ((record.get("Config") or {}).get("Labels") or {})
        role = labels.get("com.docker.compose.service")
        if role in REQUIRED_ROLES:
            by_role[role] = record
    missing = [role for role in REQUIRED_ROLES if role not in by_role]
    if missing:
        raise ValueError(f"missing required SS runtime roles: {', '.join(missing)}")

    image_refs = {
        role: str((record.get("Config") or {}).get("Image") or "")
        for role, record in by_role.items()
    }
    image_ids = {role: str(record.get("Image") or "") for role, record in by_role.items()}
    running = {
        role: bool((record.get("State") or {}).get("Running"))
        for role, record in by_role.items()
    }
    if not all(running.values()):
        raise ValueError(f"SS runtime role is not running: {running}")
    if len(set(image_refs.values())) != 1 or len(set(image_ids.values())) != 1:
        raise ValueError(
            f"SS API/worker image drift: refs={image_refs}, immutable_ids={image_ids}"
        )
    observed = next(iter(image_refs.values()))
    if expected_image and observed != expected_image:
        raise ValueError(
            f"SS runtime image mismatch: expected {expected_image}, observed {observed}"
        )
    return {
        "schema_version": "quantstack.ss-runtime-parity.v1",
        "status": "passed",
        "image": observed,
        "immutable_image_id": next(iter(image_ids.values())),
        "roles": list(REQUIRED_ROLES),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspect-json", type=Path)
    parser.add_argument("--expected-image")
    parser.add_argument("--project-directory", type=Path)
    parser.add_argument("--compose-file", type=Path, action="append", default=[])
    args = parser.parse_args()
    if args.inspect_json:
        records = json.loads(args.inspect_json.read_text(encoding="utf-8"))
    else:
        compose = ["docker", "compose"]
        if args.project_directory:
            compose.extend(["--project-directory", str(args.project_directory)])
        for path in args.compose_file:
            compose.extend(["--file", str(path)])
        ids_result = subprocess.run(
            [*compose, "ps", "-q", *REQUIRED_ROLES],
            check=True,
            capture_output=True,
            text=True,
        )
        container_ids = ids_result.stdout.split()
        result = subprocess.run(
            ["docker", "inspect", *container_ids],
            check=True,
            capture_output=True,
            text=True,
        )
        records = json.loads(result.stdout)
    print(
        json.dumps(
            evaluate_runtime_parity(records, expected_image=args.expected_image),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
