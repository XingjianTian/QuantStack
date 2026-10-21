#!/usr/bin/env python3
"""Fail-closed T-078 Batch API startup diagnostic.

The probe never pulls or builds an image. It first attests the installed Python
distribution in a network-less container, then starts one isolated container and
requests only ``/health``. Evidence is persisted before every cleanup boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ATTESTED_MODULES = {
    "quant_stack.api": "src/quant_stack/api/__init__.py",
    "quant_stack.platform": "src/quant_stack/platform.py",
}
EXPECTED_DISTRIBUTION = "personal-quant-stack"
EXPECTED_ENTRYPOINT = "quant_stack.api:run"
EXPECTED_HEALTHCHECK = [
    "CMD-SHELL",
    (
        "python -c \"import urllib.request; "
        "urllib.request.urlopen('http://127.0.0.1:8900/health')\" || exit 1"
    ),
]
FORBIDDEN_DOCKER_ACTIONS = {"build", "pull", "push"}

INSTALLED_METADATA_SCRIPT = r"""
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path

distribution_name = "personal-quant-stack"
distribution = importlib.metadata.distribution(distribution_name)
entrypoints = {
    item.name: item.value
    for item in distribution.entry_points
    if item.group == "console_scripts"
}
modules = {}
for module_name in ("quant_stack.api", "quant_stack.platform"):
    spec = importlib.util.find_spec(module_name)
    origin = None if spec is None else spec.origin
    if not origin or not Path(origin).is_file():
        raise RuntimeError(f"installed module origin unavailable: {module_name}")
    modules[module_name] = {
        "origin": origin,
        "sha256": hashlib.sha256(Path(origin).read_bytes()).hexdigest(),
    }
print(json.dumps({
    "distribution": {"name": distribution.metadata["Name"], "version": distribution.version},
    "entrypoints": entrypoints,
    "modules": modules,
}, sort_keys=True))
""".strip()


class ProbeError(RuntimeError):
    """A bounded diagnostic precondition or operation failed."""


class DockerCLI:
    def run(self, args: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if not args or args[0] in FORBIDDEN_DOCKER_ACTIONS:
            raise ProbeError("forbidden Docker action")
        return subprocess.run(
            ["docker", *args],
            check=check,
            capture_output=True,
            text=True,
        )


def _utc_now() -> str:
    # NAS host Python is 3.10; datetime.UTC was added in 3.11.
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def redact(text: str, *, secrets_to_hide: Sequence[str], paths_to_hide: Sequence[Path]) -> str:
    redacted = text
    for value in sorted((item for item in secrets_to_hide if item), key=len, reverse=True):
        redacted = redacted.replace(value, "[REDACTED_SECRET]")
    for path in sorted((str(item.resolve()) for item in paths_to_hide), key=len, reverse=True):
        redacted = redacted.replace(path, "[REDACTED_PATH]")
    redacted = re.sub(r"(?i)(x-quant-api-key\s*[:=]\s*)\S+", r"\1[REDACTED_SECRET]", redacted)
    redacted = re.sub(r"(?i)(quant_api_key\s*[:=]\s*)\S+", r"\1[REDACTED_SECRET]", redacted)
    return redacted


def serialize_process_error(
    exc: subprocess.CalledProcessError,
    *,
    secrets_to_hide: Sequence[str],
    paths_to_hide: Sequence[Path],
) -> dict[str, Any]:
    command = list(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else [str(exc.cmd)]
    action = command[1] if command and command[0] == "docker" and len(command) > 1 else command[0]
    return {
        "type": type(exc).__name__,
        "action": action,
        "returncode": exc.returncode,
        "stdout": redact(
            exc.stdout or "", secrets_to_hide=secrets_to_hide, paths_to_hide=paths_to_hide
        ),
        "stderr": redact(
            exc.stderr or "", secrets_to_hide=secrets_to_hide, paths_to_hide=paths_to_hide
        ),
    }


def validate_image_contract(image: dict[str, Any]) -> dict[str, Any]:
    config = image.get("Config") or {}
    observed = {
        "cmd": config.get("Cmd"),
        "entrypoint": config.get("Entrypoint"),
        "healthcheck": (config.get("Healthcheck") or {}).get("Test"),
    }
    expected = {
        "cmd": ["quant-stack-api"],
        "entrypoint": None,
        "healthcheck": EXPECTED_HEALTHCHECK,
    }
    matches = {
        "cmd": observed["cmd"] == expected["cmd"],
        "entrypoint": observed["entrypoint"] in (None, []),
        "healthcheck": observed["healthcheck"] == expected["healthcheck"],
    }
    return {
        "observed": observed,
        "expected": expected,
        "matches": matches,
        "passed": all(matches.values()),
    }


def validate_installed_metadata(
    metadata: dict[str, Any], reviewed_hashes: dict[str, str]
) -> dict[str, Any]:
    distribution = metadata.get("distribution") or {}
    entrypoints = metadata.get("entrypoints") or {}
    modules = metadata.get("modules") or {}
    comparisons: dict[str, Any] = {}
    for module_name, source_path in ATTESTED_MODULES.items():
        observed = modules.get(module_name) or {}
        origin = observed.get("origin")
        normalized_origin = str(origin or "").replace("\\", "/")
        suffix = source_path.removeprefix("src/")
        origin_ok = (
            bool(origin)
            and Path(str(origin)).is_absolute()
            and normalized_origin.endswith(suffix)
        )
        expected_hash = reviewed_hashes.get(module_name)
        comparisons[module_name] = {
            "origin": origin,
            "origin_supported": origin_ok,
            "observed_sha256": observed.get("sha256"),
            "reviewed_sha256": expected_hash,
            "hash_matches": bool(expected_hash) and observed.get("sha256") == expected_hash,
        }
        comparisons[module_name]["passed"] = bool(
            comparisons[module_name]["origin_supported"]
            and comparisons[module_name]["hash_matches"]
        )
    distribution_name = str(distribution.get("name", "")).lower().replace("_", "-")
    distribution_ok = distribution_name == EXPECTED_DISTRIBUTION
    entrypoint_ok = entrypoints.get("quant-stack-api") == EXPECTED_ENTRYPOINT
    return {
        "distribution": distribution,
        "distribution_matches": distribution_ok,
        "entrypoint": entrypoints.get("quant-stack-api"),
        "entrypoint_matches": entrypoint_ok,
        "modules": comparisons,
        "passed": distribution_ok
        and entrypoint_ok
        and all(item["passed"] for item in comparisons.values()),
    }


def startup_create_args(
    *,
    name: str,
    network: str,
    image: str,
    data_root: Path,
    read_gid: int,
    host_port: int,
    api_key: str,
) -> list[str]:
    return [
        "create",
        "--name",
        name,
        "--network",
        network,
        "--publish",
        f"127.0.0.1:{host_port}:8900",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,size=64m",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--memory",
        "2g",
        "--cpus",
        "1.5",
        "--pids-limit",
        "256",
        "--group-add",
        str(read_gid),
        "--env",
        "QUANT_DATA_ROOT=/quant-data",
        "--env",
        "QUANT_EXPERIMENT_ROOT=/tmp/experiments",
        "--env",
        "QUANT_API_HOST=0.0.0.0",
        "--env",
        "QUANT_API_PORT=8900",
        "--env",
        f"QUANT_API_KEY={api_key}",
        "--env",
        "QUANT_BATCH_CONSUMER_ACCESS=shadow",
        "--volume",
        f"{data_root.resolve()}:/quant-data:ro",
        image,
    ]


@dataclass
class EvidenceLedger:
    path: Path
    payload: dict[str, Any]
    events: list[str] = field(default_factory=list)

    def mark(self, event: str) -> None:
        self.events.append(event)
        self.payload["lifecycle"] = list(self.events)

    def persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.events or self.events[-1] != "evidence_persisted":
            self.mark("evidence_persisted")
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(self.payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.path)


@dataclass
class ProbeConfig:
    image: str
    evidence: Path
    workspace: Path
    reviewed_source: str
    read_gid: int = 997
    host_port: int = 28920
    timeout_seconds: float = 90.0
    interval_seconds: float = 2.0
    container_name: str = "quantstack-t083-batch-api"
    network_name: str = "quantstack-t083-internal"


class StartupProbe:
    def __init__(
        self,
        config: ProbeConfig,
        *,
        docker: DockerCLI | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        health_request: Callable[[str, float], int] | None = None,
    ) -> None:
        self.config = config
        self.docker = docker or DockerCLI()
        self.monotonic = monotonic
        self.sleep = sleep
        self.health_request = health_request or self._request_health
        self.api_key = secrets.token_urlsafe(32)
        self.work_root: Path | None = None
        self.data_root: Path | None = None
        self.attestation_container = f"{config.container_name}-attest"
        self.resolved_image_id: str | None = None

    @staticmethod
    def _request_health(url: str, timeout: float) -> int:
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(4096)
            return int(response.status)

    def _inspect_json(self, kind: str, name: str) -> dict[str, Any]:
        result = self.docker.run([kind, "inspect", name])
        payload = json.loads(result.stdout)
        if not isinstance(payload, list) or len(payload) != 1:
            raise ProbeError(f"unexpected {kind} inspect response")
        return payload[0]

    def _reviewed_bytes(self, relative_path: str) -> bytes:
        if re.fullmatch(r"[0-9a-fA-F]{40}", self.config.reviewed_source) is None:
            raise ProbeError("reviewed source must be a complete 40-hex commit SHA")
        if not (self.config.workspace / ".git").exists():
            raise ProbeError("reviewed source requires verifiable Git metadata")
        commit = self.config.reviewed_source.lower()
        try:
            subprocess.run(
                ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
                cwd=self.config.workspace,
                check=True,
                capture_output=True,
            )
            result = subprocess.run(
                ["git", "show", f"{commit}:{relative_path}"],
                cwd=self.config.workspace,
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ProbeError("reviewed Git commit or source file is unavailable") from exc
        return result.stdout

    def _reviewed_hashes(self) -> dict[str, str]:
        return {
            module_name: _sha256(self._reviewed_bytes(source_path))
            for module_name, source_path in ATTESTED_MODULES.items()
        }

    def _redaction_paths(self) -> list[Path]:
        return [
            path
            for path in (self.config.workspace, self.work_root, self.data_root)
            if path is not None
        ]

    def _attest_image(self, ledger: EvidenceLedger) -> bool:
        image = self._inspect_json("image", self.config.image)
        config = image.get("Config") or {}
        resolved_image_id = image.get("Id")
        if not isinstance(resolved_image_id, str) or not resolved_image_id.startswith("sha256:"):
            raise ProbeError("image inspect did not return an immutable image Id")
        self.resolved_image_id = resolved_image_id
        identity = {
            "requested_reference": self.config.image,
            "resolved_id": resolved_image_id,
            "repo_digests": image.get("RepoDigests") or [],
            "cmd": config.get("Cmd"),
            "entrypoint": config.get("Entrypoint"),
            "healthcheck": config.get("Healthcheck"),
            "source_labels": {
                key: value
                for key, value in (config.get("Labels") or {}).items()
                if key.startswith("org.opencontainers.image.")
            },
        }
        ledger.payload["image"] = identity
        static_contract = validate_image_contract(image)
        source_attestation: dict[str, Any] = {
            "reviewed_source": self.config.reviewed_source,
            "static_contract": static_contract,
            "installed": None,
            "passed": False,
        }
        ledger.payload["source_attestation"] = source_attestation
        if not static_contract["passed"]:
            source_attestation["failure_code"] = "IMAGE_SOURCE_ATTESTATION_FAILED"
            ledger.persist()
            return False
        created = False
        try:
            self.docker.run(
                [
                    "create",
                    "--name",
                    self.attestation_container,
                    "--network",
                    "none",
                    "--entrypoint",
                    "python",
                    resolved_image_id,
                    "-c",
                    INSTALLED_METADATA_SCRIPT,
                ]
            )
            created = True
            ledger.mark("attestation_container_created")
            result = self.docker.run(["start", "-a", self.attestation_container])
            metadata = json.loads(result.stdout)
            installed = validate_installed_metadata(metadata, self._reviewed_hashes())
            source_attestation["installed"] = installed
            source_attestation["passed"] = bool(static_contract["passed"] and installed["passed"])
            ledger.persist()
            return bool(source_attestation["passed"])
        except subprocess.CalledProcessError as exc:
            source_attestation["error"] = serialize_process_error(
                exc,
                secrets_to_hide=[self.api_key],
                paths_to_hide=self._redaction_paths(),
            )
            source_attestation["failure_code"] = "IMAGE_SOURCE_ATTESTATION_FAILED"
            ledger.persist()
            return False
        except (ProbeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            source_attestation["error"] = {
                "type": type(exc).__name__,
                "detail": redact(
                    str(exc),
                    secrets_to_hide=[self.api_key],
                    paths_to_hide=self._redaction_paths(),
                ),
            }
            source_attestation["failure_code"] = "IMAGE_SOURCE_ATTESTATION_FAILED"
            ledger.persist()
            return False
        finally:
            if created:
                self.docker.run(["rm", "-f", self.attestation_container], check=False)
                ledger.mark("attestation_container_removed")
                ledger.persist()

    def _state_and_logs(self) -> tuple[dict[str, Any], str]:
        container = self._inspect_json("container", self.config.container_name)
        state = container.get("State") or {}
        selected = {
            key: state.get(key)
            for key in ("Status", "ExitCode", "OOMKilled", "Error", "StartedAt", "FinishedAt")
        }
        logs = self.docker.run(
            ["logs", "--timestamps", self.config.container_name], check=False
        )
        combined = "\n".join(item for item in (logs.stdout, logs.stderr) if item)
        assert self.work_root is not None and self.data_root is not None
        return selected, redact(
            combined,
            secrets_to_hide=[self.api_key],
            paths_to_hide=[self.config.workspace, self.work_root, self.data_root],
        )

    def _cleanup(self, ledger: EvidenceLedger) -> None:
        ledger.mark("cleanup_started")
        first_container = self.docker.run(
            ["rm", "-f", self.config.container_name], check=False
        ).returncode
        first_network = self.docker.run(
            ["network", "rm", self.config.network_name], check=False
        ).returncode
        second_container = self.docker.run(
            ["rm", "-f", self.config.container_name], check=False
        ).returncode
        second_network = self.docker.run(
            ["network", "rm", self.config.network_name], check=False
        ).returncode
        ledger.payload["cleanup"] = {
            "first": {"container_returncode": first_container, "network_returncode": first_network},
            "replay": {
                "container_returncode": second_container,
                "network_returncode": second_network,
                "idempotent": True,
            },
            "container_present_after": self.docker.run(
                ["container", "inspect", self.config.container_name], check=False
            ).returncode
            == 0,
            "network_present_after": self.docker.run(
                ["network", "inspect", self.config.network_name], check=False
            ).returncode
            == 0,
        }
        ledger.mark("cleanup_completed")
        ledger.persist()

    def run(self) -> int:
        payload: dict[str, Any] = {
            "schema_version": "quantstack.t078-startup-diagnostic.v1",
            "target_id": "T-20260811-083",
            "started_at": _utc_now(),
            "result": "ABORT_STARTUP_DIAGNOSTIC",
            "bounds": {
                "requested_path": "/health",
                "logical_calls": 0,
                "external_fetches": 0,
                "yahoo_calls": 0,
                "provider_calls": 0,
                "production_connections": 0,
                "network_mode": "dedicated_internal",
                "auto_remove": False,
            },
            "probe_timeline": [],
            "secrets_redacted": True,
        }
        ledger = EvidenceLedger(self.config.evidence, payload)
        with tempfile.TemporaryDirectory(prefix="quantstack-t083-") as temporary:
            self.work_root = Path(temporary)
            self.data_root = self.work_root / "isolated-data"
            self.data_root.mkdir(mode=0o750)
            ledger.mark("isolated_root_created")
            try:
                if not self._attest_image(ledger):
                    payload["failure"] = "IMAGE_SOURCE_ATTESTATION_FAILED"
                    ledger.persist()
                    return 2
                ledger.mark("image_source_attested")
                self.docker.run(["network", "create", "--internal", self.config.network_name])
                ledger.mark("internal_network_created")
                if not self.resolved_image_id:
                    raise ProbeError("startup image is not bound to an attested immutable Id")
                args = startup_create_args(
                    name=self.config.container_name,
                    network=self.config.network_name,
                    image=self.resolved_image_id or "",
                    data_root=self.data_root,
                    read_gid=self.config.read_gid,
                    host_port=self.config.host_port,
                    api_key=self.api_key,
                )
                if "--rm" in args or "--network" not in args:
                    raise ProbeError("unsafe startup container lifecycle")
                self.docker.run(args)
                ledger.mark("startup_container_created")
                self.docker.run(["start", self.config.container_name])
                ledger.mark("startup_container_started")
                started = self.monotonic()
                deadline = started + self.config.timeout_seconds
                while self.monotonic() < deadline:
                    attempt_started = self.monotonic()
                    record: dict[str, Any] = {
                        "attempt": len(payload["probe_timeline"]) + 1,
                        "elapsed_seconds": round(attempt_started - started, 3),
                        "path": "/health",
                    }
                    try:
                        status = self.health_request(
                            f"http://127.0.0.1:{self.config.host_port}/health", 2.0
                        )
                        record["status"] = status
                        payload["probe_timeline"].append(record)
                        if status == 200:
                            payload["result"] = "PASS_STARTUP_DIAGNOSTIC"
                            break
                    except (OSError, urllib.error.URLError) as exc:
                        record["error"] = type(exc).__name__
                        payload["probe_timeline"].append(record)
                    state = self._inspect_json("container", self.config.container_name).get(
                        "State", {}
                    )
                    if not state.get("Running", False):
                        break
                    self.sleep(self.config.interval_seconds)
                state, logs = self._state_and_logs()
                payload["container_state"] = state
                payload["logs"] = logs
                ledger.mark("state_and_logs_captured")
                ledger.persist()
                if payload["result"] != "PASS_STARTUP_DIAGNOSTIC":
                    payload["failure"] = "BATCH_API_DID_NOT_REACH_HEALTHY_STATE"
                    ledger.persist()
                    return 3
                return 0
            except (ProbeError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
                payload["failure"] = (
                    "IMAGE_SOURCE_ATTESTATION_FAILED"
                    if "image_source_attested" not in ledger.events
                    else type(exc).__name__
                )
                if isinstance(exc, subprocess.CalledProcessError):
                    payload["failure_detail"] = serialize_process_error(
                        exc,
                        secrets_to_hide=[self.api_key],
                        paths_to_hide=self._redaction_paths(),
                    )
                else:
                    payload["failure_detail"] = redact(
                        str(exc),
                        secrets_to_hide=[self.api_key],
                        paths_to_hide=self._redaction_paths(),
                    )
                try:
                    state, logs = self._state_and_logs()
                    payload["container_state"] = state
                    payload["logs"] = logs
                    ledger.mark("state_and_logs_captured")
                except (ProbeError, subprocess.CalledProcessError, json.JSONDecodeError):
                    payload["container_state"] = {"Status": "not_created_or_unavailable"}
                ledger.persist()
                return 4
            finally:
                # Every branch has persisted its available evidence before this point.
                self._cleanup(ledger)


def _parse_args() -> ProbeConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="already-present immutable image reference")
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--reviewed-source", required=True)
    parser.add_argument("--read-gid", type=int, default=997)
    parser.add_argument("--host-port", type=int, default=28920)
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    values = parser.parse_args()
    if shutil.which("docker") is None:
        parser.error("docker CLI is required")
    return ProbeConfig(**vars(values))


def main() -> int:
    return StartupProbe(_parse_args()).run()


if __name__ == "__main__":
    raise SystemExit(main())
