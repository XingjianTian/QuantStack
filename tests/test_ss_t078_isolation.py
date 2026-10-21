from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
import urllib.error
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[1]
COMPOSE = ROOT / "docker/stock-screener/compose.shadow-t078.yaml"
PROXY = ROOT / "docker/stock-screener/yahoo_allow_proxy.py"
DRIVER = ROOT / "docker/stock-screener/shadow_soak_driver.py"
COMPAT = ROOT / "docker/stock-screener/quant_workspace_compat.py"
STARTUP_PROBE = ROOT / "scripts/t078_batch_api_startup_probe.py"


def _proxy_module():
    spec = importlib.util.spec_from_file_location("t078_yahoo_allow_proxy", PROXY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _driver_module():
    spec = importlib.util.spec_from_file_location("t078_shadow_soak_driver", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("wait_for_idle", "expected_path"),
    [
        (False, "/__t078/control/health"),
        (True, "/__t078/control/health?wait_for_idle=true"),
    ],
)
def test_t078_http_health_consumes_single_dict_response(
    monkeypatch: pytest.MonkeyPatch,
    wait_for_idle: bool,
    expected_path: str,
) -> None:
    driver = _driver_module()
    client = object.__new__(driver.T078HTTPClient)
    response = {"status": "ok"}
    calls: list[tuple[str, str, bool]] = []

    def fake_request(
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        *,
        control: bool = False,
    ) -> dict[str, object]:
        assert payload is None
        calls.append((method, path, control))
        return response

    monkeypatch.setattr(client, "request", fake_request)

    assert client.health(wait_for_idle=wait_for_idle) is response
    assert calls == [("GET", expected_path, True)]


def _startup_probe_module():
    spec = importlib.util.spec_from_file_location("t078_batch_api_startup_probe", STARTUP_PROBE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_t078_compose_is_standalone_internal_and_loopback_only() -> None:
    raw = COMPOSE.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)

    assert parsed["name"] == "quant-workspace-shadow-t078"
    assert set(parsed["services"]) == {
        "postgres",
        "redis",
        "batch-api",
        "egress-proxy",
        "backend",
    }
    assert parsed["networks"]["core"]["internal"] is True
    assert parsed["services"]["backend"]["networks"] == ["core"]
    assert set(parsed["services"]["egress-proxy"]["networks"]) == {"core", "egress"}
    assert parsed["services"]["backend"]["ports"] == ["127.0.0.1:28910:8000"]
    assert parsed["services"]["batch-api"]["ports"] == ["127.0.0.1:28920:8900"]
    assert parsed["services"]["batch-api"]["group_add"] == [
        "${T078_QUANT_DATA_READ_GID:?set the stable quant-data-read GID}"
    ]
    for service_name in ("postgres", "redis", "egress-proxy", "backend"):
        assert parsed["services"][service_name]["group_add"] == [
            "${T078_SHADOW_WRITE_GID:?set the stable quant-shadow-write GID}"
        ]
    assert "external:" not in raw
    assert "network_mode:" not in raw
    assert "container_name:" not in raw


def test_t078_compose_keeps_raw_shadow_fail_closed() -> None:
    parsed = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    backend = parsed["services"]["backend"]
    environment = backend["environment"]

    assert environment["WEB_CONCURRENCY"] == "1"
    assert environment["SS_QUANTSTACK_READ_MODE"] == "shadow"
    assert environment["SS_QUANTSTACK_SHADOW_ENABLED"] == "true"
    assert environment["SS_QUANTSTACK_SHADOW_SAMPLE_RATE"] == "1"
    assert environment["SS_QUANTSTACK_SHADOW_QUEUE_SIZE"] == "32"
    assert environment["SS_QUANTSTACK_SHADOW_MAX_SYMBOLS_PER_TASK"] == "50"
    assert environment["SS_QUANTSTACK_ALLOW_CANDIDATE"] == "false"
    assert parsed["services"]["batch-api"]["environment"]["QUANT_BATCH_CONSUMER_ACCESS"] == "shadow"
    assert all(service["restart"] == "no" for service in parsed["services"].values())
    proxy_mounts = parsed["services"]["egress-proxy"]["volumes"]
    backend_mounts = parsed["services"]["backend"]["volumes"]
    assert any("/proxy:/evidence/proxy" in mount for mount in proxy_mounts)
    assert any("/driver:/evidence/driver" in mount for mount in backend_mounts)
    assert environment["SS_QUANTSTACK_SHADOW_REPORT_ROOT"].startswith("/evidence/driver/")
    assert "app.services" not in DRIVER.read_text(encoding="utf-8")
    assert "uvicorn quant_workspace_compat:app" in backend["command"]


def test_t083_startup_probe_create_is_non_removing_and_zero_egress(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    resolved_image = "sha256:" + "a" * 64
    args = probe.startup_create_args(
        name="diagnostic",
        network="internal-only",
        image=resolved_image,
        data_root=tmp_path,
        read_gid=997,
        host_port=28920,
        api_key="ephemeral-secret",
    )

    assert args[0] == "create"
    assert "--rm" not in args
    assert args[args.index("--network") + 1] == "internal-only"
    assert "host" not in args
    assert "127.0.0.1:28920:8900" in args
    assert f"{tmp_path.resolve()}:/quant-data:ro" in args
    assert args[-1] == resolved_image
    assert not ({"pull", "build", "push"} & set(args))


def test_t083_startup_probe_redacts_secrets_headers_and_private_paths(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    secret = "t083-api-key-value"
    raw = (
        f"QUANT_API_KEY={secret} x-quant-api-key: {secret} "
        f"failed under {tmp_path.resolve()}"
    )

    redacted = probe.redact(raw, secrets_to_hide=[secret], paths_to_hide=[tmp_path])

    assert secret not in redacted
    assert str(tmp_path.resolve()) not in redacted
    assert "[REDACTED_SECRET]" in redacted
    assert "[REDACTED_PATH]" in redacted


def _installed_metadata(probe, root: str, hashes: dict[str, str]):
    return {
        "distribution": {"name": "personal-quant-stack", "version": "0.5.1"},
        "entrypoints": {"quant-stack-api": "quant_stack.api:run"},
        "modules": {
            module: {
                "origin": f"{root}/{source.removeprefix('src/')}",
                "sha256": hashes[module],
            }
            for module, source in probe.ATTESTED_MODULES.items()
        },
    }


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def _reviewed_source_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "reviewed-repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "T085 Test")
    _git(repo, "config", "user.email", "t085@example.invalid")
    api = repo / "src/quant_stack/api/__init__.py"
    platform = repo / "src/quant_stack/platform.py"
    api.parent.mkdir(parents=True)
    api.write_text("def run():\n    return 'reviewed'\n", encoding="utf-8")
    platform.write_text("PLATFORM = 'reviewed'\n", encoding="utf-8")
    _git(repo, "add", "src")
    _git(repo, "commit", "-q", "-m", "reviewed source")
    commit = _git(repo, "rev-parse", "HEAD").stdout.decode().strip()
    return repo, commit


def _source_harness(probe, workspace: Path, reviewed_source: str, tmp_path: Path):
    return probe.StartupProbe(
        probe.ProbeConfig(
            image="unused",
            evidence=tmp_path / "unused.json",
            workspace=workspace,
            reviewed_source=reviewed_source,
        ),
        docker=object(),
    )


def test_t085_reviewed_source_rejects_non_git_workspace_with_direct_file(
    tmp_path: Path, monkeypatch
) -> None:
    probe = _startup_probe_module()
    source = tmp_path / "src/quant_stack/api/__init__.py"
    source.parent.mkdir(parents=True)
    source.write_text("unreviewed workspace content", encoding="utf-8")
    harness = _source_harness(probe, tmp_path, "a" * 40, tmp_path)
    monkeypatch.setattr(
        probe.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("no-.git must fail before invoking Git")
        ),
    )

    with pytest.raises(probe.ProbeError, match="verifiable Git metadata"):
        harness._reviewed_bytes("src/quant_stack/api/__init__.py")


@pytest.mark.parametrize("ref", ["", "HEAD", "main", "93294a3", "g" * 40, "a" * 39])
def test_t085_reviewed_source_rejects_symbolic_abbreviated_or_malformed_ref(
    tmp_path: Path, ref: str
) -> None:
    probe = _startup_probe_module()
    (tmp_path / ".git").mkdir()
    harness = _source_harness(probe, tmp_path, ref, tmp_path)

    with pytest.raises(probe.ProbeError, match="complete 40-hex commit SHA"):
        harness._reviewed_bytes("src/quant_stack/api/__init__.py")


def test_t085_reviewed_source_rejects_missing_commit_and_file(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    repo, commit = _reviewed_source_repo(tmp_path)
    missing_commit = _source_harness(probe, repo, "f" * 40, tmp_path)
    with pytest.raises(probe.ProbeError, match="commit or source file is unavailable"):
        missing_commit._reviewed_bytes("src/quant_stack/api/__init__.py")

    missing_file = _source_harness(probe, repo, commit, tmp_path)
    with pytest.raises(probe.ProbeError, match="commit or source file is unavailable"):
        missing_file._reviewed_bytes("src/quant_stack/missing.py")


def test_t085_reviewed_source_ignores_workspace_drift(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    repo, commit = _reviewed_source_repo(tmp_path)
    source = repo / "src/quant_stack/api/__init__.py"
    reviewed = source.read_bytes()
    source.write_text("def run():\n    return 'drifted'\n", encoding="utf-8")
    harness = _source_harness(probe, repo, commit, tmp_path)

    assert harness._reviewed_bytes("src/quant_stack/api/__init__.py") == reviewed


def test_t085_reviewed_source_accepts_worktree_gitfile(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    repo, commit = _reviewed_source_repo(tmp_path)
    worktree = tmp_path / "reviewed-worktree"
    _git(repo, "worktree", "add", "-q", "--detach", str(worktree), commit)
    assert (worktree / ".git").is_file()
    harness = _source_harness(probe, worktree, commit, tmp_path)

    assert harness._reviewed_bytes("src/quant_stack/platform.py") == b"PLATFORM = 'reviewed'\n"


@pytest.mark.parametrize(
    "root",
    ["/app/src", "/app/.venv/lib/python3.12/site-packages"],
)
def test_t084_installed_metadata_accepts_editable_and_site_packages(root: str) -> None:
    probe = _startup_probe_module()
    hashes = {module: str(index) * 64 for index, module in enumerate(probe.ATTESTED_MODULES, 1)}
    metadata = _installed_metadata(probe, root, hashes)

    result = probe.validate_installed_metadata(metadata, hashes)

    assert result["passed"] is True
    assert all(item["origin_supported"] for item in result["modules"].values())
    assert all(item["hash_matches"] for item in result["modules"].values())


def test_t084_installed_metadata_fails_closed_on_hash_or_entrypoint_drift() -> None:
    probe = _startup_probe_module()
    hashes = {module: str(index) * 64 for index, module in enumerate(probe.ATTESTED_MODULES, 1)}
    metadata = _installed_metadata(probe, "/app/src", hashes)
    metadata["modules"]["quant_stack.platform"]["sha256"] = "f" * 64
    metadata["entrypoints"]["quant-stack-api"] = "quant_stack.api:other"

    result = probe.validate_installed_metadata(metadata, hashes)

    assert result["passed"] is False
    assert result["entrypoint_matches"] is False
    assert result["modules"]["quant_stack.platform"]["hash_matches"] is False


def test_t084_image_contract_includes_healthcheck() -> None:
    probe = _startup_probe_module()
    image = {
        "Config": {
            "Cmd": ["quant-stack-api"],
            "Entrypoint": None,
            "Healthcheck": {"Test": list(probe.EXPECTED_HEALTHCHECK)},
        }
    }
    assert probe.validate_image_contract(image)["passed"] is True
    image["Config"]["Healthcheck"]["Test"][-1] = "curl localhost:8900/health"
    result = probe.validate_image_contract(image)
    assert result["passed"] is False
    assert result["matches"]["healthcheck"] is False


def test_t084_process_error_serialization_redacts_output(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    secret = "synthetic-secret"
    error = subprocess.CalledProcessError(
        17,
        ["docker", "start", "attest"],
        output=f"QUANT_API_KEY={secret}",
        stderr=f"failed at {tmp_path.resolve()} x-quant-api-key: {secret}",
    )

    result = probe.serialize_process_error(
        error,
        secrets_to_hide=[secret],
        paths_to_hide=[tmp_path],
    )

    assert result["action"] == "start"
    assert result["returncode"] == 17
    assert secret not in result["stdout"] + result["stderr"]
    assert str(tmp_path.resolve()) not in result["stderr"]


class _AttestationFailureDocker:
    def __init__(self, evidence_path: Path, probe, private_path: Path) -> None:
        self.evidence_path = evidence_path
        self.probe = probe
        self.private_path = private_path
        self.calls: list[list[str]] = []

    def run(self, args, *, check=True):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["image", "inspect"]:
            payload = [{
                "Id": "sha256:" + "a" * 64,
                "RepoDigests": [],
                "Config": {
                    "Cmd": ["quant-stack-api"],
                    "Entrypoint": None,
                    "Healthcheck": {"Test": list(self.probe.EXPECTED_HEALTHCHECK)},
                },
            }]
            return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")
        if args[0] == "create":
            return subprocess.CompletedProcess(args, 0, "container-id", "")
        if args[:2] == ["start", "-a"]:
            raise subprocess.CalledProcessError(
                23,
                ["docker", *args],
                output="partial synthetic-secret",
                stderr=f"cannot inspect {self.private_path.resolve()}",
            )
        if args[:2] == ["rm", "-f"]:
            persisted = json.loads(self.evidence_path.read_text(encoding="utf-8"))
            attestation = persisted["source_attestation"]
            assert attestation["failure_code"] == "IMAGE_SOURCE_ATTESTATION_FAILED"
            assert attestation["error"]["returncode"] == 23
            assert "synthetic-secret" not in json.dumps(attestation)
            assert str(self.private_path.resolve()) not in json.dumps(attestation)
            assert "evidence_persisted" in persisted["lifecycle"]
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(f"unexpected fake Docker call: {args}")


def test_t084_attestation_failure_persists_partial_evidence_before_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    probe = _startup_probe_module()
    monkeypatch.setattr(
        probe.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("offline attestation test must not execute docker")
        ),
    )
    evidence = tmp_path / "attestation.json"
    docker = _AttestationFailureDocker(evidence, probe, tmp_path)
    config = probe.ProbeConfig(
        image="personal/quant-stack-api:0.5.1",
        evidence=evidence,
        workspace=tmp_path,
        reviewed_source="reviewed",
    )
    harness = probe.StartupProbe(config, docker=docker)
    harness.api_key = "synthetic-secret"
    harness.work_root = tmp_path
    harness.data_root = tmp_path / "data"
    ledger = probe.EvidenceLedger(evidence, {"lifecycle": []})

    assert harness._attest_image(ledger) is False
    assert docker.calls[-1][:2] == ["rm", "-f"]
    assert ledger.events.index("evidence_persisted") < ledger.events.index(
        "attestation_container_removed"
    )


class _AttestationSuccessDocker:
    def __init__(self, probe, metadata: dict) -> None:
        self.probe = probe
        self.metadata = metadata
        self.calls: list[list[str]] = []
        self.image_id = "sha256:" + "b" * 64

    def run(self, args, *, check=True):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["image", "inspect"]:
            payload = [{
                "Id": self.image_id,
                "RepoDigests": [],
                "Config": {
                    "Cmd": ["quant-stack-api"],
                    "Entrypoint": None,
                    "Healthcheck": {"Test": list(self.probe.EXPECTED_HEALTHCHECK)},
                },
            }]
            return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")
        if args[0] == "create":
            assert self.image_id in args
            assert "personal/quant-stack-api:0.5.1" not in args
            assert args[args.index("--network") + 1] == "none"
            return subprocess.CompletedProcess(args, 0, "container-id", "")
        if args[:2] == ["start", "-a"]:
            return subprocess.CompletedProcess(args, 0, json.dumps(self.metadata), "")
        if args[:2] == ["rm", "-f"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(f"unexpected fake Docker call: {args}")


def test_t084_attestation_success_uses_installed_metadata_and_immutable_id(
    tmp_path: Path, monkeypatch
) -> None:
    probe = _startup_probe_module()
    hashes = {module: str(index) * 64 for index, module in enumerate(probe.ATTESTED_MODULES, 1)}
    metadata = _installed_metadata(
        probe,
        "/app/.venv/lib/python3.12/site-packages",
        hashes,
    )
    docker = _AttestationSuccessDocker(probe, metadata)
    evidence = tmp_path / "attestation-success.json"
    config = probe.ProbeConfig(
        image="personal/quant-stack-api:0.5.1",
        evidence=evidence,
        workspace=tmp_path,
        reviewed_source="reviewed",
    )
    harness = probe.StartupProbe(config, docker=docker)
    monkeypatch.setattr(harness, "_reviewed_hashes", lambda: hashes)
    monkeypatch.setattr(
        probe.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("offline attestation test must not execute docker")
        ),
    )
    ledger = probe.EvidenceLedger(evidence, {})

    assert harness._attest_image(ledger) is True
    assert harness.resolved_image_id == docker.image_id
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["source_attestation"]["passed"] is True
    assert not any(call[0] == "cp" for call in docker.calls)


def test_t084_evidence_persist_is_single_atomic_payload(tmp_path: Path) -> None:
    probe = _startup_probe_module()
    evidence = tmp_path / "evidence.json"
    ledger = probe.EvidenceLedger(evidence, {"value": 1})

    ledger.persist()

    assert json.loads(evidence.read_text(encoding="utf-8"))["lifecycle"] == [
        "evidence_persisted"
    ]
    assert not evidence.with_suffix(".json.tmp").exists()


class _ProbeDocker:
    def __init__(self, evidence_path: Path) -> None:
        self.evidence_path = evidence_path
        self.calls: list[list[str]] = []
        self.cleanup_calls = 0

    def run(self, args, *, check=True):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["container", "inspect"]:
            if self.cleanup_calls:
                return subprocess.CompletedProcess(args, 1, "", "not found")
            payload = [{"State": {"Running": False}}]
            return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")
        if args[:2] == ["network", "inspect"]:
            return subprocess.CompletedProcess(args, 1, "", "not found")
        if args[:2] == ["rm", "-f"]:
            evidence = json.loads(self.evidence_path.read_text(encoding="utf-8"))
            assert "state_and_logs_captured" in evidence["lifecycle"]
            assert "evidence_persisted" in evidence["lifecycle"]
            self.cleanup_calls += 1
            return subprocess.CompletedProcess(args, 0 if self.cleanup_calls == 1 else 1, "", "")
        if args[:2] == ["network", "rm"]:
            return subprocess.CompletedProcess(args, 0 if self.cleanup_calls == 1 else 1, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")


def test_t083_failed_container_evidence_precedes_idempotent_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    probe = _startup_probe_module()
    evidence = tmp_path / "startup-evidence.json"
    docker = _ProbeDocker(evidence)
    config = probe.ProbeConfig(
        image="quantstack@example",
        evidence=evidence,
        workspace=ROOT,
        reviewed_source="93294a34e963d7010d0400b8fb75d1df7d16e315",
        timeout_seconds=1,
        interval_seconds=0,
    )
    harness = probe.StartupProbe(
        config,
        docker=docker,
        monotonic=iter([0.0, 0.0, 0.1]).__next__,
        sleep=lambda _: None,
        health_request=lambda *_: (_ for _ in ()).throw(urllib.error.URLError("refused")),
    )

    def attest(ledger):
        ledger.payload["source_attestation"] = {"passed": True}
        harness.resolved_image_id = "sha256:" + "a" * 64
        return True

    monkeypatch.setattr(harness, "_attest_image", attest)
    monkeypatch.setattr(
        harness,
        "_state_and_logs",
        lambda: (
            {
                "Status": "exited",
                "ExitCode": 1,
                "OOMKilled": False,
                "Error": "",
                "StartedAt": "2026-08-11T00:00:00Z",
                "FinishedAt": "2026-08-11T00:00:01Z",
            },
            "Traceback: startup failed",
        ),
    )

    assert harness.run() == 3
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["result"] == "ABORT_STARTUP_DIAGNOSTIC"
    assert payload["container_state"]["ExitCode"] == 1
    assert payload["logs"] == "Traceback: startup failed"
    assert payload["bounds"]["external_fetches"] == 0
    assert payload["bounds"]["production_connections"] == 0
    assert payload["cleanup"]["replay"]["idempotent"] is True
    assert payload["cleanup"]["container_present_after"] is False
    assert payload["cleanup"]["network_present_after"] is False
    assert docker.cleanup_calls == 2
    assert ["network", "create", "--internal", config.network_name] in docker.calls


def test_t078_proxy_accepts_only_absolute_http_or_connect_targets() -> None:
    proxy = _proxy_module()

    assert proxy._target("CONNECT query1.finance.yahoo.com:443 HTTP/1.1", b"\r\n") == (
        "query1.finance.yahoo.com",
        443,
        None,
    )
    host, port, forwarded = proxy._target(
        "GET http://query2.finance.yahoo.com/v8/chart/600000.SS?period=1y HTTP/1.1",
        b"Host: query2.finance.yahoo.com\r\n\r\n",
    )
    assert (host, port) == ("query2.finance.yahoo.com", 80)
    assert forwarded is not None
    assert forwarded.startswith(b"GET /v8/chart/600000.SS?period=1y HTTP/1.1\r\n")


def test_t078_proxy_budget_is_persistent_and_bounded(tmp_path: Path) -> None:
    proxy = _proxy_module()
    ledger = tmp_path / "external-fetch-ledger.jsonl"
    budget = proxy.ConnectionBudget(maximum=2, minimum_interval_seconds=0, ledger=ledger)

    assert budget.acquire(host="query1.finance.yahoo.com", port=443) == 1
    assert budget.acquire(host="query2.finance.yahoo.com", port=443) == 2
    assert budget.acquire(host="query1.finance.yahoo.com", port=443) is None
    replay = proxy.ConnectionBudget(maximum=2, minimum_interval_seconds=0, ledger=ledger)
    assert replay.acquire(host="query1.finance.yahoo.com", port=443) is None
    assert ledger.stat().st_mode & 0o777 == 0o600


class _FakeClient:
    def __init__(self) -> None:
        self.worker = {
            "thread_alive": True,
            "queue_size": 0,
            "queue_capacity": 32,
            "submitted": 0,
            "completed": 0,
            "dropped": 0,
            "candidate_failures": 0,
            "comparison_failures": 0,
            "report_failures": 0,
            "worker_failures": 0,
            "last_failure_class": None,
            "last_candidate_failure_stage": None,
            "last_candidate_failure_class": None,
        }
        self.reports: list[str] = []

    def health(self, *, wait_for_idle: bool = False):
        if wait_for_idle:
            self.worker["completed"] = self.worker["submitted"]
            self.worker["queue_size"] = 0
        return {
            "worker": dict(self.worker),
            "idle": self.worker["queue_size"] == 0,
            "report_files": list(self.reports),
        }

    def accept(self, symbols: int = 1, *, dropped: bool = False):
        self.worker["submitted"] += 1
        self.worker["completed"] += 1
        self.worker["queue_size"] = 0
        start = len(self.reports)
        self.reports.extend(f"2026-08-10/report-{start + i}.json" for i in range(symbols))
        if dropped:
            self.worker["dropped"] += 1
        return {"row_count": 1}


def _guard(driver, tmp_path: Path, client: _FakeClient, *, calls: int = 2):
    stage_root = tmp_path / "s1"
    stage_root.mkdir()
    return driver.StageGuard(
        client=client,
        stage_root=stage_root,
        evidence_root=tmp_path,
        maximum_calls=calls,
        deadline=time.monotonic() + 30,
    )


def test_t078_driver_accounts_one_task_and_reports_per_logical_call(
    tmp_path: Path,
) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)
    result = guard.call(
        family="batch_history",
        read_path="get_many_cached_only",
        symbols=["600000.SS", "000001.SZ"],
        period="2y",
        operation=lambda: client.accept(2),
    )
    assert result["row_count"] == 1
    assert guard.accepted_tasks == 1
    assert guard.expected_reports == 2
    record = json.loads((tmp_path / "s1/request-ledger.jsonl").read_text())
    assert record["accepted"] is True
    assert record["expected_report_count"] == 2


def test_t078_driver_hard_aborts_on_first_worker_failure(tmp_path: Path) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)
    with pytest.raises(RuntimeError, match="hard SLO"):
        guard.call(
            family="single_history",
            read_path="get_historical_data",
            symbols=["600000.SS"],
            period="2y",
            operation=lambda: client.accept(1, dropped=True),
        )
    assert guard.count == 1


def test_t078_driver_hard_slo_names_changed_counter(tmp_path: Path) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)
    snapshot = client.health()
    snapshot["worker"]["candidate_failures"] = 2
    snapshot["worker"]["last_candidate_failure_stage"] = "metadata_fetch"
    snapshot["worker"]["last_candidate_failure_class"] = "TimeoutError"

    with pytest.raises(
        RuntimeError,
        match=(
            r"^shadow worker hard SLO failed: candidate_failures=0->2, "
            r"candidate_stage=metadata_fetch, candidate_class=TimeoutError$"
        ),
    ):
        guard._sample("focused-test", snapshot)


def test_t078_driver_hard_slo_reports_multiple_counters_deterministically(
    tmp_path: Path,
) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)
    snapshot = client.health()
    snapshot["worker"]["dropped"] = 1
    snapshot["worker"]["worker_failures"] = 3

    with pytest.raises(
        RuntimeError,
        match=(
            r"^shadow worker hard SLO failed: "
            r"dropped=0->1, worker_failures=0->3$"
        ),
    ):
        guard._sample("focused-test", snapshot)


def test_t078_driver_hard_slo_allows_unchanged_counters(tmp_path: Path) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)

    guard._sample("focused-test", client.health())


def test_t078_driver_rejects_out_of_order_or_drifted_predecessor(tmp_path: Path) -> None:
    driver = _driver_module()
    symbols = ["600000.SS", "000001.SZ"]
    with pytest.raises(RuntimeError, match="requires a passed S1"):
        driver._predecessor("S2", tmp_path, symbols)
    root = tmp_path / "s1"
    root.mkdir()
    (root / "stage-summary.json").write_text(
        json.dumps({"status": "failed", "seeded_symbols": symbols})
    )
    with pytest.raises(RuntimeError, match="did not pass"):
        driver._predecessor("S2", tmp_path, symbols)
    with pytest.raises(RuntimeError, match="requires a passed S2"):
        driver._predecessor("S3", tmp_path, symbols)


def test_t078_driver_rejects_allowlist_mismatch(monkeypatch) -> None:
    driver = _driver_module()
    monkeypatch.setenv("SS_QUANTSTACK_READ_MODE", "shadow")
    monkeypatch.setenv("SS_QUANTSTACK_SHADOW_ENABLED", "true")
    monkeypatch.setenv("SS_QUANTSTACK_SHADOW_SAMPLE_RATE", "1")
    monkeypatch.setenv("SS_QUANTSTACK_ALLOW_CANDIDATE", "false")
    monkeypatch.setenv("SS_RUNTIME_ENV", "isolated-shadow")
    monkeypatch.setenv("SS_QUANTSTACK_SHADOW_ALLOWLIST", "600000.SS")
    with pytest.raises(RuntimeError, match="not covered"):
        driver._assert_environment("S1", ["600000.SS", "000001.SZ"])


def test_t078_driver_schedules_all_five_traffic_families(monkeypatch) -> None:
    driver = _driver_module()
    client = object()
    monkeypatch.setattr(driver, "_control_operation", lambda *args: lambda: {})
    operations = driver._traffic_operations(client, ["600000.SS", "000001.SZ"])
    assert {operation[0] for operation in operations} == set(driver.FAMILY_LIMITS)
    assert [operation[0] for operation in operations[:5]] == list(driver.FAMILY_LIMITS)


def test_t078_driver_rejects_explicit_zero_calls_before_side_effects(
    monkeypatch, tmp_path: Path
) -> None:
    driver = _driver_module()
    corpus = tmp_path / "corpus.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(DRIVER),
            "--stage",
            "S1",
            "--corpus",
            str(corpus),
            "--evidence-root",
            str(tmp_path / "evidence"),
            "--calls",
            "0",
        ],
    )

    def unexpected_side_effect(*args, **kwargs):
        raise AssertionError("zero-call validation must run before corpus or runtime access")

    monkeypatch.setattr(driver, "_load_corpus", unexpected_side_effect)
    monkeypatch.setattr(driver, "T078HTTPClient", unexpected_side_effect)
    monkeypatch.setattr(driver, "StageGuard", unexpected_side_effect)

    with pytest.raises(ValueError, match="requested calls exceed the stage contract"):
        driver.main()

    assert not (tmp_path / "evidence").exists()


def test_t078_driver_does_not_chmod_existing_evidence_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    driver = _driver_module()
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir(mode=0o770)
    evidence_root.chmod(0o770)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(DRIVER),
            "--stage",
            "S1",
            "--corpus",
            str(tmp_path / "corpus.json"),
            "--evidence-root",
            str(evidence_root),
            "--calls",
            "1",
        ],
    )
    monkeypatch.setattr(driver, "_load_corpus", lambda *_: ["600000.SS"])
    monkeypatch.setattr(driver, "_assert_environment", lambda *_: None)
    monkeypatch.setattr(driver, "_predecessor", lambda *_: set())
    original_chmod = Path.chmod

    def reject_root_chmod(path: Path, mode: int, *args, **kwargs) -> None:
        if path == evidence_root:
            raise AssertionError("existing evidence root must not be chmod'ed")
        original_chmod(path, mode, *args, **kwargs)

    class StopBeforeHTTP(RuntimeError):
        pass

    def stop_before_http():
        assert (evidence_root / "s1").stat().st_mode & 0o777 == 0o700
        assert (evidence_root / "run.lock").stat().st_mode & 0o777 == 0o600
        raise StopBeforeHTTP

    monkeypatch.setattr(Path, "chmod", reject_root_chmod)
    monkeypatch.setattr(driver, "T078HTTPClient", stop_before_http)

    with pytest.raises(StopBeforeHTTP):
        driver.main()

    assert evidence_root.stat().st_mode & 0o777 == 0o770


def test_t078_driver_creates_private_evidence_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    driver = _driver_module()
    evidence_root = tmp_path / "evidence"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(DRIVER),
            "--stage",
            "S1",
            "--corpus",
            str(tmp_path / "corpus.json"),
            "--evidence-root",
            str(evidence_root),
            "--calls",
            "1",
        ],
    )
    monkeypatch.setattr(driver, "_load_corpus", lambda *_: ["600000.SS"])
    monkeypatch.setattr(driver, "_assert_environment", lambda *_: None)
    monkeypatch.setattr(driver, "_predecessor", lambda *_: set())

    class StopBeforeHTTP(RuntimeError):
        pass

    def stop_before_http():
        assert evidence_root.stat().st_mode & 0o777 == 0o700
        assert (evidence_root / "s1").stat().st_mode & 0o777 == 0o700
        assert (evidence_root / "run.lock").stat().st_mode & 0o777 == 0o600
        raise StopBeforeHTTP

    monkeypatch.setattr(driver, "T078HTTPClient", stop_before_http)

    with pytest.raises(StopBeforeHTTP):
        driver.main()

    assert evidence_root.stat().st_mode & 0o777 == 0o700


def test_t078_default_stage_plan_fits_global_family_budget(monkeypatch) -> None:
    driver = _driver_module()
    monkeypatch.setattr(driver, "_control_operation", lambda *args: lambda: ({}, "x"))
    operations = driver._traffic_operations(
        object(),
        [f"{index:06d}.SS" for index in range(50)],
    )

    assert sum(int(stage["default_calls"]) for stage in driver.STAGE_LIMITS.values()) == sum(
        driver.FAMILY_LIMITS.values()
    )
    assert int(driver.STAGE_LIMITS["S3"]["default_calls"]) == 244
    # S1 spends 16 seed calls, leaving 64 traffic calls. The coverage prefix
    # reaches every family before any filler can consume the 80-call ceiling.
    assert set(operation[0] for operation in operations[:64]) == set(driver.FAMILY_LIMITS)

    global_counts = {family: 0 for family in driver.FAMILY_LIMITS}
    seed_calls = {"S1": 16, "S2": 50, "S3": 50}
    stage_sizes = {"S1": 8, "S2": 50, "S3": 50}
    for stage in ("S1", "S2", "S3"):
        stage_counts = {family: 0 for family in driver.FAMILY_LIMITS}
        stage_counts["price_cache_methods"] = seed_calls[stage]
        global_counts["price_cache_methods"] += seed_calls[stage]
        stage_operations = driver._traffic_operations(
            object(),
            [f"{index:06d}.SS" for index in range(stage_sizes[stage])],
        )
        ceilings = driver._stage_family_ceilings(stage)
        index = 0
        target = int(driver.STAGE_LIMITS[stage]["default_calls"])
        while sum(stage_counts.values()) < target:
            family = stage_operations[index % len(stage_operations)][0]
            index += 1
            if global_counts[family] >= ceilings[family]:
                continue
            global_counts[family] += 1
            stage_counts[family] += 1
        if stage in {"S1", "S2"}:
            assert all(stage_counts.values())
    assert global_counts == driver.FAMILY_LIMITS


def test_t078_scanner_uses_dedicated_synchronous_scan_surface() -> None:
    driver = _driver_module()

    class RecordingClient:
        def __init__(self) -> None:
            self.calls = []

        def request(self, method, path, payload=None, *, control=False):
            self.calls.append((method, path, payload, control))
            return {}

    client = RecordingClient()
    operation = next(
        item for item in driver._traffic_operations(client, ["600000.SS"]) if item[0] == "scanner"
    )
    operation[4]()

    assert client.calls == [
        (
            "POST",
            "/__t078/control/scan",
            {
                "universe": "custom",
                "symbols": ["600000.SS"],
                "screeners": ["minervini"],
                "composite_method": "weighted_average",
            },
            True,
        )
    ]
    compat = COMPAT.read_text(encoding="utf-8")
    assert 'app.post("/__t078/control/scan"' in compat
    assert "get_scan_orchestrator().scan_stock_multi(" in compat


def test_t078_driver_rechecks_deadline_after_batch_pause(tmp_path: Path, monkeypatch) -> None:
    driver = _driver_module()
    client = _FakeClient()
    guard = _guard(driver, tmp_path, client)
    guard.global_count = driver.BATCH_SIZE
    guard.deadline = 10.0
    guard._last_started_epoch = 0.0
    clock = {"now": 0.0}
    invoked = {"value": False}

    monkeypatch.setattr(driver.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(driver.time, "time", lambda: 100.0)

    def expire_during_sleep(seconds: float) -> None:
        assert seconds == driver.BATCH_PAUSE_SECONDS
        clock["now"] = 11.0

    monkeypatch.setattr(driver.time, "sleep", expire_during_sleep)

    def operation():
        invoked["value"] = True
        return client.accept()

    with pytest.raises(RuntimeError, match="after batch pause"):
        guard.call(
            family="single_history",
            read_path="get_historical_data",
            symbols=["600000.SS"],
            period="2y",
            operation=operation,
        )
    assert invoked["value"] is False


def test_t078_driver_enforces_persistent_cross_stage_family_cap(
    tmp_path: Path,
) -> None:
    driver = _driver_module()
    ledger = tmp_path / "logical-calls.jsonl"
    records = [
        json.dumps(
            {
                "global_call": index,
                "family": "single_history",
                "started_epoch": float(index),
            }
        )
        for index in range(1, driver.FAMILY_LIMITS["single_history"] + 1)
    ]
    ledger.write_text("\n".join(records) + "\n", encoding="utf-8")
    stage_root = tmp_path / "s2"
    stage_root.mkdir()
    guard = driver.StageGuard(
        client=_FakeClient(),
        stage_root=stage_root,
        evidence_root=tmp_path,
        maximum_calls=1,
        deadline=time.monotonic() + 30,
    )

    assert guard.global_family_counts["single_history"] == 200
    with pytest.raises(RuntimeError, match="T-078-wide"):
        guard.call(
            family="single_history",
            read_path="get_historical_data",
            symbols=["600000.SS"],
            period="2y",
            operation=lambda: _FakeClient().accept(),
        )


def test_t078_s2_requires_exact_ordered_s1_seed_manifest(tmp_path: Path) -> None:
    driver = _driver_module()
    symbols = [f"{index:06d}.SS" for index in range(50)]
    root = tmp_path / "s1"
    root.mkdir()
    summary = {
        "status": "passed",
        "seeded_symbols": symbols[:7],
    }
    path = root / "stage-summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")

    with pytest.raises(RuntimeError, match="complete ordered 8-symbol"):
        driver._predecessor("S2", tmp_path, symbols)

    summary["seeded_symbols"] = [*symbols[:8], symbols[8]]
    path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="complete ordered 8-symbol"):
        driver._predecessor("S2", tmp_path, symbols)

    summary["seeded_symbols"] = list(reversed(symbols[:8]))
    path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="complete ordered 8-symbol"):
        driver._predecessor("S2", tmp_path, symbols)

    summary["seeded_symbols"] = symbols[:8]
    path.write_text(json.dumps(summary), encoding="utf-8")
    assert driver._predecessor("S2", tmp_path, symbols) == set(symbols[:8])
