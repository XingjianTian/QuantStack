import json
import os
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import quant_stack.data.daily.r8_artifacts as r8_artifacts_module
import scripts.run_daily_r8_local_gate as gate_module
from quant_stack.data.daily.r8_artifacts import (
    CandidateVerification,
    DenyExternalIO,
    ExternalIORefused,
    ImmutableConflictError,
    R8ArtifactError,
    build_candidate_package,
    canonical_json_bytes,
    sha256_bytes,
    verify_candidate_package,
)
from quant_stack.data.daily.r8_preflight import run_no_write_preflight
from quant_stack.data.daily.r8_serving import (
    CN_QFQ_CONSUMER,
    CN_RAW_CONSUMER,
    US_RAW_CONSUMER,
    US_TOTAL_RETURN_CONSUMER,
    CandidateRejectedError,
    ConsumerBlockedError,
    ConsumerContract,
    GenerationConflictError,
    IsolatedCandidateRoute,
    PublicationInterrupted,
    RouteDisabledError,
)
from quant_stack.data.daily.tolerance import (
    GateStatus,
    ProviderOutcome,
    QualityStatus,
    assess_daily_tolerance,
)

ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "daily_tolerance"
SCHEMA_ROOT = ROOT / "schemas" / "data"
SNAPSHOT = "us-equity-xnas-xnys-2026-08-27.v1"


def _assessment(name: str, *, market: str = "US", partial_exception: bool = False):
    payload = json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))
    outcomes = payload.get("outcomes")
    if outcomes is None:
        returned_ids = set(payload["returned_ids"])
        outcomes = {
            identity: "returned" if identity in returned_ids else payload["unresolved_outcome"]
            for identity in payload["universe_ids"]
        }
    return assess_daily_tolerance(
        market=market,
        session=date.fromisoformat(payload["session"]),
        snapshot_id=payload["snapshot_id"],
        universe_ids=payload["universe_ids"],
        outcomes=outcomes,
        no_bar_evidence_ids=payload.get("no_bar_evidence_ids", ()),
        partial_exception_approved=partial_exception,
    ), payload["universe_ids"]


def _rows(assessment) -> list[dict[str, object]]:
    return [
        {
            "instrument_id": instrument_id,
            "market": assessment.market,
            "session": assessment.session.isoformat(),
            "close": index + 100.0,
        }
        for index, instrument_id in enumerate(assessment.returned_ids)
    ]


def _build(
    root: Path,
    candidate_id: str,
    *,
    fixture: str = "usable_partial_98.json",
    market: str = "US",
    dataset: str = "us_equity_daily_raw",
    price_basis: str = "raw",
    partial_exception: bool = False,
):
    assessment, universe = _assessment(fixture, market=market, partial_exception=partial_exception)
    return build_candidate_package(
        isolation_root=root,
        candidate_id=candidate_id,
        assessment=assessment,
        universe_ids=universe,
        rows=_rows(assessment),
        dataset=dataset,
        price_basis=price_basis,
        source_ref=f"tests/fixtures/daily_tolerance/{fixture}",
    )


def _reseal_json_artifact(package, name: str, mutate) -> dict[str, object]:
    """Modify one test artifact and update its manifest record deterministically."""

    manifest = json.loads(package.manifest_path.read_text(encoding="utf-8"))
    record = manifest["artifacts"][name]
    artifact_path = package.candidate_root / record["path"]
    value = json.loads(artifact_path.read_text(encoding="utf-8"))
    mutate(value)
    payload = canonical_json_bytes(value)
    artifact_path.write_bytes(payload)
    record["sha256"] = sha256_bytes(payload)
    record["size_bytes"] = len(payload)
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))
    return manifest


def _reseal_rows(package, mutate) -> dict[str, object]:
    manifest = json.loads(package.manifest_path.read_text(encoding="utf-8"))
    record = manifest["artifacts"]["rows"]
    rows_path = package.candidate_root / record["path"]
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    mutate(rows)
    payload = b"".join(canonical_json_bytes(row) for row in rows)
    rows_path.write_bytes(payload)
    record["sha256"] = sha256_bytes(payload)
    record["size_bytes"] = len(payload)
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))
    return manifest


def test_r8_candidate_closure_hashes_and_immutable_replay(tmp_path: Path) -> None:
    package = _build(tmp_path, "us-raw-partial-r1")
    verification = verify_candidate_package(
        package.candidate_root,
        expected_isolation_root=tmp_path,
    )

    assert verification.passed is True
    assert "R_O_P_partition_closed" in verification.checks
    assert package.manifest["canonical_eligible"] is False
    schema_values = (
        ("daily_r8_candidate_manifest.schema.json", package.manifest),
        (
            "daily_r8_ledger.schema.json",
            json.loads(
                (package.candidate_root / "ledger" / "snapshot.json").read_text(
                    encoding="utf-8"
                )
            ),
        ),
        (
            "daily_r8_ledger_event.schema.json",
            json.loads(
                (
                    package.candidate_root / "ledger" / "events" / "segment-0001.jsonl"
                ).read_text(encoding="utf-8")
            ),
        ),
    )
    for schema_name, value in schema_values:
        schema = json.loads((SCHEMA_ROOT / schema_name).read_text(encoding="utf-8"))
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(value)
    first_manifest = package.manifest_path.read_bytes()

    replay = _build(tmp_path, "us-raw-partial-r1")
    assert replay.manifest_path.read_bytes() == first_manifest
    with pytest.raises(ImmutableConflictError):
        package.manifest_path.write_bytes(b"{}\n")
        _build(tmp_path, "us-raw-partial-r1")


@pytest.mark.parametrize("case", ["duplicate", "illegal_O", "wrong_reason", "bad_revision"])
def test_r8_ledger_schema_rejects_semantic_negatives(case: str) -> None:
    entry = {
        "instrument_id": "XNAS.TEST",
        "state": "O",
        "outcome": "no_bar",
        "reason": "explicit_no_bar_evidence",
    }
    value = {
        "schema_version": "quantstack.daily-r8-ledger.v1",
        "candidate_id": "schema-negative-r1",
        "revision": "1",
        "immutable": True,
        "entries": [entry],
    }
    if case == "duplicate":
        value["entries"].append(dict(entry))
    elif case == "illegal_O":
        entry["outcome"] = "provider_error"
    elif case == "wrong_reason":
        entry["reason"] = "unresolved_provider_or_missing_outcome"
    else:
        value["revision"] = "01"
    schema = json.loads(
        (SCHEMA_ROOT / "daily_r8_ledger.schema.json").read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)
    )
    assert errors, f"ledger schema accepted semantic negative: {case}"


def test_r8_verifier_rejects_tampered_artifact(tmp_path: Path) -> None:
    package = _build(tmp_path, "us-raw-partial-r1")
    coverage_path = package.candidate_root / "coverage.json"
    original = coverage_path.read_bytes()
    coverage_path.write_bytes(original + b" ")
    verification = verify_candidate_package(
        package.candidate_root,
        expected_isolation_root=tmp_path,
    )
    assert verification.passed is False
    assert "artifact_hash:coverage" in verification.failures


def test_provider_error_stays_pending_and_guardrail_candidate_is_blocked(tmp_path: Path) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    assessment = replace(
        assessment,
        unresolved_outcomes={
            identity: ProviderOutcome.PROVIDER_ERROR.value
            for identity in assessment.unresolved_ids
        },
    )
    package = build_candidate_package(
        isolation_root=tmp_path,
        candidate_id="us-provider-error-partial-r1",
        assessment=assessment,
        universe_ids=universe,
        rows=_rows(assessment),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="fixture:usable_partial_98",
    )
    coverage = json.loads((package.candidate_root / "coverage.json").read_text(encoding="utf-8"))
    assert coverage["pending_outcomes"]
    assert set(coverage["pending_outcomes"].values()) == {"provider_error"}

    guardrail_assessment = assess_daily_tolerance(
        market="US",
        session=date(2026, 9, 18),
        snapshot_id=SNAPSHOT,
        universe_ids=[f"XNAS.GUARD{index:03d}" for index in range(100)],
        outcomes={
            f"XNAS.GUARD{index:03d}": (
                ProviderOutcome.RETURNED if index < 95 else ProviderOutcome.PROVIDER_ERROR
            )
            for index in range(100)
        },
    )
    assert guardrail_assessment.quality_status is QualityStatus.BLOCKED
    blocked = build_candidate_package(
        isolation_root=tmp_path,
        candidate_id="us-guardrail-failure-r1",
        assessment=guardrail_assessment,
        universe_ids=[f"XNAS.GUARD{index:03d}" for index in range(100)],
        rows=_rows(guardrail_assessment),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="fixture:synthetic-95-percent-guardrail",
    )
    assert blocked.manifest["quality_status"] == "blocked"
    assert verify_candidate_package(blocked.candidate_root, expected_isolation_root=tmp_path).passed


def test_r8_route_is_default_off_version_pinned_and_rolls_back(tmp_path: Path) -> None:
    complete = _build(
        tmp_path,
        "us-raw-complete-r1",
        fixture="complete_with_no_bar.json",
    )
    partial = _build(tmp_path, "us-raw-partial-r1")
    route = IsolatedCandidateRoute(tmp_path)
    with pytest.raises(RouteDisabledError):
        route.read(US_RAW_CONSUMER)

    route.enable()
    first = route.publish(complete.candidate_root)
    served_complete = route.read(US_RAW_CONSUMER, expected_generation=first["generation"])
    assert served_complete.quality_status == "complete"
    with pytest.raises(GenerationConflictError):
        route.read(US_RAW_CONSUMER, expected_generation="stale-generation")

    second = route.publish(partial.candidate_root)
    served_partial = route.read(US_RAW_CONSUMER, expected_generation=second["generation"])
    assert served_partial.quality_status == "usable_partial"
    with pytest.raises(ConsumerBlockedError):
        route.read(US_TOTAL_RETURN_CONSUMER)

    rolled_back = route.rollback()
    assert rolled_back["candidate_id"] == "us-raw-complete-r1"
    assert route.read(US_RAW_CONSUMER).candidate_id == "us-raw-complete-r1"

    with pytest.raises(PublicationInterrupted):
        route.publish(partial.candidate_root, interrupt_before_pointer=True)
    assert route.current_state()["candidate_id"] == "us-raw-complete-r1"
    assert (
        route.recover_interrupted_publication()
        == "staged_generation_not_committed_pointer_preserved"
    )
    assert route.read(US_RAW_CONSUMER).candidate_id == "us-raw-complete-r1"


def test_cn_raw_is_readable_but_qfq_is_blocked(tmp_path: Path) -> None:
    cn_package = _build(
        tmp_path,
        "cn-raw-partial-r1",
        market="CN",
        dataset="cn_equity_daily_raw",
    )
    route = IsolatedCandidateRoute(tmp_path, route_name="r8-cn-local-v1")
    route.enable()
    route.publish(cn_package.candidate_root)
    assert route.read(CN_RAW_CONSUMER).candidate_id == "cn-raw-partial-r1"
    with pytest.raises(ConsumerBlockedError):
        route.read(CN_QFQ_CONSUMER)


def test_no_write_preflight_is_pure_and_schema_valid(tmp_path: Path) -> None:
    package = _build(tmp_path, "us-raw-partial-r1")
    before = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    result = run_no_write_preflight(
        isolation_root=tmp_path,
        candidate_root=package.candidate_root,
        route_enabled=False,
        consumer_matrix=(
            {"consumer": "us_raw", "status": "pass"},
            {"consumer": "us_total_return", "status": "blocked"},
        ),
    )
    after = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert result.status == "gate_review_ready"
    assert any(check.status == "not_verified" for check in result.checks)
    assert result.side_effects["provider_requests"] == 0
    assert before == after

    schema = json.loads(
        (SCHEMA_ROOT / "daily_r8_preflight.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(result.to_dict())


def test_no_write_preflight_blocks_route_or_observed_side_effects(tmp_path: Path) -> None:
    package = _build(tmp_path, "us-raw-partial-r1")
    result = run_no_write_preflight(
        isolation_root=tmp_path,
        candidate_root=package.candidate_root,
        route_enabled=True,
        side_effects={"provider_requests": 1},
    )
    assert result.status == "blocked"
    assert {check.name for check in result.failures} >= {
        "route_default_off",
        "side_effect_provider_requests",
    }


def test_external_io_adapter_refuses_provider_like_operations() -> None:
    adapter = DenyExternalIO()
    with pytest.raises(ExternalIORefused):
        adapter.request("provider:yfinance")
    assert adapter.attempts == ["provider:yfinance"]


def test_builder_rejects_market_path_escape_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    isolation = tmp_path / "isolation"
    outside = tmp_path / "outside"
    isolation.mkdir()
    outside.mkdir()
    assessment, universe = _assessment("usable_partial_98.json")
    assessment = replace(assessment, market=str(outside))
    rows = [{**row, "market": str(outside)} for row in _rows(assessment)]
    original_open = Path.open
    outside_writes = 0

    def monitored_open(path: Path, *args, **kwargs):
        nonlocal outside_writes
        mode = args[0] if args else kwargs.get("mode", "r")
        if path.resolve().is_relative_to(outside) and any(flag in mode for flag in "wax+"):
            outside_writes += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", monitored_open)

    with pytest.raises(R8ArtifactError, match="safe path component"):
        build_candidate_package(
            isolation_root=isolation,
            candidate_id="escape-r1",
            assessment=assessment,
            universe_ids=universe,
            rows=rows,
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:escape",
        )
    assert list(outside.rglob("*")) == []
    assert outside_writes == 0
    assert not (isolation / "candidate-packages").exists()


def test_builder_rejects_parent_symlink_escape_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    isolation = tmp_path / "isolation"
    outside = tmp_path / "outside"
    isolation.mkdir()
    outside.mkdir()
    (isolation / "candidate-packages").symlink_to(outside, target_is_directory=True)
    original_open = Path.open
    outside_writes = 0

    def monitored_open(path: Path, *args, **kwargs):
        nonlocal outside_writes
        mode = args[0] if args else kwargs.get("mode", "r")
        if path.resolve().is_relative_to(outside) and any(flag in mode for flag in "wax+"):
            outside_writes += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", monitored_open)

    with pytest.raises(R8ArtifactError, match="symlink"):
        _build(isolation, "parent-symlink-r1")
    assert list(outside.rglob("*")) == []
    assert outside_writes == 0


def test_builder_rejects_evidence_symlink_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    isolation = tmp_path / "isolation"
    outside = tmp_path / "outside"
    (isolation / "evidence").mkdir(parents=True)
    outside.mkdir()
    (isolation / "evidence" / "wp-r8-01").symlink_to(
        outside, target_is_directory=True
    )
    original_open = Path.open
    outside_writes = 0

    def monitored_open(path: Path, *args, **kwargs):
        nonlocal outside_writes
        mode = args[0] if args else kwargs.get("mode", "r")
        if path.resolve().is_relative_to(outside) and any(flag in mode for flag in "wax+"):
            outside_writes += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", monitored_open)

    with pytest.raises(R8ArtifactError, match="symlink"):
        _build(isolation, "evidence-symlink-r1")
    assert list(outside.rglob("*")) == []
    assert outside_writes == 0
    assert not (isolation / "candidate-packages").exists()


@pytest.mark.parametrize("malformed_path", ["ABSOLUTE", "../outside-coverage.json"])
def test_verifier_never_opens_rejected_artifact_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, malformed_path: str
) -> None:
    package = _build(tmp_path, "unsafe-artifact-r1")
    relative = "../outside-coverage.json"
    outside = (
        tmp_path / "outside-coverage.json"
        if malformed_path == "ABSOLUTE"
        else (package.candidate_root / relative).resolve()
    )
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_text('{"outside":true}\n', encoding="utf-8")
    manifest = json.loads(package.manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["coverage"]["path"] = (
        str(outside) if malformed_path == "ABSOLUTE" else relative
    )
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))
    original_open = Path.open
    observed = 0

    def monitored_open(path: Path, *args, **kwargs):
        nonlocal observed
        if path.resolve() == outside.resolve():
            observed += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", monitored_open)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "artifact_path:coverage" in verification.failures
    assert observed == 0


def test_verifier_rejects_manifest_symlink_without_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _build(tmp_path, "manifest-symlink-r1")
    manifest_bytes = package.manifest_path.read_bytes()
    outside = tmp_path / "outside-manifest.json"
    outside.write_bytes(manifest_bytes)
    package.manifest_path.unlink()
    package.manifest_path.symlink_to(outside)
    original_read_bytes = Path.read_bytes
    observed = 0

    def monitored_read_bytes(path: Path, *args, **kwargs):
        nonlocal observed
        if path == package.manifest_path:
            observed += 1
        return original_read_bytes(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", monitored_read_bytes)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert observed == 0


def test_verifier_rejects_candidate_root_symlink(tmp_path: Path) -> None:
    package = _build(tmp_path, "candidate-real-r1")
    alias = package.candidate_root.parent / "candidate-alias-r1"
    alias.symlink_to(package.candidate_root, target_is_directory=True)
    verification = verify_candidate_package(alias, expected_isolation_root=tmp_path)
    assert verification.passed is False


def test_builder_rejects_missing_row_identity_before_write(tmp_path: Path) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    rows = _rows(assessment)
    rows[0].pop("market")
    with pytest.raises(R8ArtifactError, match="market mismatch"):
        build_candidate_package(
            isolation_root=tmp_path,
            candidate_id="missing-row-market-r1",
            assessment=assessment,
            universe_ids=universe,
            rows=rows,
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:missing-market",
        )
    assert not (tmp_path / "candidate-packages").exists()


def test_unproven_no_bar_assessment_is_not_packaged(tmp_path: Path) -> None:
    ids = ["XNAS.OK", "XNAS.UNPROVEN"]
    assessment = assess_daily_tolerance(
        market="US",
        session=date(2026, 9, 18),
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes={ids[0]: "returned", ids[1]: "no_bar"},
    )
    assert assessment.explicit_omission_ids == ()
    assert assessment.unresolved_outcomes == {ids[1]: "no_bar"}
    with pytest.raises(R8ArtifactError, match="invalid pending outcome"):
        build_candidate_package(
            isolation_root=tmp_path,
            candidate_id="unproven-no-bar-r1",
            assessment=assessment,
            universe_ids=ids,
            rows=_rows(assessment),
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:unproven-no-bar",
        )
    assert not (tmp_path / "candidate-packages").exists()


def test_verifier_rejects_duplicate_ledger_id(tmp_path: Path) -> None:
    package = _build(tmp_path, "duplicate-ledger-r1")

    def duplicate(ledger):
        ledger["entries"].append(dict(ledger["entries"][0]))

    _reseal_json_artifact(package, "ledger_snapshot", duplicate)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "ledger_duplicate_id" in verification.failures


def test_verifier_rejects_O_with_provider_error(tmp_path: Path) -> None:
    package = _build(
        tmp_path,
        "illegal-omission-r1",
        fixture="complete_with_no_bar.json",
    )

    def invalidate(ledger):
        omission = next(entry for entry in ledger["entries"] if entry["state"] == "O")
        omission["outcome"] = "provider_error"

    _reseal_json_artifact(package, "ledger_snapshot", invalidate)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert any(
        failure.startswith("ledger_outcome_mismatch:")
        for failure in verification.failures
    )


def test_verifier_rejects_cross_artifact_partition_or_count_mismatch(
    tmp_path: Path,
) -> None:
    package = _build(tmp_path, "cross-count-r1")

    def invalidate(assessment):
        assessment["metrics"]["returned_count"] -= 1

    _reseal_json_artifact(package, "assessment", invalidate)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "assessment_metrics_mismatch" in verification.failures


@pytest.mark.parametrize("artifact,field", [("coverage", "pending"), ("ledger_snapshot", "state")])
def test_verifier_malformed_non_scalar_is_a_failure(
    tmp_path: Path, artifact: str, field: str
) -> None:
    package = _build(tmp_path, f"malformed-{artifact}-r1")

    def invalidate(value):
        if artifact == "coverage":
            pending_id = value["pending"][0]
            value["pending_outcomes"][pending_id] = []
        else:
            value["entries"][0][field] = {}

    _reseal_json_artifact(package, artifact, invalidate)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert verification.failures


def test_verifier_rejects_manifest_directory_identity_mismatch(tmp_path: Path) -> None:
    package = _build(tmp_path, "path-identity-r1")
    manifest = json.loads(package.manifest_path.read_text(encoding="utf-8"))
    manifest["market"] = "CN"
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "manifest_path_identity" in verification.failures


def test_verifier_rejects_unknown_freshness_state(tmp_path: Path) -> None:
    package = _build(tmp_path, "freshness-r1")
    _reseal_json_artifact(
        package,
        "assessment",
        lambda assessment: assessment.update(freshness_status="unknown"),
    )
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "assessment_freshness_status" in verification.failures


@pytest.mark.parametrize("field", ["candidate_id", "ledger_revision"])
def test_verifier_rejects_ledger_event_identity(
    tmp_path: Path, field: str
) -> None:
    package = _build(tmp_path, f"event-{field}-r1")

    def invalidate(event):
        event[field] = "wrong"

    _reseal_json_artifact(package, "ledger_events", invalidate)
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "ledger_event_identity" in verification.failures


def test_read_rejects_resealed_manifest_under_same_generation(tmp_path: Path) -> None:
    package = _build(tmp_path, "resealed-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name="resealed-route")
    route.enable()
    state = route.publish(package.candidate_root)
    _reseal_rows(package, lambda rows: rows[0].update(close=999.0))

    with pytest.raises(CandidateRejectedError, match="manifest digest"):
        route.read(US_RAW_CONSUMER, expected_generation=state["generation"])


@pytest.mark.parametrize(
    "policy_field,policy_value,error_text",
    [
        ("quality_status", "blocked", "verification failed"),
        ("gate_status", "blocked", "gate is not ready"),
        ("freshness_status", "stale", "freshness is not fresh"),
    ],
)
def test_read_rejects_blocked_candidate(
    tmp_path: Path, policy_field: str, policy_value: str, error_text: str
) -> None:
    package = _build(tmp_path, f"blocked-{policy_field}-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name=f"blocked-{policy_field}-route")
    route.enable()
    state = route.publish(package.candidate_root)

    assessment_updates = {policy_field: policy_value}
    manifest = _reseal_json_artifact(
        package,
        "assessment",
        lambda assessment: assessment.update(assessment_updates),
    )
    if policy_field in {"quality_status", "gate_status"}:
        manifest[policy_field] = policy_value
    manifest_payload = canonical_json_bytes(manifest)
    package.manifest_path.write_bytes(manifest_payload)
    descriptor_path = route.generations_root / f"{state['generation']}.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["manifest_sha256"] = sha256_bytes(manifest_payload)
    descriptor[policy_field] = policy_value
    descriptor_path.write_bytes(canonical_json_bytes(descriptor))

    with pytest.raises(CandidateRejectedError, match=error_text):
        route.read(US_RAW_CONSUMER)


def test_publish_same_generation_rejects_content_conflict(tmp_path: Path) -> None:
    package = _build(tmp_path, "generation-conflict-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name="conflict-route")
    route.enable()
    route.publish(package.candidate_root)
    _reseal_rows(package, lambda rows: rows[0].update(close=777.0))

    with pytest.raises(CandidateRejectedError, match="generation conflict"):
        route.publish(package.candidate_root)


def test_partial_rejected_for_strict_raw_consumer_same_price_basis(
    tmp_path: Path,
) -> None:
    package = _build(tmp_path, "strict-raw-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name="strict-raw-route")
    route.enable()
    route.publish(package.candidate_root)
    strict_raw = ConsumerContract(
        name="us_strict_raw",
        market="US",
        layer="raw",
        supports_partial=False,
        required_price_basis="raw",
    )
    with pytest.raises(ConsumerBlockedError, match="does not support"):
        route.read(strict_raw)


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("route_name", "wrong-route"),
        ("generation", "wrong-generation"),
        ("candidate_id", "wrong-candidate"),
        ("candidate_root", "candidate-packages/us/2026-09-18/wrong-candidate"),
    ],
)
def test_read_rejects_descriptor_identity_mismatch(
    tmp_path: Path, field: str, bad_value: str
) -> None:
    package = _build(tmp_path, f"descriptor-{field}-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name=f"descriptor-{field}-route")
    route.enable()
    state = route.publish(package.candidate_root)
    descriptor_path = route.generations_root / f"{state['generation']}.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor[field] = bad_value
    descriptor_path.write_bytes(canonical_json_bytes(descriptor))
    with pytest.raises(CandidateRejectedError):
        route.read(US_RAW_CONSUMER)


def test_route_and_generation_traversal_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(CandidateRejectedError, match="route_name"):
        IsolatedCandidateRoute(tmp_path, route_name="../escape")
    route = IsolatedCandidateRoute(tmp_path, route_name="safe-route")
    with pytest.raises(CandidateRejectedError, match="generation"):
        route._generation_descriptor("../escape")


@pytest.mark.parametrize(
    "trap_name",
    [
        "generation",
        "generation_tmp",
        "pending",
        "pending_tmp",
        "pointer",
        "pointer_tmp",
    ],
)
def test_publish_rejects_catalog_symlink_before_any_write(
    tmp_path: Path, trap_name: str
) -> None:
    package = _build(tmp_path, f"trap-{trap_name}-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name=f"trap-{trap_name}-route")
    route.enable()
    route.catalog_root.mkdir(parents=True)
    route.generations_root.mkdir()
    generation = f"{package.manifest['candidate_id']}.r{package.manifest['revision']}"
    generation_path = route.generations_root / f"{generation}.json"
    paths = {
        "generation": generation_path,
        "generation_tmp": generation_path.with_name(f".{generation_path.name}.tmp"),
        "pending": route.pending_path,
        "pending_tmp": route.pending_path.with_name(f".{route.pending_path.name}.tmp"),
        "pointer": route.pointer_path,
        "pointer_tmp": route.pointer_path.with_name(f".{route.pointer_path.name}.tmp"),
    }
    outside = tmp_path / f"outside-{trap_name}.json"
    outside.write_bytes(b"unchanged\n")
    paths[trap_name].symlink_to(outside)

    with pytest.raises(CandidateRejectedError, match="symlink"):
        route.publish(package.candidate_root)
    assert outside.read_bytes() == b"unchanged\n"
    assert route.pointer_path.is_symlink() or not route.pointer_path.exists()


def test_publish_rejects_serving_parent_symlink_before_any_write(tmp_path: Path) -> None:
    package = _build(tmp_path, "serving-parent-r1")
    outside = tmp_path / "outside-serving"
    outside.mkdir()
    (tmp_path / "serving-fixture").symlink_to(outside, target_is_directory=True)
    route = IsolatedCandidateRoute(tmp_path, route_name="serving-parent-route")
    route.enable()
    with pytest.raises(CandidateRejectedError, match="symlink"):
        route.publish(package.candidate_root)
    assert list(outside.rglob("*")) == []


def test_preflight_rejects_verification_from_another_candidate(tmp_path: Path) -> None:
    package_a = _build(tmp_path, "cached-a-r1")
    package_b = _build(tmp_path, "current-b-r1")
    cached = verify_candidate_package(
        package_a.candidate_root, expected_isolation_root=tmp_path
    )
    result = run_no_write_preflight(
        isolation_root=tmp_path,
        candidate_root=package_b.candidate_root,
        verification=cached,
    )
    assert result.status == "blocked"
    assert result.candidate_id == "current-b-r1"
    assert "cached_verification_matches_current_candidate" in {
        check.name for check in result.failures
    }


def test_preflight_rejects_missing_candidate_with_cached_success(tmp_path: Path) -> None:
    package = _build(tmp_path, "cached-present-r1")
    cached = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    missing = package.candidate_root.parent / "missing-r1"
    result = run_no_write_preflight(
        isolation_root=tmp_path,
        candidate_root=missing,
        verification=cached,
    )
    assert result.status == "blocked"
    assert result.candidate_id == "missing-r1"


def test_preflight_rechecks_artifacts_after_cached_verification(tmp_path: Path) -> None:
    package = _build(tmp_path, "cached-stale-r1")
    cached = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    rows_path = package.candidate_root / "data" / "rows.jsonl"
    rows_path.write_bytes(rows_path.read_bytes() + b" ")
    result = run_no_write_preflight(
        isolation_root=tmp_path,
        candidate_root=package.candidate_root,
        verification=cached,
    )
    assert result.status == "blocked"
    assert {check.name for check in result.failures} >= {
        "candidate_integrity",
        "cached_verification_matches_current_candidate",
    }


def test_independent_cn_us_routes_do_not_share_current_pointer(tmp_path: Path) -> None:
    us_package = _build(tmp_path, "us-independent-r1")
    cn_package = _build(
        tmp_path,
        "cn-independent-r1",
        market="CN",
        dataset="cn_equity_daily_raw",
    )
    us_route = IsolatedCandidateRoute(tmp_path, route_name="independent-us")
    cn_route = IsolatedCandidateRoute(tmp_path, route_name="independent-cn")
    us_route.enable()
    cn_route.enable()
    us_route.publish(us_package.candidate_root)
    us_before = us_route.current_state()
    cn_route.publish(cn_package.candidate_root)
    assert us_route.pointer_path != cn_route.pointer_path
    assert us_route.current_state() == us_before
    assert us_route.read(US_RAW_CONSUMER).candidate_id == "us-independent-r1"
    assert cn_route.read(CN_RAW_CONSUMER).candidate_id == "cn-independent-r1"


def test_local_gate_us_evidence_still_points_to_us_after_cn_run(tmp_path: Path) -> None:
    result = gate_module.run_local_gate(
        isolation_root=tmp_path,
        fixture_root=FIXTURE_ROOT,
    )
    evidence = json.loads(Path(result["serving_evidence_path"]).read_text(encoding="utf-8"))
    assert result["status"] == "gate_review_ready"
    assert evidence["us"]["current_after_cn_publication"]["candidate_id"].startswith("us-")
    assert all(evidence["required_checks"].values())


def test_local_gate_blocks_if_required_integrity_check_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_summary = gate_module.verification_summary
    call_count = 0

    def failing_once(verification: CandidateVerification):
        nonlocal call_count
        summary = original_summary(verification)
        call_count += 1
        if call_count == 1:
            summary["passed"] = False
            summary["failures"] = ["synthetic_required_integrity_failure"]
        return summary

    monkeypatch.setattr(gate_module, "verification_summary", failing_once)
    result = gate_module.run_local_gate(
        isolation_root=tmp_path,
        fixture_root=FIXTURE_ROOT,
    )
    assert result["status"] == "blocked"
    preflight = json.loads(Path(result["preflight_evidence_path"]).read_text(encoding="utf-8"))
    failed = {check["name"] for check in preflight["checks"] if check["status"] == "fail"}
    assert "required_all_candidate_integrity_verifications_passed" in failed


@pytest.mark.parametrize(
    "relative,is_parent",
    [
        ("evidence/wp-r8-03", True),
        ("evidence/wp-r8-03/preflight.json", False),
        ("evidence/wp-r8-02", True),
        ("evidence/wp-r8-02/serving.json", False),
        ("gate-review-packet", True),
        ("gate-review-packet/r8-local-gate.json", False),
    ],
)
def test_local_gate_rejects_all_evidence_symlinks_before_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
    is_parent: bool,
) -> None:
    isolation = tmp_path / "isolation"
    isolation.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = isolation / relative
    if is_parent:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.symlink_to(outside, target_is_directory=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        sentinel = outside / "sentinel.json"
        sentinel.write_bytes(b"unchanged\n")
        destination.symlink_to(sentinel)

    outside_root = outside.resolve()
    outside_accesses: list[Path] = []

    def record(path: Path | str) -> None:
        resolved = Path(path).resolve(strict=False)
        if resolved == outside_root or resolved.is_relative_to(outside_root):
            outside_accesses.append(resolved)

    original_open = Path.open
    original_read_bytes = Path.read_bytes
    original_write_bytes = Path.write_bytes
    original_os_open = os.open
    original_sha256_file = r8_artifacts_module.sha256_file

    def monitored_open(path: Path, *args, **kwargs):
        record(path)
        return original_open(path, *args, **kwargs)

    def monitored_read_bytes(path: Path, *args, **kwargs):
        record(path)
        return original_read_bytes(path, *args, **kwargs)

    def monitored_write_bytes(path: Path, *args, **kwargs):
        record(path)
        return original_write_bytes(path, *args, **kwargs)

    def monitored_os_open(path, *args, **kwargs):
        if isinstance(path, (str, os.PathLike)):
            record(path)
        return original_os_open(path, *args, **kwargs)

    def monitored_sha256_file(path: Path) -> str:
        record(path)
        return original_sha256_file(path)

    monkeypatch.setattr(Path, "open", monitored_open)
    monkeypatch.setattr(Path, "read_bytes", monitored_read_bytes)
    monkeypatch.setattr(Path, "write_bytes", monitored_write_bytes)
    monkeypatch.setattr(os, "open", monitored_os_open)
    monkeypatch.setattr(r8_artifacts_module, "sha256_file", monitored_sha256_file)

    with pytest.raises(R8ArtifactError, match="symlink"):
        gate_module.run_local_gate(isolation_root=isolation, fixture_root=FIXTURE_ROOT)

    assert outside_accesses == []
    assert not (isolation / "candidate-packages").exists()
    if not is_parent:
        assert original_read_bytes(sentinel) == b"unchanged\n"


def test_publish_rejects_candidate_ancestor_symlink_before_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _build(tmp_path, "ancestor-alias-r1")
    route = IsolatedCandidateRoute(tmp_path, route_name="ancestor-alias-route")
    route.enable()
    packages_root = package.candidate_root.parents[2]
    alias = packages_root / "alias-us"
    alias.symlink_to(package.candidate_root.parents[1], target_is_directory=True)
    aliased_candidate = alias / package.candidate_root.parent.name / package.candidate_root.name

    candidate_root = package.candidate_root.resolve()
    reads: list[Path] = []
    original_open = Path.open
    original_sha256_file = r8_artifacts_module.sha256_file

    def monitored_open(path: Path, *args, **kwargs):
        resolved = path.resolve(strict=False)
        if resolved == candidate_root or resolved.is_relative_to(candidate_root):
            reads.append(resolved)
        return original_open(path, *args, **kwargs)

    def monitored_sha256_file(path: Path) -> str:
        resolved = path.resolve(strict=False)
        if resolved == candidate_root or resolved.is_relative_to(candidate_root):
            reads.append(resolved)
        return original_sha256_file(path)

    monkeypatch.setattr(Path, "open", monitored_open)
    monkeypatch.setattr(r8_artifacts_module, "sha256_file", monitored_sha256_file)

    with pytest.raises(CandidateRejectedError, match="symlink"):
        route.publish(aliased_candidate)

    assert reads == []
    assert not route.pointer_path.exists()


def test_complete_with_pending_is_rejected_before_packaging_and_serving(
    tmp_path: Path,
) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    inconsistent = replace(assessment, quality_status=QualityStatus.COMPLETE)
    with pytest.raises(R8ArtifactError, match="assessment_complete_with_pending"):
        build_candidate_package(
            isolation_root=tmp_path / "builder-rejects",
            candidate_id="complete-pending-r1",
            assessment=inconsistent,
            universe_ids=universe,
            rows=_rows(inconsistent),
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:complete-pending",
        )
    assert not (tmp_path / "builder-rejects" / "candidate-packages").exists()

    package = _build(tmp_path, "resealed-complete-pending-r1")
    manifest = _reseal_json_artifact(
        package,
        "assessment",
        lambda value: value.update(quality_status="complete"),
    )
    manifest["quality_status"] = "complete"
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "assessment_complete_with_pending" in verification.failures

    route = IsolatedCandidateRoute(tmp_path, route_name="complete-pending-route")
    route.enable()
    with pytest.raises(CandidateRejectedError, match="candidate verification failed"):
        route.publish(package.candidate_root)
    assert not route.pointer_path.exists()


def test_ready_with_hard_failures_is_rejected(tmp_path: Path) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    inconsistent = replace(
        assessment,
        hard_failures=("critical_dependency_unresolved",),
    )
    with pytest.raises(R8ArtifactError, match="assessment_quality_hard_failures_mismatch"):
        build_candidate_package(
            isolation_root=tmp_path / "builder-rejects",
            candidate_id="ready-hard-failure-r1",
            assessment=inconsistent,
            universe_ids=universe,
            rows=_rows(inconsistent),
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:ready-hard-failure",
        )
    assert not (tmp_path / "builder-rejects" / "candidate-packages").exists()

    package = _build(tmp_path, "ready-hard-failure-r1")
    manifest = _reseal_json_artifact(
        package,
        "assessment",
        lambda value: value.update(hard_failures=["critical_dependency_unresolved"]),
    )
    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "assessment_quality_hard_failures_mismatch" in verification.failures
    assert "assessment_gate_hard_failures_mismatch" in verification.failures

    route = IsolatedCandidateRoute(tmp_path, route_name="ready-hard-failure-route")
    route.enable()
    with pytest.raises(CandidateRejectedError, match="candidate verification failed"):
        route.publish(package.candidate_root)
    assert not route.pointer_path.exists()
    assert manifest["quality_status"] == "usable_partial"


def test_blocked_quality_with_ready_gate_is_rejected_by_builder_and_verifier(
    tmp_path: Path,
) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    inconsistent = replace(assessment, quality_status=QualityStatus.BLOCKED)
    with pytest.raises(R8ArtifactError, match="assessment_blocked_gate_mismatch"):
        build_candidate_package(
            isolation_root=tmp_path / "builder-rejects",
            candidate_id="blocked-ready-r1",
            assessment=inconsistent,
            universe_ids=universe,
            rows=_rows(inconsistent),
            dataset="us_equity_daily_raw",
            price_basis="raw",
            source_ref="fixture:blocked-ready",
        )
    assert not (tmp_path / "builder-rejects" / "candidate-packages").exists()

    package = _build(tmp_path, "resealed-blocked-ready-r1")
    manifest = _reseal_json_artifact(
        package,
        "assessment",
        lambda value: value.update(quality_status="blocked"),
    )
    manifest["quality_status"] = "blocked"
    package.manifest_path.write_bytes(canonical_json_bytes(manifest))

    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is False
    assert "assessment_blocked_gate_mismatch" in verification.failures
    assert "assessment_ready_quality_not_consumable" in verification.failures

    assessment_schema = json.loads(
        (SCHEMA_ROOT / "daily_tolerance_assessment.schema.json").read_text(
            encoding="utf-8"
        )
    )
    manifest_schema = json.loads(
        (SCHEMA_ROOT / "daily_r8_candidate_manifest.schema.json").read_text(
            encoding="utf-8"
        )
    )
    validator = Draft202012Validator(assessment_schema, format_checker=FormatChecker())
    assessment_value = json.loads(
        (package.candidate_root / package.manifest["artifacts"]["assessment"]["path"])
        .read_text(encoding="utf-8")
    )
    assert list(validator.iter_errors(assessment_value))
    assert list(
        Draft202012Validator(manifest_schema, format_checker=FormatChecker()).iter_errors(
            manifest
        )
    )


def test_blocked_quality_with_blocked_gate_remains_a_valid_diagnostic_package(
    tmp_path: Path,
) -> None:
    assessment, universe = _assessment("usable_partial_98.json")
    diagnostic = replace(
        assessment,
        quality_status=QualityStatus.BLOCKED,
        gate_status=GateStatus.BLOCKED,
    )
    package = build_candidate_package(
        isolation_root=tmp_path,
        candidate_id="blocked-blocked-diagnostic-r1",
        assessment=diagnostic,
        universe_ids=universe,
        rows=_rows(diagnostic),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="fixture:blocked-diagnostic",
    )

    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )
    assert verification.passed is True
    assert package.manifest["quality_status"] == "blocked"
    assert package.manifest["gate_status"] == "blocked"
    assessment_record = package.manifest["artifacts"]["assessment"]
    assessment_value = json.loads(
        (package.candidate_root / assessment_record["path"]).read_text(encoding="utf-8")
    )
    assert assessment_value["hard_failures"] == []
    assessment_schema = json.loads(
        (SCHEMA_ROOT / "daily_tolerance_assessment.schema.json").read_text(
            encoding="utf-8"
        )
    )
    manifest_schema = json.loads(
        (SCHEMA_ROOT / "daily_r8_candidate_manifest.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator(
        assessment_schema, format_checker=FormatChecker()
    ).validate(assessment_value)
    Draft202012Validator(
        manifest_schema, format_checker=FormatChecker()
    ).validate(package.manifest)


@pytest.mark.parametrize(
    "field,replacement",
    [("repair_status", {}), ("hard_failures", {}), ("alerts", {})],
)
def test_verifier_rejects_malformed_required_assessment_fields(
    tmp_path: Path, field: str, replacement: object
) -> None:
    package = _build(tmp_path, f"malformed-assessment-{field}-r1")
    _reseal_json_artifact(
        package,
        "assessment",
        lambda value: value.update({field: replacement}),
    )

    verification = verify_candidate_package(
        package.candidate_root, expected_isolation_root=tmp_path
    )

    assert verification.passed is False
    assert any(field in failure for failure in verification.failures)
    assert not any("verification_exception" in failure for failure in verification.failures)


def test_assessment_schema_rejects_policy_contradictions(tmp_path: Path) -> None:
    assessment, _ = _assessment("usable_partial_98.json")
    schema = json.loads(
        (SCHEMA_ROOT / "daily_tolerance_assessment.schema.json").read_text(encoding="utf-8")
    )
    pending_but_complete = assessment.to_dict()
    pending_but_complete["quality_status"] = "complete"
    ready_with_failures = assessment.to_dict()
    ready_with_failures["hard_failures"] = ["critical_dependency_unresolved"]

    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    assert list(validator.iter_errors(pending_but_complete))
    assert list(validator.iter_errors(ready_with_failures))

    package = _build(tmp_path, "schema-complete-pending-r1")
    manifest_schema = json.loads(
        (SCHEMA_ROOT / "daily_r8_candidate_manifest.schema.json").read_text(
            encoding="utf-8"
        )
    )
    inconsistent_manifest = {**package.manifest, "quality_status": "complete"}
    manifest_validator = Draft202012Validator(
        manifest_schema,
        format_checker=FormatChecker(),
    )
    assert list(manifest_validator.iter_errors(inconsistent_manifest))
