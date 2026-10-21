"""Isolated R8 candidate packages and immutable evidence.

The helpers in this module are deliberately local-only.  They package an already
assessed result from a fixture or replay input; they do not know how to contact a
provider, open NAS paths, acquire a production lock, or publish a catalog pointer.
"""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from math import isclose, isfinite
from pathlib import Path

from .tolerance import (
    DailyToleranceAssessment,
    DailyToleranceMetrics,
    FreshnessStatus,
    GateStatus,
    QualityStatus,
    RepairStatus,
)

R8_MANIFEST_SCHEMA = "quantstack.daily-r8-candidate-manifest.v1"
R8_COVERAGE_SCHEMA = "quantstack.daily-r8-coverage.v1"
R8_LEDGER_SCHEMA = "quantstack.daily-r8-ledger.v1"
R8_LEDGER_EVENT_SCHEMA = "quantstack.daily-r8-ledger-event.v1"
R8_EVIDENCE_SCHEMA = "quantstack.daily-r8-evidence-index.v1"

_REQUIRED_ARTIFACT_PATHS = {
    "rows": "data/rows.jsonl",
    "assessment": "assessment.json",
    "coverage": "coverage.json",
    "ledger_snapshot": "ledger/snapshot.json",
    "ledger_events": "ledger/events/segment-0001.jsonl",
}
_PENDING_OUTCOMES = frozenset({"provider_empty", "provider_error", "missing_outcome"})


class R8ArtifactError(ValueError):
    """Base error for malformed or unsafe isolated R8 artifacts."""


class ImmutableConflictError(R8ArtifactError):
    """Raised when a sealed path is asked to contain different bytes."""


class ExternalIORefused(R8ArtifactError):
    """Raised by the refusal-style adapter used by offline R8 tests."""


class DenyExternalIO:
    """Auditable refusal adapter; every attempted external operation fails closed."""

    def __init__(self) -> None:
        self.attempts: list[str] = []

    def request(self, operation: str) -> None:
        self.attempts.append(str(operation))
        raise ExternalIORefused(f"R8 offline package refused external operation: {operation}")


@dataclass(frozen=True)
class CandidatePackage:
    """Paths and manifest returned after an isolated candidate is sealed."""

    isolation_root: Path
    candidate_root: Path
    manifest_path: Path
    evidence_index_path: Path
    manifest: dict[str, object]


@dataclass(frozen=True)
class CandidateVerification:
    """Read-only verification result for one candidate package."""

    passed: bool
    candidate_id: str
    manifest_sha256: str | None
    checks: tuple[str, ...]
    failures: tuple[str, ...]
    manifest: dict[str, object] | None = None


def canonical_json_bytes(value: object) -> bytes:
    """Serialize JSON deterministically so replay produces identical bytes."""

    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assessment_document_failures(value: object) -> list[str]:
    """Check every required serialized assessment field before semantic use."""

    if not isinstance(value, dict):
        return ["assessment_object"]
    failures: list[str] = []
    string_fields = ("market", "session", "snapshot_id", "freshness_status")
    for field in string_fields:
        if not isinstance(value.get(field), str) or not value.get(field):
            failures.append(f"assessment_required_type:{field}")
    if isinstance(value.get("session"), str):
        try:
            if date.fromisoformat(value["session"]).isoformat() != value["session"]:
                failures.append("assessment_required_format:session")
        except ValueError:
            failures.append("assessment_required_format:session")
    enums = {
        "schema_version": {"quantstack.daily-tolerance-assessment.v1"},
        "quality_status": {item.value for item in QualityStatus},
        "repair_status": {item.value for item in RepairStatus},
        "gate_status": {item.value for item in GateStatus},
    }
    for field, allowed in enums.items():
        if not isinstance(value.get(field), str) or value[field] not in allowed:
            failures.append(f"assessment_required_enum:{field}")
    for field in ("returned_ids", "explicit_omission_ids", "unresolved_ids"):
        items = value.get(field)
        if (
            not isinstance(items, list)
            or any(not isinstance(item, str) or not item for item in items)
            or len(items) != len(set(items))
        ):
            failures.append(f"assessment_required_type:{field}")
    for field in ("alerts", "hard_failures"):
        items = value.get(field)
        if (
            not isinstance(items, list)
            or any(not isinstance(item, str) for item in items)
            or len(items) != len(set(items))
        ):
            failures.append(f"assessment_required_type:{field}")
    outcomes = value.get("unresolved_outcomes")
    allowed_outcomes = {"provider_empty", "provider_error", "missing_outcome", "no_bar"}
    if (
        not isinstance(outcomes, dict)
        or any(not isinstance(key, str) for key in outcomes)
        or any(
            not isinstance(item, str) or item not in allowed_outcomes
            for item in outcomes.values()
        )
    ):
        failures.append("assessment_required_type:unresolved_outcomes")

    metrics = value.get("metrics")
    if not isinstance(metrics, dict):
        failures.append("assessment_required_type:metrics")
    else:
        metric_strings = ("schema_version", "market", "session", "snapshot_id")
        for field in metric_strings:
            if not isinstance(metrics.get(field), str) or not metrics.get(field):
                failures.append(f"assessment_required_type:metrics.{field}")
        metric_integers = (
            "requested_count",
            "returned_count",
            "no_bar_count",
            "unresolved_count",
            "critical_unresolved_count",
            "max_consecutive_unresolved",
        )
        for field in metric_integers:
            item = metrics.get(field)
            if not isinstance(item, int) or isinstance(item, bool) or item < 0:
                failures.append(f"assessment_required_type:metrics.{field}")
        metric_numbers = (
            "bar_coverage",
            "exact_closure",
            "ledger_closure",
            "max_unresolved_concentration",
            "missing_weight_ratio",
        )
        for field in metric_numbers:
            item = metrics.get(field)
            if (
                not isinstance(item, (int, float))
                or isinstance(item, bool)
                or not isfinite(float(item))
            ):
                failures.append(f"assessment_required_type:metrics.{field}")
        if metrics.get("schema_version") != "quantstack.daily-tolerance-metrics.v1":
            failures.append("assessment_required_enum:metrics.schema_version")
    return failures


def _assessment_model_failures(assessment: DailyToleranceAssessment) -> list[str]:
    """Reject malformed in-memory dataclass values before package creation begins."""

    failures: list[str] = []
    if not isinstance(assessment.market, str) or not assessment.market:
        failures.append("market")
    if not isinstance(assessment.session, date) or isinstance(assessment.session, datetime):
        failures.append("session")
    if not isinstance(assessment.snapshot_id, str) or not assessment.snapshot_id:
        failures.append("snapshot_id")
    for field, expected in (
        ("quality_status", QualityStatus),
        ("repair_status", RepairStatus),
        ("gate_status", GateStatus),
        ("freshness_status", FreshnessStatus),
    ):
        if not isinstance(getattr(assessment, field), expected):
            failures.append(field)
    for field in (
        "returned_ids",
        "explicit_omission_ids",
        "unresolved_ids",
        "alerts",
        "hard_failures",
    ):
        items = getattr(assessment, field)
        if not isinstance(items, tuple) or any(not isinstance(item, str) for item in items):
            failures.append(field)
    if not isinstance(assessment.unresolved_outcomes, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in assessment.unresolved_outcomes.items()
    ):
        failures.append("unresolved_outcomes")
    if not isinstance(assessment.metrics, DailyToleranceMetrics):
        failures.append("metrics")
    return failures


def _validate_assessment_policy(
    assessment: Mapping[str, object], *, pending_ids: set[str]
) -> list[str]:
    failures: list[str] = []
    quality = assessment.get("quality_status")
    gate = assessment.get("gate_status")
    hard_failures = assessment.get("hard_failures")
    if quality == "complete" and pending_ids:
        failures.append("assessment_complete_with_pending")
    if quality == "blocked" and gate != "blocked":
        failures.append("assessment_blocked_gate_mismatch")
    if gate == "ready":
        if quality not in {"complete", "usable_partial", "usable_partial_exception"}:
            failures.append("assessment_ready_quality_not_consumable")
        if isinstance(hard_failures, list) and hard_failures:
            failures.append("assessment_ready_with_hard_failures")
    if isinstance(hard_failures, list) and isinstance(quality, str) and isinstance(gate, str):
        if hard_failures and quality != "blocked":
            failures.append("assessment_quality_hard_failures_mismatch")
        if hard_failures and gate != "blocked":
            failures.append("assessment_gate_hard_failures_mismatch")
    return failures


def _serialize_assessment(assessment: DailyToleranceAssessment) -> dict[str, object]:
    model_failures = _assessment_model_failures(assessment)
    if model_failures:
        raise R8ArtifactError(
            "R8 candidate assessment has malformed fields: " + ",".join(model_failures)
        )
    try:
        document = assessment.to_dict()
    except (AttributeError, TypeError, ValueError) as error:
        raise R8ArtifactError("R8 candidate assessment serialization failed") from error
    document_failures = _assessment_document_failures(document)
    if document_failures:
        raise R8ArtifactError(
            "R8 candidate assessment has malformed fields: " + ",".join(document_failures)
        )
    return document


def resolve_isolation_root(root: Path) -> Path:
    """Return an absolute root while rejecting known production/data roots."""

    candidate = Path(root).expanduser()
    if not candidate.is_absolute():
        raise R8ArtifactError("R8 isolation root must be an absolute path")
    resolved = candidate.resolve()
    forbidden = (Path("/app/data"), Path("/Volumes"), Path("/mnt"), Path("/media"))
    if resolved == Path("/") or any(
        resolved == item or resolved.is_relative_to(item) for item in forbidden
    ):
        raise R8ArtifactError(f"R8 isolation root is outside the local-only boundary: {resolved}")
    return resolved


def _safe_component(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."}:
        raise R8ArtifactError(f"{field} must be a non-empty safe path component")
    component = Path(value)
    if component.is_absolute() or len(component.parts) != 1 or "/" in value or "\\" in value:
        raise R8ArtifactError(f"{field} must be a single safe path component")
    return value


def _validate_path_target(
    path: Path,
    *,
    boundary: Path,
    kind: str,
    must_exist: bool,
) -> Path:
    """Validate a local path without following a symlink component.

    This is deliberately called for every path before a build performs its first write,
    and for every artifact before verification performs its first artifact read.
    """

    target = Path(path)
    fixed_boundary = Path(boundary)
    if not target.is_absolute() or not fixed_boundary.is_absolute():
        raise R8ArtifactError(f"R8 {kind} path and boundary must be absolute: {target}")
    try:
        relative = target.relative_to(fixed_boundary)
    except ValueError as error:
        raise R8ArtifactError(f"R8 {kind} path escapes fixed boundary: {target}") from error
    if relative.is_absolute() or ".." in relative.parts:
        raise R8ArtifactError(f"R8 {kind} path is not a safe relative target: {target}")

    cursor = fixed_boundary
    parts = relative.parts
    for index, part in enumerate(parts):
        cursor = cursor / part
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            if must_exist:
                raise R8ArtifactError(
                    f"R8 {kind} path does not exist: {cursor}"
                ) from None
            continue
        if stat.S_ISLNK(mode):
            raise R8ArtifactError(f"R8 {kind} path contains a symlink: {cursor}")
        is_leaf = index == len(parts) - 1
        if not is_leaf and not stat.S_ISDIR(mode):
            raise R8ArtifactError(f"R8 {kind} parent is not a directory: {cursor}")
        if is_leaf and kind == "directory" and not stat.S_ISDIR(mode):
            raise R8ArtifactError(f"R8 directory target is not a directory: {cursor}")
        if is_leaf and kind == "file" and not stat.S_ISREG(mode):
            raise R8ArtifactError(f"R8 file target is not a regular file: {cursor}")

    try:
        resolved = target.resolve(strict=must_exist)
        resolved.relative_to(fixed_boundary)
    except (OSError, RuntimeError, ValueError) as error:
        raise R8ArtifactError(
            f"R8 {kind} path resolves outside fixed boundary: {target}"
        ) from error
    return target


def _write_immutable(path: Path, payload: bytes, *, boundary: Path) -> None:
    _validate_path_target(path, boundary=boundary, kind="file", must_exist=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    _validate_path_target(path.parent, boundary=boundary, kind="directory", must_exist=True)
    _validate_path_target(path, boundary=boundary, kind="file", must_exist=False)
    if path.exists():
        existing = path.read_bytes()
        if existing != payload:
            raise ImmutableConflictError(f"immutable path conflict: {path}")
        return
    path.write_bytes(payload)


def validate_isolated_path(
    path: Path,
    *,
    boundary: Path,
    kind: str = "file",
    must_exist: bool = False,
) -> Path:
    """Expose the fixed-root path check to other local R8 evidence writers."""

    return _validate_path_target(path, boundary=boundary, kind=kind, must_exist=must_exist)


def write_immutable_artifact(path: Path, payload: bytes, *, boundary: Path) -> None:
    """Write immutable bytes beneath a fixed local root after path validation."""

    _write_immutable(path, payload, boundary=boundary)


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as error:
        raise R8ArtifactError(f"path escapes isolation root: {path}") from error


def _candidate_relative_path(path: Path, candidate_root: Path) -> str:
    try:
        relative = path.resolve().relative_to(candidate_root.resolve())
    except ValueError as error:
        raise R8ArtifactError(f"candidate path escapes package root: {path}") from error
    if relative.is_absolute() or ".." in relative.parts:
        raise R8ArtifactError(f"unsafe candidate relative path: {relative}")
    return relative.as_posix()


def _artifact_record(path: Path, candidate_root: Path) -> dict[str, object]:
    return {
        "path": _candidate_relative_path(path, candidate_root),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _identity_sequence(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise R8ArtifactError(f"{field} must be an identity sequence")
    identities = tuple(value)
    if any(not isinstance(identity, str) or not identity for identity in identities):
        raise R8ArtifactError(f"{field} contains an empty or non-string identity")
    if len(identities) != len(set(identities)):
        raise R8ArtifactError(f"{field} contains duplicate identities")
    return identities


def _expected_partition_metrics(
    *,
    universe: set[str],
    returned: set[str],
    omissions: set[str],
    pending: set[str],
) -> dict[str, int | float]:
    requested_count = len(universe)
    returned_count = len(returned)
    no_bar_count = len(omissions)
    unresolved_count = len(pending)
    return {
        "requested_count": requested_count,
        "returned_count": returned_count,
        "no_bar_count": no_bar_count,
        "unresolved_count": unresolved_count,
        "bar_coverage": returned_count / requested_count if requested_count else 0.0,
        "exact_closure": (
            (returned_count + no_bar_count) / requested_count if requested_count else 0.0
        ),
        "ledger_closure": (
            (returned_count + no_bar_count + unresolved_count) / requested_count
            if requested_count
            else 0.0
        ),
    }


def _metrics_match_partition(
    metrics: object,
    *,
    identity: Mapping[str, object],
    expected: Mapping[str, int | float],
) -> bool:
    if not isinstance(metrics, dict):
        return False
    if metrics.get("schema_version") != "quantstack.daily-tolerance-metrics.v1":
        return False
    if any(metrics.get(field) != identity[field] for field in ("market", "session", "snapshot_id")):
        return False
    for field, expected_value in expected.items():
        observed = metrics.get(field)
        if isinstance(expected_value, float):
            if (
                not isinstance(observed, (int, float))
                or isinstance(observed, bool)
                or not isclose(float(observed), expected_value, rel_tol=0.0, abs_tol=1e-12)
            ):
                return False
        elif observed != expected_value:
            return False
    return True


def _identity_partition(
    *,
    universe_ids: Iterable[str],
    assessment: DailyToleranceAssessment,
) -> dict[str, object]:
    raw_universe = tuple(str(identity) for identity in universe_ids)
    if any(not identity for identity in raw_universe):
        raise R8ArtifactError("R8 candidate universe contains an empty ID")
    universe = tuple(sorted(set(raw_universe)))
    if len(raw_universe) != len(universe):
        raise R8ArtifactError("R8 candidate universe contains duplicate IDs")
    universe_set = set(universe)
    returned_values = _identity_sequence(assessment.returned_ids, field="assessment.returned_ids")
    omission_values = _identity_sequence(
        assessment.explicit_omission_ids,
        field="assessment.explicit_omission_ids",
    )
    unresolved_values = _identity_sequence(
        assessment.unresolved_ids,
        field="assessment.unresolved_ids",
    )
    returned = set(returned_values)
    omissions = set(omission_values)
    unresolved = set(unresolved_values)
    if returned & omissions or returned & unresolved or omissions & unresolved:
        raise R8ArtifactError("R8 candidate R/O/P sets are not mutually exclusive")
    if returned | omissions | unresolved != universe_set:
        raise R8ArtifactError("R8 candidate R/O/P sets do not close the fixed universe")
    pending_outcomes = dict(assessment.unresolved_outcomes)
    if set(pending_outcomes) != unresolved:
        raise R8ArtifactError("R8 candidate pending outcomes do not match P identities")
    if any(outcome not in _PENDING_OUTCOMES for outcome in pending_outcomes.values()):
        raise R8ArtifactError("R8 candidate contains an invalid pending outcome")
    if omissions and "no_bar_without_explicit_evidence" in assessment.hard_failures:
        raise R8ArtifactError("R8 candidate O identity lacks explicit no_bar evidence")
    expected_metrics = _expected_partition_metrics(
        universe=universe_set,
        returned=returned,
        omissions=omissions,
        pending=unresolved,
    )
    assessment_identity = {
        "market": assessment.market,
        "session": assessment.session.isoformat(),
        "snapshot_id": assessment.snapshot_id,
    }
    if not _metrics_match_partition(
        assessment.metrics.to_dict(),
        identity=assessment_identity,
        expected=expected_metrics,
    ):
        raise R8ArtifactError("R8 candidate assessment metrics do not match R/O/P partition")
    return {
        "schema_version": R8_COVERAGE_SCHEMA,
        "universe": list(universe),
        "returned": sorted(returned),
        "explicit_no_bar": sorted(omissions),
        "pending": sorted(unresolved),
        "pending_outcomes": dict(sorted(pending_outcomes.items())),
        "counts": {
            "universe": len(universe),
            "returned": len(returned),
            "explicit_no_bar": len(omissions),
            "pending": len(unresolved),
        },
    }


def _ledger_snapshot(
    *,
    candidate_id: str,
    assessment: DailyToleranceAssessment,
    coverage: Mapping[str, object],
) -> dict[str, object]:
    returned = set(assessment.returned_ids)
    omissions = set(assessment.explicit_omission_ids)
    pending = set(assessment.unresolved_ids)
    entries = []
    for identity in coverage["universe"]:  # type: ignore[index]
        instrument_id = str(identity)
        if instrument_id in returned:
            state, outcome, reason = "R", "returned", "bar_present"
        elif instrument_id in omissions:
            state, outcome, reason = "O", "no_bar", "explicit_no_bar_evidence"
        elif instrument_id in pending:
            state = "P"
            outcome = assessment.unresolved_outcomes.get(instrument_id, "missing_outcome")
            reason = "unresolved_provider_or_missing_outcome"
        else:
            raise R8ArtifactError(f"ledger identity is outside R/O/P sets: {instrument_id}")
        entries.append(
            {
                "instrument_id": instrument_id,
                "state": state,
                "outcome": outcome,
                "reason": reason,
            }
        )
    return {
        "schema_version": R8_LEDGER_SCHEMA,
        "candidate_id": candidate_id,
        "revision": "1",
        "immutable": True,
        "entries": entries,
    }


def build_candidate_package(
    *,
    isolation_root: Path,
    candidate_id: str,
    assessment: DailyToleranceAssessment,
    universe_ids: Iterable[str],
    rows: Iterable[Mapping[str, object]],
    dataset: str,
    price_basis: str,
    source_ref: str,
    source_kind: str = "synthetic_fixture",
) -> CandidatePackage:
    """Build one deterministic, sealed candidate without external I/O.

    The caller supplies already-normalized rows and the pure R8 assessment.  The function
    never reads the source reference; it is recorded only as provenance text.
    """

    root = resolve_isolation_root(isolation_root)
    candidate_id = _safe_component(candidate_id, field="candidate_id")
    market_component = _safe_component(assessment.market, field="assessment.market").lower()
    if (
        not isinstance(assessment.session, date)
        or isinstance(assessment.session, datetime)
        or not isinstance(assessment.snapshot_id, str)
        or not assessment.snapshot_id
    ):
        raise R8ArtifactError("assessment market, session, and snapshot identity are required")
    if any(
        not isinstance(value, str) or not value
        for value in (dataset, price_basis, source_ref, source_kind)
    ):
        raise R8ArtifactError("candidate provenance and data identity fields are required")
    assessment_document = _serialize_assessment(assessment)
    coverage = _identity_partition(universe_ids=universe_ids, assessment=assessment)
    policy_failures = _validate_assessment_policy(
        assessment_document,
        pending_ids=set(assessment_document["unresolved_ids"]),  # type: ignore[arg-type]
    )
    if policy_failures:
        raise R8ArtifactError(
            "R8 candidate assessment violates policy: " + ",".join(policy_failures)
        )
    returned_ids = set(assessment.returned_ids)
    normalized_rows: list[dict[str, object]] = []
    row_ids: set[str] = set()
    for raw_row in rows:
        row = {str(key): value for key, value in raw_row.items()}
        instrument_id = row.get("instrument_id")
        if not isinstance(instrument_id, str) or not instrument_id:
            raise R8ArtifactError("candidate row is missing instrument_id")
        if instrument_id in row_ids:
            raise R8ArtifactError(f"candidate contains duplicate row ID: {instrument_id}")
        if instrument_id not in returned_ids:
            raise R8ArtifactError(f"candidate row is not in R: {instrument_id}")
        if "market" not in row or row.get("market") != assessment.market:
            raise R8ArtifactError(f"candidate row market mismatch: {instrument_id}")
        if "session" not in row or row.get("session") != assessment.session.isoformat():
            raise R8ArtifactError(f"candidate row session mismatch: {instrument_id}")
        row_ids.add(instrument_id)
        normalized_rows.append(row)
    if row_ids != returned_ids:
        raise R8ArtifactError("candidate rows do not exactly cover returned IDs")
    normalized_rows.sort(key=lambda row: str(row["instrument_id"]))

    candidate_root = (
        root
        / "candidate-packages"
        / market_component
        / assessment.session.isoformat()
        / candidate_id
    )
    data_path = candidate_root / "data" / "rows.jsonl"
    assessment_path = candidate_root / "assessment.json"
    coverage_path = candidate_root / "coverage.json"
    ledger_path = candidate_root / "ledger" / "snapshot.json"
    event_path = candidate_root / "ledger" / "events" / "segment-0001.jsonl"
    manifest_path = candidate_root / "manifest.json"
    evidence_path = root / "evidence" / "wp-r8-01" / f"{candidate_id}.json"

    # Validate the complete output graph before the first mkdir/open/write.  This catches
    # parent symlinks (including candidate-packages), sealed-file symlinks, and escapes.
    _validate_path_target(candidate_root, boundary=root, kind="directory", must_exist=False)
    for output_path in (
        data_path,
        assessment_path,
        coverage_path,
        ledger_path,
        event_path,
        manifest_path,
        evidence_path,
    ):
        _validate_path_target(output_path, boundary=root, kind="file", must_exist=False)

    rows_bytes = b"".join(canonical_json_bytes(row) for row in normalized_rows)
    _write_immutable(data_path, rows_bytes, boundary=root)
    assessment_bytes = canonical_json_bytes(assessment_document)
    _write_immutable(assessment_path, assessment_bytes, boundary=root)
    coverage_bytes = canonical_json_bytes(coverage)
    _write_immutable(coverage_path, coverage_bytes, boundary=root)
    ledger = _ledger_snapshot(candidate_id=candidate_id, assessment=assessment, coverage=coverage)
    ledger_bytes = canonical_json_bytes(ledger)
    _write_immutable(ledger_path, ledger_bytes, boundary=root)
    event = {
        "schema_version": R8_LEDGER_EVENT_SCHEMA,
        "event_id": f"{candidate_id}:created:r1",
        "event_type": "candidate_created",
        "candidate_id": candidate_id,
        "ledger_revision": "1",
        "source_kind": source_kind,
    }
    _write_immutable(event_path, canonical_json_bytes(event), boundary=root)

    manifest = {
        "schema_version": R8_MANIFEST_SCHEMA,
        "candidate_id": candidate_id,
        "revision": "1",
        "market": assessment.market,
        "session": assessment.session.isoformat(),
        "snapshot_id": assessment.snapshot_id,
        "dataset": dataset,
        "price_basis": price_basis,
        "quality_status": assessment.quality_status.value,
        "gate_status": assessment.gate_status.value,
        "immutable": True,
        "canonical_eligible": False,
        "production_installable": False,
        "source": {"kind": source_kind, "ref": source_ref},
        "artifacts": {
            "rows": _artifact_record(data_path, candidate_root),
            "assessment": _artifact_record(assessment_path, candidate_root),
            "coverage": _artifact_record(coverage_path, candidate_root),
            "ledger_snapshot": _artifact_record(ledger_path, candidate_root),
            "ledger_events": _artifact_record(event_path, candidate_root),
        },
        "coverage": coverage["counts"],
    }
    _write_immutable(manifest_path, canonical_json_bytes(manifest), boundary=root)

    evidence = {
        "schema_version": R8_EVIDENCE_SCHEMA,
        "candidate_id": candidate_id,
        "manifest_path": _relative_path(manifest_path, root),
        "manifest_sha256": sha256_file(manifest_path),
        "artifact_hashes": {
            name: record["sha256"]
            for name, record in manifest["artifacts"].items()  # type: ignore[union-attr]
        },
        "production_route_changed": False,
        "provider_requests": 0,
        "nas_writes": 0,
    }
    _write_immutable(evidence_path, canonical_json_bytes(evidence), boundary=root)
    return CandidatePackage(
        isolation_root=root,
        candidate_root=candidate_root,
        manifest_path=manifest_path,
        evidence_index_path=evidence_path,
        manifest=manifest,
    )


def verify_candidate_package(
    candidate_root: Path,
    *,
    expected_isolation_root: Path | None = None,
) -> CandidateVerification:
    """Verify package references and R/O/P closure without writing anything."""

    candidate = Path(candidate_root).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate_id = candidate.name
    checks: list[str] = []
    failures: list[str] = []
    manifest: dict[str, object] | None = None
    manifest_sha: str | None = None
    path_identity: tuple[str, str, str] | None = None

    def result() -> CandidateVerification:
        return CandidateVerification(
            passed=not failures,
            candidate_id=candidate_id,
            manifest_sha256=manifest_sha,
            checks=tuple(checks),
            failures=tuple(failures),
            manifest=manifest,
        )

    try:
        if expected_isolation_root is not None:
            root = resolve_isolation_root(expected_isolation_root)
            packages_root = root / "candidate-packages"
            _validate_path_target(
                packages_root,
                boundary=root,
                kind="directory",
                must_exist=True,
            )
            relative_candidate = candidate.relative_to(packages_root)
            if ".." in relative_candidate.parts or len(relative_candidate.parts) != 3:
                raise R8ArtifactError("candidate is not at market/session/id package depth")
            path_identity = (
                relative_candidate.parts[0],
                relative_candidate.parts[1],
                relative_candidate.parts[2],
            )
            _validate_path_target(
                candidate,
                boundary=packages_root,
                kind="directory",
                must_exist=True,
            )
            checks.append("candidate_under_fixed_isolation_root")
        else:
            _validate_path_target(
                candidate,
                boundary=candidate.parent,
                kind="directory",
                must_exist=True,
            )
        manifest_path = candidate / "manifest.json"
        _validate_path_target(
            manifest_path,
            boundary=candidate,
            kind="file",
            must_exist=True,
        )
        manifest_bytes = manifest_path.read_bytes()
        manifest_sha = sha256_bytes(manifest_bytes)
        raw_manifest = json.loads(manifest_bytes)
        if not isinstance(raw_manifest, dict):
            raise R8ArtifactError("candidate manifest must be an object")
        manifest = raw_manifest
        if manifest.get("schema_version") != R8_MANIFEST_SCHEMA:
            failures.append("manifest_schema_version")
        if manifest.get("candidate_id") != candidate_id:
            failures.append("manifest_candidate_id")
        if manifest.get("immutable") is not True:
            failures.append("manifest_not_immutable")
        if manifest.get("canonical_eligible") is not False:
            failures.append("candidate_marked_canonical")
        if manifest.get("production_installable") is not False:
            failures.append("candidate_marked_production_installable")
        revision = manifest.get("revision")
        if (
            not isinstance(revision, str)
            or not revision.isdecimal()
            or revision.startswith("0")
        ):
            failures.append("manifest_revision")
        for field in ("market", "session", "snapshot_id", "dataset", "price_basis"):
            if not isinstance(manifest.get(field), str) or not manifest.get(field):
                failures.append(f"manifest_identity:{field}")
        market = manifest.get("market")
        session = manifest.get("session")
        try:
            market_component = _safe_component(market, field="manifest.market").lower()
            if not isinstance(session, str) or date.fromisoformat(session).isoformat() != session:
                raise ValueError("session is not a canonical ISO date")
        except (R8ArtifactError, ValueError):
            failures.append("manifest_path_identity")
        else:
            if path_identity is not None and path_identity != (
                market_component,
                session,
                candidate_id,
            ):
                failures.append("manifest_path_identity")
        if manifest.get("quality_status") not in {
            "complete",
            "usable_partial",
            "usable_partial_exception",
            "blocked",
        }:
            failures.append("manifest_quality_status")
        if manifest.get("gate_status") not in {"ready", "blocked"}:
            failures.append("manifest_gate_status")
        source = manifest.get("source")
        if (
            not isinstance(source, dict)
            or set(source) != {"kind", "ref"}
            or any(not isinstance(source.get(key), str) or not source.get(key) for key in source)
        ):
            failures.append("manifest_source")
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict):
            raise R8ArtifactError("manifest artifacts must be an object")
        if set(artifacts) != set(_REQUIRED_ARTIFACT_PATHS):
            failures.append("manifest_artifact_set")
            return result()

        # Validate every artifact record and path before opening any artifact.  A rejected
        # path therefore cannot be observed later through a role-specific lookup.
        artifact_paths: dict[str, Path] = {}
        for name, expected_relative in _REQUIRED_ARTIFACT_PATHS.items():
            raw_record = artifacts[name]
            if not isinstance(raw_record, dict):
                failures.append(f"artifact_record:{name}")
                continue
            relative = raw_record.get("path")
            relative_path = Path(relative) if isinstance(relative, str) else None
            if (
                relative_path is None
                or not relative
                or relative != expected_relative
                or relative_path.is_absolute()
                or ".." in relative_path.parts
            ):
                failures.append(f"artifact_path:{name}")
                continue
            if (
                not isinstance(raw_record.get("size_bytes"), int)
                or isinstance(raw_record.get("size_bytes"), bool)
                or raw_record["size_bytes"] < 0
                or not isinstance(raw_record.get("sha256"), str)
                or len(raw_record["sha256"]) != 64
                or any(character not in "0123456789abcdef" for character in raw_record["sha256"])
            ):
                failures.append(f"artifact_record:{name}")
                continue
            artifact_path = candidate / relative_path
            try:
                _validate_path_target(
                    artifact_path,
                    boundary=candidate,
                    kind="file",
                    must_exist=True,
                )
            except R8ArtifactError:
                failures.append(f"artifact_missing:{name}")
                continue
            artifact_paths[name] = artifact_path
        if failures:
            return result()
        if len(set(artifact_paths.values())) != len(artifact_paths):
            failures.append("artifact_paths_not_unique")
            return result()

        # Integrity is checked before parsing any artifact content.
        for name, artifact_path in artifact_paths.items():
            raw_record = artifacts[name]
            assert isinstance(raw_record, dict)
            if artifact_path.stat().st_size != raw_record.get("size_bytes"):
                failures.append(f"artifact_size:{name}")
            if sha256_file(artifact_path) != raw_record.get("sha256"):
                failures.append(f"artifact_hash:{name}")
        if failures:
            return result()
        checks.append("all_manifest_artifacts_match_size_and_sha256")

        coverage = json.loads(artifact_paths["coverage"].read_text(encoding="utf-8"))
        assessment = json.loads(artifact_paths["assessment"].read_text(encoding="utf-8"))
        if not isinstance(coverage, dict) or not isinstance(assessment, dict):
            raise R8ArtifactError("coverage and assessment artifacts must be objects")
        failures.extend(_assessment_document_failures(assessment))
        if failures:
            return result()
        if assessment.get("schema_version") != "quantstack.daily-tolerance-assessment.v1":
            failures.append("assessment_schema_version")
        for field in ("market", "session", "snapshot_id", "quality_status", "gate_status"):
            if assessment.get(field) != manifest.get(field):
                failures.append(f"assessment_{field}_mismatch")
        if assessment.get("freshness_status") not in {"fresh", "stale"}:
            failures.append("assessment_freshness_status")
        if coverage.get("schema_version") != R8_COVERAGE_SCHEMA:
            failures.append("coverage_schema_version")

        coverage_sequences: dict[str, tuple[str, ...]] = {}
        for field in ("universe", "returned", "explicit_no_bar", "pending"):
            try:
                coverage_sequences[field] = _identity_sequence(
                    coverage.get(field),
                    field=f"coverage.{field}",
                )
            except R8ArtifactError:
                failures.append(f"coverage_identity_sequence:{field}")
        if failures:
            return result()
        universe = set(coverage_sequences["universe"])
        returned = set(coverage_sequences["returned"])
        omissions = set(coverage_sequences["explicit_no_bar"])
        pending = set(coverage_sequences["pending"])
        if returned & omissions or returned & pending or omissions & pending:
            failures.append("coverage_sets_not_disjoint")
        if returned | omissions | pending != universe:
            failures.append("coverage_sets_not_closed")
        else:
            checks.append("R_O_P_partition_closed")

        expected_metrics = _expected_partition_metrics(
            universe=universe,
            returned=returned,
            omissions=omissions,
            pending=pending,
        )
        expected_counts = {
            "universe": expected_metrics["requested_count"],
            "returned": expected_metrics["returned_count"],
            "explicit_no_bar": expected_metrics["no_bar_count"],
            "pending": expected_metrics["unresolved_count"],
        }
        if coverage.get("counts") != expected_counts:
            failures.append("coverage_counts_not_recomputed")
        if manifest.get("coverage") != expected_counts:
            failures.append("manifest_coverage_counts")

        pending_outcomes = coverage.get("pending_outcomes")
        if (
            not isinstance(pending_outcomes, dict)
            or any(not isinstance(key, str) for key in pending_outcomes)
            or set(pending_outcomes) != pending
            or any(
                not isinstance(outcome, str) or outcome not in _PENDING_OUTCOMES
                for outcome in pending_outcomes.values()
            )
        ):
            failures.append("coverage_pending_outcomes")

        assessment_sets: dict[str, set[str]] = {}
        assessment_fields = {
            "returned_ids": returned,
            "explicit_omission_ids": omissions,
            "unresolved_ids": pending,
        }
        for field, expected_set in assessment_fields.items():
            try:
                observed = set(
                    _identity_sequence(assessment.get(field), field=f"assessment.{field}")
                )
            except R8ArtifactError:
                failures.append(f"assessment_identity_sequence:{field}")
                continue
            assessment_sets[field] = observed
            if observed != expected_set:
                failures.append(f"assessment_partition_mismatch:{field}")
        assessment_outcomes = assessment.get("unresolved_outcomes")
        if not isinstance(assessment_outcomes, dict) or assessment_outcomes != pending_outcomes:
            failures.append("assessment_pending_outcomes")
        hard_failures = assessment.get("hard_failures")
        failures.extend(_validate_assessment_policy(assessment, pending_ids=pending))
        if omissions and (
            not isinstance(hard_failures, list)
            or "no_bar_without_explicit_evidence" in hard_failures
        ):
            failures.append("unproven_explicit_no_bar")
        identity = {
            "market": manifest.get("market"),
            "session": manifest.get("session"),
            "snapshot_id": manifest.get("snapshot_id"),
        }
        if not _metrics_match_partition(
            assessment.get("metrics"),
            identity=identity,
            expected=expected_metrics,
        ):
            failures.append("assessment_metrics_mismatch")

        row_ids: list[str] = []
        for line in artifact_paths["rows"].read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if not isinstance(row, dict):
                raise R8ArtifactError("candidate row must be an object")
            instrument_id = row.get("instrument_id")
            if not isinstance(instrument_id, str) or not instrument_id:
                failures.append("row_missing_instrument_id")
                continue
            row_ids.append(instrument_id)
            if row.get("session") != manifest.get("session"):
                failures.append("row_session_mismatch")
            if row.get("market") != manifest.get("market"):
                failures.append("row_market_mismatch")
        if len(row_ids) != len(set(row_ids)):
            failures.append("row_duplicate_id")
        if set(row_ids) != returned:
            failures.append("rows_do_not_match_returned_set")
        elif len(row_ids) != expected_metrics["returned_count"]:
            failures.append("row_count_mismatch")
        else:
            checks.append("rows_match_returned_set")

        ledger = json.loads(artifact_paths["ledger_snapshot"].read_text(encoding="utf-8"))
        if not isinstance(ledger, dict):
            raise R8ArtifactError("ledger snapshot must be an object")
        if ledger.get("schema_version") != R8_LEDGER_SCHEMA:
            failures.append("ledger_schema_version")
        if ledger.get("candidate_id") != candidate_id:
            failures.append("ledger_candidate_id")
        if ledger.get("revision") != revision:
            failures.append("ledger_revision")
        entries = ledger.get("entries")
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise R8ArtifactError("ledger entries must be object records")
        ledger_id_sequence = [entry.get("instrument_id") for entry in entries]
        if any(not isinstance(identity, str) or not identity for identity in ledger_id_sequence):
            failures.append("ledger_invalid_identity")
            ledger_ids: set[str] = set()
        else:
            ledger_ids = set(ledger_id_sequence)  # type: ignore[arg-type]
        if len(ledger_id_sequence) != len(ledger_ids):
            failures.append("ledger_duplicate_id")
        if ledger_ids != universe:
            failures.append("ledger_does_not_cover_universe")
        elif len(entries) != len(universe):
            failures.append("ledger_entry_count")
        else:
            checks.append("ledger_covers_universe")
        expected_states = {
            **{identity: "R" for identity in returned},
            **{identity: "O" for identity in omissions},
            **{identity: "P" for identity in pending},
        }
        for entry in entries:
            instrument_id = entry.get("instrument_id")
            if not isinstance(instrument_id, str) or not instrument_id:
                continue
            expected_state = expected_states.get(instrument_id)
            if entry.get("state") != expected_state:
                failures.append("ledger_state_mismatch")
                continue
            if expected_state == "R":
                expected_outcome, expected_reason = "returned", "bar_present"
            elif expected_state == "O":
                expected_outcome, expected_reason = "no_bar", "explicit_no_bar_evidence"
            else:
                expected_outcome = (
                    pending_outcomes.get(instrument_id)
                    if isinstance(pending_outcomes, dict)
                    else None
                )
                expected_reason = "unresolved_provider_or_missing_outcome"
            if entry.get("outcome") != expected_outcome:
                failures.append(f"ledger_outcome_mismatch:{instrument_id}")
            if entry.get("reason") != expected_reason:
                failures.append(f"ledger_reason_mismatch:{instrument_id}")
        if ledger.get("immutable") is not True:
            failures.append("ledger_not_immutable")

        events = [
            json.loads(line)
            for line in artifact_paths["ledger_events"].read_text(encoding="utf-8").splitlines()
        ]
        event_ids = [event.get("event_id") for event in events if isinstance(event, dict)]
        source_kind = source.get("kind") if isinstance(source, dict) else None
        expected_event_id = f"{candidate_id}:created:r{revision}"
        if (
            len(events) != 1
            or any(not isinstance(event, dict) for event in events)
            or len(event_ids) != len(set(event_ids))
            or any(
                event.get("schema_version") != R8_LEDGER_EVENT_SCHEMA
                or event.get("event_id") != expected_event_id
                or event.get("event_type") != "candidate_created"
                or event.get("candidate_id") != candidate_id
                or event.get("ledger_revision") != revision
                or event.get("ledger_revision") != ledger.get("revision")
                or event.get("source_kind") != source_kind
                for event in events
                if isinstance(event, dict)
            )
        ):
            failures.append("ledger_event_identity")
        else:
            checks.append("ledger_event_identity_valid")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        failures.append(f"verification_exception:{type(error).__name__}:{error}")

    return result()
