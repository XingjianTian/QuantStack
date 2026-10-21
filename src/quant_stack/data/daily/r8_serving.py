"""Default-off versioned serving and rollback for isolated R8 candidates.

This is a local integration harness, not a production publisher.  It persists only below the
caller-provided isolation root and refuses a candidate that cannot be verified first.
"""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .r8_artifacts import (
    CandidateVerification,
    R8ArtifactError,
    canonical_json_bytes,
    resolve_isolation_root,
    verify_candidate_package,
)

R8_ROUTE_SCHEMA = "quantstack.daily-r8-route-state.v1"
R8_GENERATION_SCHEMA = "quantstack.daily-r8-generation.v1"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SERVABLE_QUALITY = frozenset({"complete", "usable_partial", "usable_partial_exception"})


class R8ServingError(R8ArtifactError):
    """Base error for the isolated candidate route."""


class RouteDisabledError(R8ServingError):
    """Raised when a caller has not explicitly enabled the local route."""


class CandidateRejectedError(R8ServingError):
    """Raised when candidate verification or quality policy rejects a candidate."""


class ConsumerBlockedError(R8ServingError):
    """Raised when a downstream consumer cannot use the candidate contract."""


class GenerationConflictError(R8ServingError):
    """Raised when a reader asks for a generation other than the pinned one."""


class PublicationInterrupted(R8ServingError):
    """Test-only interruption after isolated staging and before pointer commit."""


@dataclass(frozen=True)
class ConsumerContract:
    name: str
    market: str
    layer: str
    supports_partial: bool
    required_price_basis: str


CN_RAW_CONSUMER = ConsumerContract(
    name="cn_raw",
    market="CN",
    layer="raw",
    supports_partial=True,
    required_price_basis="raw",
)
CN_QFQ_CONSUMER = ConsumerContract(
    name="cn_qfq",
    market="CN",
    layer="qfq",
    supports_partial=False,
    required_price_basis="qfq",
)
US_RAW_CONSUMER = ConsumerContract(
    name="us_raw",
    market="US",
    layer="raw",
    supports_partial=True,
    required_price_basis="raw",
)
US_TOTAL_RETURN_CONSUMER = ConsumerContract(
    name="us_total_return",
    market="US",
    layer="total_return",
    supports_partial=False,
    required_price_basis="total_return",
)


@dataclass(frozen=True)
class ServedCandidate:
    generation: str
    candidate_id: str
    quality_status: str
    rows: tuple[dict[str, object], ...]


def _safe_component(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise CandidateRejectedError(f"{label} must be a string")
    component = value
    if (
        not _SAFE_COMPONENT.fullmatch(component)
        or component in {".", ".."}
        or ".." in component
    ):
        raise CandidateRejectedError(f"unsafe {label}: {component!r}")
    return component


def _assert_safe_path(
    path: Path,
    *,
    boundary: Path,
    final_kind: str = "file",
    allow_missing: bool = True,
) -> None:
    """Reject traversal and every existing symlink before a local Catalog access."""

    boundary = boundary.absolute()
    path = path.absolute()
    try:
        path.relative_to(boundary)
    except ValueError as error:
        raise CandidateRejectedError(
            f"isolated Catalog path escaped route boundary: {path}"
        ) from error

    # Walk from the filesystem root, rather than from the route boundary, so an
    # existing serving-fixture/routes/route component cannot redirect the access.
    current = Path(path.anchor)
    components = path.parts[1:]
    for index, component in enumerate(components):
        current = current / component
        is_final = index == len(components) - 1
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if not allow_missing and is_final:
                raise CandidateRejectedError(
                    f"isolated Catalog path does not exist: {current}"
                ) from None
            continue
        if stat.S_ISLNK(metadata.st_mode):
            raise CandidateRejectedError(f"isolated Catalog path must not be a symlink: {current}")
        if is_final:
            expected = (
                stat.S_ISDIR(metadata.st_mode)
                if final_kind == "directory"
                else stat.S_ISREG(metadata.st_mode)
            )
            if not expected:
                raise CandidateRejectedError(
                    f"isolated Catalog {final_kind} has the wrong type: {current}"
                )
        elif not stat.S_ISDIR(metadata.st_mode):
            raise CandidateRejectedError(f"isolated Catalog parent is not a directory: {current}")


def _prepare_parent(path: Path, *, boundary: Path) -> None:
    _assert_safe_path(path.parent, boundary=boundary, final_kind="directory")
    path.parent.mkdir(parents=True, exist_ok=True)
    _assert_safe_path(path.parent, boundary=boundary, final_kind="directory", allow_missing=False)


def _atomic_json_write(path: Path, value: object, *, boundary: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    # Validate the final path, its temporary sibling, and every existing parent before mkdir/write.
    _assert_safe_path(path, boundary=boundary)
    _assert_safe_path(temporary, boundary=boundary)
    if temporary.exists():
        raise CandidateRejectedError(f"isolated Catalog temporary path already exists: {temporary}")
    _prepare_parent(path, boundary=boundary)
    _assert_safe_path(path, boundary=boundary)
    _assert_safe_path(temporary, boundary=boundary)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        _assert_safe_path(path, boundary=boundary)
        _assert_safe_path(temporary, boundary=boundary, allow_missing=False)
        os.replace(temporary, path)
    finally:
        if temporary.exists() and not temporary.is_symlink():
            temporary.unlink()


def _immutable_json_write(path: Path, value: object, *, boundary: Path) -> None:
    payload = canonical_json_bytes(value)
    temporary = path.with_name(f".{path.name}.tmp")
    _assert_safe_path(path, boundary=boundary)
    _assert_safe_path(temporary, boundary=boundary)
    if path.exists():
        if path.read_bytes() != payload:
            raise CandidateRejectedError(f"isolated generation conflict: {path}")
        return
    if temporary.exists():
        raise CandidateRejectedError(
            f"isolated generation temporary path already exists: {temporary}"
        )
    _prepare_parent(path, boundary=boundary)
    _assert_safe_path(path, boundary=boundary)
    _assert_safe_path(temporary, boundary=boundary)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _assert_safe_path(path, boundary=boundary)
        _assert_safe_path(temporary, boundary=boundary, allow_missing=False)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            _assert_safe_path(path, boundary=boundary, allow_missing=False)
            if path.read_bytes() != payload:
                raise CandidateRejectedError(
                    f"isolated generation conflict: {path}"
                ) from None
    finally:
        if temporary.exists() and not temporary.is_symlink():
            temporary.unlink()


class IsolatedCandidateRoute:
    """A small local Catalog/pointer copy with explicit enable, pin, and rollback."""

    def __init__(self, isolation_root: Path, *, route_name: str = "r8-candidate-v1") -> None:
        self.isolation_root = resolve_isolation_root(isolation_root)
        self.route_name = _safe_component(route_name, label="route_name")
        self.serving_root = self.isolation_root / "serving-fixture"
        self.route_root = self.serving_root / "routes" / self.route_name
        self.catalog_root = self.route_root / "catalog"
        self.pointer_path = self.catalog_root / "current.json"
        self.pending_path = self.catalog_root / "pending-publication.json"
        self.generations_root = self.catalog_root / "generations"
        self.enabled = False

    def enable(self) -> None:
        self.enabled = True

    def disable(self) -> None:
        self.enabled = False

    def _require_enabled(self) -> None:
        if not self.enabled:
            raise RouteDisabledError(f"R8 route is disabled: {self.route_name}")

    def current_state(self) -> dict[str, object] | None:
        _assert_safe_path(self.pointer_path, boundary=self.route_root)
        pointer_temporary = self.pointer_path.with_name(f".{self.pointer_path.name}.tmp")
        _assert_safe_path(pointer_temporary, boundary=self.route_root)
        if pointer_temporary.exists():
            raise CandidateRejectedError("isolated current pointer temporary path exists")
        if not self.pointer_path.exists():
            return None
        value = json.loads(self.pointer_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise CandidateRejectedError("isolated current pointer is not an object")
        if value.get("schema_version") != R8_ROUTE_SCHEMA:
            raise CandidateRejectedError("isolated current pointer schema is invalid")
        if value.get("route_name") != self.route_name:
            raise CandidateRejectedError("isolated current pointer route identity mismatch")
        if value.get("committed") is not True:
            raise CandidateRejectedError("isolated current pointer is not committed")
        _safe_component(value.get("generation"), label="pointer generation")
        _safe_component(value.get("candidate_id"), label="pointer candidate_id")
        return value

    def _generation_descriptor(self, generation: str) -> dict[str, object]:
        generation = _safe_component(generation, label="generation")
        path = self.generations_root / f"{generation}.json"
        _assert_safe_path(path, boundary=self.route_root, allow_missing=False)
        temporary = path.with_name(f".{path.name}.tmp")
        _assert_safe_path(temporary, boundary=self.route_root)
        if temporary.exists():
            raise CandidateRejectedError("generation descriptor temporary path exists")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise CandidateRejectedError("generation descriptor is not an object")
        if value.get("schema_version") != R8_GENERATION_SCHEMA:
            raise CandidateRejectedError("generation descriptor schema is invalid")
        if value.get("route_name") != self.route_name:
            raise CandidateRejectedError("generation descriptor route identity mismatch")
        if value.get("generation") != generation:
            raise CandidateRejectedError("generation descriptor identity mismatch")
        _safe_component(value.get("candidate_id"), label="descriptor candidate_id")
        return value

    def _candidate_relative_root(self, candidate_root: Path) -> tuple[Path, str]:
        candidate = Path(candidate_root).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if ".." in candidate.parts:
            raise CandidateRejectedError("candidate_root must not contain traversal")
        packages_root = self.isolation_root / "candidate-packages"
        try:
            relative = candidate.relative_to(packages_root)
        except ValueError as error:
            raise CandidateRejectedError(
                "candidate_root escaped fixed candidate-packages"
            ) from error
        if len(relative.parts) != 3:
            raise CandidateRejectedError(
                "candidate_root is not at market/session/id package depth"
            )
        # Inspect the caller's original path before resolve() can erase an alias
        # symlink that points back inside the fixed candidate-packages root.
        _assert_safe_path(
            candidate,
            boundary=packages_root,
            final_kind="directory",
            allow_missing=False,
        )
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(packages_root)
        except (OSError, RuntimeError, ValueError) as error:
            raise CandidateRejectedError(
                "candidate_root escaped fixed candidate-packages"
            ) from error
        return resolved, resolved.relative_to(self.isolation_root).as_posix()

    def _assessment_state(
        self,
        *,
        candidate_root: Path,
        manifest: dict[str, object],
    ) -> dict[str, object]:
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict) or not isinstance(artifacts.get("assessment"), dict):
            raise CandidateRejectedError("candidate assessment artifact is missing")
        relative = artifacts["assessment"].get("path")
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
            raise CandidateRejectedError("candidate assessment path is malformed")
        assessment_path = candidate_root / relative
        try:
            assessment_path.resolve().relative_to(candidate_root)
        except ValueError as error:
            raise CandidateRejectedError("candidate assessment path escaped package") from error
        if assessment_path.is_symlink():
            raise CandidateRejectedError("candidate assessment must not be a symlink")
        value = json.loads(assessment_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise CandidateRejectedError("candidate assessment is not an object")
        return value

    def publish(
        self,
        candidate_root: Path,
        *,
        interrupt_before_pointer: bool = False,
    ) -> dict[str, object]:
        """Stage a verified generation and atomically replace the local pointer."""

        self._require_enabled()
        candidate_root_resolved, candidate_relative = self._candidate_relative_root(candidate_root)
        verification = verify_candidate_package(
            candidate_root_resolved,
            expected_isolation_root=self.isolation_root,
        )
        if not verification.passed or verification.manifest is None:
            raise CandidateRejectedError(
                "candidate verification failed: " + ",".join(verification.failures)
            )
        manifest = verification.manifest
        candidate_id = _safe_component(manifest["candidate_id"], label="candidate_id")
        if candidate_root_resolved.name != candidate_id:
            raise CandidateRejectedError("candidate_root and manifest candidate identity mismatch")
        revision = _safe_component(manifest["revision"], label="revision")
        generation = _safe_component(f"{candidate_id}.r{revision}", label="generation")
        assessment = self._assessment_state(
            candidate_root=candidate_root_resolved,
            manifest=manifest,
        )
        quality_status = str(manifest.get("quality_status"))
        gate_status = str(manifest.get("gate_status"))
        freshness_status = str(assessment.get("freshness_status"))
        if quality_status not in _SERVABLE_QUALITY:
            raise CandidateRejectedError(f"R8 candidate quality is not servable: {quality_status}")
        if gate_status != "ready" or assessment.get("gate_status") != "ready":
            raise CandidateRejectedError("R8 candidate gate is not ready")
        if freshness_status != "fresh":
            raise CandidateRejectedError("R8 candidate freshness is not fresh")
        current = self.current_state()
        previous_generation = str(current["generation"]) if current else None
        previous_candidate = str(current["candidate_id"]) if current else None
        descriptor = {
            "schema_version": R8_GENERATION_SCHEMA,
            "route_name": self.route_name,
            "generation": generation,
            "candidate_id": candidate_id,
            "candidate_root": candidate_relative,
            "manifest_sha256": verification.manifest_sha256,
            "quality_status": quality_status,
            "gate_status": gate_status,
            "freshness_status": freshness_status,
        }
        generation_path = self.generations_root / f"{generation}.json"
        if current and current.get("generation") == generation:
            if current.get("candidate_id") != candidate_id:
                raise CandidateRejectedError("current pointer candidate identity mismatch")
            if self._generation_descriptor(generation) != descriptor:
                raise CandidateRejectedError(f"isolated generation conflict: {generation_path}")
            return current
        # A later pointer/pending trap must not be able to leave a newly staged descriptor.
        temporary_paths = (
            generation_path.with_name(f".{generation_path.name}.tmp"),
            self.pending_path.with_name(f".{self.pending_path.name}.tmp"),
            self.pointer_path.with_name(f".{self.pointer_path.name}.tmp"),
        )
        for mutation_path in (
            generation_path,
            self.pending_path,
            self.pointer_path,
            *temporary_paths,
        ):
            _assert_safe_path(mutation_path, boundary=self.route_root)
        if any(path.exists() for path in temporary_paths):
            raise CandidateRejectedError("isolated Catalog temporary path already exists")
        _immutable_json_write(generation_path, descriptor, boundary=self.route_root)
        pending = {
            "schema_version": R8_ROUTE_SCHEMA,
            "route_name": self.route_name,
            "generation": generation,
            "candidate_id": candidate_id,
            "previous_generation": previous_generation,
            "previous_candidate_id": previous_candidate,
        }
        _atomic_json_write(self.pending_path, pending, boundary=self.route_root)
        if interrupt_before_pointer:
            raise PublicationInterrupted(
                "publication interrupted after isolated generation staging; pointer unchanged"
            )
        state = {
            **pending,
            "committed": True,
        }
        _atomic_json_write(self.pointer_path, state, boundary=self.route_root)
        _assert_safe_path(self.pending_path, boundary=self.route_root, allow_missing=False)
        self.pending_path.unlink(missing_ok=True)
        return state

    def recover_interrupted_publication(self) -> str:
        """Clear an uncommitted local marker while preserving the current pointer."""

        _assert_safe_path(self.pending_path, boundary=self.route_root)
        pending_temporary = self.pending_path.with_name(f".{self.pending_path.name}.tmp")
        _assert_safe_path(pending_temporary, boundary=self.route_root)
        if pending_temporary.exists():
            raise CandidateRejectedError("pending publication temporary path exists")
        if not self.pending_path.exists():
            return "no_pending_publication"
        pending = json.loads(self.pending_path.read_text(encoding="utf-8"))
        if (
            not isinstance(pending, dict)
            or pending.get("schema_version") != R8_ROUTE_SCHEMA
            or pending.get("route_name") != self.route_name
        ):
            raise CandidateRejectedError("pending publication route identity mismatch")
        _safe_component(pending.get("generation"), label="pending generation")
        _safe_component(pending.get("candidate_id"), label="pending candidate_id")
        current = self.current_state()
        if current and current.get("generation") == pending.get("generation"):
            _assert_safe_path(self.pending_path, boundary=self.route_root, allow_missing=False)
            self.pending_path.unlink(missing_ok=True)
            return "pointer_already_committed"
        _assert_safe_path(self.pending_path, boundary=self.route_root, allow_missing=False)
        self.pending_path.unlink(missing_ok=True)
        return "staged_generation_not_committed_pointer_preserved"

    def rollback(self) -> dict[str, object]:
        """Switch the local pointer to the previous generation without changing ledgers."""

        self._require_enabled()
        current = self.current_state()
        if current is None or not current.get("previous_generation"):
            raise CandidateRejectedError("no isolated rollback generation is available")
        previous_generation = str(current["previous_generation"])
        previous = self._generation_descriptor(previous_generation)
        if previous.get("candidate_id") != current.get("previous_candidate_id"):
            raise CandidateRejectedError("rollback candidate identity mismatch")
        rolled_back = {
            **current,
            "generation": previous_generation,
            "candidate_id": previous["candidate_id"],
            "previous_generation": current["generation"],
            "previous_candidate_id": current["candidate_id"],
            "committed": True,
        }
        _atomic_json_write(self.pointer_path, rolled_back, boundary=self.route_root)
        return rolled_back

    def read(
        self,
        consumer: ConsumerContract,
        *,
        expected_generation: str | None = None,
    ) -> ServedCandidate:
        """Read one pinned generation and enforce the consumer's partial-data contract."""

        self._require_enabled()
        current = self.current_state()
        if current is None:
            raise CandidateRejectedError("isolated route has no current candidate")
        generation = str(current["generation"])
        if expected_generation is not None and expected_generation != generation:
            raise GenerationConflictError(
                f"generation pin mismatch: expected {expected_generation}, got {generation}"
            )
        descriptor = self._generation_descriptor(generation)
        if descriptor.get("candidate_id") != current.get("candidate_id"):
            raise CandidateRejectedError("pointer and descriptor candidate identity mismatch")
        raw_candidate_root = descriptor.get("candidate_root")
        if not isinstance(raw_candidate_root, str) or Path(raw_candidate_root).is_absolute():
            raise CandidateRejectedError("generation descriptor candidate_root is malformed")
        candidate_root, candidate_relative = self._candidate_relative_root(
            self.isolation_root / raw_candidate_root
        )
        if candidate_relative != raw_candidate_root:
            raise CandidateRejectedError("generation descriptor candidate_root identity mismatch")
        verification = verify_candidate_package(
            candidate_root,
            expected_isolation_root=self.isolation_root,
        )
        if not verification.passed or verification.manifest is None:
            raise CandidateRejectedError(
                "pinned candidate verification failed: " + ",".join(verification.failures)
            )
        manifest = verification.manifest
        if verification.manifest_sha256 != descriptor.get("manifest_sha256"):
            raise CandidateRejectedError(
                "pinned candidate manifest digest changed after publication"
            )
        candidate_id = _safe_component(manifest.get("candidate_id"), label="manifest candidate_id")
        revision = _safe_component(manifest.get("revision"), label="manifest revision")
        manifest_generation = _safe_component(
            f"{candidate_id}.r{revision}", label="manifest generation"
        )
        if (
            manifest_generation != generation
            or candidate_id != descriptor.get("candidate_id")
            or candidate_id != current.get("candidate_id")
            or candidate_root.name != candidate_id
        ):
            raise CandidateRejectedError(
                "pointer, descriptor and candidate generation identity mismatch"
            )
        assessment = self._assessment_state(candidate_root=candidate_root, manifest=manifest)
        quality_status = str(manifest.get("quality_status"))
        if quality_status not in _SERVABLE_QUALITY:
            raise CandidateRejectedError(
                f"pinned candidate quality is not servable: {quality_status}"
            )
        if manifest.get("gate_status") != "ready" or assessment.get("gate_status") != "ready":
            raise CandidateRejectedError("pinned candidate gate is not ready")
        if assessment.get("freshness_status") != "fresh":
            raise CandidateRejectedError("pinned candidate freshness is not fresh")
        if (
            descriptor.get("quality_status") != quality_status
            or descriptor.get("gate_status") != manifest.get("gate_status")
            or descriptor.get("freshness_status") != assessment.get("freshness_status")
        ):
            raise CandidateRejectedError("generation descriptor policy state mismatch")
        if str(manifest["market"]) != consumer.market:
            raise ConsumerBlockedError(
                f"consumer market mismatch: {consumer.name} requires {consumer.market}"
            )
        if str(manifest["price_basis"]) != consumer.required_price_basis:
            raise ConsumerBlockedError(
                f"consumer price basis mismatch: {consumer.name} requires "
                f"{consumer.required_price_basis}"
            )
        partial = quality_status in {"usable_partial", "usable_partial_exception"}
        if partial and not consumer.supports_partial:
            raise ConsumerBlockedError(
                f"consumer {consumer.name} does not support {quality_status} candidates"
            )
        if consumer.layer != "raw" and str(manifest["dataset"]).endswith("_raw"):
            raise ConsumerBlockedError(
                f"raw candidate cannot satisfy downstream {consumer.layer} inputs"
            )
        artifacts = manifest["artifacts"]
        if not isinstance(artifacts, dict):
            raise CandidateRejectedError("candidate artifact index is malformed")
        rows_path = candidate_root / str(artifacts["rows"]["path"])
        if rows_path.is_symlink():
            raise CandidateRejectedError("candidate rows must not be a symlink")
        rows = tuple(
            json.loads(line)
            for line in rows_path.read_text(encoding="utf-8").splitlines()
            if line
        )
        return ServedCandidate(
            generation=generation,
            candidate_id=candidate_id,
            quality_status=quality_status,
            rows=rows,
        )


def verification_summary(verification: CandidateVerification) -> dict[str, Any]:
    """Return a JSON-safe compact verification summary for reports."""

    return {
        "passed": verification.passed,
        "candidate_id": verification.candidate_id,
        "manifest_sha256": verification.manifest_sha256,
        "checks": list(verification.checks),
        "failures": list(verification.failures),
    }
