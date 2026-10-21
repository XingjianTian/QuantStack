"""Pure no-write preflight and Gate packet primitives for R8.

The preflight only evaluates supplied local paths and counters.  It intentionally does not
instantiate a Provider, NAS client, database connection, production writer, or lock manager.
"""

from __future__ import annotations

import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .r8_artifacts import CandidateVerification, resolve_isolation_root, verify_candidate_package

CheckStatus = Literal["pass", "fail", "not_verified"]


@dataclass(frozen=True)
class PreflightCheck:
    name: str
    status: CheckStatus
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "status": self.status, "detail": self.detail}


@dataclass(frozen=True)
class NoWritePreflight:
    status: Literal["gate_review_ready", "blocked"]
    isolation_root: str
    candidate_id: str
    checks: tuple[PreflightCheck, ...]
    side_effects: Mapping[str, int]
    consumer_matrix: tuple[Mapping[str, object], ...]

    @property
    def failures(self) -> tuple[PreflightCheck, ...]:
        return tuple(check for check in self.checks if check.status == "fail")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "quantstack.daily-r8-preflight.v1",
            "status": self.status,
            "isolation_root": self.isolation_root,
            "candidate_id": self.candidate_id,
            "checks": [check.to_dict() for check in self.checks],
            "side_effects": dict(self.side_effects),
            "consumer_matrix": [dict(item) for item in self.consumer_matrix],
        }


def _candidate_path_safety(path: Path, root: Path) -> tuple[bool, str]:
    candidate = Path(path).expanduser()
    packages_root = root / "candidate-packages"
    if ".." in candidate.parts:
        return False, "candidate path contains traversal"
    try:
        candidate.absolute().relative_to(packages_root)
        resolved = candidate.resolve()
        resolved.relative_to(packages_root)
    except (OSError, ValueError) as error:
        return False, f"candidate is outside fixed candidate-packages: {error}"
    try:
        packages_metadata = packages_root.lstat()
    except FileNotFoundError:
        packages_metadata = None
    if packages_metadata is not None and stat.S_ISLNK(packages_metadata.st_mode):
        return False, f"candidate-packages is a symlink: {packages_root}"
    current = packages_root
    for component in candidate.absolute().relative_to(packages_root).parts:
        current = current / component
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            return False, f"candidate path contains symlink: {current}"
    return True, str(resolved)


def run_no_write_preflight(
    *,
    isolation_root: Path,
    candidate_root: Path,
    verification: CandidateVerification | None = None,
    route_enabled: bool = False,
    side_effects: Mapping[str, int] | None = None,
    dynamic_facts: Mapping[str, str] | None = None,
    consumer_matrix: tuple[Mapping[str, object], ...] = (),
    required_checks: Mapping[str, bool] | None = None,
) -> NoWritePreflight:
    """Evaluate an isolated candidate without creating or changing any file."""

    root = resolve_isolation_root(isolation_root)
    path_safe, path_detail = _candidate_path_safety(candidate_root, root)
    if path_safe:
        current_verification = verify_candidate_package(
            candidate_root,
            expected_isolation_root=root,
        )
    else:
        current_verification = CandidateVerification(
            passed=False,
            candidate_id=Path(candidate_root).name,
            manifest_sha256=None,
            checks=(),
            failures=("candidate_path_not_safe",),
        )
    effects = {
        "provider_requests": 0,
        "nas_reads": 0,
        "nas_writes": 0,
        "production_db_calls": 0,
        "production_writes": 0,
        "production_locks": 0,
    }
    effects.update({str(key): int(value) for key, value in (side_effects or {}).items()})
    checks: list[PreflightCheck] = [
        PreflightCheck(
            "candidate_integrity",
            "pass" if current_verification.passed else "fail",
            "; ".join(current_verification.failures)
            if current_verification.failures
            else "manifest, artifact hashes, rows, ledger and R/O/P closure verified",
        ),
        PreflightCheck(
            "candidate_under_fixed_isolation_root",
            "pass" if path_safe else "fail",
            path_detail,
        ),
        PreflightCheck(
            "route_default_off",
            "pass" if not route_enabled else "fail",
            "R8 route is not enabled by preflight",
        ),
    ]
    if verification is not None:
        cached_matches = (
            verification.passed == current_verification.passed
            and verification.candidate_id == current_verification.candidate_id
            and verification.manifest_sha256 == current_verification.manifest_sha256
            and verification.checks == current_verification.checks
            and verification.failures == current_verification.failures
        )
        checks.append(
            PreflightCheck(
                "cached_verification_matches_current_candidate",
                "pass" if cached_matches else "fail",
                "cached verification compared with a fresh candidate verification",
            )
        )
    for effect_name, count in sorted(effects.items()):
        checks.append(
            PreflightCheck(
                f"side_effect_{effect_name}",
                "pass" if count == 0 else "fail",
                f"observed_count={count}",
            )
        )
    facts = dynamic_facts or {
        "production_generation": "not_verified",
        "production_lock_or_lease": "not_verified",
        "production_resource_reachability": "not_verified",
    }
    for name, status in sorted(facts.items()):
        normalized = status if status in {"pass", "fail", "not_verified"} else "not_verified"
        checks.append(
            PreflightCheck(
                f"dynamic_{name}",
                normalized,  # type: ignore[arg-type]
                "dynamic production fact is intentionally outside this no-write preflight",
            )
        )
    for name, passed in sorted((required_checks or {}).items()):
        checks.append(
            PreflightCheck(
                f"required_{name}",
                "pass" if passed else "fail",
                "required local Gate assertion evaluated from observed evidence",
            )
        )
    final_status: Literal["gate_review_ready", "blocked"] = (
        "gate_review_ready" if not any(check.status == "fail" for check in checks) else "blocked"
    )
    return NoWritePreflight(
        status=final_status,
        isolation_root=str(root),
        candidate_id=current_verification.candidate_id,
        checks=tuple(checks),
        side_effects=effects,
        consumer_matrix=consumer_matrix,
    )
