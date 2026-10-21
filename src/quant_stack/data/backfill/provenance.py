"""Fail-closed runtime provenance checks for live backfill workers.

The check deliberately uses the declared Git commit and the live checkout state,
not a content snapshot or a workflow fingerprint.  It is a small operational
guard: a worker must run from a supported Python version, from the declared
commit, with no tracked source edits before it can create Provider side effects.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Collection, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_SUPPORTED_PYTHON = ">=3.12,<3.15"


@dataclass(frozen=True)
class RuntimeProvenanceReport:
    schema_version: str
    checked_at: str
    environment_id: str
    python_version: str
    supported_python: str
    declared_source_commit: str
    actual_checkout_commit: str | None
    source_tree_clean: bool
    result: str
    runtime_source_paths: tuple[str, ...]
    runtime_source_paths_tracked: bool
    failure_reasons: tuple[str, ...]

    def model_dump(self) -> dict[str, object]:
        """Return a JSON-friendly report without introducing a new model dependency."""

        return asdict(self)


class RuntimeProvenanceError(RuntimeError):
    """Raised before a live worker can create an external side effect."""

    def __init__(self, report: RuntimeProvenanceReport):
        self.report = report
        reasons = "; ".join(report.failure_reasons) or "unknown provenance failure"
        super().__init__(reasons)


def _git_value(repo_root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ("git", "-C", str(repo_root), *args),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _format_python_version(version: tuple[int, int, int]) -> str:
    return ".".join(str(part) for part in version)


def _relative_source_path(repo_root: Path, path: Path) -> str | None:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return None


def _is_tracked_source_path(
    repo_root: Path,
    relative_path: str,
    tracked_paths: Collection[str] | None,
) -> bool:
    if tracked_paths is not None:
        return relative_path in tracked_paths
    return (
        _git_value(repo_root, "ls-files", "--error-unmatch", "--", relative_path)
        == relative_path
    )


def run_runtime_provenance_preflight(
    *,
    declared_source_commit: str,
    repo_root: Path,
    environment_id: str,
    python_version: tuple[int, int, int] | None = None,
    actual_checkout_commit: str | None = None,
    git_status: str | None = None,
    entrypoint_path: Path | None = None,
    module_origin_paths: Iterable[Path] | None = None,
    tracked_paths: Collection[str] | None = None,
) -> RuntimeProvenanceReport:
    """Validate the exact source/runtime identity before a live Provider call.

    ``python_version``, ``actual_checkout_commit`` and ``git_status`` are injectable
    only to make the negative paths deterministic in offline tests.  Production
    callers omit them and the values are read from the running interpreter and
    checkout.
    """

    checked_at = datetime.now(UTC).isoformat()
    resolved_repo_root = repo_root.resolve()
    current_python = python_version or (
        sys.version_info.major,
        sys.version_info.minor,
        sys.version_info.micro,
    )
    actual_commit = (
        actual_checkout_commit
        if actual_checkout_commit is not None
        else _git_value(resolved_repo_root, "rev-parse", "HEAD")
    )
    status = (
        git_status
        if git_status is not None
        else _git_value(resolved_repo_root, "status", "--porcelain=v1", "--untracked-files=no")
    )
    reasons: list[str] = []
    module_origins = tuple(module_origin_paths or ())
    runtime_paths = [] if entrypoint_path is None else [entrypoint_path]
    runtime_paths.extend(module_origins)
    runtime_source_paths: list[str] = []
    runtime_paths_tracked = bool(runtime_paths)

    if entrypoint_path is None or not module_origins:
        runtime_paths_tracked = False
        reasons.append("runtime entrypoint and module origins are required")
    for runtime_path in runtime_paths:
        relative_path = _relative_source_path(resolved_repo_root, runtime_path)
        if relative_path is None:
            runtime_source_paths.append("<outside-repo>")
            runtime_paths_tracked = False
            reasons.append("runtime entrypoint/module origin is outside repo_root")
            continue
        runtime_source_paths.append(relative_path)
        if not _is_tracked_source_path(resolved_repo_root, relative_path, tracked_paths):
            runtime_paths_tracked = False
            reasons.append(f"runtime source path is not tracked by Git: {relative_path}")

    if not _COMMIT_PATTERN.fullmatch(declared_source_commit):
        reasons.append("declared_source_commit must be a full 40-character lowercase Git SHA")
    if not (
        current_python[0] == 3
        and 12 <= current_python[1] < 15
    ):
        reasons.append(
            f"unsupported Python runtime { _format_python_version(current_python) }; "
            f"required {_SUPPORTED_PYTHON}"
        )
    if actual_commit is None:
        reasons.append("unable to read the live checkout commit")
    elif actual_commit != declared_source_commit:
        reasons.append(
            "live checkout commit does not equal declared_source_commit "
            f"({actual_commit} != {declared_source_commit})"
        )
    if status is None:
        reasons.append("unable to read tracked source-tree status")
    elif status:
        reasons.append("tracked source tree is dirty; refusing to run a rewritten source copy")

    report = RuntimeProvenanceReport(
        schema_version="quantstack.runtime-provenance.v1",
        checked_at=checked_at,
        environment_id=environment_id,
        python_version=_format_python_version(current_python),
        supported_python=_SUPPORTED_PYTHON,
        declared_source_commit=declared_source_commit,
        actual_checkout_commit=actual_commit,
        source_tree_clean=status == "",
        runtime_source_paths=tuple(runtime_source_paths),
        runtime_source_paths_tracked=runtime_paths_tracked,
        result="pass" if not reasons else "fail",
        failure_reasons=tuple(reasons),
    )
    if reasons:
        raise RuntimeProvenanceError(report)
    return report
