from __future__ import annotations

from pathlib import Path

import pytest

from quant_stack.data.backfill import (
    RuntimeProvenanceError,
    run_runtime_provenance_preflight,
)

_COMMIT = "a" * 40


def _run(tmp_path: Path, **overrides):
    values = {
        "declared_source_commit": _COMMIT,
        "repo_root": tmp_path,
        "environment_id": "test-worker",
        "python_version": (3, 12, 4),
        "actual_checkout_commit": _COMMIT,
        "git_status": "",
        "entrypoint_path": tmp_path / "scripts/run_cn_backfill_campaign.py",
        "module_origin_paths": (tmp_path / "src/quant_stack/__init__.py",),
        "tracked_paths": {
            "scripts/run_cn_backfill_campaign.py",
            "src/quant_stack/__init__.py",
        },
    }
    values.update(overrides)
    return run_runtime_provenance_preflight(**values)


def test_exact_clean_supported_runtime_passes(tmp_path: Path) -> None:
    report = _run(tmp_path)
    assert report.result == "pass"
    assert report.source_tree_clean is True
    assert report.runtime_source_paths_tracked is True
    assert report.runtime_source_paths == (
        "scripts/run_cn_backfill_campaign.py",
        "src/quant_stack/__init__.py",
    )
    assert report.failure_reasons == ()


@pytest.mark.parametrize("version", [(3, 11, 9), (3, 15, 0), (2, 7, 18)])
def test_unsupported_python_fails_closed(tmp_path: Path, version: tuple[int, int, int]) -> None:
    with pytest.raises(RuntimeProvenanceError, match="unsupported Python") as error:
        _run(tmp_path, python_version=version)
    assert error.value.report.result == "fail"


def test_commit_mismatch_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(RuntimeProvenanceError, match="does not equal"):
        _run(tmp_path, actual_checkout_commit="b" * 40)


def test_dirty_tracked_source_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(RuntimeProvenanceError, match="dirty"):
        _run(tmp_path, git_status=" M src/quant_stack/data/backfill/campaign.py")


def test_missing_git_evidence_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(RuntimeProvenanceError, match="unable to read"):
        _run(tmp_path, actual_checkout_commit=None, git_status=None)


def test_foreign_module_origin_fails_closed(tmp_path: Path) -> None:
    foreign_origin = tmp_path.parent / "foreign-source" / "src/quant_stack/__init__.py"
    with pytest.raises(RuntimeProvenanceError, match="outside repo_root"):
        _run(tmp_path, module_origin_paths=(foreign_origin,))


def test_untracked_runtime_entrypoint_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(RuntimeProvenanceError, match="not tracked"):
        _run(
            tmp_path,
            tracked_paths={"src/quant_stack/__init__.py"},
        )
