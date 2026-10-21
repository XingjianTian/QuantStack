from __future__ import annotations

import os
from pathlib import Path

import pytest

from quant_stack.data.storage import atomic
from quant_stack.data.storage.atomic import (
    atomic_replace_bytes,
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)


def _configure_group_policy(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    monkeypatch.setenv("QUANT_DATA_SHARED_GID", str(os.getgid()))
    monkeypatch.setenv("QUANT_DATA_PERMISSION_ROOT", str(root))
    monkeypatch.setenv("QUANT_DATA_FILE_MODE", "0640")
    monkeypatch.setenv("QUANT_DATA_DIRECTORY_MODE", "2750")


def test_atomic_replace_applies_shared_group_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_group_policy(monkeypatch, tmp_path)
    target = tmp_path / "canonical" / "bars.parquet"

    atomic_replace_bytes(target, b"version-1")
    atomic_replace_bytes(target, b"version-2")

    assert target.read_bytes() == b"version-2"
    assert target.stat().st_gid == os.getgid()
    assert target.stat().st_mode & 0o7777 == 0o640
    assert target.parent.stat().st_gid == os.getgid()
    assert target.parent.stat().st_mode & 0o7777 == 0o2750


def test_atomic_replace_does_not_reown_permission_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    permission_root.mkdir()
    _configure_group_policy(monkeypatch, permission_root)
    target = permission_root / "daily" / "state.json"
    real_chown = os.chown

    def chown_below_root(path: os.PathLike[str] | str, uid: int, gid: int) -> None:
        if Path(path) == permission_root:
            raise PermissionError("permission root is administrator-owned")
        real_chown(path, uid, gid)

    monkeypatch.setattr(atomic.os, "chown", chown_below_root)

    atomic_replace_bytes(target, b"{}")

    assert target.read_bytes() == b"{}"
    assert target.parent.stat().st_mode & 0o7777 == 0o2750


def test_atomic_replace_does_not_reown_existing_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    existing_ancestor = permission_root / "shadow-runs"
    final_parent = existing_ancestor / "daily"
    final_parent.mkdir(parents=True)
    _configure_group_policy(monkeypatch, permission_root)
    target = final_parent / "state.json"
    real_chown = os.chown

    def chown_managed_path(path: os.PathLike[str] | str, uid: int, gid: int) -> None:
        if Path(path) in {permission_root, existing_ancestor}:
            raise PermissionError("existing ancestor is administrator-owned")
        real_chown(path, uid, gid)

    monkeypatch.setattr(atomic.os, "chown", chown_managed_path)

    atomic_replace_bytes(target, b"{}")

    assert target.read_bytes() == b"{}"
    assert final_parent.stat().st_mode & 0o7777 == 0o2750


def test_immutable_replay_repairs_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_group_policy(monkeypatch, tmp_path)
    target = tmp_path / "catalog" / "manifest.json"
    target.parent.mkdir()
    target.write_bytes(b"same")
    target.chmod(0o600)

    assert not atomic_write_immutable_bytes(target, b"same", artifact_kind="manifest")
    assert target.stat().st_mode & 0o7777 == 0o640


def test_immutable_bytes_rejects_final_symlink_without_touching_referent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    _configure_group_policy(monkeypatch, permission_root)
    permission_root.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"same")
    outside.chmod(0o600)
    before = (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode)
    target = permission_root / "artifact.bin"
    target.symlink_to(outside)

    with pytest.raises(PermissionError, match="final artifact must not be a symlink"):
        atomic_write_immutable_bytes(target, b"same", artifact_kind="test")

    assert (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode) == before


def test_immutable_file_rejects_final_symlink_before_equivalence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    _configure_group_policy(monkeypatch, permission_root)
    permission_root.mkdir()
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"same")
    outside.chmod(0o600)
    before = (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode)
    target = permission_root / "artifact.parquet"
    target.symlink_to(outside)
    equivalence_called = False

    def equivalent(_existing: Path, _temporary: Path) -> bool:
        nonlocal equivalence_called
        equivalence_called = True
        return True

    with pytest.raises(PermissionError, match="final artifact must not be a symlink"):
        atomic_write_immutable_file(
            target,
            write=lambda path: path.write_bytes(b"same"),
            validate=lambda _path: None,
            equivalent=equivalent,
            artifact_kind="test",
        )

    assert not equivalence_called
    assert (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode) == before


def test_immutable_lock_rejects_symlink_without_touching_referent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    _configure_group_policy(monkeypatch, permission_root)
    permission_root.mkdir()
    target = permission_root / "artifact.bin"
    outside = tmp_path / "outside.lock"
    outside.write_bytes(b"lock-referent")
    outside.chmod(0o600)
    before = (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode)
    target.with_name(f".{target.name}.write.lock").symlink_to(outside)

    with pytest.raises(PermissionError, match="lock must not be a symlink"):
        atomic_write_immutable_bytes(target, b"new", artifact_kind="test")

    assert not target.exists()
    assert (outside.read_bytes(), outside.stat().st_gid, outside.stat().st_mode) == before


@pytest.mark.parametrize(
    ("name", "value", "match"),
    [
        ("QUANT_DATA_SHARED_GID", "not-a-gid", "numeric GID"),
        ("QUANT_DATA_FILE_MODE", "0644", "other users"),
        ("QUANT_DATA_FILE_MODE", "0750", "must be one of"),
        ("QUANT_DATA_DIRECTORY_MODE", "0750", "must be one of"),
    ],
)
def test_invalid_permission_policy_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    match: str,
) -> None:
    _configure_group_policy(monkeypatch, tmp_path)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=match):
        atomic_replace_bytes(tmp_path / "artifact", b"payload")


def test_modes_without_shared_gid_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("QUANT_DATA_FILE_MODE", "0640")

    with pytest.raises(ValueError, match="QUANT_DATA_SHARED_GID is required"):
        atomic_replace_bytes(tmp_path / "artifact", b"payload")


def test_shared_gid_without_permission_root_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("QUANT_DATA_SHARED_GID", str(os.getgid()))

    with pytest.raises(ValueError, match="QUANT_DATA_PERMISSION_ROOT is required"):
        atomic_replace_bytes(tmp_path / "artifact", b"payload")


def test_permission_policy_rejects_paths_outside_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    permission_root = tmp_path / "data"
    _configure_group_policy(monkeypatch, permission_root)

    with pytest.raises(ValueError, match="escapes QUANT_DATA_PERMISSION_ROOT"):
        atomic_replace_bytes(tmp_path / "outside", b"payload")


def test_permission_policy_requires_absolute_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_group_policy(monkeypatch, tmp_path)
    monkeypatch.setenv("QUANT_DATA_PERMISSION_ROOT", "relative/data")

    with pytest.raises(ValueError, match="must be an absolute path"):
        atomic_replace_bytes(tmp_path / "artifact", b"payload")
