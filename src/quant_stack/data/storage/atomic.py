from __future__ import annotations

import errno
import fcntl
import os
import tempfile
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

PathValidator = Callable[[Path], None]

_SHARED_GID_ENV = "QUANT_DATA_SHARED_GID"
_PERMISSION_ROOT_ENV = "QUANT_DATA_PERMISSION_ROOT"
_FILE_MODE_ENV = "QUANT_DATA_FILE_MODE"
_DIRECTORY_MODE_ENV = "QUANT_DATA_DIRECTORY_MODE"


@dataclass(frozen=True)
class _PermissionPolicy:
    gid: int
    root: Path
    file_mode: int
    directory_mode: int


def _parse_mode(name: str, value: str) -> int:
    try:
        mode = int(value, 8)
    except ValueError as exc:
        raise ValueError(f"{name} must be an octal mode") from exc
    if not 0 <= mode <= 0o7777:
        raise ValueError(f"{name} is outside the supported permission bits")
    if mode & 0o007:
        raise ValueError(f"{name} must not grant permissions to other users")
    return mode


def _permission_policy() -> _PermissionPolicy | None:
    gid_value = os.environ.get(_SHARED_GID_ENV)
    root_value = os.environ.get(_PERMISSION_ROOT_ENV)
    file_mode_value = os.environ.get(_FILE_MODE_ENV)
    directory_mode_value = os.environ.get(_DIRECTORY_MODE_ENV)
    if gid_value is None:
        if (
            root_value is not None
            or file_mode_value is not None
            or directory_mode_value is not None
        ):
            raise ValueError(
                f"{_SHARED_GID_ENV} is required when configuring shared data permissions"
            )
        return None
    if not root_value:
        raise ValueError(f"{_PERMISSION_ROOT_ENV} is required with {_SHARED_GID_ENV}")
    permission_root = Path(root_value)
    if not permission_root.is_absolute():
        raise ValueError(f"{_PERMISSION_ROOT_ENV} must be an absolute path")
    try:
        gid = int(gid_value, 10)
    except ValueError as exc:
        raise ValueError(f"{_SHARED_GID_ENV} must be a numeric GID") from exc
    if gid < 0:
        raise ValueError(f"{_SHARED_GID_ENV} must be non-negative")
    file_mode = _parse_mode(_FILE_MODE_ENV, file_mode_value or "0640")
    directory_mode = _parse_mode(_DIRECTORY_MODE_ENV, directory_mode_value or "2750")
    if file_mode not in {0o600, 0o640, 0o660}:
        raise ValueError(f"{_FILE_MODE_ENV} must be one of 0600, 0640, or 0660")
    if directory_mode not in {0o2700, 0o2750, 0o2770}:
        raise ValueError(f"{_DIRECTORY_MODE_ENV} must be one of 2700, 2750, or 2770")
    return _PermissionPolicy(
        gid=gid,
        root=permission_root.resolve(strict=False),
        file_mode=file_mode,
        directory_mode=directory_mode,
    )


def _apply_permissions(path: Path, *, mode: int, policy: _PermissionPolicy) -> None:
    try:
        os.chown(path, -1, policy.gid)
        os.chmod(path, mode)
    except OSError as exc:
        raise PermissionError(
            f"Cannot apply QuantStack shared-data permissions to {path}: {exc}"
        ) from exc


def _apply_fd_permissions(
    descriptor: int, *, path: Path, mode: int, policy: _PermissionPolicy
) -> None:
    try:
        os.fchown(descriptor, -1, policy.gid)
        os.fchmod(descriptor, mode)
    except OSError as exc:
        raise PermissionError(
            f"Cannot apply QuantStack shared-data permissions to {path}: {exc}"
        ) from exc


def _reject_final_symlink(path: Path, policy: _PermissionPolicy | None) -> None:
    if policy is not None and path.is_symlink():
        raise PermissionError(
            f"QuantStack shared-data final artifact must not be a symlink: {path}"
        )


def _no_follow_flag() -> int:
    flag = getattr(os, "O_NOFOLLOW", None)
    if flag is None:
        raise PermissionError(
            "QuantStack shared-data permissions require O_NOFOLLOW support"
        )
    return flag


def _existing_bytes_match_and_repair(
    path: Path, expected: bytes, *, policy: _PermissionPolicy
) -> bool:
    try:
        descriptor = os.open(path, os.O_RDONLY | _no_follow_flag())
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise PermissionError(
                f"QuantStack shared-data final artifact must not be a symlink: {path}"
            ) from exc
        raise
    with os.fdopen(descriptor, "rb") as stream:
        payload = stream.read()
        if payload == expected:
            _apply_fd_permissions(
                stream.fileno(), path=path, mode=policy.file_mode, policy=policy
            )
            return True
        return False


def _repair_existing_permissions(path: Path, *, policy: _PermissionPolicy) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | _no_follow_flag())
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise PermissionError(
                f"QuantStack shared-data final artifact must not be a symlink: {path}"
            ) from exc
        raise
    try:
        _apply_fd_permissions(
            descriptor, path=path, mode=policy.file_mode, policy=policy
        )
    finally:
        os.close(descriptor)


def _prepare_parent(path: Path, policy: _PermissionPolicy | None) -> None:
    if policy is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        return
    parent = path.parent.resolve(strict=False)
    if not parent.is_relative_to(policy.root):
        raise ValueError(f"Atomic data path escapes {_PERMISSION_ROOT_ENV}: {path}")
    relative = parent.relative_to(policy.root)
    # The permission root is an administrator-provisioned boundary and may be
    # owned by a parent service account.  Writers manage only directories
    # below that boundary; attempting to chown the boundary itself makes an
    # otherwise valid nested write fail for unprivileged daily workers.
    directories: list[Path] = []
    current = policy.root
    for part in relative.parts:
        current /= part
        directories.append(current)
    missing = [directory for directory in directories if not directory.exists()]
    parent.mkdir(parents=True, exist_ok=True)
    managed = [*missing]
    if parent != policy.root and parent not in managed:
        managed.append(parent)
    for directory in managed:
        _apply_permissions(directory, mode=policy.directory_mode, policy=policy)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def _write_lock(path: Path):
    policy = _permission_policy()
    lock_path = path.with_name(f".{path.name}.write.lock")
    flags = os.O_RDWR | os.O_CREAT | os.O_APPEND
    if policy is not None:
        flags |= _no_follow_flag()
    try:
        descriptor = os.open(lock_path, flags, 0o666)
    except OSError as exc:
        if policy is not None and exc.errno == errno.ELOOP:
            raise PermissionError(
                f"QuantStack shared-data lock must not be a symlink: {lock_path}"
            ) from exc
        raise
    with os.fdopen(descriptor, "a+b") as lock:
        if policy is not None:
            _apply_fd_permissions(
                lock.fileno(),
                path=lock_path,
                mode=policy.file_mode | 0o200,
                policy=policy,
            )
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _temporary_path(path: Path) -> tuple[int, Path]:
    descriptor, value = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    return descriptor, Path(value)


def atomic_replace_bytes(
    path: Path,
    payload: bytes,
    *,
    validate: PathValidator | None = None,
) -> None:
    """Durably replace a mutable file after validating a random-suffix temp file."""

    policy = _permission_policy()
    _prepare_parent(path, policy)
    _reject_final_symlink(path, policy)
    descriptor, temporary = _temporary_path(path)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if policy is not None:
            _apply_permissions(temporary, mode=policy.file_mode, policy=policy)
        if temporary.read_bytes() != payload:
            raise OSError(f"Temporary file readback mismatch: {path}")
        if validate is not None:
            validate(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_replace_file(
    path: Path,
    *,
    write: Callable[[Path], None],
    validate: PathValidator | None = None,
) -> None:
    """Durably replace a generated file without loading it into memory."""

    policy = _permission_policy()
    _prepare_parent(path, policy)
    _reject_final_symlink(path, policy)
    descriptor, temporary = _temporary_path(path)
    os.close(descriptor)
    try:
        temporary.unlink()
        write(temporary)
        if policy is not None:
            _apply_permissions(temporary, mode=policy.file_mode, policy=policy)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        if validate is not None:
            validate(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_immutable_bytes(
    path: Path,
    payload: bytes,
    *,
    artifact_kind: str,
    validate: PathValidator | None = None,
) -> bool:
    """Create immutable content crash-safely; exact replay is idempotent.

    Returns ``True`` when a new final file was installed and ``False`` for an
    exact replay. A per-path local ``fcntl`` lock makes the compare-and-replace
    sequence safe between Mac/Linux worker processes.
    """

    policy = _permission_policy()
    _prepare_parent(path, policy)
    with _write_lock(path):
        _reject_final_symlink(path, policy)
        if path.exists():
            matches = (
                _existing_bytes_match_and_repair(path, payload, policy=policy)
                if policy is not None
                else path.read_bytes() == payload
            )
            if matches:
                return False
            raise ValueError(f"Immutable {artifact_kind} conflict: {path}")
        atomic_replace_bytes(path, payload, validate=validate)
        return True


def atomic_write_immutable_file(
    path: Path,
    *,
    write: Callable[[Path], None],
    validate: PathValidator,
    equivalent: Callable[[Path, Path], bool],
    artifact_kind: str,
) -> bool:
    """Create an immutable generated file such as Parquet via a durable temp."""

    policy = _permission_policy()
    _prepare_parent(path, policy)
    with _write_lock(path):
        _reject_final_symlink(path, policy)
        descriptor, temporary = _temporary_path(path)
        os.close(descriptor)
        try:
            write(temporary)
            if policy is not None:
                _apply_permissions(temporary, mode=policy.file_mode, policy=policy)
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            validate(temporary)
            _reject_final_symlink(path, policy)
            if path.exists():
                if equivalent(path, temporary):
                    if policy is not None:
                        _repair_existing_permissions(path, policy=policy)
                    return False
                raise ValueError(f"Immutable {artifact_kind} conflict: {path}")
            os.replace(temporary, path)
            _fsync_directory(path.parent)
            return True
        finally:
            temporary.unlink(missing_ok=True)
