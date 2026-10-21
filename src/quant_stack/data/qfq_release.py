from __future__ import annotations

import fcntl
import json
import os
import shutil
import stat
from collections.abc import Collection
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .models import DatasetManifest
from .publication import CatalogPublisher, PublicationReceipt
from .storage.atomic import (
    atomic_replace_bytes,
    atomic_replace_file,
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)
from .worker_bundle import WorkerImportEvidence, publish_imported_worker_bundle


@dataclass(frozen=True)
class QFQReleaseResult:
    receipt: PublicationReceipt
    snapshot_dir: Path
    previous_bundle_id: str
    dataset_id: str
    dataset_version: str
    row_count: int
    instrument_count: int
    first_date: date
    last_date: date
    copied_artifacts: int
    ss_previous_version: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": "quantstack.qfq-release.v1",
            "status": "published",
            "receipt_id": self.receipt.receipt_id,
            "idempotent_replay": self.receipt.idempotent_replay,
            "previous_bundle_id": self.previous_bundle_id,
            "published_bundle_id": self.receipt.bundle_id,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "row_count": self.row_count,
            "instrument_count": self.instrument_count,
            "first_date": self.first_date.isoformat(),
            "last_date": self.last_date.isoformat(),
            "copied_artifacts": self.copied_artifacts,
            "catalog_replay_verified": True,
            "ss_exact_version_pinned": self.ss_previous_version is not None,
            "ss_previous_version": self.ss_previous_version,
            "rollback_snapshot": str(self.snapshot_dir),
        }


def _file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_artifact(path: Path, *, size_bytes: int, sha256: str) -> None:
    if not path.is_file() or path.stat().st_size != size_bytes or _file_sha256(path) != sha256:
        raise ValueError(f"qfq release artifact verification failed: {path}")


@contextmanager
def _release_lock(data_root: Path):
    path = data_root / "catalog/.qfq-release.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _copy_immutable(source: Path, destination: Path, *, size_bytes: int, sha256: str) -> bool:
    _verify_artifact(source, size_bytes=size_bytes, sha256=sha256)
    if destination.exists():
        _verify_artifact(destination, size_bytes=size_bytes, sha256=sha256)
        return False
    return atomic_write_immutable_file(
        destination,
        write=lambda temporary: shutil.copyfile(source, temporary),
        validate=lambda candidate: _verify_artifact(
            candidate,
            size_bytes=size_bytes,
            sha256=sha256,
        ),
        equivalent=lambda current, _candidate: (
            current.stat().st_size == size_bytes and _file_sha256(current) == sha256
        ),
        artifact_kind="qfq release artifact",
    )


def _grant_catalog_read_access(path: Path, *, data_root: Path, read_gid: int) -> None:
    """Keep root-published artifacts readable by the established data-reader group."""
    current = path
    while current != data_root:
        os.chown(current, -1, read_gid)
        os.chmod(current, 0o2750 if current.is_dir() else 0o640)
        current = current.parent


@contextmanager
def _catalog_atomic_permissions(data_root: Path, *, read_gid: int):
    """Apply the established reader permissions before atomic Catalog exposure."""
    values = {
        "QUANT_DATA_SHARED_GID": str(read_gid),
        "QUANT_DATA_PERMISSION_ROOT": str(data_root),
        "QUANT_DATA_FILE_MODE": "0640",
        "QUANT_DATA_DIRECTORY_MODE": "2750",
    }
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _preserve_existing_access(path: Path, *, mode: int, gid: int) -> None:
    os.chown(path, -1, gid)
    os.chmod(path, stat.S_IMODE(mode))


def _snapshot_catalog(
    data_root: Path, snapshot_dir: Path, *, expected_published_bundle_id: str
) -> str:
    catalog = data_root / "catalog"
    database = catalog / "quant.duckdb"
    pointer = catalog / "current-manifest-set.json"
    if not database.is_file() or not pointer.is_file():
        raise FileNotFoundError("qfq release requires an existing Catalog database and pointer")
    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    previous_bundle_id = str(pointer_payload["bundle_id"])
    snapshot_catalog = snapshot_dir / "catalog"
    snapshot_database = snapshot_catalog / "quant.duckdb"
    snapshot_pointer = snapshot_catalog / "current-manifest-set.json"
    if snapshot_database.exists() or snapshot_pointer.exists():
        if not snapshot_database.is_file() or not snapshot_pointer.is_file():
            raise ValueError("qfq release rollback snapshot is incomplete")
        saved_pointer = json.loads(snapshot_pointer.read_text(encoding="utf-8"))
        if pointer_payload.get("bundle_id") not in {
            saved_pointer.get("bundle_id"),
            expected_published_bundle_id,
        }:
            raise ValueError("qfq release snapshot belongs to a different Catalog state")
        return str(saved_pointer["bundle_id"])
    atomic_write_immutable_file(
        snapshot_database,
        write=lambda temporary: shutil.copyfile(database, temporary),
        validate=lambda candidate: _validate_catalog_database(candidate),
        equivalent=lambda current, candidate: current.read_bytes() == candidate.read_bytes(),
        artifact_kind="qfq release Catalog rollback database",
    )
    atomic_write_immutable_bytes(
        snapshot_pointer,
        pointer.read_bytes(),
        artifact_kind="qfq release Catalog rollback pointer",
        validate=lambda candidate: json.loads(candidate.read_text(encoding="utf-8")),
    )
    return previous_bundle_id


def _validate_catalog_database(path: Path) -> None:
    import duckdb

    connection = duckdb.connect(str(path), read_only=True)
    try:
        connection.execute("SELECT count(*) FROM datasets").fetchone()
    finally:
        connection.close()


def _profile_manifest(data_root: Path, manifest: DatasetManifest) -> tuple[int, int, date, date]:
    import duckdb

    bars = data_root / manifest.storage_uri.removeprefix("quant-data://")
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
            "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
            "count(*) FILTER (WHERE lineage.adjustment != 'qfq') FROM read_parquet(?)",
            [str(bars)],
        ).fetchone()
    finally:
        connection.close()
    if row is None or row[0] != row[1] or row[5] != 0 or row[0] != manifest.row_count:
        raise ValueError("published qfq bars failed row, key, or adjustment verification")
    return int(row[0]), int(row[2]), row[3], row[4]


def _read_env(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return lines, values


_SS_PIN_KEYS = (
    "SS_QUANTSTACK_READ_MODE",
    "SS_QUANTSTACK_PRIMARY_ENABLED",
    "SS_QUANTSTACK_SHADOW_ENABLED",
    "SS_QUANTSTACK_BARS_DATASET_ID",
    "SS_QUANTSTACK_DATASET_VERSION",
    "SS_QUANTSTACK_ALLOW_CANDIDATE",
)


def _restore_ss_environment(path: Path, backup: Path) -> None:
    """Restore only the qfq pin owned by this release workflow."""
    previous_stat = path.stat()
    lines, _ = _read_env(path)
    _, backup_values = _read_env(backup)
    restored: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            key = line.split("=", 1)[0]
            if key in _SS_PIN_KEYS:
                if key not in seen and key in backup_values:
                    restored.append(f"{key}={backup_values[key]}")
                seen.add(key)
                continue
        restored.append(line)
    restored.extend(
        f"{key}={backup_values[key]}"
        for key in _SS_PIN_KEYS
        if key not in seen and key in backup_values
    )
    atomic_replace_bytes(path, ("\n".join(restored) + "\n").encode())
    _preserve_existing_access(path, mode=previous_stat.st_mode, gid=previous_stat.st_gid)


def _pin_ss_environment(path: Path, snapshot_dir: Path, dataset_id: str, version: str) -> str:
    previous_stat = path.stat()
    lines, previous = _read_env(path)
    current_version = previous.get("SS_QUANTSTACK_DATASET_VERSION")
    if not current_version:
        raise ValueError("SS environment has no exact QuantStack dataset version")
    backup = snapshot_dir / "ss/.env.docker"
    if backup.is_file():
        _, backup_values = _read_env(backup)
        previous_version = backup_values.get("SS_QUANTSTACK_DATASET_VERSION")
        if not previous_version or current_version not in {previous_version, version}:
            raise ValueError("SS environment no longer matches this qfq release")
    else:
        previous_version = current_version
        atomic_write_immutable_bytes(
            backup,
            path.read_bytes(),
            artifact_kind="qfq release SS environment rollback",
        )
    replacements = {
        "SS_QUANTSTACK_READ_MODE": "quantstack_primary",
        "SS_QUANTSTACK_PRIMARY_ENABLED": "true",
        "SS_QUANTSTACK_SHADOW_ENABLED": "false",
        "SS_QUANTSTACK_BARS_DATASET_ID": dataset_id,
        "SS_QUANTSTACK_DATASET_VERSION": version,
        "SS_QUANTSTACK_ALLOW_CANDIDATE": "false",
    }
    updated: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            key = line.split("=", 1)[0]
            if key in replacements:
                updated.append(f"{key}={replacements[key]}")
                seen.add(key)
                continue
        updated.append(line)
    updated.extend(f"{key}={value}" for key, value in replacements.items() if key not in seen)
    atomic_replace_bytes(path, ("\n".join(updated) + "\n").encode())
    _preserve_existing_access(path, mode=previous_stat.st_mode, gid=previous_stat.st_gid)
    return previous_version


def promote_isolated_qfq_release(
    *,
    source_data_root: Path,
    central_data_root: Path,
    import_receipt_path: Path,
    allowed_source_commits: Collection[str],
    snapshot_dir: Path,
    expected_dataset_id: str,
    expected_dataset_version: str,
    expected_row_count: int,
    expected_instrument_count: int,
    expected_watermark: date,
    ss_environment_path: Path | None = None,
) -> QFQReleaseResult:
    source = source_data_root.expanduser().resolve()
    central = central_data_root.expanduser().resolve()
    snapshot = snapshot_dir.expanduser().resolve()
    receipt_path = import_receipt_path.expanduser().resolve()
    evidence = WorkerImportEvidence.model_validate_json(receipt_path.read_text(encoding="utf-8"))
    expected_receipt = source / "staging/worker_import_receipts" / f"{evidence.bundle_id}.json"
    if receipt_path != expected_receipt:
        raise PermissionError("qfq release requires an isolated persisted import receipt")
    if (evidence.envelope.dataset_id, evidence.envelope.dataset_version) != (
        expected_dataset_id,
        expected_dataset_version,
    ):
        raise ValueError("qfq release dataset identity does not match the approved release")

    copied = 0
    with _release_lock(central):
        read_gid = (central / "catalog").stat().st_gid
        for artifact in evidence.envelope.artifacts:
            destination = central / artifact.relative_path
            copied += int(
                _copy_immutable(
                    source / artifact.relative_path,
                    destination,
                    size_bytes=artifact.size_bytes,
                    sha256=artifact.sha256,
                )
            )
            _grant_catalog_read_access(destination, data_root=central, read_gid=read_gid)
        central_receipt = central / "staging/worker_import_receipts" / receipt_path.name
        receipt_payload = receipt_path.read_bytes()
        atomic_write_immutable_bytes(
            central_receipt,
            receipt_payload,
            artifact_kind="qfq release worker import receipt",
            validate=lambda candidate: WorkerImportEvidence.model_validate_json(
                candidate.read_text(encoding="utf-8")
            ),
        )
        _grant_catalog_read_access(central_receipt, data_root=central, read_gid=read_gid)
        previous_bundle_id = _snapshot_catalog(
            central,
            snapshot,
            expected_published_bundle_id=evidence.publication_bundle_id,
        )
        try:
            with _catalog_atomic_permissions(central, read_gid=read_gid):
                receipt = publish_imported_worker_bundle(
                    data_root=central,
                    import_receipt_path=central_receipt,
                    allowed_source_commits=allowed_source_commits,
                )
                replay = CatalogPublisher(central).replay_receipt(receipt)
            if not replay.verified or not replay.worker_import_verified:
                raise ValueError("qfq release Catalog receipt replay failed")
            published_paths = [
                central / "catalog/quant.duckdb",
                central / "catalog/current-manifest-set.json",
                central / "catalog/publication_bundles" / f"{receipt.bundle_id}.json",
                central / "catalog/publication_receipts" / f"{receipt.bundle_id}.json",
                *(central / uri.removeprefix("quant-data://") for uri in receipt.manifest_uris),
            ]
            for published_path in published_paths:
                _grant_catalog_read_access(
                    published_path,
                    data_root=central,
                    read_gid=read_gid,
                )
            manifests = [
                DatasetManifest.model_validate_json(
                    (central / uri.removeprefix("quant-data://")).read_text(encoding="utf-8")
                )
                for uri in receipt.manifest_uris
            ]
            manifest = next(
                item
                for item in manifests
                if (item.dataset_id, item.version)
                == (expected_dataset_id, expected_dataset_version)
            )
            row_count, instrument_count, first_date, last_date = _profile_manifest(
                central, manifest
            )
            if (
                row_count != expected_row_count
                or instrument_count != expected_instrument_count
                or last_date != expected_watermark
            ):
                raise ValueError("published qfq profile does not match the approved release")
            ss_previous_version = (
                _pin_ss_environment(
                    ss_environment_path.expanduser().resolve(),
                    snapshot,
                    expected_dataset_id,
                    expected_dataset_version,
                )
                if ss_environment_path is not None
                else None
            )
        except Exception:
            ss_backup = snapshot / "ss/.env.docker"
            restore_qfq_release(
                central_data_root=central,
                snapshot_dir=snapshot,
                ss_environment_path=(
                    ss_environment_path
                    if ss_environment_path is not None and ss_backup.is_file()
                    else None
                ),
                lock=False,
            )
            raise
    return QFQReleaseResult(
        receipt=receipt,
        snapshot_dir=snapshot,
        previous_bundle_id=previous_bundle_id,
        dataset_id=expected_dataset_id,
        dataset_version=expected_dataset_version,
        row_count=row_count,
        instrument_count=instrument_count,
        first_date=first_date,
        last_date=last_date,
        copied_artifacts=copied,
        ss_previous_version=ss_previous_version,
    )


def restore_qfq_release(
    *,
    central_data_root: Path,
    snapshot_dir: Path,
    ss_environment_path: Path | None = None,
    lock: bool = True,
) -> dict[str, object]:
    central = central_data_root.expanduser().resolve()
    snapshot = snapshot_dir.expanduser().resolve()

    def restore() -> dict[str, object]:
        catalog_gid = (central / "catalog").stat().st_gid
        source_database = snapshot / "catalog/quant.duckdb"
        source_pointer = snapshot / "catalog/current-manifest-set.json"
        if not source_database.is_file() or not source_pointer.is_file():
            raise FileNotFoundError("qfq release rollback snapshot is incomplete")
        with _catalog_atomic_permissions(central, read_gid=catalog_gid):
            atomic_replace_file(
                central / "catalog/quant.duckdb",
                write=lambda temporary: shutil.copyfile(source_database, temporary),
                validate=_validate_catalog_database,
            )
            atomic_replace_bytes(
                central / "catalog/current-manifest-set.json",
                source_pointer.read_bytes(),
                validate=lambda candidate: json.loads(candidate.read_text(encoding="utf-8")),
            )
        _grant_catalog_read_access(
            central / "catalog/quant.duckdb", data_root=central, read_gid=catalog_gid
        )
        _grant_catalog_read_access(
            central / "catalog/current-manifest-set.json",
            data_root=central,
            read_gid=catalog_gid,
        )
        ss_restored = False
        if ss_environment_path is not None:
            ss_backup = snapshot / "ss/.env.docker"
            if not ss_backup.is_file():
                raise FileNotFoundError("qfq release SS rollback snapshot is missing")
            ss_path = ss_environment_path.expanduser().resolve()
            _restore_ss_environment(ss_path, ss_backup)
            ss_restored = True
        pointer = json.loads(source_pointer.read_text(encoding="utf-8"))
        return {
            "schema_version": "quantstack.qfq-release-rollback.v1",
            "status": "restored",
            "restored_bundle_id": pointer["bundle_id"],
            "ss_environment_restored": ss_restored,
            "immutable_candidate_retained": True,
        }

    if not lock:
        return restore()
    with _release_lock(central):
        return restore()
