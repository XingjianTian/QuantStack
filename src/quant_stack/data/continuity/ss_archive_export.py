from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ..storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file
from .ss_archive_models import (
    ArchiveRunState,
    BackupStatus,
    RestoreStatus,
    SSArchiveArtifactEvidence,
    SSArchiveFile,
    SSDatabaseInventory,
    SSLegacyArchiveManifest,
    SSRestrictedBackupEvidence,
)
from .ss_archive_safety import (
    ArchiveLayout,
    scan_secret_text,
    sha256_file,
    validate_identifier,
    validate_schema_listing,
)
from .ss_archive_state import ArchiveStateStore


def _pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("SS archive export requires the 'archive' or 'data' extra") from exc
    return pa, pq


def arrow_schema_for_postgres_columns(
    columns: Sequence[tuple[str, str]],
    *,
    metadata: Mapping[str, str],
):
    pa, _ = _pyarrow()
    mappings = {
        "smallint": pa.int16(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "real": pa.float32(),
        "double precision": pa.float64(),
        "numeric": pa.decimal128(38, 12),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp without time zone": pa.timestamp("us"),
        "timestamp with time zone": pa.timestamp("us", tz="UTC"),
        "character varying": pa.string(),
        "text": pa.string(),
        "uuid": pa.string(),
        "json": pa.string(),
        "jsonb": pa.string(),
    }
    fields = []
    for name, postgres_type in columns:
        validate_identifier(name)
        if postgres_type not in mappings:
            raise ValueError(
                f"PostgreSQL type requires an explicit Parquet mapping: {postgres_type}"
            )
        fields.append(pa.field(name, mappings[postgres_type]))
    encoded_metadata = {key.encode(): value.encode() for key, value in metadata.items()}
    return pa.schema(fields, metadata=encoded_metadata)


class StreamingParquetExporter:
    """Bounded-memory, stable-order Parquet shard writer with immutable final paths."""

    def __init__(
        self,
        layout: ArchiveLayout,
        *,
        table_name: str,
        columns: Sequence[tuple[str, str]],
        primary_key: Sequence[str],
        shard_rows: int = 250_000,
        compression: str = "zstd",
    ) -> None:
        if shard_rows < 1:
            raise ValueError("shard_rows must be positive")
        validate_identifier(table_name)
        if not primary_key:
            raise ValueError("a stable primary key is required for frozen export")
        self.layout = layout
        self.table_name = table_name
        self.column_names = tuple(item[0] for item in columns)
        self.primary_key = tuple(validate_identifier(item) for item in primary_key)
        if not set(self.primary_key).issubset(self.column_names):
            raise ValueError("primary key columns must be exported")
        self.shard_rows = shard_rows
        self.compression = compression
        self.schema = arrow_schema_for_postgres_columns(
            columns,
            metadata={
                "archive_id": layout.archive_id,
                "source_system": "stock_screener",
                "source_table": table_name,
                "lineage_quality": "legacy",
                "canonical_eligible": "false",
                "quarantined": "true",
                "volume_unit": "unknown_or_mixed",
                "compression": compression,
            },
        )

    def _key(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        return tuple(row[name] for name in self.primary_key)

    def write(
        self, row_batches: Iterable[Iterable[Mapping[str, Any]]]
    ) -> tuple[SSArchiveFile, ...]:
        buffer: list[dict[str, Any]] = []
        files: list[SSArchiveFile] = []
        previous_key: tuple[Any, ...] | None = None
        shard_number = 0
        saw_rows = False
        for batch in row_batches:
            for source_row in batch:
                row = {name: source_row[name] for name in self.column_names}
                key = self._key(row)
                if any(value is None for value in key):
                    raise ValueError("primary key cannot be null in frozen export")
                if previous_key is not None and key <= previous_key:
                    raise ValueError("source rows are not strictly ordered by primary key")
                previous_key = key
                saw_rows = True
                buffer.append(row)
                if len(buffer) == self.shard_rows:
                    files.append(self._write_shard(shard_number, buffer))
                    shard_number += 1
                    buffer = []
        if buffer or not saw_rows:
            files.append(self._write_shard(shard_number, buffer))
        return tuple(files)

    def _write_shard(self, shard_number: int, rows: list[dict[str, Any]]) -> SSArchiveFile:
        pa, pq = _pyarrow()
        relative_name = f"part-{shard_number:05d}.parquet"
        final_path = self.layout.archive_path("frozen-exports", self.table_name, relative_name)

        def write(path: Path) -> None:
            table = pa.Table.from_pylist(rows, schema=self.schema)
            pq.write_table(
                table,
                path,
                compression=self.compression,
                use_dictionary=True,
                write_statistics=True,
                row_group_size=min(self.shard_rows, 64_000),
            )

        def validate(path: Path) -> None:
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows != len(rows):
                raise ValueError("Parquet readback row count mismatch")
            if tuple(parquet.schema_arrow.names) != self.column_names:
                raise ValueError("Parquet readback schema mismatch")

        atomic_write_immutable_file(
            final_path,
            write=write,
            validate=validate,
            equivalent=lambda first, second: sha256_file(first) == sha256_file(second),
            artifact_kind=f"{self.table_name} frozen export shard",
        )
        parquet = pq.ParquetFile(final_path)
        date_values = [row.get("date") for row in rows if isinstance(row.get("date"), date)]
        schema_hash = hashlib.sha256(str(parquet.schema_arrow).encode()).hexdigest()
        return SSArchiveFile(
            logical_uri=self.layout.logical_uri("frozen-exports", self.table_name, relative_name),
            table_name=self.table_name,
            row_count=parquet.metadata.num_rows,
            size_bytes=final_path.stat().st_size,
            sha256=sha256_file(final_path),
            min_date=min(date_values) if date_values else None,
            max_date=max(date_values) if date_values else None,
            columns=tuple(parquet.schema_arrow.names),
            parquet_schema_sha256=schema_hash,
        )


def write_archive_manifest(
    layout: ArchiveLayout, manifest: SSLegacyArchiveManifest
) -> tuple[Path, bool]:
    if manifest.archive_id != layout.archive_id:
        raise ValueError("manifest archive ID does not match destination")
    path = layout.archive_path("frozen-exports", "manifest.json")
    payload = (
        json.dumps(manifest.model_dump(mode="json"), sort_keys=True, indent=2) + "\n"
    ).encode()
    created = atomic_write_immutable_bytes(
        path,
        payload,
        artifact_kind="SS legacy archive manifest",
        validate=lambda value: SSLegacyArchiveManifest.model_validate_json(
            value.read_text(encoding="utf-8")
        ),
    )
    digest = hashlib.sha256(payload).hexdigest()
    checksum_path = layout.archive_path("frozen-exports", "manifest.json.sha256")
    atomic_write_immutable_bytes(
        checksum_path,
        f"{digest}  manifest.json\n".encode(),
        artifact_kind="SS legacy archive manifest checksum",
        validate=lambda value: _validate_manifest_checksum(path, value),
    )
    return path, created


def _validate_manifest_checksum(manifest_path: Path, checksum_path: Path) -> str:
    fields = checksum_path.read_text(encoding="utf-8").strip().split()
    if len(fields) != 2 or fields[1] != "manifest.json":
        raise ValueError("manifest checksum sidecar is malformed")
    if fields[0] != sha256_file(manifest_path):
        raise ValueError("manifest checksum sidecar does not match manifest")
    return fields[0]


def archive_artifact_evidence(
    layout: ArchiveLayout, *, section: str, relative_path: str
) -> SSArchiveArtifactEvidence:
    path = layout.archive_path(section, relative_path)
    if not path.is_file():
        raise ValueError("archive evidence file is missing")
    return SSArchiveArtifactEvidence(
        logical_uri=layout.logical_uri(section, relative_path),
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
    )


def verify_archive_manifest(layout: ArchiveLayout, manifest: SSLegacyArchiveManifest) -> None:
    _, pq = _pyarrow()
    table_rows: dict[str, int] = {}
    previous_keys: dict[str, tuple[Any, ...]] = {}
    coverage_price_rows = 0
    observed_paths: set[Path] = set()
    inventories = {item.table_name: item for item in manifest.table_inventories}
    ordered_files = sorted(manifest.files, key=lambda item: item.logical_uri)
    for item in ordered_files:
        marker = "archive://ss-legacy/"
        if not item.logical_uri.startswith(marker):
            raise ValueError("unexpected archive logical URI")
        relative = item.logical_uri.removeprefix(marker)
        path = layout.resolve(relative)
        observed_paths.add(path)
        if not path.is_file() or path.stat().st_size != item.size_bytes:
            raise ValueError(f"archive file size mismatch: {item.logical_uri}")
        if sha256_file(path) != item.sha256:
            raise ValueError(f"archive file hash mismatch: {item.logical_uri}")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != item.row_count:
            raise ValueError(f"archive file row count mismatch: {item.logical_uri}")
        if tuple(parquet.schema_arrow.names) != item.columns:
            raise ValueError(f"archive file schema mismatch: {item.logical_uri}")
        schema_hash = hashlib.sha256(str(parquet.schema_arrow).encode()).hexdigest()
        if item.parquet_schema_sha256 != schema_hash:
            raise ValueError(f"archive file schema hash mismatch: {item.logical_uri}")
        metadata = {
            key.decode(): value.decode()
            for key, value in (parquet.schema_arrow.metadata or {}).items()
        }
        required_metadata = {
            "archive_id": manifest.archive_id,
            "source_system": "stock_screener",
            "source_table": item.table_name,
            "lineage_quality": "legacy",
            "canonical_eligible": "false",
            "quarantined": "true",
            "volume_unit": "unknown_or_mixed",
        }
        if any(metadata.get(key) != value for key, value in required_metadata.items()):
            raise ValueError(f"archive file lineage metadata mismatch: {item.logical_uri}")
        primary_key = (
            ("market", "symbol")
            if item.table_name == "coverage"
            else inventories[item.table_name].primary_key
        )
        columns = list(primary_key)
        if item.table_name == "stock_prices":
            columns.append("date")
        elif item.table_name == "coverage":
            columns.extend(("min_date", "max_date", "row_count"))
        observed_min: date | None = None
        observed_max: date | None = None
        for batch in parquet.iter_batches(columns=columns, batch_size=64_000):
            for row in batch.to_pylist():
                key = tuple(row[name] for name in primary_key)
                if any(value is None for value in key):
                    raise ValueError(f"archive primary key is null: {item.logical_uri}")
                previous = previous_keys.get(item.table_name)
                if previous is not None and key <= previous:
                    raise ValueError(f"archive primary key order is unstable: {item.logical_uri}")
                previous_keys[item.table_name] = key
                if item.table_name == "stock_prices":
                    value = row["date"]
                    observed_min = value if observed_min is None else min(observed_min, value)
                    observed_max = value if observed_max is None else max(observed_max, value)
                elif item.table_name == "coverage":
                    coverage_price_rows += int(row["row_count"])
        if item.table_name == "stock_prices" and (
            observed_min != item.min_date or observed_max != item.max_date
        ):
            raise ValueError(f"archive file date coverage mismatch: {item.logical_uri}")
        table_rows[item.table_name] = table_rows.get(item.table_name, 0) + item.row_count
    archive_root = layout.archive_path("frozen-exports")
    actual_paths = set(archive_root.glob("**/*.parquet"))
    if actual_paths != observed_paths:
        raise ValueError("manifest Parquet inventory does not match archive files")
    expected = {table.table_name: table.exact_row_count for table in manifest.table_inventories}
    for table_name in ("stock_prices", "stock_universe"):
        if table_rows.get(table_name) != expected.get(table_name):
            raise ValueError(f"archive total row count mismatch: {table_name}")
    if coverage_price_rows != expected.get("stock_prices"):
        raise ValueError("per-symbol coverage does not reconcile to stock_prices")
    price_files = tuple(item for item in manifest.files if item.table_name == "stock_prices")
    if price_files:
        observed_min = min(item.min_date for item in price_files if item.min_date is not None)
        observed_max = max(item.max_date for item in price_files if item.max_date is not None)
        if observed_min != manifest.coverage.min_date or observed_max != manifest.coverage.max_date:
            raise ValueError("manifest date coverage does not reconcile")


class SSArchiveVerifier:
    """Independently verify every persisted component of one frozen SS archive."""

    def __init__(
        self,
        layout: ArchiveLayout,
        *,
        pg_restore: str = "pg_restore",
        environment: dict[str, str] | None = None,
    ) -> None:
        self.layout = layout
        self.pg_restore = pg_restore
        inherited = os.environ if environment is None else environment
        self.environment = {
            key: value for key, value in inherited.items() if not key.startswith("PG")
        }

    def _artifact_path(self, evidence: SSArchiveArtifactEvidence) -> Path:
        marker = "archive://ss-legacy/"
        if not evidence.logical_uri.startswith(marker):
            raise ValueError("archive artifact URI is invalid")
        relative = evidence.logical_uri.removeprefix(marker)
        parts = Path(relative).parts
        if len(parts) < 3 or parts[1] != self.layout.archive_id:
            raise ValueError("archive artifact URI belongs to a different archive")
        path = self.layout.resolve(relative)
        if not path.is_file() or path.stat().st_size != evidence.size_bytes:
            raise ValueError("archive artifact size evidence does not match")
        if sha256_file(path) != evidence.sha256:
            raise ValueError("archive artifact hash evidence does not match")
        return path

    def _run_pg_restore(self, arguments: list[str], *, stdin: bytes | None = None) -> str:
        result = subprocess.run(
            [self.pg_restore, *arguments],
            input=stdin,
            env=self.environment,
            check=False,
            capture_output=True,
        )
        if result.returncode:
            raise RuntimeError("schema archive verification subprocess failed: pg_restore")
        return result.stdout.decode(errors="strict")

    def _verify_inventory(
        self, manifest: SSLegacyArchiveManifest
    ) -> SSDatabaseInventory:
        path = self._artifact_path(manifest.inventory_artifact)
        inventory = SSDatabaseInventory.model_validate_json(path.read_text(encoding="utf-8"))
        comparable = (
            inventory.archive_id == manifest.archive_id,
            inventory.snapshot_fingerprint == manifest.snapshot_fingerprint,
            inventory.source_commit == manifest.source_commit,
            inventory.source_schema_revision == manifest.source_schema_revision,
            inventory.postgresql_version == manifest.postgresql_version,
            inventory.tables == manifest.table_inventories,
            inventory.coverage == manifest.coverage,
        )
        if not all(comparable):
            raise ValueError("inventory and manifest evidence do not reconcile")
        return inventory

    def _verify_backup_evidence(
        self, manifest: SSLegacyArchiveManifest
    ) -> SSRestrictedBackupEvidence:
        path = self._artifact_path(manifest.backup_evidence_artifact)
        evidence = SSRestrictedBackupEvidence.model_validate_json(path.read_text(encoding="utf-8"))
        if evidence != manifest.backup:
            raise ValueError("persisted backup evidence does not match manifest")
        return evidence

    def _verify_schema(self, backup: SSRestrictedBackupEvidence) -> None:
        schema = backup.schema_snapshot
        if schema is None:
            raise ValueError("archive has no schema snapshot evidence")
        path = self._artifact_path(
            SSArchiveArtifactEvidence(
                logical_uri=schema.logical_uri,
                size_bytes=schema.size_bytes,
                sha256=schema.sha256,
            )
        )
        payload = path.read_bytes()
        listing = self._run_pg_restore(["--list"], stdin=payload)
        entry_count, table_data_count = validate_schema_listing(listing)
        if (
            entry_count != schema.archive_entry_count
            or table_data_count != schema.table_data_entry_count
        ):
            raise ValueError("schema listing evidence changed")
        schema_sql = self._run_pg_restore(
            ["--schema-only", "--no-owner", "--no-privileges", "--file=-"],
            stdin=payload,
        )
        if scan_secret_text(f"{listing}\n{schema_sql}", schema_only=True):
            raise ValueError("schema snapshot secret scan failed")

    def _verify_backup_payload(self, backup: SSRestrictedBackupEvidence) -> None:
        encrypted_root = self.layout.archive_path("backups", "encrypted")
        table_data_root = self.layout.archive_path("backups", "table-data")
        encrypted_files = tuple(path for path in encrypted_root.glob("**/*") if path.is_file())
        plaintext_files = tuple(path for path in table_data_root.glob("**/*") if path.is_file())
        backup_root = self.layout.archive_path("backups")
        forbidden_plaintext = tuple(
            path
            for path in backup_root.glob("**/*")
            if path.is_file()
            and (
                path.suffix.lower() == ".tar"
                or path.name in {"required-table-data.dump", "restricted-backup.tar"}
            )
        )
        if backup.status == BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE:
            if (
                backup.logical_uri is not None
                or encrypted_files
                or plaintext_files
                or forbidden_plaintext
            ):
                raise ValueError("blocked backup references or retains table data")
            return
        if backup.status == BackupStatus.VERIFIED_ENCRYPTED:
            if backup.logical_uri is None or not backup.decryptability_verified:
                raise ValueError("verified backup lacks decryptability evidence")
            path = self._artifact_path(
                SSArchiveArtifactEvidence(
                    logical_uri=backup.logical_uri,
                    size_bytes=backup.encrypted_size_bytes or 0,
                    sha256=backup.encrypted_sha256 or "0" * 64,
                )
            )
            if path not in encrypted_files or plaintext_files:
                raise ValueError("encrypted backup file inventory is inconsistent")

    def _verify_state(self, manifest: SSLegacyArchiveManifest) -> None:
        events = ArchiveStateStore(self.layout).events()
        states = [event.state for event in events]
        try:
            planned = states.index(ArchiveRunState.PLANNED)
            first_open = states.index(ArchiveRunState.SNAPSHOT_OPEN, planned + 1)
            inventory = states.index(ArchiveRunState.INVENTORY_COMPLETE, first_open + 1)
            export = states.index(ArchiveRunState.EXPORT_COMPLETE, inventory + 1)
            closed = states.index(ArchiveRunState.SNAPSHOT_CLOSED, export + 1)
            verified = states.index(ArchiveRunState.VERIFICATION_COMPLETE, closed + 1)
            pending = states.index(
                ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING, verified + 1
            )
        except ValueError as exc:
            raise ValueError("archive state is missing required lifecycle evidence") from exc
        if not planned < first_open < inventory < export < closed < verified < pending:
            raise ValueError("archive state chronology contradicts execution")
        lifecycle_events = events[planned : pending + 1]
        if manifest.consistency.value == "exported_snapshot":
            if len({event.attempt_id for event in lifecycle_events}) != 1:
                raise ValueError("exported-snapshot lifecycle spans multiple attempts")
            lifecycle_counts = {
                state: states.count(state)
                for state in (
                    ArchiveRunState.PLANNED,
                    ArchiveRunState.SNAPSHOT_OPEN,
                    ArchiveRunState.INVENTORY_COMPLETE,
                    ArchiveRunState.EXPORT_COMPLETE,
                    ArchiveRunState.SNAPSHOT_CLOSED,
                    ArchiveRunState.VERIFICATION_COMPLETE,
                    ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
                )
            }
            if any(count != 1 for count in lifecycle_counts.values()):
                raise ValueError("exported-snapshot archive has an extra snapshot lifecycle")
        if manifest.restore_status == RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE:
            expected_terminal = ArchiveRunState.BLOCKED
        elif manifest.restore_status == RestoreStatus.VERIFIED:
            expected_terminal = ArchiveRunState.COMPLETED
        else:
            expected_terminal = ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING
        if not states or states[-1] != expected_terminal:
            raise ValueError("archive terminal state does not match manifest")

    def verify(self, *, verify_state: bool = True) -> SSLegacyArchiveManifest:
        manifest_path = self.layout.archive_path("frozen-exports", "manifest.json")
        checksum_path = self.layout.archive_path("frozen-exports", "manifest.json.sha256")
        if not manifest_path.is_file() or not checksum_path.is_file():
            raise ValueError("archive manifest or detached checksum is missing")
        _validate_manifest_checksum(manifest_path, checksum_path)
        manifest = SSLegacyArchiveManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        inventory = self._verify_inventory(manifest)
        backup = self._verify_backup_evidence(manifest)
        self._verify_schema(backup)
        self._verify_backup_payload(backup)
        if inventory.connection_fingerprint != backup.connection_fingerprint:
            raise ValueError("inventory and backup source connections do not match")
        verify_archive_manifest(self.layout, manifest)
        if verify_state:
            self._verify_state(manifest)
        return manifest
