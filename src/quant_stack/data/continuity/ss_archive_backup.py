from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from ..storage.atomic import atomic_write_immutable_file
from .ss_archive_connection import (
    postgres_connection_fingerprint,
    postgres_parameters_from_environment,
)
from .ss_archive_models import (
    BackupStatus,
    SSRestrictedBackupEvidence,
    SSSchemaSnapshotEvidence,
)
from .ss_archive_safety import (
    ArchiveLayout,
    allowed_backup_tables,
    scan_secret_text,
    sha256_file,
    validate_backup_listing,
    validate_schema_listing,
)


class EncryptionRecipientUnavailable(RuntimeError):
    """The safety gate forbids retaining a plaintext PostgreSQL backup."""


class RestrictedBackupRunner:
    def __init__(
        self,
        layout: ArchiveLayout,
        *,
        environment: dict[str, str] | None = None,
        connection_fingerprint: str | None = None,
        pg_dump: str = "pg_dump",
        pg_restore: str = "pg_restore",
        gpg: str = "gpg",
    ) -> None:
        self.layout = layout
        self.environment = dict(os.environ if environment is None else environment)
        self.connection_fingerprint = connection_fingerprint
        self.pg_dump = pg_dump
        self.pg_restore = pg_restore
        self.gpg = gpg

    def _run(
        self, command: list[str], *, capture: bool = False
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            command,
            env=self.environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if result.returncode:
            executable = Path(command[0]).name
            raise RuntimeError(f"restricted archive subprocess failed: {executable}")
        return result

    def run(
        self,
        *,
        snapshot_id: str,
        inventory_tables: tuple[str, ...],
        expected_connection_fingerprint: str,
    ) -> SSRestrictedBackupEvidence:
        environment_fingerprint = postgres_connection_fingerprint(
            postgres_parameters_from_environment(self.environment)
        )
        if (
            self.connection_fingerprint is None
            or self.connection_fingerprint != expected_connection_fingerprint
            or environment_fingerprint != expected_connection_fingerprint
        ):
            raise ValueError("backup source connection does not match inventory snapshot")
        tables = allowed_backup_tables(inventory_tables)
        snapshot_fingerprint = hashlib.sha256(snapshot_id.encode()).hexdigest()
        encrypted_destination = self.layout.archive_path(
            "backups", "encrypted", "legacy-required-tables.tar.gpg"
        )
        schema_destination = self.layout.archive_path("backups", "schema", "full-schema.dump")
        encrypted_destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        schema_destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary_root = Path(
            tempfile.mkdtemp(
                prefix=".backup-staging-",
                dir=self.layout.archive_path("backups"),
            )
        )
        os.chmod(temporary_root, 0o700)
        schema_dump = temporary_root / "schema.dump"
        data_dump = temporary_root / "required-table-data.dump"
        bundle = temporary_root / "restricted-backup.tar"
        encrypted = temporary_root / "restricted-backup.tar.gpg"
        try:
            common = [
                "--format=custom",
                "--no-owner",
                "--no-privileges",
                f"--snapshot={snapshot_id}",
            ]
            self._run([self.pg_dump, *common, "--schema-only", f"--file={schema_dump}"])
            schema_listing = self._run(
                [self.pg_restore, "--list", str(schema_dump)], capture=True
            ).stdout
            archive_entry_count, table_data_entry_count = validate_schema_listing(
                schema_listing,
                required_tables=tables,
            )
            schema_sql = self._run(
                [
                    self.pg_restore,
                    "--schema-only",
                    "--no-owner",
                    "--no-privileges",
                    "--file=-",
                    str(schema_dump),
                ],
                capture=True,
            ).stdout
            findings = scan_secret_text(f"{schema_listing}\n{schema_sql}", schema_only=True)
            if findings:
                raise ValueError("schema listing secret scan failed")
            schema_hash = sha256_file(schema_dump)
            atomic_write_immutable_file(
                schema_destination,
                write=lambda path: shutil.copyfile(schema_dump, path),
                validate=lambda path: (
                    None
                    if sha256_file(path) == schema_hash
                    else (_ for _ in ()).throw(ValueError("schema snapshot hash mismatch"))
                ),
                equivalent=lambda first, second: sha256_file(first) == sha256_file(second),
                artifact_kind="schema-only PostgreSQL snapshot",
            )
            pg_dump_version = self._run([self.pg_dump, "--version"], capture=True).stdout.strip()
            schema_evidence = SSSchemaSnapshotEvidence(
                logical_uri=self.layout.logical_uri("backups", "schema", "full-schema.dump"),
                sha256=schema_hash,
                size_bytes=schema_destination.stat().st_size,
                archive_entry_count=archive_entry_count,
                table_data_entry_count=table_data_entry_count,
                required_table_schema_entries=True,
                secret_scan_findings=findings,
                snapshot_fingerprint=snapshot_fingerprint,
                connection_fingerprint=self.connection_fingerprint,
            )
            recipient = self.environment.get("SS_LEGACY_ARCHIVE_RECIPIENT")
            if not recipient:
                return SSRestrictedBackupEvidence(
                    status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
                    snapshot_fingerprint=snapshot_fingerprint,
                    connection_fingerprint=self.connection_fingerprint,
                    schema_snapshot=schema_evidence,
                    schema_dump_sha256=schema_hash,
                    plaintext_deleted=True,
                    required_tables_listed=False,
                    secret_table_data_absent=True,
                    decryptability_verified=False,
                    pg_dump_version=pg_dump_version,
                )
            data_command = [self.pg_dump, *common, "--data-only"]
            for table in tables:
                data_command.extend(("--table", f"public.{table}"))
            data_command.append(f"--file={data_dump}")
            self._run(data_command)
            listing = self._run([self.pg_restore, "--list", str(data_dump)], capture=True).stdout
            validate_backup_listing(listing, required_tables=tables)
            with tarfile.open(bundle, "w") as archive:
                archive.add(schema_dump, arcname="schema.dump", recursive=False)
                archive.add(data_dump, arcname="required-table-data.dump", recursive=False)
            plaintext_hash = sha256_file(bundle)
            self._run(
                [
                    self.gpg,
                    "--batch",
                    "--yes",
                    "--trust-model",
                    "always",
                    "--recipient",
                    recipient,
                    "--output",
                    str(encrypted),
                    "--encrypt",
                    str(bundle),
                ]
            )
            self._run(
                [
                    self.gpg,
                    "--batch",
                    "--quiet",
                    "--output",
                    os.devnull,
                    "--decrypt",
                    str(encrypted),
                ]
            )
            encrypted_hash = sha256_file(encrypted)
            atomic_write_immutable_file(
                encrypted_destination,
                write=lambda path: shutil.copyfile(encrypted, path),
                validate=lambda path: (
                    None
                    if sha256_file(path) == encrypted_hash
                    else (_ for _ in ()).throw(ValueError("encrypted backup hash mismatch"))
                ),
                equivalent=lambda first, second: sha256_file(first) == sha256_file(second),
                artifact_kind="encrypted restricted PostgreSQL backup",
            )
            return SSRestrictedBackupEvidence(
                status=BackupStatus.VERIFIED_ENCRYPTED,
                logical_uri=self.layout.logical_uri(
                    "backups", "encrypted", "legacy-required-tables.tar.gpg"
                ),
                snapshot_fingerprint=snapshot_fingerprint,
                connection_fingerprint=self.connection_fingerprint,
                schema_snapshot=schema_evidence,
                schema_dump_sha256=schema_hash,
                plaintext_sha256=plaintext_hash,
                encrypted_sha256=encrypted_hash,
                encrypted_size_bytes=encrypted_destination.stat().st_size,
                plaintext_deleted=True,
                required_tables_listed=True,
                secret_table_data_absent=True,
                decryptability_verified=True,
                pg_dump_version=pg_dump_version,
            )
        finally:
            shutil.rmtree(temporary_root, ignore_errors=True)
