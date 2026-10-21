from __future__ import annotations

import re
from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from ..models import DataModel
from .stock_screener import SS_LEGACY_EXPORT_DATASET_ID


class SnapshotConsistency(StrEnum):
    EXPORTED_SNAPSHOT = "exported_snapshot"
    BEST_EFFORT = "best_effort"


class EvidenceStatus(StrEnum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


class SourceHistoryStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class BackupStatus(StrEnum):
    VERIFIED_ENCRYPTED = "verified_encrypted"
    BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE = "blocked_encryption_recipient_unavailable"
    FAILED = "failed"
    NOT_RUN = "not_run"


class RestoreStatus(StrEnum):
    VERIFIED = "verified"
    LISTING_VERIFIED_RESTORE_PENDING = "listing_verified_restore_pending"
    BLOCKED_BACKUP_UNAVAILABLE = "blocked_backup_unavailable"
    FAILED = "failed"
    NOT_RUN = "not_run"


class ArchiveRunState(StrEnum):
    PLANNED = "planned"
    SNAPSHOT_OPEN = "snapshot_open"
    INVENTORY_COMPLETE = "inventory_complete"
    BACKUP_STAGED = "backup_staged"
    BACKUP_ENCRYPTED = "backup_encrypted"
    EXPORT_COMPLETE = "export_complete"
    SNAPSHOT_CLOSED = "snapshot_closed"
    VERIFICATION_COMPLETE = "verification_complete"
    VERIFICATION_COMPLETE_RESTORE_PENDING = "verification_complete_restore_pending"
    RESTORE_VERIFIED = "restore_verified"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class SSArchiveColumn(DataModel):
    ordinal_position: int = Field(ge=1)
    name: str = Field(min_length=1)
    data_type: str = Field(min_length=1)
    nullable: bool
    default_expression: str | None = None
    domain_name: str | None = None
    enum_values: tuple[str, ...] = ()


class SSArchiveConstraint(DataModel):
    name: str = Field(min_length=1)
    kind: Literal["primary_key", "unique", "foreign_key", "check"]
    columns: tuple[str, ...] = ()
    referenced_table: str | None = None
    referenced_columns: tuple[str, ...] = ()


class SSArchiveIndex(DataModel):
    name: str = Field(min_length=1)
    columns: tuple[str, ...]
    unique: bool
    primary: bool = False
    size_bytes: int = Field(ge=0)


class SSArchiveTableInventory(DataModel):
    schema_name: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    estimated_row_count: int = Field(ge=0)
    exact_row_count: int = Field(ge=0)
    table_size_bytes: int = Field(ge=0)
    index_size_bytes: int = Field(ge=0)
    columns: tuple[SSArchiveColumn, ...] = Field(min_length=1)
    constraints: tuple[SSArchiveConstraint, ...] = ()
    indexes: tuple[SSArchiveIndex, ...] = ()
    primary_key: tuple[str, ...] = ()


class SSArchiveTableSummary(DataModel):
    schema_name: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    estimated_row_count: int = Field(ge=0)
    exact_row_count: int = Field(ge=0)
    table_size_bytes: int = Field(ge=0)
    index_size_bytes: int = Field(ge=0)


class SSMarketCoverage(DataModel):
    market: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    distinct_symbols: int = Field(ge=0)
    min_date: date | None = None
    max_date: date | None = None


class SSDataQualityInventory(DataModel):
    primary_key_null_count: int = Field(ge=0)
    primary_key_duplicate_count: int = Field(ge=0)
    symbol_date_duplicate_count: int = Field(ge=0)
    null_counts: dict[str, int]
    negative_price_count: int = Field(ge=0)
    negative_volume_count: int = Field(ge=0)
    zero_volume_count: int = Field(ge=0)
    adj_close_null_count: int = Field(ge=0)
    adj_close_difference_count: int = Field(ge=0)
    volume_quantiles: dict[str, float | None]
    suspicious_volume_jump_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_counts(self) -> SSDataQualityInventory:
        if any(value < 0 for value in self.null_counts.values()):
            raise ValueError("null counts cannot be negative")
        return self


class SSCoverageSummary(DataModel):
    stock_prices_row_count: int = Field(ge=0)
    stock_universe_row_count: int = Field(ge=0)
    distinct_price_symbols: int = Field(ge=0)
    min_date: date | None = None
    max_date: date | None = None
    markets: tuple[SSMarketCoverage, ...]
    active_universe_symbols: int = Field(ge=0)
    non_active_universe_symbols: int = Field(ge=0)
    orphan_price_symbols: int = Field(ge=0)
    universe_symbols_without_prices: int = Field(ge=0)
    delisted_coverage: EvidenceStatus = EvidenceStatus.UNKNOWN
    source_history_export: SourceHistoryStatus = SourceHistoryStatus.UNKNOWN
    market_history_completeness: Literal["not_claimed"] = "not_claimed"
    quality: SSDataQualityInventory
    per_symbol_coverage_uri: str = Field(pattern=r"^archive://")

    @model_validator(mode="after")
    def validate_date_range(self) -> SSCoverageSummary:
        if self.stock_prices_row_count == 0 and (
            self.min_date is not None or self.max_date is not None
        ):
            raise ValueError("empty price coverage cannot declare dates")
        if self.stock_prices_row_count > 0 and (self.min_date is None or self.max_date is None):
            raise ValueError("non-empty price coverage requires dates")
        if self.min_date and self.max_date and self.max_date < self.min_date:
            raise ValueError("max_date cannot be earlier than min_date")
        return self


class SSDatabaseInventory(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    archive_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    database_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    connection_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    postgresql_version: str = Field(min_length=1)
    encoding: str = Field(min_length=1)
    timezone: str = Field(min_length=1)
    snapshot_timestamp: datetime
    snapshot_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    transaction_marker: str = Field(min_length=1)
    consistency: SnapshotConsistency
    source_commit: str = Field(min_length=7)
    source_schema_revision: str = Field(min_length=1)
    all_table_names: tuple[str, ...] = Field(min_length=2)
    all_table_summaries: tuple[SSArchiveTableSummary, ...] = Field(min_length=2)
    type_dependencies: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    tables: tuple[SSArchiveTableInventory, ...] = Field(min_length=2)
    coverage: SSCoverageSummary
    retention_policy: str = Field(min_length=1)
    cleanup_evidence: str = Field(min_length=1)
    inventory_uri: str = Field(pattern=r"^archive://")

    @model_validator(mode="after")
    def require_legacy_tables(self) -> SSDatabaseInventory:
        table_names = {table.table_name for table in self.tables}
        summary_names = {table.table_name for table in self.all_table_summaries}
        required = {"stock_prices", "stock_universe"}
        if not required.issubset(table_names):
            missing = ", ".join(sorted(required - table_names))
            raise ValueError(f"missing required legacy table(s): {missing}")
        if summary_names != set(self.all_table_names):
            raise ValueError("all-table summaries do not match discovered table names")
        if table_names != set(self.all_table_names):
            raise ValueError("detailed table inventories do not match discovered table names")
        if self.snapshot_timestamp.tzinfo is None:
            raise ValueError("snapshot_timestamp must be timezone-aware")
        expected = {table.table_name: table.exact_row_count for table in self.tables}
        if expected["stock_prices"] != self.coverage.stock_prices_row_count:
            raise ValueError("stock_prices inventory and coverage row counts differ")
        if expected["stock_universe"] != self.coverage.stock_universe_row_count:
            raise ValueError("stock_universe inventory and coverage row counts differ")
        return self


class SSArchiveFile(DataModel):
    logical_uri: str = Field(pattern=r"^archive://")
    table_name: Literal["stock_prices", "stock_universe", "coverage", "schema"]
    row_count: int = Field(ge=0)
    size_bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    min_date: date | None = None
    max_date: date | None = None
    columns: tuple[str, ...]
    parquet_schema_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class SSArchiveArtifactEvidence(DataModel):
    logical_uri: str = Field(pattern=r"^archive://")
    size_bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class SSSchemaSnapshotEvidence(DataModel):
    logical_uri: str = Field(pattern=r"^archive://")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(gt=0)
    archive_entry_count: int = Field(ge=1)
    table_data_entry_count: Literal[0] = 0
    required_table_schema_entries: bool
    secret_scan_findings: tuple[str, ...] = ()
    snapshot_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    connection_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def require_safe_schema_snapshot(self) -> SSSchemaSnapshotEvidence:
        if not self.required_table_schema_entries:
            raise ValueError("schema snapshot must list every required table")
        if self.secret_scan_findings:
            raise ValueError("schema snapshot cannot contain secret values")
        return self


class SSRestrictedBackupEvidence(DataModel):
    status: BackupStatus
    logical_uri: str | None = Field(default=None, pattern=r"^archive://")
    snapshot_fingerprint: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    connection_fingerprint: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    schema_snapshot: SSSchemaSnapshotEvidence | None = None
    schema_dump_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    plaintext_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    encrypted_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    encrypted_size_bytes: int | None = Field(default=None, ge=1)
    plaintext_deleted: bool
    required_tables_listed: bool
    secret_table_data_absent: bool
    decryptability_verified: bool
    pg_dump_version: str | None = None

    @model_validator(mode="after")
    def enforce_encrypted_backup_gate(self) -> SSRestrictedBackupEvidence:
        if self.status in {
            BackupStatus.VERIFIED_ENCRYPTED,
            BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        }:
            if self.schema_snapshot is None or self.snapshot_fingerprint is None:
                raise ValueError("executed backup requires snapshot-bound schema evidence")
            if self.connection_fingerprint is None:
                raise ValueError("executed backup requires source connection evidence")
        if self.schema_snapshot is not None:
            if (
                self.schema_dump_sha256 is not None
                and self.schema_dump_sha256 != self.schema_snapshot.sha256
            ):
                raise ValueError("schema dump hashes disagree")
            self.schema_dump_sha256 = self.schema_snapshot.sha256
            if self.snapshot_fingerprint != self.schema_snapshot.snapshot_fingerprint:
                raise ValueError("schema and backup snapshot fingerprints disagree")
            if self.connection_fingerprint != self.schema_snapshot.connection_fingerprint:
                raise ValueError("schema and backup connection fingerprints disagree")
        if self.status == BackupStatus.VERIFIED_ENCRYPTED:
            required = (
                self.logical_uri,
                self.plaintext_sha256,
                self.encrypted_sha256,
                self.encrypted_size_bytes,
            )
            if any(value is None for value in required):
                raise ValueError("verified encrypted backup requires complete hash evidence")
            if not all(
                (
                    self.plaintext_deleted,
                    self.required_tables_listed,
                    self.secret_table_data_absent,
                    self.decryptability_verified,
                )
            ):
                raise ValueError("verified encrypted backup requires every safety gate")
        if (
            self.status == BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE
            and not self.plaintext_deleted
        ):
            raise ValueError("blocked backup cannot retain plaintext")
        return self


class SSArchiveOperationalEvidence(DataModel):
    started_at: datetime
    finished_at: datetime
    duration_seconds: float = Field(ge=0)
    peak_rss_kib: int = Field(ge=0)
    disk_available_bytes_pre: int = Field(ge=0)
    disk_available_bytes_post: int = Field(ge=0)
    db_connections_pre: int = Field(ge=0)
    db_connections_during: int = Field(ge=0)
    db_connections_post: int = Field(ge=0)
    api_health_pre: str = Field(min_length=1)
    api_health_during: str = Field(min_length=1)
    api_health_post: str = Field(min_length=1)
    postgres_health_pre: str = Field(min_length=1)
    postgres_health_during: str = Field(min_length=1)
    postgres_health_post: str = Field(min_length=1)
    source_mutated: Literal[False] = False

    @model_validator(mode="after")
    def validate_timing(self) -> SSArchiveOperationalEvidence:
        if self.started_at.tzinfo is None or self.finished_at.tzinfo is None:
            raise ValueError("operational timestamps must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("operational finish cannot precede start")
        return self


class SSArchiveSourceLineage(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    source_system: Literal["stock_screener"] = "stock_screener"
    source_snapshot_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_inventory_uri: str = Field(pattern=r"^archive://")
    source_inventory_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_export_id: str = Field(min_length=1)
    source_export_dataset_id: Literal[
        SS_LEGACY_EXPORT_DATASET_ID
    ] = SS_LEGACY_EXPORT_DATASET_ID
    source_backup_id: str | None = None
    source_backup_status: BackupStatus
    source_backup_uri: str | None = Field(default=None, pattern=r"^archive://")
    lineage_quality: Literal["legacy"] = "legacy"
    volume_unit: Literal["unknown_or_mixed"] = "unknown_or_mixed"
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True

    @model_validator(mode="after")
    def validate_optional_backup(self) -> SSArchiveSourceLineage:
        if self.source_backup_status == BackupStatus.VERIFIED_ENCRYPTED:
            if not self.source_backup_id or not self.source_backup_uri:
                raise ValueError("verified backup lineage requires backup ID and URI")
        elif self.source_backup_id is not None or self.source_backup_uri is not None:
            raise ValueError("unavailable backup lineage cannot reference a backup artifact")
        return self


class SSLegacyArchiveManifest(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    archive_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    dataset_id: Literal[SS_LEGACY_EXPORT_DATASET_ID] = SS_LEGACY_EXPORT_DATASET_ID
    source_system: Literal["stock_screener"] = "stock_screener"
    source_commit: str = Field(min_length=7)
    source_schema_revision: str = Field(min_length=1)
    postgresql_version: str = Field(min_length=1)
    snapshot_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    consistency: SnapshotConsistency
    artifact_snapshot_fingerprints: dict[str, str] = Field(default_factory=dict)
    exported_at: datetime
    archive_root_uri: str = Field(pattern=r"^archive://")
    inventory_artifact: SSArchiveArtifactEvidence
    backup_evidence_artifact: SSArchiveArtifactEvidence
    table_inventories: tuple[SSArchiveTableInventory, ...]
    files: tuple[SSArchiveFile, ...] = Field(min_length=1)
    coverage: SSCoverageSummary
    backup: SSRestrictedBackupEvidence
    restore_status: RestoreStatus
    operational: SSArchiveOperationalEvidence
    lineage: SSArchiveSourceLineage
    immutable: Literal[True] = True
    includes_active_symbols: bool
    delisted_coverage: EvidenceStatus
    source_history_export: SourceHistoryStatus
    market_history_completeness: Literal["not_claimed"] = "not_claimed"
    known_retention_policy: str = Field(min_length=1)
    adjustment_semantics: Literal["unverified"] = "unverified"
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True
    limitations: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def reconcile_manifest(self) -> SSLegacyArchiveManifest:
        if self.exported_at.tzinfo is None:
            raise ValueError("exported_at must be timezone-aware")
        if self.lineage.source_export_id != self.archive_id:
            raise ValueError("lineage source_export_id must equal archive_id")
        expected_root = f"archive://ss-legacy/frozen-exports/{self.archive_id}/"
        if self.archive_root_uri != expected_root:
            raise ValueError("archive root URI must identify this frozen archive")
        if self.lineage.source_snapshot_fingerprint != self.snapshot_fingerprint:
            raise ValueError("lineage snapshot fingerprint must match manifest")
        if self.lineage.source_inventory_uri != self.inventory_artifact.logical_uri:
            raise ValueError("lineage inventory URI must match inventory evidence")
        if self.lineage.source_inventory_sha256 != self.inventory_artifact.sha256:
            raise ValueError("lineage inventory hash must match inventory evidence")
        if self.lineage.source_backup_status != self.backup.status:
            raise ValueError("lineage backup status must match backup evidence")
        if self.lineage.source_backup_uri != self.backup.logical_uri:
            raise ValueError("lineage backup URI must match backup evidence")
        if self.backup.status == BackupStatus.VERIFIED_ENCRYPTED:
            if self.lineage.source_backup_id != self.archive_id:
                raise ValueError("verified backup lineage ID must equal archive ID")
        elif self.lineage.source_backup_id is not None:
            raise ValueError("blocked backup cannot have a lineage backup ID")
        if any(
            re.fullmatch(r"[a-f0-9]{64}", value) is None
            for value in self.artifact_snapshot_fingerprints.values()
        ):
            raise ValueError("artifact snapshot fingerprints must be SHA-256 values")
        required_fingerprints = {"inventory", "schema", "export"}
        if not required_fingerprints.issubset(self.artifact_snapshot_fingerprints):
            raise ValueError("manifest is missing required artifact snapshot fingerprints")
        if self.backup.snapshot_fingerprint != self.artifact_snapshot_fingerprints["schema"]:
            raise ValueError("backup schema snapshot does not match manifest fingerprint evidence")
        if self.consistency == SnapshotConsistency.EXPORTED_SNAPSHOT and any(
            value != self.snapshot_fingerprint
            for value in self.artifact_snapshot_fingerprints.values()
        ):
            raise ValueError("exported-snapshot artifacts must share one snapshot fingerprint")
        if self.delisted_coverage != self.coverage.delisted_coverage:
            raise ValueError("delisted evidence must match coverage")
        if self.source_history_export != self.coverage.source_history_export:
            raise ValueError("history evidence must match coverage")
        table_rows = {table.table_name: table.exact_row_count for table in self.table_inventories}
        file_rows: dict[str, int] = {}
        uris: set[str] = set()
        for item in self.files:
            expected_prefix = f"archive://ss-legacy/frozen-exports/{self.archive_id}/"
            if not item.logical_uri.startswith(expected_prefix):
                raise ValueError("archive file URI does not belong to this archive")
            if item.logical_uri in uris:
                raise ValueError("archive file logical URIs must be unique")
            uris.add(item.logical_uri)
            file_rows[item.table_name] = file_rows.get(item.table_name, 0) + item.row_count
        for required in ("stock_prices", "stock_universe"):
            if file_rows.get(required) != table_rows.get(required):
                raise ValueError(f"{required} export row count does not reconcile")
        if file_rows.get("coverage") != self.coverage.distinct_price_symbols:
            raise ValueError("per-symbol coverage row count does not reconcile")
        if self.includes_active_symbols != (self.coverage.active_universe_symbols > 0):
            raise ValueError("active-symbol evidence does not reconcile")
        if (
            self.backup.status == BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE
            and self.restore_status != RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE
        ):
            raise ValueError("blocked backup requires blocked restore evidence")
        return self


class SSArchiveEvent(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    sequence: int = Field(ge=1)
    archive_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    state: ArchiveRunState
    occurred_at: datetime
    attempt_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    evidence: dict[str, str | int | bool | None] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_aware_timestamp(self) -> SSArchiveEvent:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return self
