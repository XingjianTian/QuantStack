from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import Field, model_validator

from ..models import DataModel

SS_LEGACY_EXPORT_DATASET_ID = "stock-screener-legacy-price-export-v1"


class SSLegacyCoverageRecord(DataModel):
    """Coverage evidence captured before any legacy transformation."""

    market: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    min_date: date | None = None
    max_date: date | None = None
    row_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_date_range(self) -> SSLegacyCoverageRecord:
        if self.row_count == 0 and (self.min_date is not None or self.max_date is not None):
            raise ValueError("empty coverage cannot declare a date range")
        if self.row_count > 0 and (self.min_date is None or self.max_date is None):
            raise ValueError("non-empty coverage requires min_date and max_date")
        if self.min_date and self.max_date and self.max_date < self.min_date:
            raise ValueError("max_date cannot be earlier than min_date")
        return self


class SSLegacyDatabaseBackup(DataModel):
    """Restricted Layer 1 backup inventory; this contract does not perform a backup."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    backup_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    storage_uri: str = Field(pattern=r"^quant-data://")
    source_database: Literal["stock_screener_postgresql"] = "stock_screener_postgresql"
    source_schema_version: str = Field(min_length=1)
    source_commit: str = Field(min_length=7)
    tables: tuple[str, ...]
    table_row_counts: dict[str, int]
    coverage: tuple[SSLegacyCoverageRecord, ...]
    backup_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    created_at: datetime
    encrypted: Literal[True] = True
    access_scope: Literal["restricted"] = "restricted"
    secrets_excluded: Literal[True] = True

    @model_validator(mode="after")
    def validate_inventory(self) -> SSLegacyDatabaseBackup:
        required_tables = {"stock_prices", "stock_universe"}
        if not required_tables.issubset(self.tables):
            raise ValueError("backup inventory requires stock_prices and stock_universe")
        if set(self.tables) != set(self.table_row_counts):
            raise ValueError("table_row_counts must cover every declared table exactly")
        if any(row_count < 0 for row_count in self.table_row_counts.values()):
            raise ValueError("table row counts cannot be negative")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        return self


class SSLegacyExportManifest(DataModel):
    """Immutable Layer 2 export inventory for the complete legacy price archive."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: Literal[SS_LEGACY_EXPORT_DATASET_ID] = SS_LEGACY_EXPORT_DATASET_ID
    export_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    source_backup_id: str = Field(min_length=1)
    storage_uri: str = Field(pattern=r"^quant-data://")
    source_commit: str = Field(min_length=7)
    source_schema_version: str = Field(min_length=1)
    original_primary_key: str = Field(min_length=1)
    exported_columns: tuple[str, ...]
    exported_at: datetime
    row_count: int = Field(ge=0)
    file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    includes_active: bool = True
    delisted_coverage: Literal["present", "absent", "unknown"] = "unknown"
    source_history_export: Literal["complete", "partial", "unknown"] = "unknown"
    market_history_completeness: Literal["not_claimed"] = "not_claimed"
    immutable: Literal[True] = True

    @model_validator(mode="after")
    def validate_export(self) -> SSLegacyExportManifest:
        required_columns = {
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close",
        }
        if not required_columns.issubset(self.exported_columns):
            raise ValueError("legacy price export is missing required columns")
        if self.original_primary_key not in self.exported_columns:
            raise ValueError("the original primary key must be preserved in the export")
        if self.exported_at.tzinfo is None:
            raise ValueError("exported_at must be timezone-aware")
        return self


class SSLegacyLineage(DataModel):
    """Fixed provenance for a converted SS artifact before canonical eligibility review."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    source_provider: Literal["stock_screener_legacy"] = "stock_screener_legacy"
    source_endpoint: Literal["stock_prices_export_v1"] = "stock_prices_export_v1"
    volume_unit: Literal["unknown_or_mixed"] = "unknown_or_mixed"
    lineage_quality: Literal["legacy"] = "legacy"
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True
    source_backup_id: str = Field(min_length=1)
    source_export_id: str = Field(min_length=1)


class SSLegacyConvertedArtifact(DataModel):
    """Layer 3 audit artifact; conversion alone never promotes data to canonical."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    artifact_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    storage_uri: str = Field(pattern=r"^quant-data://")
    lineage: SSLegacyLineage
    schema_validated: bool = False
    symbol_mic_mapping_audited: bool = False
    unit_adjustment_audited: bool = False
    cross_validated: bool = False
    quarantined: bool = True
    discrepancy_report_uri: str | None = Field(default=None, pattern=r"^quant-data://")

    @model_validator(mode="after")
    def preserve_pre_promotion_boundary(self) -> SSLegacyConvertedArtifact:
        if not self.quarantined and not self.cross_validated:
            raise ValueError("legacy artifacts require cross-validation before leaving quarantine")
        return self
