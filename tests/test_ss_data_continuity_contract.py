from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from quant_stack.data.continuity import (
    SS_LEGACY_EXPORT_DATASET_ID,
    SSLegacyConvertedArtifact,
    SSLegacyCoverageRecord,
    SSLegacyDatabaseBackup,
    SSLegacyExportManifest,
    SSLegacyLineage,
)


def _lineage() -> SSLegacyLineage:
    return SSLegacyLineage(
        source_backup_id="ss-backup-2026-07-29",
        source_export_id="ss-export-2026-07-29",
    )


def test_ss_legacy_lineage_defaults_are_explicit_and_not_canonical() -> None:
    lineage = _lineage()

    assert lineage.source_provider == "stock_screener_legacy"
    assert lineage.source_endpoint == "stock_prices_export_v1"
    assert lineage.volume_unit == "unknown_or_mixed"
    assert lineage.lineage_quality == "legacy"
    assert lineage.canonical_eligible is False

    with pytest.raises(ValidationError, match="canonical_eligible"):
        SSLegacyLineage(
            source_backup_id="ss-backup-2026-07-29",
            source_export_id="ss-export-2026-07-29",
            canonical_eligible=True,
        )


def test_ss_legacy_backup_contract_requires_complete_restricted_inventory() -> None:
    backup = SSLegacyDatabaseBackup(
        backup_id="ss-backup-2026-07-29",
        storage_uri="quant-data://backups/stock-screener/ss-backup-2026-07-29.dump",
        source_schema_version="stock-model-v1",
        source_commit="abcdef123456",
        tables=("stock_prices", "stock_universe"),
        table_row_counts={"stock_prices": 100, "stock_universe": 2},
        coverage=(
            SSLegacyCoverageRecord(
                market="CN",
                symbol="600000.SS",
                min_date=date(2020, 1, 2),
                max_date=date(2026, 7, 28),
                row_count=100,
            ),
        ),
        backup_sha256="a" * 64,
        created_at=datetime(2026, 7, 29, tzinfo=UTC),
    )

    assert backup.encrypted is True
    assert backup.access_scope == "restricted"
    assert backup.secrets_excluded is True

    with pytest.raises(ValidationError, match="stock_prices and stock_universe"):
        SSLegacyDatabaseBackup(
            backup_id="incomplete",
            storage_uri="quant-data://backups/stock-screener/incomplete.dump",
            source_schema_version="stock-model-v1",
            source_commit="abcdef123456",
            tables=("stock_prices",),
            table_row_counts={"stock_prices": 100},
            coverage=(),
            backup_sha256="b" * 64,
            created_at=datetime(2026, 7, 29, tzinfo=UTC),
        )


def test_ss_legacy_export_is_immutable_complete_and_preserves_original_key() -> None:
    export = SSLegacyExportManifest(
        export_id="ss-export-2026-07-29",
        source_backup_id="ss-backup-2026-07-29",
        storage_uri="quant-data://legacy/stock-screener/ss-export-2026-07-29.parquet",
        source_commit="abcdef123456",
        source_schema_version="stock-model-v1",
        original_primary_key="id",
        exported_columns=(
            "id",
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adj_close",
        ),
        exported_at=datetime(2026, 7, 29, tzinfo=UTC),
        row_count=100,
        file_sha256="c" * 64,
    )

    assert export.dataset_id == SS_LEGACY_EXPORT_DATASET_ID
    assert export.includes_active and export.immutable
    assert export.delisted_coverage == "unknown"
    assert export.source_history_export == "unknown"
    assert export.market_history_completeness == "not_claimed"


def test_converted_ss_artifact_stays_quarantined_until_cross_validation() -> None:
    artifact = SSLegacyConvertedArtifact(
        artifact_id="ss-converted-2026-07-29",
        storage_uri="quant-data://legacy/stock-screener/normalized/v1/",
        lineage=_lineage(),
    )
    assert artifact.quarantined is True
    assert artifact.lineage.canonical_eligible is False

    with pytest.raises(ValidationError, match="cross-validation"):
        SSLegacyConvertedArtifact(
            artifact_id="unsafe-promotion",
            storage_uri="quant-data://legacy/stock-screener/normalized/unsafe/",
            lineage=_lineage(),
            quarantined=False,
            cross_validated=False,
        )
