from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_stack.data.catalog.duckdb_catalog import DuckDBCatalog
from quant_stack.data.continuity import (
    ArchiveRunState,
    BackupStatus,
    EvidenceStatus,
    RestoreStatus,
    SnapshotConsistency,
    SourceHistoryStatus,
    SSArchiveArtifactEvidence,
    SSArchiveColumn,
    SSArchiveConstraint,
    SSArchiveFile,
    SSArchiveOperationalEvidence,
    SSArchiveSourceLineage,
    SSArchiveTableInventory,
    SSArchiveTableSummary,
    SSCoverageSummary,
    SSDatabaseInventory,
    SSDataQualityInventory,
    SSLegacyArchiveManifest,
    SSMarketCoverage,
    SSRestrictedBackupEvidence,
    SSSchemaSnapshotEvidence,
)
from quant_stack.data.continuity import ss_archive_migration as migration
from quant_stack.data.continuity.ss_archive_backup import RestrictedBackupRunner
from quant_stack.data.continuity.ss_archive_connection import (
    exact_pg_environment,
    postgres_connection_fingerprint,
)
from quant_stack.data.continuity.ss_archive_export import (
    SSArchiveVerifier,
    StreamingParquetExporter,
    verify_archive_manifest,
    write_archive_manifest,
)
from quant_stack.data.continuity.ss_archive_inventory import (
    SnapshotMetadata,
    SSInventoryCollector,
)
from quant_stack.data.continuity.ss_archive_safety import (
    ArchiveLayout,
    allowed_backup_tables,
    assert_read_only_sql,
    assert_restore_target_isolated,
    build_archive_id,
    scan_secret_text,
    validate_backup_listing,
    validate_schema_listing,
)
from quant_stack.data.continuity.ss_archive_state import ArchiveStateStore
from quant_stack.data.continuity.ss_legacy_conversion import (
    CONVERSION_MANIFEST_SCHEMA_VERSION,
    CONVERTER_VERSION,
    SSLegacyMappingStatus,
    SSLegacyOfflineConverter,
    _resolve_mapping,
    verify_ss_legacy_conversion,
)
from quant_stack.data.models import DatasetKind, DatasetLayer, DatasetManifest
from quant_stack.ss_archive_cli import build_parser
from quant_stack.ss_archive_cli import main as archive_cli_main

ROOT = Path(__file__).resolve().parents[1]


def _columns(table_name: str) -> tuple[SSArchiveColumn, ...]:
    if table_name == "stock_prices":
        values = (
            ("id", "integer", False),
            ("symbol", "character varying", False),
            ("date", "date", False),
            ("open", "double precision", True),
            ("high", "double precision", True),
            ("low", "double precision", True),
            ("close", "double precision", True),
            ("volume", "bigint", True),
            ("adj_close", "double precision", True),
        )
    else:
        values = (
            ("id", "integer", False),
            ("symbol", "character varying", False),
            ("market", "character varying", False),
            ("is_active", "boolean", True),
            ("status", "character varying", False),
        )
    return tuple(
        SSArchiveColumn(
            ordinal_position=position,
            name=name,
            data_type=data_type,
            nullable=nullable,
        )
        for position, (name, data_type, nullable) in enumerate(values, start=1)
    )


def _table(table_name: str, rows: int) -> SSArchiveTableInventory:
    return SSArchiveTableInventory(
        schema_name="public",
        table_name=table_name,
        estimated_row_count=rows,
        exact_row_count=rows,
        table_size_bytes=1024,
        index_size_bytes=256,
        columns=_columns(table_name),
        constraints=(
            SSArchiveConstraint(
                name=f"{table_name}_pkey",
                kind="primary_key",
                columns=("id",),
            ),
        ),
        primary_key=("id",),
    )


def _quality() -> SSDataQualityInventory:
    return SSDataQualityInventory(
        primary_key_null_count=0,
        primary_key_duplicate_count=0,
        symbol_date_duplicate_count=0,
        null_counts={"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0},
        negative_price_count=0,
        negative_volume_count=0,
        zero_volume_count=1,
        adj_close_null_count=0,
        adj_close_difference_count=2,
        volume_quantiles={"p50": 1000.0, "p90": 5000.0, "p99": 9000.0},
        suspicious_volume_jump_count=1,
    )


def _coverage(archive_id: str) -> SSCoverageSummary:
    return SSCoverageSummary(
        stock_prices_row_count=2,
        stock_universe_row_count=2,
        distinct_price_symbols=2,
        min_date=date(2026, 7, 27),
        max_date=date(2026, 7, 28),
        markets=(
            SSMarketCoverage(
                market="CN",
                row_count=2,
                distinct_symbols=2,
                min_date=date(2026, 7, 27),
                max_date=date(2026, 7, 28),
            ),
        ),
        active_universe_symbols=1,
        non_active_universe_symbols=1,
        orphan_price_symbols=0,
        universe_symbols_without_prices=0,
        delisted_coverage=EvidenceStatus.PRESENT,
        source_history_export=SourceHistoryStatus.PARTIAL,
        quality=_quality(),
        per_symbol_coverage_uri=(
            f"archive://ss-legacy/frozen-exports/{archive_id}/coverage/part-00000.parquet"
        ),
    )


def _inventory(archive_id: str) -> SSDatabaseInventory:
    return SSDatabaseInventory(
        archive_id=archive_id,
        database_fingerprint="a" * 64,
        connection_fingerprint="c" * 64,
        postgresql_version="16.4",
        encoding="UTF8",
        timezone="UTC",
        snapshot_timestamp=datetime(2026, 7, 29, tzinfo=UTC),
        snapshot_fingerprint="b" * 64,
        transaction_marker="1:2:",
        consistency=SnapshotConsistency.EXPORTED_SNAPSHOT,
        source_commit="abcdef123456",
        source_schema_revision="fixture-v1",
        all_table_names=("app_settings", "stock_prices", "stock_universe"),
        all_table_summaries=tuple(
            SSArchiveTableSummary(
                schema_name="public",
                table_name=name,
                estimated_row_count=rows,
                exact_row_count=rows,
                table_size_bytes=1024,
                index_size_bytes=256,
            )
            for name, rows in (
                ("app_settings", 1),
                ("stock_prices", 2),
                ("stock_universe", 2),
            )
        ),
        tables=(
            _table("app_settings", 1),
            _table("stock_prices", 2),
            _table("stock_universe", 2),
        ),
        coverage=_coverage(archive_id),
        retention_policy="fixture cleanup keeps a bounded window",
        cleanup_evidence="fixture monthly cleanup schedule",
        inventory_uri=f"archive://ss-legacy/reports/{archive_id}/inventory.json",
    )


def _schema_evidence(archive_id: str) -> SSSchemaSnapshotEvidence:
    return SSSchemaSnapshotEvidence(
        logical_uri=f"archive://ss-legacy/backups/{archive_id}/schema/full-schema.dump",
        sha256="e" * 64,
        size_bytes=128,
        archive_entry_count=4,
        table_data_entry_count=0,
        required_table_schema_entries=True,
        snapshot_fingerprint="b" * 64,
        connection_fingerprint="c" * 64,
    )


def _blocked_backup(archive_id: str) -> SSRestrictedBackupEvidence:
    return SSRestrictedBackupEvidence(
        status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        snapshot_fingerprint="b" * 64,
        connection_fingerprint="c" * 64,
        schema_snapshot=_schema_evidence(archive_id),
        plaintext_deleted=True,
        required_tables_listed=False,
        secret_table_data_absent=True,
        decryptability_verified=False,
    )


def _operational() -> SSArchiveOperationalEvidence:
    return SSArchiveOperationalEvidence(
        started_at=datetime(2026, 7, 29, tzinfo=UTC),
        finished_at=datetime(2026, 7, 29, 0, 1, tzinfo=UTC),
        duration_seconds=60,
        peak_rss_kib=100_000,
        disk_available_bytes_pre=10_000_000_000,
        disk_available_bytes_post=9_900_000_000,
        db_connections_pre=10,
        db_connections_during=11,
        db_connections_post=10,
        api_health_pre="healthy",
        api_health_during="healthy",
        api_health_post="healthy",
        postgres_health_pre="healthy",
        postgres_health_during="healthy",
        postgres_health_post="healthy",
    )


def _write_model(path: Path, model: object) -> SSArchiveArtifactEvidence:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(model.model_dump(mode="json"), sort_keys=True, indent=2) + "\n"  # type: ignore[attr-defined]
    ).encode()
    path.write_bytes(payload)
    archive_id = next(part for part in path.parts if part.startswith("ss-legacy-"))
    section_index = path.parts.index(archive_id) - 1
    logical = "/".join(path.parts[section_index:])
    return SSArchiveArtifactEvidence(
        logical_uri=f"archive://ss-legacy/{logical}",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _fixture_frozen_files(layout: ArchiveLayout) -> tuple[SSArchiveFile, ...]:
    files: list[SSArchiveFile] = []
    definitions = (
        (
            "coverage",
            (
                ("market", "character varying"),
                ("symbol", "character varying"),
                ("min_date", "date"),
                ("max_date", "date"),
                ("row_count", "bigint"),
            ),
            ("market", "symbol"),
            (
                {
                    "market": "CN",
                    "symbol": "A",
                    "min_date": date(2026, 7, 27),
                    "max_date": date(2026, 7, 27),
                    "row_count": 1,
                },
                {
                    "market": "CN",
                    "symbol": "B",
                    "min_date": date(2026, 7, 28),
                    "max_date": date(2026, 7, 28),
                    "row_count": 1,
                },
            ),
        ),
        (
            "stock_prices",
            (("id", "integer"), ("symbol", "character varying"), ("date", "date")),
            ("id",),
            (
                {"id": 1, "symbol": "A", "date": date(2026, 7, 27)},
                {"id": 2, "symbol": "B", "date": date(2026, 7, 28)},
            ),
        ),
        (
            "stock_universe",
            (("id", "integer"), ("symbol", "character varying")),
            ("id",),
            ({"id": 1, "symbol": "A"}, {"id": 2, "symbol": "B"}),
        ),
    )
    for table_name, columns, primary_key, rows in definitions:
        files.extend(
            StreamingParquetExporter(
                layout,
                table_name=table_name,
                columns=columns,
                primary_key=primary_key,
                shard_rows=10,
            ).write((rows,))
        )
    return tuple(files)


def _fixture_verifier(
    layout: ArchiveLayout, *, listing: str | None = None
) -> SSArchiveVerifier:
    verifier = SSArchiveVerifier(layout, environment={})
    valid_listing = "\n".join(
        (
            "1; 0 0 TABLE public stock_prices owner",
            "2; 0 0 TABLE public stock_universe owner",
        )
    )

    def fake_pg_restore(arguments: list[str], *, stdin: bytes | None = None) -> str:
        assert stdin == b"fixture schema archive"
        if arguments == ["--list"]:
            return valid_listing if listing is None else listing
        return "CREATE TABLE public.stock_prices (); CREATE TABLE public.stock_universe ();"

    verifier._run_pg_restore = fake_pg_restore  # type: ignore[method-assign]
    return verifier


def _complete_fixture_archive(
    tmp_path: Path,
) -> tuple[ArchiveLayout, SSLegacyArchiveManifest, SSArchiveVerifier]:
    archive_id = "ss-legacy-complete-fixture"
    layout = ArchiveLayout(tmp_path, archive_id)
    layout.initialize()
    inventory = _inventory(archive_id)
    inventory_artifact = _write_model(
        layout.archive_path("reports", "inventory.json"), inventory
    )
    schema_path = layout.archive_path("backups", "schema", "full-schema.dump")
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_bytes(b"fixture schema archive")
    schema = SSSchemaSnapshotEvidence(
        logical_uri=layout.logical_uri("backups", "schema", "full-schema.dump"),
        sha256=hashlib.sha256(schema_path.read_bytes()).hexdigest(),
        size_bytes=schema_path.stat().st_size,
        archive_entry_count=2,
        table_data_entry_count=0,
        required_table_schema_entries=True,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        connection_fingerprint=inventory.connection_fingerprint,
    )
    backup = SSRestrictedBackupEvidence(
        status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        connection_fingerprint=inventory.connection_fingerprint,
        schema_snapshot=schema,
        plaintext_deleted=True,
        required_tables_listed=False,
        secret_table_data_absent=True,
        decryptability_verified=False,
    )
    backup_artifact = _write_model(
        layout.archive_path(
            "backups", "restricted-manifest", "evidence-blocked-fixture.json"
        ),
        backup,
    )
    files = _fixture_frozen_files(layout)
    manifest = SSLegacyArchiveManifest(
        archive_id=archive_id,
        source_commit=inventory.source_commit,
        source_schema_revision=inventory.source_schema_revision,
        postgresql_version=inventory.postgresql_version,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        consistency=SnapshotConsistency.EXPORTED_SNAPSHOT,
        artifact_snapshot_fingerprints={
            "inventory": inventory.snapshot_fingerprint,
            "schema": inventory.snapshot_fingerprint,
            "export": inventory.snapshot_fingerprint,
        },
        exported_at=datetime(2026, 7, 29, tzinfo=UTC),
        archive_root_uri=f"archive://ss-legacy/frozen-exports/{archive_id}/",
        inventory_artifact=inventory_artifact,
        backup_evidence_artifact=backup_artifact,
        table_inventories=inventory.tables,
        files=files,
        coverage=inventory.coverage,
        backup=backup,
        restore_status=RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE,
        operational=_operational(),
        lineage=SSArchiveSourceLineage(
            source_snapshot_fingerprint=inventory.snapshot_fingerprint,
            source_inventory_uri=inventory_artifact.logical_uri,
            source_inventory_sha256=inventory_artifact.sha256,
            source_export_id=archive_id,
            source_backup_status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        ),
        includes_active_symbols=True,
        delisted_coverage=EvidenceStatus.PRESENT,
        source_history_export=SourceHistoryStatus.PARTIAL,
        known_retention_policy=inventory.retention_policy,
        limitations=("fixture limitation",),
    )
    store = ArchiveStateStore(layout)
    attempt_id = "attempt-fixture"
    for state in (
        ArchiveRunState.PLANNED,
        ArchiveRunState.SNAPSHOT_OPEN,
        ArchiveRunState.INVENTORY_COMPLETE,
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
    ):
        store.record(state, attempt_id=attempt_id)
    write_archive_manifest(layout, manifest)
    verifier = _fixture_verifier(layout)
    verifier.verify(verify_state=False)
    for state in (
        ArchiveRunState.VERIFICATION_COMPLETE,
        ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
        ArchiveRunState.BLOCKED,
    ):
        store.record(state, attempt_id=attempt_id)
    verifier.verify()
    return layout, manifest, verifier


def _replace_manifest(layout: ArchiveLayout, manifest: SSLegacyArchiveManifest) -> None:
    layout.archive_path("frozen-exports", "manifest.json").unlink()
    layout.archive_path("frozen-exports", "manifest.json.sha256").unlink()
    write_archive_manifest(layout, manifest)


def test_inventory_contract_requires_actual_required_tables() -> None:
    inventory = _inventory("ss-legacy-fixture")
    assert {table.table_name for table in inventory.tables} == {
        "app_settings",
        "stock_prices",
        "stock_universe",
    }
    payload = inventory.model_dump(mode="json")
    payload["tables"] = [
        table for table in payload["tables"] if table["table_name"] != "stock_universe"
    ]
    with pytest.raises(ValidationError, match="missing required legacy table"):
        SSDatabaseInventory.model_validate(payload)


def test_inventory_contract_requires_details_for_every_discovered_table() -> None:
    inventory = _inventory("ss-legacy-fixture")
    extra = SSArchiveTableSummary(
        schema_name="public",
        table_name="other_public_table",
        estimated_row_count=0,
        exact_row_count=0,
        table_size_bytes=0,
        index_size_bytes=0,
    )
    payload = inventory.model_dump(mode="json")
    payload["all_table_names"].append("other_public_table")
    payload["all_table_summaries"].append(extra.model_dump(mode="json"))
    with pytest.raises(ValidationError, match="detailed table inventories"):
        SSDatabaseInventory.model_validate(payload)


class _DiscoveryCursor:
    description = [("table_name",)]

    def __init__(self, table_names: tuple[str, ...]):
        self.table_names = table_names

    def execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        assert "information_schema.tables" in sql
        assert parameters == ()

    def fetchall(self) -> list[tuple[str]]:
        return [(value,) for value in self.table_names]


class _DiscoveryConnection:
    def __init__(self, table_names: tuple[str, ...]):
        self.table_names = table_names

    def cursor(self) -> _DiscoveryCursor:
        return _DiscoveryCursor(self.table_names)


def _snapshot_metadata() -> SnapshotMetadata:
    return SnapshotMetadata(
        opened_at=datetime(2026, 7, 29, tzinfo=UTC),
        raw_snapshot_id="00000003-0000001B-1",
        snapshot_fingerprint="b" * 64,
        transaction_marker="1:2:",
        database_fingerprint="a" * 64,
        connection_fingerprint="c" * 64,
        postgresql_version="16.4",
        encoding="UTF8",
        timezone="UTC",
    )


def test_actual_schema_discovery_and_missing_required_table_rejection() -> None:
    collector = SSInventoryCollector(
        _DiscoveryConnection(("app_settings", "stock_prices", "stock_universe")),
        _snapshot_metadata(),
    )
    assert collector.discover_table_names() == (
        "app_settings",
        "stock_prices",
        "stock_universe",
    )
    missing = SSInventoryCollector(_DiscoveryConnection(("stock_prices",)), _snapshot_metadata())
    with pytest.raises(ValueError, match="stock_universe"):
        missing.discover_table_names()


def test_read_only_sql_guard_rejects_mutation_and_maintenance() -> None:
    assert_read_only_sql("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT 1; ROLLBACK")
    assert_read_only_sql("SET TRANSACTION SNAPSHOT 'fixture'; SELECT 1")
    for sql in (
        "UPDATE stock_prices SET close = 1",
        "DELETE FROM stock_prices",
        "VACUUM stock_prices",
        "CREATE INDEX unsafe ON stock_prices(symbol)",
    ):
        with pytest.raises(ValueError, match="read-only"):
            assert_read_only_sql(sql)


def test_secret_table_exclusion_and_backup_listing_validation() -> None:
    assert allowed_backup_tables(
        ("stock_prices", "stock_universe", "app_settings", "user_sessions")
    ) == ("stock_prices", "stock_universe")
    valid = "\n".join(
        (
            "1; 0 0 TABLE DATA public stock_prices owner",
            "2; 0 0 TABLE DATA public stock_universe owner",
        )
    )
    validate_backup_listing(valid)
    with pytest.raises(ValueError, match="secret-bearing"):
        validate_backup_listing(valid + "\n3; 0 0 TABLE DATA public app_settings owner")
    schema_listing = "\n".join(
        (
            "1; 0 0 TABLE public stock_prices owner",
            "2; 0 0 TABLE public stock_universe owner",
            "3; 0 0 INDEX public stock_prices_pkey owner",
        )
    )
    assert validate_schema_listing(schema_listing) == (3, 0)
    with pytest.raises(ValueError, match="table data"):
        validate_schema_listing(schema_listing + "\n4; 0 0 TABLE DATA public stock_prices owner")


def test_archive_path_traversal_and_id_immutability(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    with pytest.raises(ValueError, match="traversal"):
        layout.resolve("../../outside")
    first = build_archive_id("stock_screener", datetime(2026, 7, 29, tzinfo=UTC), nonce="fixed")
    second = build_archive_id("stock_screener", datetime(2026, 7, 29, tzinfo=UTC), nonce="fixed")
    assert first == second


def test_streaming_parquet_export_preserves_primary_key_and_resumes_verified_shard(
    tmp_path: Path,
) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    exporter = StreamingParquetExporter(
        layout,
        table_name="stock_prices",
        columns=(
            ("id", "integer"),
            ("symbol", "character varying"),
            ("date", "date"),
            ("close", "double precision"),
            ("volume", "bigint"),
        ),
        primary_key=("id",),
        shard_rows=1,
    )
    rows = (
        ({"id": 1, "symbol": "A", "date": date(2026, 7, 27), "close": 1.0, "volume": 10},),
        ({"id": 2, "symbol": "B", "date": date(2026, 7, 28), "close": 2.0, "volume": 20},),
    )
    first = exporter.write(rows)
    mtimes = [
        layout.archive_path("frozen-exports", "stock_prices", f"part-{index:05d}.parquet")
        .stat()
        .st_mtime_ns
        for index in range(2)
    ]
    second = exporter.write(rows)
    assert first == second
    assert [item.row_count for item in first] == [1, 1]
    assert [
        layout.archive_path("frozen-exports", "stock_prices", f"part-{index:05d}.parquet")
        .stat()
        .st_mtime_ns
        for index in range(2)
    ] == mtimes


def test_export_rejects_unstable_order_and_null_primary_key(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    exporter = StreamingParquetExporter(
        layout,
        table_name="stock_universe",
        columns=(("id", "integer"), ("symbol", "character varying")),
        primary_key=("id",),
        shard_rows=10,
    )
    with pytest.raises(ValueError, match="strictly ordered"):
        exporter.write((({"id": 2, "symbol": "B"}, {"id": 1, "symbol": "A"}),))
    with pytest.raises(ValueError, match="cannot be null"):
        exporter.write((({"id": None, "symbol": "A"},),))


def test_manifest_reconciles_rows_hashes_and_rejects_partial_export(tmp_path: Path) -> None:
    archive_id = "ss-legacy-fixture"
    inventory = _inventory(archive_id)
    backup = _blocked_backup(archive_id)
    files = (
        SSArchiveFile(
            logical_uri=(f"archive://ss-legacy/frozen-exports/{archive_id}/coverage/part.parquet"),
            table_name="coverage",
            row_count=2,
            size_bytes=10,
            sha256="a" * 64,
            columns=("market", "symbol", "min_date", "max_date", "row_count"),
        ),
        SSArchiveFile(
            logical_uri=(
                f"archive://ss-legacy/frozen-exports/{archive_id}/stock_prices/part.parquet"
            ),
            table_name="stock_prices",
            row_count=2,
            size_bytes=10,
            sha256="c" * 64,
            columns=("id",),
        ),
        SSArchiveFile(
            logical_uri=(
                f"archive://ss-legacy/frozen-exports/{archive_id}/stock_universe/part.parquet"
            ),
            table_name="stock_universe",
            row_count=2,
            size_bytes=10,
            sha256="d" * 64,
            columns=("id",),
        ),
    )
    manifest = SSLegacyArchiveManifest(
        archive_id=archive_id,
        source_commit=inventory.source_commit,
        source_schema_revision=inventory.source_schema_revision,
        postgresql_version=inventory.postgresql_version,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        consistency=SnapshotConsistency.EXPORTED_SNAPSHOT,
        artifact_snapshot_fingerprints={
            "inventory": inventory.snapshot_fingerprint,
            "schema": inventory.snapshot_fingerprint,
            "export": inventory.snapshot_fingerprint,
        },
        exported_at=datetime(2026, 7, 29, tzinfo=UTC),
        archive_root_uri=f"archive://ss-legacy/frozen-exports/{archive_id}/",
        inventory_artifact=SSArchiveArtifactEvidence(
            logical_uri=f"archive://ss-legacy/reports/{archive_id}/inventory.json",
            size_bytes=10,
            sha256="1" * 64,
        ),
        backup_evidence_artifact=SSArchiveArtifactEvidence(
            logical_uri=(
                f"archive://ss-legacy/backups/{archive_id}/restricted-manifest/evidence.json"
            ),
            size_bytes=10,
            sha256="2" * 64,
        ),
        table_inventories=inventory.tables,
        files=files,
        coverage=inventory.coverage,
        backup=backup,
        restore_status=RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE,
        operational=_operational(),
        lineage=SSArchiveSourceLineage(
            source_snapshot_fingerprint=inventory.snapshot_fingerprint,
            source_inventory_uri=f"archive://ss-legacy/reports/{archive_id}/inventory.json",
            source_inventory_sha256="1" * 64,
            source_export_id=archive_id,
            source_backup_status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        ),
        includes_active_symbols=True,
        delisted_coverage=EvidenceStatus.PRESENT,
        source_history_export=SourceHistoryStatus.PARTIAL,
        known_retention_policy=inventory.retention_policy,
        limitations=("fixture limitation",),
    )
    assert manifest.canonical_eligible is False
    assert manifest.quarantined is True
    assert manifest.lineage.volume_unit == "unknown_or_mixed"
    payload = manifest.model_dump(mode="json")
    payload["files"][0]["row_count"] = 1
    with pytest.raises(ValidationError, match="does not reconcile"):
        SSLegacyArchiveManifest.model_validate(payload)


def test_blocked_backup_lineage_has_no_backup_id_or_uri() -> None:
    lineage = SSArchiveSourceLineage(
        source_snapshot_fingerprint="b" * 64,
        source_inventory_uri="archive://ss-legacy/reports/fixture/inventory.json",
        source_inventory_sha256="1" * 64,
        source_export_id="fixture",
        source_backup_status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
    )
    assert lineage.source_backup_id is None
    assert lineage.source_backup_uri is None


def test_verified_backup_lineage_requires_backup_id_and_uri() -> None:
    with pytest.raises(ValidationError, match="requires backup ID and URI"):
        SSArchiveSourceLineage(
            source_snapshot_fingerprint="b" * 64,
            source_inventory_uri="archive://ss-legacy/reports/fixture/inventory.json",
            source_inventory_sha256="1" * 64,
            source_export_id="fixture",
            source_backup_status=BackupStatus.VERIFIED_ENCRYPTED,
        )


def test_backup_status_and_lineage_mismatch_is_rejected(tmp_path: Path) -> None:
    _, manifest, _ = _complete_fixture_archive(tmp_path)
    payload = manifest.model_dump(mode="json")
    payload["lineage"]["source_backup_status"] = BackupStatus.NOT_RUN
    with pytest.raises(ValidationError, match="backup status"):
        SSLegacyArchiveManifest.model_validate(payload)


def test_schema_evidence_snapshot_matches_inventory(tmp_path: Path) -> None:
    _, manifest, _ = _complete_fixture_archive(tmp_path)
    payload = manifest.model_dump(mode="json")
    payload["backup"]["snapshot_fingerprint"] = "f" * 64
    payload["backup"]["schema_snapshot"]["snapshot_fingerprint"] = "f" * 64
    with pytest.raises(ValidationError, match="manifest fingerprint"):
        SSLegacyArchiveManifest.model_validate(payload)


def test_backup_overrides_inherited_wrong_pgdatabase() -> None:
    parameters = {
        "host": "source-db",
        "port": "5432",
        "dbname": "source_database",
        "user": "archive_reader",
        "password": "fixture-password-value",
    }
    environment = exact_pg_environment(
        parameters,
        {
            "PGHOST": "wrong-db",
            "PGDATABASE": "wrong_database",
            "PGUSER": "wrong_user",
            "PGOPTIONS": "unsafe inherited options",
            "PATH": "/usr/bin",
        },
    )
    assert environment["PGHOST"] == "source-db"
    assert environment["PGDATABASE"] == "source_database"
    assert environment["PGUSER"] == "archive_reader"
    assert environment["PGPASSWORD"] == "fixture-password-value"
    assert "PGOPTIONS" not in environment
    assert environment["PATH"] == "/usr/bin"


def test_backup_uses_snapshot_source_connection(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    source_fingerprint = postgres_connection_fingerprint(
        {"dbname": "source_database"}
    )
    runner = RestrictedBackupRunner(
        layout,
        environment={"PGDATABASE": "source_database"},
        connection_fingerprint=source_fingerprint,
    )
    with pytest.raises(ValueError, match="does not match inventory snapshot"):
        runner.run(
            snapshot_id="fixture",
            inventory_tables=("stock_prices", "stock_universe"),
            expected_connection_fingerprint="b" * 64,
        )


def test_backup_command_does_not_contain_credentials() -> None:
    parameters = {
        "dbname": "source_database",
        "user": "archive_reader",
        "password": "fixture-password-value",
    }
    environment = exact_pg_environment(parameters, {})
    command = ["pg_dump", "--format=custom", "--schema-only", "--snapshot=fixture"]
    assert "fixture-password-value" not in " ".join(command)
    assert environment["PGPASSWORD"] == "fixture-password-value"


def test_backup_error_does_not_echo_dsn(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    runner = RestrictedBackupRunner(
        layout,
        environment={
            "PGDATABASE": "source_database",
            "PGPASSWORD": "fixture-password-value",
        },
        connection_fingerprint=postgres_connection_fingerprint(
            {"dbname": "source_database", "user": "archive_reader"}
        ),
    )
    with pytest.raises(RuntimeError) as captured:
        runner._run(["/usr/bin/false"])  # noqa: SLF001
    assert "source_database" not in str(captured.value)
    assert "fixture-password-value" not in str(captured.value)


def test_backup_encryption_gate_retains_schema_but_no_table_plaintext(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    connection_fingerprint = postgres_connection_fingerprint(
        {"dbname": "source_database"}
    )
    runner = RestrictedBackupRunner(
        layout,
        environment={"PGDATABASE": "source_database"},
        connection_fingerprint=connection_fingerprint,
    )

    def fake_run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
        if command[0] == "pg_dump" and "--version" in command:
            return subprocess.CompletedProcess(command, 0, "pg_dump fixture", "")
        if command[0] == "pg_dump":
            output = next(
                value.removeprefix("--file=") for value in command if value.startswith("--file=")
            )
            Path(output).write_bytes(b"fixture schema archive")
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ["pg_restore", "--list"]:
            listing = "\n".join(
                (
                    "1; 0 0 TABLE public stock_prices owner",
                    "2; 0 0 TABLE public stock_universe owner",
                )
            )
            return subprocess.CompletedProcess(command, 0, listing, "")
        if command[0] == "pg_restore" and "--schema-only" in command:
            return subprocess.CompletedProcess(
                command,
                0,
                "CREATE TABLE public.stock_prices (); CREATE TABLE public.stock_universe ();",
                "",
            )
        raise AssertionError(command)

    runner._run = fake_run  # type: ignore[method-assign]
    evidence = runner.run(
        snapshot_id="fixture",
        inventory_tables=("stock_prices", "stock_universe"),
        expected_connection_fingerprint=connection_fingerprint,
    )
    assert evidence.status == BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE
    assert evidence.schema_snapshot is not None
    assert evidence.schema_snapshot.required_table_schema_entries is True
    assert layout.archive_path("backups", "schema", "full-schema.dump").is_file()
    assert evidence.plaintext_deleted is True
    assert evidence.secret_table_data_absent is True
    assert not tuple(layout.archive_path("backups").glob(".backup-staging-*"))


def test_subprocess_and_cli_errors_do_not_echo_sensitive_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    runner = RestrictedBackupRunner(layout, environment={})
    with pytest.raises(RuntimeError) as captured:
        runner._run(["/usr/bin/false", "recipient-fixture-value"])  # noqa: SLF001
    assert "recipient-fixture-value" not in str(captured.value)

    monkeypatch.delenv("SS_LEGACY_ARCHIVE_ROOT", raising=False)
    assert archive_cli_main(["verify", "ss-legacy-fixture"]) == 2
    assert "SS_LEGACY_ARCHIVE_ROOT" not in capsys.readouterr().err


def test_restore_target_source_protection_and_secret_scan() -> None:
    with pytest.raises(ValueError, match="source database"):
        assert_restore_target_isolated("same", "same")
    assert_restore_target_isolated("source", "isolated")
    assert "credential_assignment" in scan_secret_text("password=supersecretvalue")
    assert "database_uri_with_credentials" in scan_secret_text(
        "postgresql://user:secret@example.invalid/db"
    )
    assert scan_secret_text("password character varying", schema_only=True) == ()
    assert "private_hostname" in scan_secret_text("service.private.internal")
    assert "private_path" in scan_secret_text("/Users/example/private/archive")
    assert "email_identifier" in scan_secret_text("person@example.invalid")


def test_archive_state_is_append_only_crash_safe_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    store = ArchiveStateStore(layout)
    states = (
        ArchiveRunState.PLANNED,
        ArchiveRunState.SNAPSHOT_OPEN,
        ArchiveRunState.INVENTORY_COMPLETE,
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
        ArchiveRunState.VERIFICATION_COMPLETE,
        ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
        ArchiveRunState.BLOCKED,
    )
    for state in states:
        store.record(state)
    assert tuple(event.state for event in store.events()) == states
    assert len(tuple(store.events_root.glob("*.json"))) == len(states)
    with pytest.raises(ValueError, match="terminal"):
        store.record(ArchiveRunState.COMPLETED)


def test_archive_state_rejects_blocked_before_verification_pending(tmp_path: Path) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    store = ArchiveStateStore(layout)
    store.record(ArchiveRunState.PLANNED)
    with pytest.raises(ValueError, match="invalid archive transition"):
        store.record(ArchiveRunState.BLOCKED)


def test_archive_state_rejects_wrong_archive_id_and_non_monotonic_time(
    tmp_path: Path,
) -> None:
    layout = ArchiveLayout(tmp_path, "ss-legacy-fixture")
    layout.initialize()
    store = ArchiveStateStore(layout)
    store.record(
        ArchiveRunState.PLANNED,
        occurred_at=datetime(2026, 7, 30, 1, 0, tzinfo=UTC),
    )
    store.record(
        ArchiveRunState.SNAPSHOT_OPEN,
        occurred_at=datetime(2026, 7, 30, 0, 59, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="not monotonic"):
        store.events()

    second = json.loads(
        layout.archive_path("state", "events", "000002.json").read_text(encoding="utf-8")
    )
    second["occurred_at"] = "2026-07-30T01:01:00Z"
    second["archive_id"] = "ss-legacy-other"
    layout.archive_path("state", "events", "000002.json").write_text(
        json.dumps(second), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="different archive"):
        store.events()


def test_archive_manifest_verify_rejects_missing_or_mutated_file(tmp_path: Path) -> None:
    archive_id = "ss-legacy-fixture"
    layout = ArchiveLayout(tmp_path, archive_id)
    layout.initialize()
    inventory = _inventory(archive_id)
    backup = _blocked_backup(archive_id)
    exporters = []
    files = []
    for table_name, columns, primary_key, rows in (
        (
            "coverage",
            (
                ("market", "character varying"),
                ("symbol", "character varying"),
                ("min_date", "date"),
                ("max_date", "date"),
                ("row_count", "bigint"),
            ),
            ("market", "symbol"),
            (
                {
                    "market": "CN",
                    "symbol": "A",
                    "min_date": date(2026, 7, 27),
                    "max_date": date(2026, 7, 27),
                    "row_count": 1,
                },
                {
                    "market": "CN",
                    "symbol": "B",
                    "min_date": date(2026, 7, 28),
                    "max_date": date(2026, 7, 28),
                    "row_count": 1,
                },
            ),
        ),
        (
            "stock_prices",
            (("id", "integer"), ("symbol", "character varying"), ("date", "date")),
            ("id",),
            (
                {"id": 1, "symbol": "A", "date": date(2026, 7, 27)},
                {"id": 2, "symbol": "B", "date": date(2026, 7, 28)},
            ),
        ),
        (
            "stock_universe",
            (("id", "integer"), ("symbol", "character varying")),
            ("id",),
            (
                {"id": 1, "symbol": "A"},
                {"id": 2, "symbol": "B"},
            ),
        ),
    ):
        exporter = StreamingParquetExporter(
            layout,
            table_name=table_name,
            columns=columns,
            primary_key=primary_key,
            shard_rows=10,
        )
        exporters.append(exporter)
        files.extend(exporter.write((rows,)))
    manifest = SSLegacyArchiveManifest(
        archive_id=archive_id,
        source_commit=inventory.source_commit,
        source_schema_revision=inventory.source_schema_revision,
        postgresql_version=inventory.postgresql_version,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        consistency=SnapshotConsistency.EXPORTED_SNAPSHOT,
        artifact_snapshot_fingerprints={
            "inventory": inventory.snapshot_fingerprint,
            "schema": inventory.snapshot_fingerprint,
            "export": inventory.snapshot_fingerprint,
        },
        exported_at=datetime(2026, 7, 29, tzinfo=UTC),
        archive_root_uri=f"archive://ss-legacy/frozen-exports/{archive_id}/",
        inventory_artifact=SSArchiveArtifactEvidence(
            logical_uri=f"archive://ss-legacy/reports/{archive_id}/inventory.json",
            size_bytes=10,
            sha256="1" * 64,
        ),
        backup_evidence_artifact=SSArchiveArtifactEvidence(
            logical_uri=(
                f"archive://ss-legacy/backups/{archive_id}/restricted-manifest/evidence.json"
            ),
            size_bytes=10,
            sha256="2" * 64,
        ),
        table_inventories=inventory.tables,
        files=tuple(files),
        coverage=inventory.coverage,
        backup=backup,
        restore_status=RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE,
        operational=_operational(),
        lineage=SSArchiveSourceLineage(
            source_snapshot_fingerprint=inventory.snapshot_fingerprint,
            source_inventory_uri=f"archive://ss-legacy/reports/{archive_id}/inventory.json",
            source_inventory_sha256="1" * 64,
            source_export_id=archive_id,
            source_backup_status=BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE,
        ),
        includes_active_symbols=True,
        delisted_coverage=EvidenceStatus.PRESENT,
        source_history_export=SourceHistoryStatus.PARTIAL,
        known_retention_policy=inventory.retention_policy,
        limitations=("fixture limitation",),
    )
    verify_archive_manifest(layout, manifest)
    mtimes = {
        path: path.stat().st_mtime_ns
        for path in layout.archive_path("frozen-exports").glob("**/*.parquet")
    }
    verify_archive_manifest(layout, manifest)
    assert {
        path: path.stat().st_mtime_ns
        for path in layout.archive_path("frozen-exports").glob("**/*.parquet")
    } == mtimes
    broken_files = list(manifest.files)
    broken_files[0] = broken_files[0].model_copy(update={"parquet_schema_sha256": "f" * 64})
    with pytest.raises(ValueError, match="schema hash"):
        verify_archive_manifest(
            layout,
            manifest.model_copy(update={"files": tuple(broken_files)}),
        )
    path = layout.archive_path("frozen-exports", "stock_prices", "part-00000.parquet")
    path.unlink()
    with pytest.raises(ValueError, match="size mismatch"):
        verify_archive_manifest(layout, manifest)


def test_verify_rejects_mutated_inventory(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    path = layout.archive_path("reports", "inventory.json")
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="artifact (size|hash)"):
        verifier.verify()


def test_verify_rejects_inventory_manifest_mismatch(tmp_path: Path) -> None:
    layout, manifest, verifier = _complete_fixture_archive(tmp_path)
    inventory = _inventory(layout.archive_id).model_copy(update={"source_commit": "different123"})
    evidence = _write_model(layout.archive_path("reports", "inventory.json"), inventory)
    lineage = manifest.lineage.model_copy(
        update={
            "source_inventory_uri": evidence.logical_uri,
            "source_inventory_sha256": evidence.sha256,
        }
    )
    replacement = SSLegacyArchiveManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "inventory_artifact": evidence.model_dump(mode="json"),
            "lineage": lineage.model_dump(mode="json"),
        }
    )
    _replace_manifest(layout, replacement)
    with pytest.raises(ValueError, match="inventory and manifest"):
        verifier.verify()


def test_verify_rejects_missing_schema_dump(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    layout.archive_path("backups", "schema", "full-schema.dump").unlink()
    with pytest.raises(ValueError, match="artifact size"):
        verifier.verify()


def test_verify_rejects_mutated_schema_dump(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    path = layout.archive_path("backups", "schema", "full-schema.dump")
    path.write_bytes(b"changed schema archive")
    with pytest.raises(ValueError, match="artifact (size|hash)"):
        verifier.verify()


def test_verify_rechecks_schema_listing(tmp_path: Path) -> None:
    layout, _, _ = _complete_fixture_archive(tmp_path)
    verifier = _fixture_verifier(
        layout,
        listing="1; 0 0 TABLE public stock_prices owner",
    )
    with pytest.raises(ValueError, match="missing table definitions"):
        verifier.verify()


def test_verify_rejects_backup_evidence_mismatch(tmp_path: Path) -> None:
    layout, manifest, verifier = _complete_fixture_archive(tmp_path)
    changed = manifest.backup.model_copy(update={"required_tables_listed": True})
    evidence = _write_model(
        layout.archive_path(
            "backups", "restricted-manifest", "evidence-blocked-fixture.json"
        ),
        changed,
    )
    replacement = SSLegacyArchiveManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "backup_evidence_artifact": evidence.model_dump(mode="json"),
        }
    )
    _replace_manifest(layout, replacement)
    with pytest.raises(ValueError, match="backup evidence does not match"):
        verifier.verify()


def test_verify_rejects_plaintext_backup_left_by_blocked_run(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    path = layout.archive_path("backups", "restricted-backup.tar")
    path.write_bytes(b"plaintext fixture")
    with pytest.raises(ValueError, match="retains table data"):
        verifier.verify()


def test_verify_rejects_manifest_checksum_mismatch(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    layout.archive_path("frozen-exports", "manifest.json.sha256").write_text(
        f"{'0' * 64}  manifest.json\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="checksum sidecar"):
        verifier.verify()


def test_verify_rejects_state_manifest_mismatch(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    layout.archive_path("state", "events", "000008.json").unlink()
    with pytest.raises(ValueError, match="terminal state"):
        verifier.verify()


def test_verify_rejects_exported_snapshot_across_attempts(tmp_path: Path) -> None:
    layout, _, verifier = _complete_fixture_archive(tmp_path)
    event_path = layout.archive_path("state", "events", "000004.json")
    event = json.loads(event_path.read_text(encoding="utf-8"))
    event["attempt_id"] = "attempt-different"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    with pytest.raises(ValueError, match="multiple attempts"):
        verifier.verify()


def _configure_fixture_as_designated(monkeypatch, layout, manifest, verifier) -> None:
    manifest_path = layout.archive_path("frozen-exports", "manifest.json")
    monkeypatch.setattr(migration, "DESIGNATED_SS_ARCHIVE_ID", layout.archive_id)
    monkeypatch.setattr(
        migration,
        "DESIGNATED_SS_MANIFEST_SHA256",
        hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(migration, "DESIGNATED_SS_PARQUET_FILES", len(manifest.files))
    rows = {item.table_name: item.exact_row_count for item in manifest.table_inventories}
    monkeypatch.setattr(
        migration, "DESIGNATED_SS_STOCK_PRICES_ROWS", rows["stock_prices"]
    )
    monkeypatch.setattr(
        migration, "DESIGNATED_SS_STOCK_UNIVERSE_ROWS", rows["stock_universe"]
    )
    monkeypatch.setattr(migration, "SSArchiveVerifier", lambda *_args, **_kwargs: verifier)


def test_designated_archive_enters_phase_23_only_with_explicit_blocker_acceptance(
    tmp_path: Path, monkeypatch
) -> None:
    layout, manifest, verifier = _complete_fixture_archive(tmp_path)
    _configure_fixture_as_designated(monkeypatch, layout, manifest, verifier)

    accepted = migration.accept_ss_archive_for_phase_23(
        tmp_path,
        archive_id=layout.archive_id,
        policy=migration.SSArchiveMigrationPolicy(
            accept_backup_blocker=True,
            accept_restore_blocker=True,
        ),
    )

    assert accepted.accepted_for_phase_23 is True
    assert accepted.backup_complete is False
    assert accepted.restore_verified is False
    assert accepted.cutover_blocker_open is True
    assert accepted.canonical_eligible is False
    assert accepted.quarantined is True


def test_phase_23_archive_gate_rejects_superseded_and_unaccepted_blockers(
    tmp_path: Path, monkeypatch
) -> None:
    layout, manifest, verifier = _complete_fixture_archive(tmp_path)
    _configure_fixture_as_designated(monkeypatch, layout, manifest, verifier)
    monkeypatch.setattr(migration, "SUPERSEDED_SS_ARCHIVE_IDS", {layout.archive_id})
    with pytest.raises(ValueError, match="superseded"):
        migration.accept_ss_archive_for_phase_23(
            tmp_path,
            archive_id=layout.archive_id,
            policy=migration.SSArchiveMigrationPolicy(
                accept_backup_blocker=True,
                accept_restore_blocker=True,
            ),
        )

    monkeypatch.setattr(migration, "SUPERSEDED_SS_ARCHIVE_IDS", set())
    with pytest.raises(ValueError, match="explicitly accepted"):
        migration.accept_ss_archive_for_phase_23(
            tmp_path,
            archive_id=layout.archive_id,
            policy=migration.SSArchiveMigrationPolicy(),
        )


def test_phase_23_archive_gate_rejects_hash_and_canonical_anomalies(
    tmp_path: Path, monkeypatch
) -> None:
    layout, manifest, verifier = _complete_fixture_archive(tmp_path)
    _configure_fixture_as_designated(monkeypatch, layout, manifest, verifier)
    policy = migration.SSArchiveMigrationPolicy(
        accept_backup_blocker=True,
        accept_restore_blocker=True,
    )
    monkeypatch.setattr(migration, "DESIGNATED_SS_MANIFEST_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="manifest hash"):
        migration.accept_ss_archive_for_phase_23(
            tmp_path, archive_id=layout.archive_id, policy=policy
        )

    _configure_fixture_as_designated(monkeypatch, layout, manifest, verifier)
    anomalous = manifest.model_copy(update={"canonical_eligible": True})

    class _AnomalousVerifier:
        def verify(self):
            return anomalous

    monkeypatch.setattr(
        migration, "SSArchiveVerifier", lambda *_args, **_kwargs: _AnomalousVerifier()
    )
    with pytest.raises(ValueError, match="non-canonical"):
        migration.accept_ss_archive_for_phase_23(
            tmp_path, archive_id=layout.archive_id, policy=policy
        )


def test_legacy_symbol_mapping_requires_explicit_suffix_and_exchange_evidence() -> None:
    mapped = _resolve_mapping(
        [{"symbol": "600000.SS", "market": "CN", "exchange": "SSE"}]
    )
    assert mapped.mapping_status == SSLegacyMappingStatus.MAPPED
    assert mapped.candidate_instrument_id == "XSHG.600000"

    conflict = _resolve_mapping(
        [{"symbol": "000001.SZ", "market": "CN", "exchange": "SSE"}]
    )
    assert conflict.mapping_status == SSLegacyMappingStatus.CONFLICT
    assert conflict.candidate_instrument_id is None

    ambiguous = _resolve_mapping(
        [{"symbol": "600000", "market": "CN", "exchange": None}]
    )
    assert ambiguous.mapping_status == SSLegacyMappingStatus.AMBIGUOUS
    assert ambiguous.candidate_instrument_id is None


def _conversion_fixture(tmp_path: Path):
    layout, manifest, _ = _complete_fixture_archive(tmp_path)
    for path in layout.archive_path("frozen-exports").glob("**/*.parquet"):
        path.unlink()
    files: list[SSArchiveFile] = []
    definitions = (
        (
            "coverage",
            (
                ("market", "character varying"),
                ("symbol", "character varying"),
                ("min_date", "date"),
                ("max_date", "date"),
                ("row_count", "bigint"),
            ),
            ("market", "symbol"),
            (
                {
                    "market": "CN",
                    "symbol": "600000.SS",
                    "min_date": date(2026, 7, 29),
                    "max_date": date(2026, 7, 30),
                    "row_count": 2,
                },
                {
                    "market": "unknown",
                    "symbol": "999999.XX",
                    "min_date": date(2026, 7, 30),
                    "max_date": date(2026, 7, 30),
                    "row_count": 1,
                },
            ),
        ),
        (
            "stock_prices",
            (
                ("id", "integer"),
                ("symbol", "character varying"),
                ("date", "date"),
                ("open", "double precision"),
                ("high", "double precision"),
                ("low", "double precision"),
                ("close", "double precision"),
                ("volume", "bigint"),
                ("adj_close", "double precision"),
            ),
            ("id",),
            (
                {
                    "id": 1,
                    "symbol": "600000.SS",
                    "date": date(2026, 7, 29),
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 100,
                    "adj_close": 10.4,
                },
                {
                    "id": 2,
                    "symbol": "600000.SS",
                    "date": date(2026, 7, 30),
                    "open": 10.5,
                    "high": 12.0,
                    "low": 10.0,
                    "close": 11.0,
                    "volume": 10_000,
                    "adj_close": 11.0,
                },
                {
                    "id": 3,
                    "symbol": "999999.XX",
                    "date": date(2026, 7, 30),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 0,
                    "adj_close": 1.0,
                },
            ),
        ),
        (
            "stock_universe",
            (
                ("id", "integer"),
                ("symbol", "character varying"),
                ("market", "character varying"),
                ("exchange", "character varying"),
                ("local_code", "character varying"),
            ),
            ("id",),
            (
                {
                    "id": 1,
                    "symbol": "600000.SS",
                    "market": "CN",
                    "exchange": "SSE",
                    "local_code": "600000",
                },
                {
                    "id": 2,
                    "symbol": "000001.SZ",
                    "market": "CN",
                    "exchange": "SSE",
                    "local_code": "000001",
                },
            ),
        ),
    )
    for table_name, columns, primary_key, rows in definitions:
        files.extend(
            StreamingParquetExporter(
                layout,
                table_name=table_name,
                columns=columns,
                primary_key=primary_key,
                shard_rows=2 if table_name == "stock_prices" else 10,
            ).write(
                (rows[:2], rows[2:]) if table_name == "stock_prices" else (rows,)
            )
        )
    inventories = tuple(
        item.model_copy(
            update={
                "exact_row_count": 3 if item.table_name == "stock_prices" else 2,
                "estimated_row_count": 3 if item.table_name == "stock_prices" else 2,
            }
        )
        for item in manifest.table_inventories
    )
    coverage = manifest.coverage.model_copy(
        update={
            "stock_prices_row_count": 3,
            "stock_universe_row_count": 2,
            "distinct_price_symbols": 2,
            "min_date": date(2026, 7, 29),
            "max_date": date(2026, 7, 30),
            "orphan_price_symbols": 1,
        }
    )
    replacement = SSLegacyArchiveManifest.model_validate(
        {
            **manifest.model_dump(mode="json"),
            "table_inventories": [item.model_dump(mode="json") for item in inventories],
            "files": [item.model_dump(mode="json") for item in files],
            "coverage": coverage.model_dump(mode="json"),
        }
    )
    _replace_manifest(layout, replacement)
    manifest_path = layout.archive_path("frozen-exports", "manifest.json")
    migration_input = migration.SSArchiveMigrationInput(
        archive_id=layout.archive_id,
        archive_root_uri=replacement.archive_root_uri,
        manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        snapshot_fingerprint=replacement.snapshot_fingerprint,
        source_commit=replacement.source_commit,
        source_schema_revision=replacement.source_schema_revision,
        stock_prices_rows=3,
        stock_universe_rows=2,
    )
    return layout, migration_input


def _persist_conversion_manifest(data_root: Path, result) -> None:
    conversion_root = (
        data_root
        / "legacy"
        / "stock_screener"
        / result.migration_input.archive_id
        / result.conversion_id
    )
    manifest_path = conversion_root / "conversion-manifest.json"
    manifest_path.write_text(result.model_dump_json() + "\n", encoding="utf-8")
    (conversion_root / "conversion-manifest.json.sha256").write_text(
        f"{hashlib.sha256(manifest_path.read_bytes()).hexdigest()}  "
        "conversion-manifest.json\n",
        encoding="ascii",
    )


def _refresh_artifact_evidence(data_root: Path, result, kind: str):
    artifact = next(item for item in result.artifacts if item.kind == kind)
    path = data_root / artifact.uri.removeprefix("quant-data://")
    replacement = artifact.model_copy(
        update={
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    )
    updated = result.model_copy(
        update={
            "artifacts": tuple(
                replacement if item.kind == kind else item for item in result.artifacts
            )
        }
    )
    _persist_conversion_manifest(data_root, updated)
    return updated


def _canonical_manifest(
    *, dataset_id: str = "cn_equity_daily", storage_uri: str = "quant-data://normalized/a"
) -> DatasetManifest:
    return DatasetManifest(
        dataset_id=dataset_id,
        version="2026-08-03.1",
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider="fixture",
        frequency="1d",
        schema_ref="fixture-v1",
        storage_uri=storage_uri,
        created_at=datetime(2026, 8, 3, tzinfo=UTC),
        available_at_policy="fixture",
        revision_policy="immutable",
        row_count=1,
        sha256="1" * 64,
        quality_status="passed",
        license_scope="test",
    )


def _empty_canonical_catalog(data_root: Path) -> None:
    catalog_root = data_root / "catalog"
    catalog_root.mkdir(parents=True, exist_ok=True)
    DuckDBCatalog(catalog_root / "quant.duckdb").close()
    (catalog_root / "current-manifest-set.json").write_text(
        json.dumps(
            {"schema_version": "1.0.0", "bundle_id": "bundle-empty", "manifests": []}
        ),
        encoding="utf-8",
    )


def test_legacy_converter_is_idempotent_hash_complete_and_quarantined(
    tmp_path: Path,
) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    converter = SSLegacyOfflineConverter(tmp_path, data_root)

    first = converter.convert(
        migration_input, created_at=datetime(2026, 7, 31, tzinfo=UTC)
    )
    second = converter.convert(
        migration_input, created_at=datetime(2026, 8, 1, tzinfo=UTC)
    )

    assert second == first
    assert CONVERTER_VERSION == "1.0.2"
    assert first.schema_version == CONVERSION_MANIFEST_SCHEMA_VERSION == "1.2.0"
    assert first.conversion_id != "ssconv-57fca7ca2527053731b333d5abf554d8"
    assert first.canonical_eligible is False
    assert first.quarantined is True
    assert first.mapped_symbols == 1
    assert first.unresolved_symbols == 2
    assert first.orphan_price_symbols == 1
    assert first.volume_anomaly_rows == 1
    verification = verify_ss_legacy_conversion(
        data_root, first, archive_root=tmp_path
    )
    assert verification.catalog_status == "canonical_catalog_absent"
    assert not (data_root / "catalog").exists()
    conversion_root = (
        data_root
        / "legacy"
        / "stock_screener"
        / first.migration_input.archive_id
        / first.conversion_id
    )
    manifest_path = conversion_root / "conversion-manifest.json"
    checksum_path = conversion_root / "conversion-manifest.json.sha256"
    assert checksum_path.read_text(encoding="ascii") == (
        f"{hashlib.sha256(manifest_path.read_bytes()).hexdigest()}  "
        "conversion-manifest.json\n"
    )

    import pyarrow.parquet as pq

    audited = next(item for item in first.artifacts if item.kind == "audited_prices")
    frame = pq.read_table(data_root / audited.uri.removeprefix("quant-data://")).to_pylist()
    assert [row["original_volume"] for row in frame] == [100, 10_000, 0]
    assert all(row["volume_unit"] == "unknown_or_mixed" for row in frame)
    assert all(row["volume_shares"] is None for row in frame)
    assert all(row["adjustment_semantics"] == "unverified" for row in frame)
    assert all(row["adjusted_value_usable"] is False for row in frame)
    assert all("filename" not in row for row in frame)

    mapping_artifact = next(
        item for item in first.artifacts if item.kind == "instrument_mapping"
    )
    mapping_path = data_root / mapping_artifact.uri.removeprefix("quant-data://")
    mapping_path.write_bytes(mapping_path.read_bytes() + b"mutation")
    with pytest.raises(ValueError, match="hash evidence"):
        verify_ss_legacy_conversion(data_root, first, archive_root=tmp_path)


def test_legacy_converter_resumes_byte_identical_artifacts_after_pre_manifest_crash(
    tmp_path: Path,
) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    converter = SSLegacyOfflineConverter(tmp_path, data_root)

    with pytest.raises(RuntimeError, match="simulated crash"):
        converter.convert(
            migration_input,
            created_at=datetime(2026, 7, 31, tzinfo=UTC),
            fail_after_artifacts=True,
        )
    conversion_root = next(
        (data_root / "legacy" / "stock_screener" / migration_input.archive_id).glob(
            "ssconv-*"
        )
    )
    assert not (conversion_root / "conversion-manifest.json").exists()
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in conversion_root.iterdir()
        if path.is_file() and not path.name.startswith(".")
    }

    result = converter.convert(
        migration_input, created_at=datetime(2026, 7, 31, tzinfo=UTC)
    )
    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in conversion_root.iterdir()
        if path.name in before
    }
    assert after == before
    verify_ss_legacy_conversion(data_root, result, archive_root=tmp_path)


def test_legacy_converter_refuses_conflicting_pre_manifest_artifact(
    tmp_path: Path,
) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "conflicting-quant-data"
    converter = SSLegacyOfflineConverter(tmp_path, data_root)
    with pytest.raises(RuntimeError, match="simulated crash"):
        converter.convert(migration_input, fail_after_artifacts=True)
    mapping_path = next(data_root.rglob("instrument-mapping.parquet"))
    mapping_path.write_bytes(mapping_path.read_bytes() + b"conflict")

    with pytest.raises(ValueError, match="Immutable SS legacy instrument mapping conflict"):
        converter.convert(migration_input)


def test_legacy_conversion_verifier_recomputes_identity_and_rejects_catalog_registration(
    tmp_path: Path,
) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    forged = result.model_copy(update={"conversion_identity_sha256": "0" * 64})
    with pytest.raises(ValueError, match="identity hash"):
        verify_ss_legacy_conversion(data_root, forged, archive_root=tmp_path)

    catalog = data_root / "catalog"
    catalog.mkdir()
    (catalog / "current-manifest-set.json").write_text(
        json.dumps({"manifests": [result.conversion_id]}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="canonical catalog"):
        verify_ss_legacy_conversion(data_root, result, archive_root=tmp_path)


def test_conversion_rejects_fake_source_file_hash(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    first, *remaining = result.source_price_file_inventory
    forged = result.model_copy(
        update={
            "source_price_file_inventory": (
                first.model_copy(update={"sha256": "0" * 64}),
                *remaining,
            )
        }
    )
    with pytest.raises(ValueError, match="source price file inventory"):
        verify_ss_legacy_conversion(data_root, forged, archive_root=tmp_path)


def test_conversion_rejects_missing_source_file_contribution(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    assert len(result.source_price_file_inventory) == 2
    artifact = next(item for item in result.artifacts if item.kind == "audited_prices")
    path = data_root / artifact.uri.removeprefix("quant-data://")
    table = pq.read_table(path)
    rows = table.to_pylist()
    second = result.source_price_file_inventory[1]
    rows[0]["source_file_uri"] = second.logical_uri
    rows[0]["source_file_sha256"] = second.sha256
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    forged = _refresh_artifact_evidence(data_root, result, "audited_prices")

    with pytest.raises(ValueError, match="source file contributions"):
        verify_ss_legacy_conversion(data_root, forged, archive_root=tmp_path)


def test_conversion_reconciles_source_rows_per_file(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)

    verification = verify_ss_legacy_conversion(
        data_root, result, archive_root=tmp_path
    )
    assert verification.source_price_file_count == 2
    assert sum(item.row_count for item in result.source_price_file_inventory) == 3


def test_conversion_catalog_exclusion_uses_duckdb_query(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    _empty_canonical_catalog(data_root)
    (data_root / "catalog" / "unrelated.bin").write_bytes(
        result.conversion_id.encode("ascii")
    )

    verification = verify_ss_legacy_conversion(
        data_root, result, archive_root=tmp_path
    )
    assert verification.catalog_status == "canonical_catalog_verified"


def test_conversion_rejects_pointer_registration(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    _empty_canonical_catalog(data_root)
    registered = _canonical_manifest(dataset_id=result.conversion_id)
    relative = "catalog/manifest_sets/fixture/registered.json"
    path = data_root / relative
    path.parent.mkdir(parents=True)
    path.write_text(registered.model_dump_json(), encoding="utf-8")
    (data_root / "catalog/current-manifest-set.json").write_text(
        json.dumps(
            {"schema_version": "1.0.0", "bundle_id": "bundle-fixture", "manifests": [relative]}
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="canonical pointer"):
        verify_ss_legacy_conversion(data_root, result, archive_root=tmp_path)


def test_conversion_rejects_duckdb_registration(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    _empty_canonical_catalog(data_root)
    catalog = DuckDBCatalog(data_root / "catalog/quant.duckdb")
    try:
        catalog.register(
            _canonical_manifest(
                storage_uri=(
                    "quant-data://legacy/stock_screener/"
                    f"{result.migration_input.archive_id}/{result.conversion_id}/"
                    "audited-prices.parquet"
                )
            )
        )
    finally:
        catalog.close()

    with pytest.raises(ValueError, match="canonical DuckDB"):
        verify_ss_legacy_conversion(data_root, result, archive_root=tmp_path)


def test_conversion_rejects_publication_receipt(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    _empty_canonical_catalog(data_root)
    receipts = data_root / "catalog/publication_receipts"
    receipts.mkdir()
    (receipts / "bundle-fixture.json").write_text(
        json.dumps(
            {
                "bundle_id": "bundle-fixture",
                "manifest_uris": [result.artifacts[0].uri],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="publication receipt"):
        verify_ss_legacy_conversion(data_root, result, archive_root=tmp_path)


def test_conversion_mapping_mic_consistency(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    artifact = next(item for item in result.artifacts if item.kind == "instrument_mapping")
    path = data_root / artifact.uri.removeprefix("quant-data://")
    table = pq.read_table(path)
    rows = table.to_pylist()
    mapped = next(row for row in rows if row["mapping_status"] == "mapped")
    mapped["canonical_mic"] = "XSHE"
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    forged = _refresh_artifact_evidence(data_root, result, "instrument_mapping")

    with pytest.raises(ValueError, match="candidate ID or MIC"):
        verify_ss_legacy_conversion(data_root, forged, archive_root=tmp_path)


def test_conversion_reconciles_all_report_versions_and_counts(tmp_path: Path) -> None:
    _layout, migration_input = _conversion_fixture(tmp_path)
    data_root = tmp_path / "quant-data"
    result = SSLegacyOfflineConverter(tmp_path, data_root).convert(migration_input)
    assert verify_ss_legacy_conversion(
        data_root, result, archive_root=tmp_path
    ).catalog_status == "canonical_catalog_absent"

    artifact = next(item for item in result.artifacts if item.kind == "mapping_report")
    path = data_root / artifact.uri.removeprefix("quant-data://")
    report = json.loads(path.read_text(encoding="utf-8"))
    report["contract_version"] = "0.0.0"
    path.write_text(json.dumps(report), encoding="utf-8")
    forged = _refresh_artifact_evidence(data_root, result, "mapping_report")
    with pytest.raises(ValueError, match="mapping report"):
        verify_ss_legacy_conversion(data_root, forged, archive_root=tmp_path)

def test_failed_exported_snapshot_archive_is_audit_only_and_restart_required(
    tmp_path: Path,
) -> None:
    first = ArchiveLayout(tmp_path, "ss-legacy-failed-fixture")
    first.initialize()
    store = ArchiveStateStore(first)
    store.record(ArchiveRunState.PLANNED)
    store.record(ArchiveRunState.SNAPSHOT_OPEN)
    failure = store.record(
        ArchiveRunState.FAILED,
        evidence={"audit_only": True, "restart_required": True},
    )
    assert failure.evidence == {"audit_only": True, "restart_required": True}
    first_id = build_archive_id("stock_screener", datetime.now(UTC), nonce="first")
    second_id = build_archive_id("stock_screener", datetime.now(UTC), nonce="second")
    assert first_id != second_id


def test_archive_cli_exposes_required_commands() -> None:
    parser = build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if action.dest == "command"  # noqa: SLF001
    )
    assert {"inventory", "backup", "export", "verify", "report", "run"}.issubset(subparsers.choices)


@pytest.mark.skipif(
    not (ROOT / "docs/SS_LEGACY_ARCHIVE_SNAPSHOT_2026-07-30.md").is_file(),
    reason="private operations snapshots are excluded from the public source",
)
def test_handoff_and_roadmap_phase_statuses_are_consistent() -> None:
    handoff = (ROOT / "docs" / "DATA_LAYER_HANDOFF.md").read_text(encoding="utf-8")
    roadmap = (ROOT / "docs" / "DATA_LAYER_ROADMAP.md").read_text(encoding="utf-8")
    archive = (ROOT / "docs" / "SS_LEGACY_ARCHIVE.md").read_text(encoding="utf-8")
    snapshot = (ROOT / "docs" / "SS_LEGACY_ARCHIVE_SNAPSHOT_2026-07-30.md").read_text(
        encoding="utf-8"
    )
    superseded = (
        ROOT / "docs" / "SS_LEGACY_ARCHIVE_SNAPSHOT_2026-07-29.md"
    ).read_text(encoding="utf-8")
    assert "Phase 2.1：completed" in handoff
    assert "Phase 2.2：completed" in handoff
    assert "Phase 2.3：current" in handoff
    assert "Phase 2.3A：completed（PR #9 merged）" in handoff
    assert "Phase 2.3B：completed" in handoff
    assert "Phase 2.3C：completed" in handoff
    assert "Phase 2.3D：current" in handoff
    assert "Phase 2.1（completed）" in roadmap
    assert "Phase 2.2（completed）" in roadmap
    assert "Phase 2.3（current）" in roadmap
    assert "2.3C（current）" not in roadmap
    assert "2.3D（current）" in roadmap
    assert "canonical_eligible: false" in archive
    assert "quarantined: true" in archive
    assert "blocked_encryption_recipient_unavailable" in snapshot
    assert "source_backup_id` 和" in snapshot
    assert "superseded / audit-only" in superseded
    for document in (handoff, roadmap, archive, snapshot, superseded):
        assert "/Users/" not in document
        assert "/Volumes/" not in document
        assert "postgresql://" not in document
