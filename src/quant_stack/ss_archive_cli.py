from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import secrets
import shutil
import sys
import time
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .data.continuity import (
    ArchiveRunState,
    BackupStatus,
    RestoreStatus,
    SnapshotConsistency,
    SourceHistoryStatus,
    SSArchiveArtifactEvidence,
    SSArchiveOperationalEvidence,
    SSArchiveSourceLineage,
    SSDatabaseInventory,
    SSLegacyArchiveManifest,
    SSRestrictedBackupEvidence,
)
from .data.continuity.ss_archive_backup import RestrictedBackupRunner
from .data.continuity.ss_archive_export import (
    SSArchiveVerifier,
    StreamingParquetExporter,
    archive_artifact_evidence,
    verify_archive_manifest,
    write_archive_manifest,
)
from .data.continuity.ss_archive_inventory import PostgresSnapshot, SSInventoryCollector
from .data.continuity.ss_archive_safety import (
    ArchiveLayout,
    build_archive_id,
    scan_secret_text,
    validate_identifier,
)
from .data.continuity.ss_archive_state import ArchiveStateStore
from .data.storage.atomic import atomic_write_immutable_bytes


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is not configured: {name}")
    return value


def _layout(archive_id: str) -> ArchiveLayout:
    root = Path(_required_environment("SS_LEGACY_ARCHIVE_ROOT"))
    layout = ArchiveLayout(root=root, archive_id=archive_id)
    layout.initialize()
    return layout


def _api_health() -> str:
    url = os.environ.get("SS_LEGACY_API_HEALTH_URL")
    if not url:
        return "not_configured"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            if 200 <= response.status < 300:
                return "healthy"
    except Exception as exc:
        raise RuntimeError("SS API health gate failed") from exc
    raise RuntimeError("SS API health gate failed")


def _database_health(dsn: str) -> tuple[str, int]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - optional environment
        raise RuntimeError("PostgreSQL health probe requires the 'archive' extra") from exc
    with psycopg.connect(dsn) as connection:
        cursor = connection.cursor()
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")
        count = int(cursor.fetchone()[0])
        connection.rollback()
    return "healthy", count


def _operational_evidence(
    *,
    started_at: datetime,
    started_monotonic: float,
    layout: ArchiveLayout,
    db_connections_pre: int,
    db_connections_during: int,
    db_connections_post: int,
    api_health_pre: str,
    api_health_during: str,
    api_health_post: str,
    postgres_health_pre: str = "healthy",
    postgres_health_during: str = "healthy",
    postgres_health_post: str = "healthy",
    disk_available_bytes_pre: int,
) -> SSArchiveOperationalEvidence:
    finished_at = datetime.now(UTC)
    raw_peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_kib = raw_peak_rss // 1024 if sys.platform == "darwin" else raw_peak_rss
    return SSArchiveOperationalEvidence(
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=time.monotonic() - started_monotonic,
        peak_rss_kib=peak_rss_kib,
        disk_available_bytes_pre=disk_available_bytes_pre,
        disk_available_bytes_post=shutil.disk_usage(layout.root).free,
        db_connections_pre=db_connections_pre,
        db_connections_during=db_connections_during,
        db_connections_post=db_connections_post,
        api_health_pre=api_health_pre,
        api_health_during=api_health_during,
        api_health_post=api_health_post,
        postgres_health_pre=postgres_health_pre,
        postgres_health_during=postgres_health_during,
        postgres_health_post=postgres_health_post,
    )


def _write_immutable_json(path: Path, model: Any, *, artifact_kind: str) -> None:
    payload = (json.dumps(model.model_dump(mode="json"), sort_keys=True, indent=2) + "\n").encode()
    atomic_write_immutable_bytes(
        path,
        payload,
        artifact_kind=artifact_kind,
        validate=lambda temporary: type(model).model_validate_json(
            temporary.read_text(encoding="utf-8")
        ),
    )


def _load_inventory(layout: ArchiveLayout) -> SSDatabaseInventory:
    return SSDatabaseInventory.model_validate_json(
        layout.archive_path("reports", "inventory.json").read_text(encoding="utf-8")
    )


def _load_manifest(layout: ArchiveLayout) -> SSLegacyArchiveManifest:
    return SSLegacyArchiveManifest.model_validate_json(
        layout.archive_path("frozen-exports", "manifest.json").read_text(encoding="utf-8")
    )


def _backup_evidence_path(layout: ArchiveLayout) -> Path:
    return layout.archive_path("backups", "restricted-manifest", "evidence.json")


def _write_backup_evidence(
    layout: ArchiveLayout, evidence: SSRestrictedBackupEvidence
) -> SSArchiveArtifactEvidence:
    payload = (
        json.dumps(evidence.model_dump(mode="json"), sort_keys=True, indent=2) + "\n"
    ).encode()
    digest = hashlib.sha256(payload).hexdigest()
    relative = f"restricted-manifest/evidence-{evidence.status}-{digest[:12]}.json"
    _write_immutable_json(
        layout.archive_path("backups", relative),
        evidence,
        artifact_kind="restricted backup evidence",
    )
    return archive_artifact_evidence(
        layout,
        section="backups",
        relative_path=relative,
    )


def _load_backup_evidence(layout: ArchiveLayout) -> SSRestrictedBackupEvidence:
    root = layout.archive_path("backups", "restricted-manifest")
    versioned = tuple(root.glob("evidence-*.json")) if root.exists() else ()
    if versioned:
        path = max(versioned, key=lambda item: item.stat().st_mtime_ns)
        return SSRestrictedBackupEvidence.model_validate_json(path.read_text(encoding="utf-8"))
    legacy_path = _backup_evidence_path(layout)
    if legacy_path.exists():
        return SSRestrictedBackupEvidence.model_validate_json(
            legacy_path.read_text(encoding="utf-8")
        )
    return SSRestrictedBackupEvidence(
        status=BackupStatus.NOT_RUN,
        plaintext_deleted=True,
        required_tables_listed=False,
        secret_table_data_absent=False,
        decryptability_verified=False,
    )


def _backup_evidence_artifact(layout: ArchiveLayout) -> SSArchiveArtifactEvidence:
    root = layout.archive_path("backups", "restricted-manifest")
    versioned = tuple(root.glob("evidence-*.json")) if root.exists() else ()
    if not versioned:
        raise RuntimeError("versioned backup evidence is unavailable")
    path = max(versioned, key=lambda item: item.stat().st_mtime_ns)
    return archive_artifact_evidence(
        layout,
        section="backups",
        relative_path=f"restricted-manifest/{path.name}",
    )


def _inventory_artifact(layout: ArchiveLayout) -> SSArchiveArtifactEvidence:
    return archive_artifact_evidence(
        layout,
        section="reports",
        relative_path="inventory.json",
    )


def _cursor_batches(
    connection: Any,
    *,
    table_name: str,
    columns: Sequence[str],
    primary_key: Sequence[str],
    fetch_rows: int,
) -> Iterator[list[Mapping[str, Any]]]:
    validate_identifier(table_name)
    selected = tuple(validate_identifier(name) for name in columns)
    ordering = tuple(validate_identifier(name) for name in primary_key)
    sql = f"SELECT {', '.join(selected)} FROM public.{table_name} ORDER BY {', '.join(ordering)}"
    try:
        cursor = connection.cursor(name=f"ss_archive_{table_name}_{secrets.token_hex(4)}")
    except TypeError:
        cursor = connection.cursor()
    cursor.execute(sql)
    description = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
    while True:
        rows = cursor.fetchmany(fetch_rows)
        if not rows:
            break
        if isinstance(rows[0], Mapping):
            yield list(rows)
        else:
            yield [dict(zip(description, row, strict=True)) for row in rows]


def _column_types(inventory: SSDatabaseInventory, table_name: str) -> tuple[tuple[str, str], ...]:
    table = next(item for item in inventory.tables if item.table_name == table_name)
    return tuple((column.name, column.data_type) for column in table.columns)


def _table_primary_key(inventory: SSDatabaseInventory, table_name: str) -> tuple[str, ...]:
    table = next(item for item in inventory.tables if item.table_name == table_name)
    if not table.primary_key:
        raise ValueError(f"required table has no stable primary key: {table_name}")
    return table.primary_key


def _export_required_tables(
    snapshot: PostgresSnapshot,
    layout: ArchiveLayout,
    inventory: SSDatabaseInventory,
    *,
    shard_rows: int,
    coverage_rows: Sequence[Mapping[str, Any]],
) -> tuple[Any, ...]:
    files: list[Any] = []
    coverage_exporter = StreamingParquetExporter(
        layout,
        table_name="coverage",
        columns=(
            ("market", "character varying"),
            ("symbol", "character varying"),
            ("min_date", "date"),
            ("max_date", "date"),
            ("row_count", "bigint"),
        ),
        primary_key=("market", "symbol"),
        shard_rows=max(shard_rows, len(coverage_rows) or 1),
    )
    files.extend(coverage_exporter.write((coverage_rows,)))
    for table_name in ("stock_prices", "stock_universe"):
        follower = snapshot.follower_connection()
        try:
            column_types = _column_types(inventory, table_name)
            primary_key = _table_primary_key(inventory, table_name)
            exporter = StreamingParquetExporter(
                layout,
                table_name=table_name,
                columns=column_types,
                primary_key=primary_key,
                shard_rows=shard_rows,
            )
            files.extend(
                exporter.write(
                    _cursor_batches(
                        follower,
                        table_name=table_name,
                        columns=tuple(name for name, _ in column_types),
                        primary_key=primary_key,
                        fetch_rows=min(50_000, shard_rows),
                    )
                )
            )
        finally:
            follower.rollback()
            follower.close()
    return tuple(files)


def _collect_inventory(
    snapshot: PostgresSnapshot,
    layout: ArchiveLayout,
    *,
    source_commit: str,
    source_history: SourceHistoryStatus,
    retention_policy: str,
    cleanup_evidence: str,
) -> tuple[SSDatabaseInventory, tuple[Mapping[str, Any], ...]]:
    assert snapshot.connection is not None and snapshot.metadata is not None
    collector = SSInventoryCollector(snapshot.connection, snapshot.metadata)
    coverage_rows = tuple(collector.per_symbol_coverage())
    inventory = collector.collect(
        archive_id=layout.archive_id,
        source_commit=source_commit,
        retention_policy=retention_policy,
        cleanup_evidence=cleanup_evidence,
        per_symbol_coverage_uri=layout.logical_uri(
            "frozen-exports", "coverage", "part-00000.parquet"
        ),
        source_history_export=source_history,
    )
    _write_immutable_json(
        layout.archive_path("reports", "inventory.json"),
        inventory,
        artifact_kind="SS source inventory",
    )
    return inventory, coverage_rows


def _backup(
    snapshot: PostgresSnapshot,
    layout: ArchiveLayout,
    inventory: SSDatabaseInventory,
) -> SSRestrictedBackupEvidence:
    assert snapshot.metadata is not None
    environment = snapshot.pg_environment(os.environ)
    return RestrictedBackupRunner(
        layout,
        environment=environment,
        connection_fingerprint=snapshot.metadata.connection_fingerprint,
    ).run(
        snapshot_id=snapshot.metadata.raw_snapshot_id,
        inventory_tables=inventory.all_table_names,
        expected_connection_fingerprint=inventory.connection_fingerprint,
    )


def _manifest(
    layout: ArchiveLayout,
    inventory: SSDatabaseInventory,
    files: tuple[Any, ...],
    backup: SSRestrictedBackupEvidence,
    *,
    inventory_artifact: SSArchiveArtifactEvidence,
    backup_evidence_artifact: SSArchiveArtifactEvidence,
    consistency: SnapshotConsistency,
    operational: SSArchiveOperationalEvidence,
    export_snapshot_fingerprint: str | None = None,
) -> SSLegacyArchiveManifest:
    if backup.schema_snapshot is None or backup.snapshot_fingerprint is None:
        raise RuntimeError("manifest requires snapshot-bound schema evidence")
    artifact_fingerprints = {
        "inventory": inventory.snapshot_fingerprint,
        "schema": backup.snapshot_fingerprint,
        "export": export_snapshot_fingerprint or inventory.snapshot_fingerprint,
    }
    if backup.status == BackupStatus.VERIFIED_ENCRYPTED:
        artifact_fingerprints["restricted_backup"] = backup.snapshot_fingerprint
    return SSLegacyArchiveManifest(
        archive_id=layout.archive_id,
        source_commit=inventory.source_commit,
        source_schema_revision=inventory.source_schema_revision,
        postgresql_version=inventory.postgresql_version,
        snapshot_fingerprint=inventory.snapshot_fingerprint,
        consistency=consistency,
        artifact_snapshot_fingerprints=artifact_fingerprints,
        exported_at=datetime.now(UTC),
        archive_root_uri=f"archive://ss-legacy/frozen-exports/{layout.archive_id}/",
        inventory_artifact=inventory_artifact,
        backup_evidence_artifact=backup_evidence_artifact,
        table_inventories=inventory.tables,
        files=files,
        coverage=inventory.coverage,
        backup=backup,
        restore_status=(
            RestoreStatus.LISTING_VERIFIED_RESTORE_PENDING
            if backup.status == BackupStatus.VERIFIED_ENCRYPTED
            else RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE
        ),
        operational=operational,
        lineage=SSArchiveSourceLineage(
            source_snapshot_fingerprint=inventory.snapshot_fingerprint,
            source_inventory_uri=inventory_artifact.logical_uri,
            source_inventory_sha256=inventory_artifact.sha256,
            source_export_id=layout.archive_id,
            source_backup_id=(
                layout.archive_id if backup.status == BackupStatus.VERIFIED_ENCRYPTED else None
            ),
            source_backup_status=backup.status,
            source_backup_uri=backup.logical_uri,
        ),
        includes_active_symbols=inventory.coverage.active_universe_symbols > 0,
        delisted_coverage=inventory.coverage.delisted_coverage,
        source_history_export=inventory.coverage.source_history_export,
        known_retention_policy=inventory.retention_policy,
        limitations=(
            "Legacy volume units are unknown or mixed.",
            "adj_close semantics have not been independently verified.",
            "Market-history completeness is not claimed.",
            "The archive is quarantined and is not registered in the canonical catalog.",
        ),
    )


def _record_if_needed(
    store: ArchiveStateStore,
    state: ArchiveRunState,
    evidence: dict[str, str | int | bool | None] | None = None,
    *,
    attempt_id: str | None = None,
) -> None:
    if store.current_state() == state:
        return
    store.record(state, evidence=evidence, attempt_id=attempt_id)


def _sanitized_result(
    inventory: SSDatabaseInventory,
    manifest: SSLegacyArchiveManifest | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "archive_id": inventory.archive_id,
        "snapshot_timestamp": inventory.snapshot_timestamp.isoformat(),
        "consistency": inventory.consistency,
        "required_tables": {
            table.table_name: table.exact_row_count
            for table in inventory.tables
            if table.table_name in {"stock_prices", "stock_universe"}
        },
        "distinct_symbols": inventory.coverage.distinct_price_symbols,
        "date_range": [inventory.coverage.min_date, inventory.coverage.max_date],
        "markets": [item.model_dump(mode="json") for item in inventory.coverage.markets],
        "archive_uri": f"archive://ss-legacy/reports/{inventory.archive_id}/",
    }
    if manifest is not None:
        result.update(
            {
                "backup_status": manifest.backup.status,
                "restore_status": manifest.restore_status,
                "file_count": len(manifest.files),
                "export_rows": sum(
                    item.row_count
                    for item in manifest.files
                    if item.table_name in {"stock_prices", "stock_universe"}
                ),
                "export_size_bytes": sum(item.size_bytes for item in manifest.files),
                "verified": True,
                "canonical_eligible": False,
                "quarantined": True,
                "duration_seconds": manifest.operational.duration_seconds,
                "peak_rss_kib": manifest.operational.peak_rss_kib,
            }
        )
    return result


def _write_report(
    layout: ArchiveLayout,
    inventory: SSDatabaseInventory,
    manifest: SSLegacyArchiveManifest | None,
) -> Path:
    markets = ", ".join(
        f"{item.market}: {item.row_count} rows / {item.distinct_symbols} symbols"
        for item in inventory.coverage.markets
    )
    backup_status = manifest.backup.status if manifest else BackupStatus.NOT_RUN
    restore_status = manifest.restore_status if manifest else RestoreStatus.NOT_RUN
    files = len(manifest.files) if manifest else 0
    size = sum(item.size_bytes for item in manifest.files) if manifest else 0
    duration = manifest.operational.duration_seconds if manifest else "not_recorded"
    peak_rss = manifest.operational.peak_rss_kib if manifest else "not_recorded"
    price_counts = (
        f"stock_prices={inventory.coverage.stock_prices_row_count}, "
        f"stock_universe={inventory.coverage.stock_universe_row_count}"
    )
    universe_counts = (
        f"{inventory.coverage.active_universe_symbols}/"
        f"{inventory.coverage.non_active_universe_symbols}"
    )
    content = f"""# SS Legacy Archive Sanitized Coverage Summary

Snapshot timestamp: {inventory.snapshot_timestamp.isoformat()}

- Archive ID: `{inventory.archive_id}`
- Snapshot consistency: `{inventory.consistency}`
- PostgreSQL version: `{inventory.postgresql_version}`
- Schema revision: `{inventory.source_schema_revision}`
- Required tables: {price_counts}
- Price symbols: {inventory.coverage.distinct_price_symbols}
- Price date range: {inventory.coverage.min_date} to {inventory.coverage.max_date}
- Market coverage: {markets}
- Active/non-active universe: {universe_counts}
- Orphan price symbols: {inventory.coverage.orphan_price_symbols}
- Universe symbols without prices: {inventory.coverage.universe_symbols_without_prices}
- Delisted coverage evidence: `{inventory.coverage.delisted_coverage}`
- Source-history status: `{inventory.coverage.source_history_export}`
- Backup status: `{backup_status}`
- Restore status: `{restore_status}`
- Frozen export files/bytes: {files}/{size}
- Execution duration seconds: {duration}
- Peak RSS KiB: {peak_rss}
- Volume unit: `unknown_or_mixed`
- Adjustment semantics: `unverified`
- Canonical eligible: `false`
- Quarantined: `true`

This is a point-in-time aggregate. It contains no database name, user, host, credential,
or physical archive path.
"""
    findings = scan_secret_text(content)
    if findings:
        raise ValueError(f"sanitized report secret scan failed: {', '.join(findings)}")
    path = layout.archive_path("reports", "coverage-summary.md")
    atomic_write_immutable_bytes(
        path,
        content.encode(),
        artifact_kind="sanitized SS coverage report",
    )
    return path


def _verify_archive_text_secret_boundary(layout: ArchiveLayout) -> None:
    findings: set[str] = set()
    for section in ("backups", "frozen-exports", "verification", "reports", "state"):
        root = layout.archive_path(section)
        for path in root.glob("**/*"):
            if path.is_file() and path.suffix.lower() in {".json", ".md", ".sql", ".txt"}:
                findings.update(scan_secret_text(path.read_text(encoding="utf-8")))
    if findings:
        raise ValueError("archive text secret scan failed")


def _run_full(args: argparse.Namespace) -> int:
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    attempt_id = f"attempt-{secrets.token_hex(8)}"
    archive_id = args.archive_id or build_archive_id("stock_screener", datetime.now(UTC))
    layout = _layout(archive_id)
    state = ArchiveStateStore(layout)
    current_state = state.current_state()
    if current_state in {ArchiveRunState.BLOCKED, ArchiveRunState.COMPLETED}:
        inventory = _load_inventory(layout)
        manifest = SSArchiveVerifier(layout).verify()
        _verify_archive_text_secret_boundary(layout)
        print(json.dumps(_sanitized_result(inventory, manifest), default=str, indent=2))
        return 0 if current_state == ArchiveRunState.COMPLETED else 3
    if current_state is not None:
        raise RuntimeError(
            "interrupted exported-snapshot archive is audit-only; restart with a new archive ID"
        )
    dsn = _required_environment("SS_LEGACY_DATABASE_DSN")
    disk_available_bytes_pre = shutil.disk_usage(layout.root).free
    minimum_free = int(os.environ.get("SS_LEGACY_MIN_FREE_BYTES", "2000000000"))
    if disk_available_bytes_pre < minimum_free:
        raise RuntimeError("archive disk space gate failed")
    api_health_pre = _api_health()
    postgres_health_pre, db_connections_pre = _database_health(dsn)
    _record_if_needed(state, ArchiveRunState.PLANNED, attempt_id=attempt_id)
    try:
        with PostgresSnapshot(dsn) as snapshot:
            _record_if_needed(
                state, ArchiveRunState.SNAPSHOT_OPEN, attempt_id=attempt_id
            )
            inventory, coverage_rows = _collect_inventory(
                snapshot,
                layout,
                source_commit=args.source_commit,
                source_history=SourceHistoryStatus(args.source_history),
                retention_policy=args.retention_policy,
                cleanup_evidence=args.cleanup_evidence,
            )
            _record_if_needed(
                state,
                ArchiveRunState.INVENTORY_COMPLETE,
                {"stock_prices_rows": inventory.coverage.stock_prices_row_count},
                attempt_id=attempt_id,
            )
            backup = _backup(snapshot, layout, inventory)
            backup_artifact = _write_backup_evidence(layout, backup)
            if backup.status == BackupStatus.VERIFIED_ENCRYPTED:
                _record_if_needed(
                    state, ArchiveRunState.BACKUP_STAGED, attempt_id=attempt_id
                )
                _record_if_needed(
                    state, ArchiveRunState.BACKUP_ENCRYPTED, attempt_id=attempt_id
                )
            files = _export_required_tables(
                snapshot,
                layout,
                inventory,
                shard_rows=args.shard_rows,
                coverage_rows=coverage_rows,
            )
            api_health_during = _api_health()
            postgres_health_during, db_connections_during = _database_health(dsn)
            _record_if_needed(
                state,
                ArchiveRunState.EXPORT_COMPLETE,
                {"file_count": len(files)},
                attempt_id=attempt_id,
            )
        _record_if_needed(
            state, ArchiveRunState.SNAPSHOT_CLOSED, attempt_id=attempt_id
        )
        api_health_post = _api_health()
        postgres_health_post, db_connections_post = _database_health(dsn)
        operational = _operational_evidence(
            started_at=started_at,
            started_monotonic=started_monotonic,
            layout=layout,
            db_connections_pre=db_connections_pre,
            db_connections_during=db_connections_during,
            db_connections_post=db_connections_post,
            api_health_pre=api_health_pre,
            api_health_during=api_health_during,
            api_health_post=api_health_post,
            postgres_health_pre=postgres_health_pre,
            postgres_health_during=postgres_health_during,
            postgres_health_post=postgres_health_post,
            disk_available_bytes_pre=disk_available_bytes_pre,
        )
        manifest = _manifest(
            layout,
            inventory,
            files,
            backup,
            inventory_artifact=_inventory_artifact(layout),
            backup_evidence_artifact=backup_artifact,
            consistency=SnapshotConsistency.EXPORTED_SNAPSHOT,
            operational=operational,
            export_snapshot_fingerprint=inventory.snapshot_fingerprint,
        )
        verify_archive_manifest(layout, manifest)
        manifest = manifest.model_copy(
            update={
                "operational": _operational_evidence(
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    layout=layout,
                    db_connections_pre=db_connections_pre,
                    db_connections_during=db_connections_during,
                    db_connections_post=db_connections_post,
                    api_health_pre=api_health_pre,
                    api_health_during=api_health_during,
                    api_health_post=api_health_post,
                    postgres_health_pre=postgres_health_pre,
                    postgres_health_during=postgres_health_during,
                    postgres_health_post=postgres_health_post,
                    disk_available_bytes_pre=disk_available_bytes_pre,
                )
            }
        )
        write_archive_manifest(layout, manifest)
        SSArchiveVerifier(layout).verify(verify_state=False)
        _record_if_needed(
            state, ArchiveRunState.VERIFICATION_COMPLETE, attempt_id=attempt_id
        )
        _record_if_needed(
            state,
            ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
            attempt_id=attempt_id,
        )
        if backup.status != BackupStatus.VERIFIED_ENCRYPTED:
            _record_if_needed(
                state,
                ArchiveRunState.BLOCKED,
                {"reason": "encryption_recipient_unavailable"},
                attempt_id=attempt_id,
            )
        _write_report(layout, inventory, manifest)
        _verify_archive_text_secret_boundary(layout)
        SSArchiveVerifier(layout).verify()
        result = _sanitized_result(inventory, manifest)
        print(json.dumps(result, default=str, indent=2))
        # A verified encrypted backup still needs an isolated restore before this
        # archive can be completed. Code 3 means a documented safety gate remains.
        return 3
    except Exception as exc:
        if state.current_state() not in {
            ArchiveRunState.FAILED,
            ArchiveRunState.BLOCKED,
            ArchiveRunState.COMPLETED,
        }:
            state.record(
                ArchiveRunState.FAILED,
                evidence={
                    "error_type": type(exc).__name__,
                    "audit_only": True,
                    "restart_required": True,
                },
                attempt_id=attempt_id,
            )
        raise


def _inventory_command(args: argparse.Namespace) -> int:
    dsn = _required_environment("SS_LEGACY_DATABASE_DSN")
    archive_id = args.archive_id or build_archive_id("stock_screener", datetime.now(UTC))
    layout = _layout(archive_id)
    state = ArchiveStateStore(layout)
    _record_if_needed(state, ArchiveRunState.PLANNED)
    with PostgresSnapshot(dsn) as snapshot:
        _record_if_needed(state, ArchiveRunState.SNAPSHOT_OPEN)
        inventory, coverage_rows = _collect_inventory(
            snapshot,
            layout,
            source_commit=args.source_commit,
            source_history=SourceHistoryStatus(args.source_history),
            retention_policy=args.retention_policy,
            cleanup_evidence=args.cleanup_evidence,
        )
        _record_if_needed(state, ArchiveRunState.INVENTORY_COMPLETE)
        coverage_exporter = StreamingParquetExporter(
            layout,
            table_name="coverage",
            columns=(
                ("market", "character varying"),
                ("symbol", "character varying"),
                ("min_date", "date"),
                ("max_date", "date"),
                ("row_count", "bigint"),
            ),
            primary_key=("market", "symbol"),
            shard_rows=max(250_000, len(coverage_rows) or 1),
        )
        coverage_exporter.write((coverage_rows,))
    _record_if_needed(state, ArchiveRunState.SNAPSHOT_CLOSED)
    print(json.dumps(_sanitized_result(inventory), default=str, indent=2))
    return 0


def _verify_command(args: argparse.Namespace) -> int:
    layout = _layout(args.archive_id)
    inventory = _load_inventory(layout)
    state = ArchiveStateStore(layout)
    if state.current_state() in {
        ArchiveRunState.BLOCKED,
        ArchiveRunState.COMPLETED,
        ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
    }:
        manifest = SSArchiveVerifier(layout).verify()
        _verify_archive_text_secret_boundary(layout)
        print(json.dumps(_sanitized_result(inventory, manifest), default=str, indent=2))
        return 0
    manifest = SSArchiveVerifier(layout).verify(verify_state=False)
    _verify_archive_text_secret_boundary(layout)
    _record_if_needed(state, ArchiveRunState.VERIFICATION_COMPLETE)
    _record_if_needed(state, ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING)
    if manifest.backup.status != BackupStatus.VERIFIED_ENCRYPTED:
        _record_if_needed(
            state,
            ArchiveRunState.BLOCKED,
            {"reason": "encryption_recipient_unavailable"},
        )
    SSArchiveVerifier(layout).verify()
    print(json.dumps(_sanitized_result(inventory, manifest), default=str, indent=2))
    return 0


def _backup_command(args: argparse.Namespace) -> int:
    layout = _layout(args.archive_id)
    inventory = _load_inventory(layout)
    state = ArchiveStateStore(layout)
    dsn = _required_environment("SS_LEGACY_DATABASE_DSN")
    if state.current_state() != ArchiveRunState.SNAPSHOT_CLOSED:
        raise RuntimeError(
            "backup requires a closed inventory snapshot; interrupted archives are audit-only"
        )
    with PostgresSnapshot(dsn) as snapshot:
        _record_if_needed(state, ArchiveRunState.SNAPSHOT_OPEN)
        evidence = _backup(snapshot, layout, inventory)
        _write_backup_evidence(layout, evidence)
        if evidence.status == BackupStatus.VERIFIED_ENCRYPTED:
            _record_if_needed(state, ArchiveRunState.BACKUP_STAGED)
            _record_if_needed(state, ArchiveRunState.BACKUP_ENCRYPTED)
    _record_if_needed(state, ArchiveRunState.SNAPSHOT_CLOSED)
    print(
        json.dumps(
            {
                "archive_id": layout.archive_id,
                "backup_status": evidence.status,
                "plaintext_deleted": evidence.plaintext_deleted,
                "required_tables_listed": evidence.required_tables_listed,
            },
            default=str,
            indent=2,
        )
    )
    return 0 if evidence.status == BackupStatus.VERIFIED_ENCRYPTED else 3


def _export_command(args: argparse.Namespace) -> int:
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    layout = _layout(args.archive_id)
    disk_available_bytes_pre = shutil.disk_usage(layout.root).free
    inventory = _load_inventory(layout)
    state = ArchiveStateStore(layout)
    dsn = _required_environment("SS_LEGACY_DATABASE_DSN")
    api_health_pre = _api_health()
    postgres_health_pre, db_connections_pre = _database_health(dsn)
    backup = _load_backup_evidence(layout)
    if backup.schema_snapshot is None:
        raise RuntimeError("export requires a verified schema-only snapshot")
    if state.current_state() != ArchiveRunState.SNAPSHOT_CLOSED:
        raise RuntimeError(
            "export requires a closed prior step; interrupted archives are audit-only"
        )
    with PostgresSnapshot(dsn) as snapshot:
        _record_if_needed(state, ArchiveRunState.SNAPSHOT_OPEN)
        assert snapshot.connection is not None and snapshot.metadata is not None
        collector = SSInventoryCollector(snapshot.connection, snapshot.metadata)
        coverage_rows = tuple(collector.per_symbol_coverage())
        files = _export_required_tables(
            snapshot,
            layout,
            inventory,
            shard_rows=args.shard_rows,
            coverage_rows=coverage_rows,
        )
        api_health_during = _api_health()
        postgres_health_during, db_connections_during = _database_health(dsn)
        export_snapshot_fingerprint = snapshot.metadata.snapshot_fingerprint
        _record_if_needed(state, ArchiveRunState.EXPORT_COMPLETE, {"file_count": len(files)})
    _record_if_needed(state, ArchiveRunState.SNAPSHOT_CLOSED)
    api_health_post = _api_health()
    postgres_health_post, db_connections_post = _database_health(dsn)
    operational = _operational_evidence(
        started_at=started_at,
        started_monotonic=started_monotonic,
        layout=layout,
        db_connections_pre=db_connections_pre,
        db_connections_during=db_connections_during,
        db_connections_post=db_connections_post,
        api_health_pre=api_health_pre,
        api_health_during=api_health_during,
        api_health_post=api_health_post,
        postgres_health_pre=postgres_health_pre,
        postgres_health_during=postgres_health_during,
        postgres_health_post=postgres_health_post,
        disk_available_bytes_pre=disk_available_bytes_pre,
    )
    manifest = _manifest(
        layout,
        inventory,
        files,
        backup,
        inventory_artifact=_inventory_artifact(layout),
        backup_evidence_artifact=_backup_evidence_artifact(layout),
        consistency=SnapshotConsistency.BEST_EFFORT,
        operational=operational,
        export_snapshot_fingerprint=export_snapshot_fingerprint,
    )
    write_archive_manifest(layout, manifest)
    print(json.dumps(_sanitized_result(inventory, manifest), default=str, indent=2))
    return 0


def _report_command(args: argparse.Namespace) -> int:
    layout = _layout(args.archive_id)
    inventory = _load_inventory(layout)
    manifest_path = layout.archive_path("frozen-exports", "manifest.json")
    if not manifest_path.exists():
        raise RuntimeError("report requires a frozen export manifest")
    manifest = _load_manifest(layout)
    _write_report(layout, inventory, manifest)
    print(json.dumps(_sanitized_result(inventory, manifest), default=str, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-stack-ss-archive")
    commands = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--archive-id")
    common.add_argument("--source-commit", required=True)
    common.add_argument("--source-history", choices=tuple(SourceHistoryStatus), default="unknown")
    common.add_argument(
        "--retention-policy",
        default="source cleanup policy observed; market-history completeness not claimed",
    )
    common.add_argument(
        "--cleanup-evidence",
        default="cleanup schedule and task definition observed read-only",
    )
    commands.add_parser("inventory", parents=[common])
    run = commands.add_parser("run", parents=[common])
    run.add_argument("--shard-rows", type=int, default=250_000)
    for name in ("backup", "export", "verify", "report"):
        command = commands.add_parser(name)
        command.add_argument("archive_id")
        if name == "export":
            command.add_argument("--shard-rows", type=int, default=250_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "run":
            return _run_full(args)
        if args.command == "inventory":
            return _inventory_command(args)
        if args.command == "verify":
            return _verify_command(args)
        if args.command == "report":
            return _report_command(args)
        if args.command == "backup":
            return _backup_command(args)
        if args.command == "export":
            return _export_command(args)
    except Exception as exc:
        print(
            f"{type(exc).__name__}: archive operation failed; inspect restricted state evidence",
            file=sys.stderr,
        )
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
