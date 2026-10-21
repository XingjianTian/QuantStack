from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Literal

from pydantic import Field

from ..models import DataModel
from .ss_archive_export import SSArchiveVerifier
from .ss_archive_models import (
    BackupStatus,
    RestoreStatus,
    SnapshotConsistency,
)
from .ss_archive_safety import ArchiveLayout

DESIGNATED_SS_ARCHIVE_ID = "ss-legacy-20260730t145750z-6b90c3173ee2"
SUPERSEDED_SS_ARCHIVE_IDS = frozenset({"ss-legacy-20260729t153614z-0e6a6deb49e5"})
DESIGNATED_SS_MANIFEST_SHA256 = (
    "6e41fa170b419e421fb2d1a9d67667c767f592e0b75608613d99039ecc7ee70d"
)
DESIGNATED_SS_STOCK_PRICES_ROWS = 2_651_950
DESIGNATED_SS_STOCK_UNIVERSE_ROWS = 5_542
DESIGNATED_SS_PARQUET_FILES = 13


class SSArchiveMigrationPolicy(DataModel):
    """Explicit acknowledgement of Phase 2.2 blockers for conversion-only use."""

    accept_backup_blocker: bool = False
    accept_restore_blocker: bool = False
    purpose: Literal["phase_23_offline_conversion"] = "phase_23_offline_conversion"


class SSArchiveMigrationInput(DataModel):
    archive_id: str
    archive_root_uri: str = Field(pattern=r"^archive://")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    snapshot_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(min_length=7)
    source_schema_revision: str = Field(min_length=1)
    stock_prices_rows: int = Field(ge=0)
    stock_universe_rows: int = Field(ge=0)
    accepted_for_phase_23: Literal[True] = True
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True
    backup_complete: Literal[False] = False
    restore_verified: Literal[False] = False
    cutover_blocker_open: Literal[True] = True


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def accept_ss_archive_for_phase_23(
    archive_root: str | Path,
    *,
    archive_id: str,
    policy: SSArchiveMigrationPolicy,
    pg_restore: str = "pg_restore",
) -> SSArchiveMigrationInput:
    """Run the complete archive verifier and return a conversion-only input record."""

    if archive_id in SUPERSEDED_SS_ARCHIVE_IDS:
        raise ValueError("superseded SS archive cannot be selected for Phase 2.3")
    if archive_id != DESIGNATED_SS_ARCHIVE_ID:
        raise ValueError("SS archive is not the designated Phase 2.3 migration input")
    if not policy.accept_backup_blocker or not policy.accept_restore_blocker:
        raise ValueError("backup and restore blockers must be explicitly accepted")

    layout = ArchiveLayout(Path(archive_root), archive_id)
    manifest = SSArchiveVerifier(layout, pg_restore=pg_restore).verify()
    manifest_path = layout.archive_path("frozen-exports", "manifest.json")
    manifest_hash = _sha256(manifest_path)
    if manifest_hash != DESIGNATED_SS_MANIFEST_SHA256:
        raise ValueError("designated archive manifest hash does not match the locked input")
    if manifest.consistency != SnapshotConsistency.EXPORTED_SNAPSHOT:
        raise ValueError("designated archive must use exported-snapshot consistency")
    if len(manifest.files) != DESIGNATED_SS_PARQUET_FILES:
        raise ValueError("designated archive Parquet file count does not match")
    rows = {item.table_name: item.exact_row_count for item in manifest.table_inventories}
    if rows.get("stock_prices") != DESIGNATED_SS_STOCK_PRICES_ROWS:
        raise ValueError("designated archive stock_prices row count does not match")
    if rows.get("stock_universe") != DESIGNATED_SS_STOCK_UNIVERSE_ROWS:
        raise ValueError("designated archive stock_universe row count does not match")
    if manifest.backup.status != BackupStatus.BLOCKED_ENCRYPTION_RECIPIENT_UNAVAILABLE:
        raise ValueError("designated archive backup blocker changed")
    if manifest.restore_status != RestoreStatus.BLOCKED_BACKUP_UNAVAILABLE:
        raise ValueError("designated archive restore blocker changed")
    if manifest.lineage.source_backup_id is not None:
        raise ValueError("blocked designated archive cannot name a source backup")
    if manifest.canonical_eligible or not manifest.quarantined:
        raise ValueError("legacy archive must remain non-canonical and quarantined")

    return SSArchiveMigrationInput(
        archive_id=manifest.archive_id,
        archive_root_uri=manifest.archive_root_uri,
        manifest_sha256=manifest_hash,
        snapshot_fingerprint=manifest.snapshot_fingerprint,
        source_commit=manifest.source_commit,
        source_schema_revision=manifest.source_schema_revision,
        stock_prices_rows=rows["stock_prices"],
        stock_universe_rows=rows["stock_universe"],
    )
