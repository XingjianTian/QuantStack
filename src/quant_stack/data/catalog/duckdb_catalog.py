from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

from ..models import DatasetManifest


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def manifest_sha256(manifest: DatasetManifest) -> str:
    return hashlib.sha256(
        _canonical_json(manifest.model_dump(mode="json")).encode()
    ).hexdigest()


_CATALOG_COLUMN_TYPES: dict[str, str] = {
    "manifest_schema_version": "VARCHAR",
    "dataset_id": "VARCHAR",
    "version": "VARCHAR",
    "layer": "VARCHAR",
    "kind": "VARCHAR",
    "market": "VARCHAR",
    "provider": "VARCHAR",
    "provider_version": "VARCHAR",
    "adapter_version": "VARCHAR",
    "native_schema_version": "VARCHAR",
    "frequency": "VARCHAR",
    "storage_uri": "VARCHAR",
    "schema_ref": "VARCHAR",
    "created_at": "TIMESTAMPTZ",
    "available_at_policy": "VARCHAR",
    "revision_policy": "VARCHAR",
    "row_count": "BIGINT",
    "sha256": "VARCHAR",
    "quality_status": "VARCHAR",
    "quality_json": "VARCHAR",
    "source_timestamp": "TIMESTAMPTZ",
    "available_at": "TIMESTAMPTZ",
    "quality_report_uri": "VARCHAR",
    "license_scope": "VARCHAR",
    "source_url": "VARCHAR",
    "input_dataset_versions_json": "VARCHAR",
    "raw_storage_uri": "VARCHAR",
    "job_id": "VARCHAR",
    "manifest_sha256": "VARCHAR",
}
_CATALOG_COLUMNS = tuple(_CATALOG_COLUMN_TYPES)


def manifest_catalog_values(manifest: DatasetManifest) -> dict[str, object]:
    """Project one authoritative Manifest into every DuckDB catalog column."""

    return {
        "manifest_schema_version": manifest.manifest_schema_version,
        "dataset_id": manifest.dataset_id,
        "version": manifest.version,
        "layer": str(manifest.layer),
        "kind": str(manifest.kind),
        "market": manifest.market,
        "provider": manifest.provider,
        "provider_version": manifest.provider_version,
        "adapter_version": manifest.adapter_version,
        "native_schema_version": manifest.native_schema_version,
        "frequency": manifest.frequency,
        "storage_uri": manifest.storage_uri,
        "schema_ref": manifest.schema_ref,
        "created_at": manifest.created_at,
        "available_at_policy": manifest.available_at_policy,
        "revision_policy": manifest.revision_policy,
        "row_count": manifest.row_count,
        "sha256": manifest.sha256,
        "quality_status": manifest.quality_status,
        "quality_json": _canonical_json(manifest.quality.model_dump(mode="json")),
        "source_timestamp": manifest.source_timestamp,
        "available_at": manifest.available_at,
        "quality_report_uri": manifest.quality.report_uri,
        "license_scope": manifest.license_scope,
        "source_url": manifest.source_url,
        "input_dataset_versions_json": _canonical_json(manifest.input_dataset_versions),
        "raw_storage_uri": manifest.raw_storage_uri,
        "job_id": manifest.job_id,
        "manifest_sha256": manifest_sha256(manifest),
    }


def _projection_matches(
    existing: Mapping[str, object], expected: Mapping[str, object]
) -> bool:
    return all(existing.get(column) == expected[column] for column in _CATALOG_COLUMNS)


class DuckDBCatalog:
    """Small metadata index; Manifest and Parquet remain authoritative."""

    def __init__(self, path: str | Path) -> None:
        import duckdb

        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._duckdb = duckdb
        self.connection = duckdb.connect(str(self.path))
        declarations = ", ".join(
            f"{name} {sql_type}" for name, sql_type in _CATALOG_COLUMN_TYPES.items()
        )
        self.connection.execute(
            f"CREATE TABLE IF NOT EXISTS datasets ({declarations}, "
            "PRIMARY KEY (dataset_id, version))"
        )
        existing_columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info('datasets')").fetchall()
        }
        for name, sql_type in _CATALOG_COLUMN_TYPES.items():
            if name not in existing_columns:
                self.connection.execute(
                    f"ALTER TABLE datasets ADD COLUMN {name} {sql_type}"
                )

    def register(
        self,
        manifest: DatasetManifest,
        *,
        authoritative_manifest: DatasetManifest | None = None,
    ) -> None:
        expected = manifest_catalog_values(manifest)
        existing = self.find(manifest.dataset_id, manifest.version)
        if existing is not None:
            existing_fingerprint = existing["manifest_sha256"]
            if existing_fingerprint == expected["manifest_sha256"]:
                if not _projection_matches(existing, expected):
                    raise ValueError(
                        "Catalog projection conflict: "
                        f"{manifest.dataset_id}@{manifest.version}"
                    )
                return
            if existing_fingerprint is None:
                if authoritative_manifest is None:
                    raise ValueError(
                        "Legacy catalog row is missing a manifest fingerprint and no "
                        "authoritative manifest was supplied: "
                        f"{manifest.dataset_id}@{manifest.version}"
                    )
                if authoritative_manifest != manifest:
                    raise ValueError(
                        f"Immutable manifest conflict: {manifest.dataset_id}@{manifest.version}"
                    )
                authoritative_values = manifest_catalog_values(authoritative_manifest)
                update_columns = tuple(
                    column for column in _CATALOG_COLUMNS if column not in {"dataset_id", "version"}
                )
                assignments = ", ".join(f"{column} = ?" for column in update_columns)
                self.connection.execute(
                    f"UPDATE datasets SET {assignments} "
                    "WHERE dataset_id = ? AND version = ? AND manifest_sha256 IS NULL",
                    [
                        *(authoritative_values[column] for column in update_columns),
                        manifest.dataset_id,
                        manifest.version,
                    ],
                )
                reread = self.find(manifest.dataset_id, manifest.version)
                if reread is None or not _projection_matches(reread, authoritative_values):
                    raise ValueError(
                        "Legacy catalog projection backfill verification failed: "
                        f"{manifest.dataset_id}@{manifest.version}"
                    )
                return
            raise ValueError(
                f"Immutable manifest conflict: {manifest.dataset_id}@{manifest.version}"
            )
        placeholders = ", ".join("?" for _ in _CATALOG_COLUMNS)
        self.connection.execute(
            f"INSERT INTO datasets ({', '.join(_CATALOG_COLUMNS)}) "
            f"VALUES ({placeholders})",
            [expected[column] for column in _CATALOG_COLUMNS],
        )

    def register_many_atomic(
        self,
        manifests: list[DatasetManifest],
        *,
        authoritative_manifests: dict[tuple[str, str], DatasetManifest] | None = None,
    ) -> None:
        """Register a publication as one DuckDB transaction."""

        self.connection.execute("BEGIN TRANSACTION")
        try:
            for manifest in manifests:
                key = (manifest.dataset_id, manifest.version)
                self.register(
                    manifest,
                    authoritative_manifest=(authoritative_manifests or {}).get(key),
                )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def find(self, dataset_id: str, version: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM datasets WHERE dataset_id = ? AND version = ?", [dataset_id, version]
        ).fetchone()
        if row is None:
            return None
        columns = [item[0] for item in self.connection.description]
        return dict(zip(columns, row, strict=True))

    def list(self) -> list[dict]:
        rows = self.connection.execute(
            "SELECT * FROM datasets ORDER BY created_at DESC, dataset_id, version DESC"
        ).fetchall()
        columns = [item[0] for item in self.connection.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def close(self) -> None:
        self.connection.close()
