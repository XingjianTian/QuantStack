from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..catalog import DuckDBCatalog
from ..models import DatasetKind, DatasetLayer, DatasetManifest, Market
from .atomic import atomic_write_immutable_bytes, atomic_write_immutable_file


@dataclass(frozen=True)
class StorageArtifact:
    uri: str
    path: Path
    sha256: str
    row_count: int | None = None


class LocalFileSystemStorage:
    """Filesystem implementation usable on local disks or an injected NAS mount."""

    def __init__(self, data_root: str | Path) -> None:
        self._data_root = Path(data_root).expanduser().resolve()
        self._data_root.mkdir(parents=True, exist_ok=True)

    @property
    def data_root(self) -> Path:
        return self._data_root

    def _resolve_relative(self, relative_path: str | Path) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute():
            raise ValueError("Storage paths must be relative to the configured data root")
        resolved = (self.data_root / relative).resolve()
        if resolved != self.data_root and self.data_root not in resolved.parents:
            raise ValueError("Storage path escapes the configured data root")
        return resolved

    def _resolve_uri(self, storage_uri: str) -> Path:
        prefix = "quant-data://"
        if not storage_uri.startswith(prefix):
            raise ValueError("Storage URI must use quant-data://")
        return self._resolve_relative(storage_uri.removeprefix(prefix))

    def resolve_uri(self, storage_uri: str) -> Path:
        """Resolve a public storage URI without exposing unchecked filesystem paths."""

        return self._resolve_uri(storage_uri)

    @staticmethod
    def _artifact(path: Path, root: Path, row_count: int | None = None) -> StorageArtifact:
        return StorageArtifact(
            uri=f"quant-data://{path.relative_to(root).as_posix()}",
            path=path,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            row_count=row_count,
        )

    def write_raw_object(self, relative_path: str | Path, payload: bytes) -> StorageArtifact:
        return self._write_immutable_object(relative_path, payload, "raw object")

    def write_quality_report(self, relative_path: str | Path, payload: bytes) -> StorageArtifact:
        return self._write_immutable_object(relative_path, payload, "quality report")

    def _write_immutable_object(
        self, relative_path: str | Path, payload: bytes, artifact_kind: str
    ) -> StorageArtifact:
        path = self._resolve_relative(relative_path)
        atomic_write_immutable_bytes(
            path,
            payload,
            artifact_kind=artifact_kind,
            validate=lambda temporary: temporary.read_bytes(),
        )
        return self._artifact(path, self.data_root)

    def write_parquet_dataset(
        self, relative_path: str | Path, records: Sequence[Mapping[str, object]]
    ) -> StorageArtifact:
        path = self._resolve_relative(relative_path)
        table = pa.Table.from_pylist([dict(record) for record in records])
        def validate(temporary: Path) -> None:
            parquet = pq.ParquetFile(temporary)
            if parquet.metadata.num_rows != table.num_rows:
                raise OSError(f"Parquet row-count readback mismatch: {path}")
            if not parquet.read().equals(table):
                raise OSError(f"Parquet content readback mismatch: {path}")

        atomic_write_immutable_file(
            path,
            write=lambda temporary: pq.write_table(table, temporary, compression="zstd"),
            validate=validate,
            equivalent=lambda existing, temporary: existing.read_bytes()
            == temporary.read_bytes(),
            artifact_kind="parquet dataset",
        )
        return self._artifact(path, self.data_root, table.num_rows)

    def read_dataset(self, storage_uri: str) -> list[dict[str, object]]:
        path = self._resolve_uri(storage_uri)
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.suffix != ".parquet":
            raise ValueError("read_dataset currently supports Parquet datasets only")
        # Read the concrete file, not a partitioned dataset. The latter would infer
        # market=US from the path and collide with the canonical market column.
        return pq.ParquetFile(path).read().to_pylist()

    def register_manifest(self, manifest: DatasetManifest) -> Path:
        path = self._resolve_relative(
            Path("catalog/manifests") / f"{manifest.dataset_id}@{manifest.version}.json"
        )
        payload = json.dumps(manifest.model_dump(mode="json"), indent=2) + "\n"
        atomic_write_immutable_bytes(
            path,
            payload.encode(),
            artifact_kind=f"manifest {manifest.dataset_id}@{manifest.version}",
            validate=lambda temporary: DatasetManifest.model_validate_json(
                temporary.read_text(encoding="utf-8")
            ),
        )
        catalog = DuckDBCatalog(self.data_root / "catalog/quant.duckdb")
        try:
            catalog.register(manifest, authoritative_manifest=manifest)
        finally:
            catalog.close()
        return path

    def list_manifests(
        self,
        *,
        kind: DatasetKind | None = None,
        market: Market | None = None,
        layer: DatasetLayer | None = None,
        frequency: str | None = None,
    ) -> list[DatasetManifest]:
        manifest_root = self.data_root / "catalog/manifests"
        pointer_path = self.data_root / "catalog/current-manifest-set.json"
        if pointer_path.is_file():
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            if pointer.get("schema_version") != "1.0.0":
                raise ValueError("Unsupported manifest set pointer schema")
            paths = [self._resolve_relative(item) for item in pointer["manifests"]]
        else:
            paths = sorted(manifest_root.glob("*.json")) if manifest_root.is_dir() else []
        manifests = [
            DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
            for path in paths
        ]
        filtered = [
            item
            for item in manifests
            if (kind is None or item.kind == kind)
            and (market is None or item.market == market)
            and (layer is None or item.layer == layer)
            and (frequency is None or item.frequency == frequency)
        ]
        return sorted(filtered, key=lambda item: (item.created_at, item.version), reverse=True)


class StagingFileSystemStorage(LocalFileSystemStorage):
    """Write immutable artifacts and proposed manifests without touching DuckDB.

    Replay/backfill workers use this storage. Only ``CatalogPublisher`` promotes the
    proposed manifests into the authoritative catalog.
    """

    def register_manifest(self, manifest: DatasetManifest) -> Path:
        path = self._resolve_relative(
            Path("staging/proposed_manifests")
            / manifest.job_id
            / f"{manifest.dataset_id}@{manifest.version}.json"
        )
        payload = json.dumps(manifest.model_dump(mode="json"), indent=2) + "\n"
        self._write_immutable_object(path.relative_to(self.data_root), payload.encode(), "manifest")
        return path
