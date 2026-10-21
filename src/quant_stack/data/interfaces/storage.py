from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol

from ..models import DatasetKind, DatasetLayer, DatasetManifest, Market


class StoredArtifact(Protocol):
    uri: str
    path: Path
    sha256: str
    row_count: int | None


class DataStorage(Protocol):
    """Provider-neutral storage contract for local disks and future NAS adapters."""

    @property
    def data_root(self) -> Path: ...

    def write_raw_object(self, relative_path: str | Path, payload: bytes) -> StoredArtifact: ...

    def write_quality_report(self, relative_path: str | Path, payload: bytes) -> StoredArtifact: ...

    def write_parquet_dataset(
        self, relative_path: str | Path, records: Sequence[Mapping[str, object]]
    ) -> StoredArtifact: ...

    def read_dataset(self, storage_uri: str) -> list[dict[str, object]]: ...

    def register_manifest(self, manifest: DatasetManifest) -> Path: ...

    def list_manifests(
        self,
        *,
        kind: DatasetKind | None = None,
        market: Market | None = None,
        layer: DatasetLayer | None = None,
        frequency: str | None = None,
    ) -> list[DatasetManifest]: ...
