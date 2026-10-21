from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .models import DatasetManifest


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_storage_uri(manifest: DatasetManifest, data_root: str | Path | None = None) -> Path:
    prefix = "quant-data://"
    if not manifest.storage_uri.startswith(prefix):
        raise ValueError("Phase 1 only accepts quant-data:// URIs")
    configured_root = data_root if data_root is not None else os.environ.get("QUANT_DATA_ROOT")
    if not configured_root:
        raise ValueError("QUANT_DATA_ROOT is not configured")
    root = Path(configured_root).expanduser()
    root = root.resolve()
    path = (root / manifest.storage_uri.removeprefix(prefix)).resolve()
    if root not in path.parents and path != root:
        raise ValueError("Dataset URI escapes QUANT_DATA_ROOT")
    return path


def verify_dataset(manifest: DatasetManifest, data_root: str | Path | None = None) -> Path:
    path = resolve_storage_uri(manifest, data_root)
    if not path.is_file():
        raise FileNotFoundError(path)
    if manifest.sha256 and sha256_file(path) != manifest.sha256:
        raise ValueError(f"Checksum mismatch: {manifest.dataset_id}@{manifest.version}")
    return path
