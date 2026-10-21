from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from quant_stack.data.catalog.duckdb_catalog import manifest_sha256
from quant_stack.data.models import DatasetManifest
from quant_stack.data.publication import CatalogPublisher, load_publication_bundle
from quant_stack.data.storage.atomic import (
    atomic_replace_bytes,
    atomic_replace_file,
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_artifact(path: Path, *, size_bytes: int, sha256: str) -> None:
    if path.stat().st_size != size_bytes or _sha256(path) != sha256:
        raise ValueError("staged artifact hash conflict")


def _materialize_artifacts(stage: Path, central: Path, bundle) -> None:
    for name in ("canonical", "staging"):
        link = stage / name
        if link.is_symlink():
            if link.resolve() != central / name:
                raise ValueError(f"stage path conflict: {link}")
            link.unlink()
    for artifact in bundle.artifacts:
        prefix = "quant-data://"
        if not artifact.uri.startswith(prefix):
            raise ValueError(f"unsupported artifact URI: {artifact.uri}")
        relative = Path(artifact.uri.removeprefix(prefix))
        source = central / relative
        destination = stage / relative
        atomic_write_immutable_file(
            destination,
            write=lambda temporary, source=source: shutil.copyfile(source, temporary),
            validate=lambda temporary, artifact=artifact: _validate_artifact(
                temporary,
                size_bytes=artifact.size_bytes,
                sha256=artifact.sha256,
            ),
            equivalent=lambda current, candidate: _sha256(current) == _sha256(candidate),
            artifact_kind=f"local staged artifact {relative.as_posix()}",
        )


def _copy_catalog(stage: Path, central: Path) -> None:
    stage_catalog = stage / "catalog"
    central_catalog = central / "catalog"
    central_catalog.mkdir(parents=True, exist_ok=True)
    deferred = {"quant.duckdb", "current-manifest-set.json"}
    for source in sorted(path for path in stage_catalog.rglob("*") if path.is_file()):
        relative = source.relative_to(stage_catalog)
        if source.name.startswith(".") or source.name in deferred:
            continue
        atomic_write_immutable_bytes(
            central_catalog / relative,
            source.read_bytes(),
            artifact_kind=f"staged catalog artifact {relative.as_posix()}",
        )

    source_database = stage_catalog / "quant.duckdb"

    def validate_database(path: Path) -> None:
        import duckdb

        connection = duckdb.connect(str(path), read_only=True)
        try:
            connection.execute("SELECT count(*) FROM datasets").fetchone()
        finally:
            connection.close()

    atomic_replace_file(
        central_catalog / "quant.duckdb",
        write=lambda temporary: shutil.copyfile(source_database, temporary),
        validate=validate_database,
    )
    atomic_replace_bytes(
        central_catalog / "current-manifest-set.json",
        (stage_catalog / "current-manifest-set.json").read_bytes(),
    )


def _verify_central(stage: Path, central: Path, receipt) -> dict[str, object]:
    import duckdb

    receipt_path = central / "catalog/publication_receipts" / f"{receipt.bundle_id}.json"
    receipt_payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    pointer = json.loads(
        (central / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    manifest_uri = receipt_payload["manifest_uris"][0]
    relative = manifest_uri.removeprefix("quant-data://")
    if relative not in pointer["manifests"]:
        raise ValueError("central catalog pointer does not contain receipt manifest")
    manifest = DatasetManifest.model_validate_json(
        (central / relative).read_text(encoding="utf-8")
    )
    connection = duckdb.connect(str(central / "catalog/quant.duckdb"), read_only=True)
    try:
        row = connection.execute(
            "SELECT dataset_id, version, row_count, manifest_sha256 "
            "FROM datasets WHERE dataset_id = ? AND version = ?",
            [manifest.dataset_id, manifest.version],
        ).fetchone()
    finally:
        connection.close()
    expected = (
        manifest.dataset_id,
        manifest.version,
        manifest.row_count,
        manifest_sha256(manifest),
    )
    if row != expected:
        raise ValueError("central DuckDB projection does not match published manifest")
    return {
        "receipt_id": receipt.receipt_id,
        "central_replay_verified": True,
        "dataset_id": manifest.dataset_id,
        "dataset_version": manifest.version,
        "row_count": manifest.row_count,
        "stage_root": str(stage),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish a Catalog bundle through a local stage for SMB-backed NAS"
    )
    parser.add_argument("--central-data-root", type=Path, required=True)
    parser.add_argument("--stage-root", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    central = args.central_data_root.expanduser().resolve()
    stage = args.stage_root.expanduser().resolve()
    stage.mkdir(parents=True, exist_ok=True)
    bundle = load_publication_bundle(args.bundle)
    _materialize_artifacts(stage, central, bundle)
    publisher = CatalogPublisher(stage)
    receipt = publisher.publish(bundle)
    if not publisher.replay_receipt(receipt).verified:
        raise ValueError("local staged Catalog receipt replay failed")
    _copy_catalog(stage, central)
    report = _verify_central(stage, central, receipt)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
