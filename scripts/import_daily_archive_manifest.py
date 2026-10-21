from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from quant_stack.daily_cli import FullMarketArchiveManifest
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import import_worker_archive


def _key(path: Path) -> bytes:
    payload = path.read_bytes()
    if len(payload) < 32:
        raise ValueError("worker HMAC key file must contain at least 32 bytes")
    return payload


def run(args: argparse.Namespace) -> dict[str, object]:
    manifest_path = args.manifest.resolve()
    manifest = FullMarketArchiveManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    manifest_root = manifest_path.parent
    hmac_key = _key(args.hmac_key_file)
    imported_entries: list[dict[str, object]] = []
    source_commits: set[str] = set()
    for entry in manifest.entries:
        archive_path = (manifest_root / entry.archive_relative_path).resolve()
        if not archive_path.is_relative_to(manifest_root):
            raise ValueError("archive manifest path escapes its manifest directory")
        imported = import_worker_archive(
            archive_path=archive_path,
            data_root=args.data_root,
            hmac_key=hmac_key,
        )
        if imported.archive_sha256 != entry.archive_sha256:
            raise ValueError("imported archive hash conflicts with the full-market manifest")
        if imported.envelope.bundle_id != entry.worker_bundle_id:
            raise ValueError("imported worker bundle conflicts with the full-market manifest")
        source_commits.add(imported.envelope.source_commit)
        imported_entries.append(
            {
                "task_id": entry.task_id,
                "worker_bundle_id": entry.worker_bundle_id,
                "receipt_relative_path": imported.receipt_path.relative_to(
                    args.data_root.resolve()
                ).as_posix(),
                "idempotent_replay": imported.idempotent_replay,
                "artifact_count": len(imported.envelope.artifacts),
            }
        )
    if len(source_commits) > 1:
        raise ValueError("full-market archives contain multiple source commits")
    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "dataset_version": manifest.dataset_version,
        "session_date": manifest.session_date.isoformat(),
        "source_commit": next(iter(source_commits), None),
        "manifest_archives": len(manifest.entries),
        "imported_archives": len(imported_entries),
        "new_imports": sum(not item["idempotent_replay"] for item in imported_entries),
        "idempotent_replays": sum(bool(item["idempotent_replay"]) for item in imported_entries),
        "records_received": manifest.records_received,
        "unresolved_instruments": manifest.unresolved_instruments,
        "duplicate_keys": manifest.duplicate_keys,
        "catalog_published": False,
        "production_pointer_updated": False,
        "entries": imported_entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify and import every authenticated archive in a Daily Worker manifest"
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = run(args)
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {
                "imported_archives": report["imported_archives"],
                "new_imports": report["new_imports"],
                "idempotent_replays": report["idempotent_replays"],
                "records_received": report["records_received"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
