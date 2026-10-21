from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from quant_stack.data.publication import CatalogPublisher
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import (
    import_worker_archive,
    publish_imported_worker_bundle,
)


def _key(path: Path) -> bytes:
    payload = path.read_bytes()
    if len(payload) < 32:
        raise ValueError("worker HMAC key file must contain at least 32 bytes")
    return payload


def run(args: argparse.Namespace) -> dict[str, object]:
    imported = import_worker_archive(
        archive_path=args.archive,
        data_root=args.data_root,
        hmac_key=_key(args.hmac_key_file),
    )
    receipt = None
    replay = None
    if args.publish:
        allowed_source_commits = set(args.allowed_source_commit or ())
        if args.allowed_source_commits_file is not None:
            allowed_source_commits.update(
                line.strip()
                for line in args.allowed_source_commits_file.read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
        receipt = publish_imported_worker_bundle(
            data_root=args.data_root,
            import_receipt_path=imported.receipt_path,
            allowed_source_commits=allowed_source_commits,
        )
        replay = CatalogPublisher(args.data_root).replay_receipt(receipt)
    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "bundle_id": imported.envelope.bundle_id,
        "archive_sha256": imported.archive_sha256,
        "import_idempotent_replay": imported.idempotent_replay,
        "artifact_count": len(imported.envelope.artifacts),
        "provider_contract": imported.envelope.provider_contract.model_dump(mode="json"),
        "source_commit": imported.envelope.source_commit,
        "worker_environment": imported.envelope.worker_environment.model_dump(mode="json"),
        "transfer_protocol": imported.envelope.transfer_protocol,
        "publication_authority": imported.envelope.publication_authority,
        "direct_duckdb_write": imported.envelope.direct_duckdb_write,
        "webdav_active_write": imported.envelope.webdav_active_write,
        "catalog_published": receipt is not None,
        "publication_receipt_id": receipt.receipt_id if receipt is not None else None,
        "publication_idempotent_replay": (
            receipt.idempotent_replay if receipt is not None else None
        ),
        "publication_replay_verified": replay.verified if replay is not None else None,
        "pointer_verified": replay.pointer_verified if replay is not None else None,
        "duckdb_verified": replay.duckdb_verified if replay is not None else None,
        "worker_import_verified": (
            replay.worker_import_verified if replay is not None else None
        ),
        "allowed_source_commit_matched": (
            imported.envelope.source_commit in allowed_source_commits
            if args.publish
            else None
        ),
        "ss_database_accessed": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify and import a signed worker bundle")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--allowed-source-commit", action="append")
    parser.add_argument("--allowed-source-commits-file", type=Path)
    args = parser.parse_args()
    report = run(args)
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {
                "bundle_id": report["bundle_id"],
                "import_idempotent_replay": report["import_idempotent_replay"],
                "catalog_published": report["catalog_published"],
                "publication_replay_verified": report["publication_replay_verified"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
