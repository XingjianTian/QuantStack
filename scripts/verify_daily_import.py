from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from quant_stack.daily_cli import FullMarketArchiveManifest
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import WorkerImportEvidence

_CN_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _session_date(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("persisted bar timestamp must be timezone-aware")
        return value.astimezone(_CN_TIMEZONE).date().isoformat()
    return str(value)[:10]


def run(*, manifest_path: Path, data_root: Path) -> dict[str, object]:
    manifest = FullMarketArchiveManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    root = data_root.resolve()
    expected_bundles = {entry.worker_bundle_id: entry for entry in manifest.entries}
    source_commits: set[str] = set()
    bar_rows = 0
    instruments: set[str] = set()
    keys: set[tuple[str, str]] = set()
    duplicate_keys = 0
    verified_artifacts = 0

    for bundle_id, entry in expected_bundles.items():
        receipt_path = root / "staging" / "worker_import_receipts" / f"{bundle_id}.json"
        evidence = WorkerImportEvidence.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        )
        if evidence.bundle_id != bundle_id or evidence.archive_sha256 != entry.archive_sha256:
            raise ValueError("persisted worker receipt conflicts with the full-market manifest")
        if evidence.catalog_published is not False:
            raise ValueError("daily intake unexpectedly published a catalog entry")
        source_commits.add(evidence.source_commit)
        for artifact in evidence.envelope.artifacts:
            path = (root / artifact.relative_path).resolve()
            if not path.is_relative_to(root):
                raise ValueError("persisted artifact path escapes the daily data root")
            if (
                not path.is_file()
                or path.stat().st_size != artifact.size_bytes
                or _sha256(path) != artifact.sha256
            ):
                raise ValueError("persisted daily artifact failed independent readback")
            verified_artifacts += 1
            if artifact.kind != "bars":
                continue
            table = pq.read_table(path, columns=["instrument_id", "ts_event"])
            bar_rows += table.num_rows
            for row in table.to_pylist():
                instrument_id = str(row["instrument_id"])
                session = _session_date(row["ts_event"])
                if session != manifest.session_date.isoformat():
                    raise ValueError("persisted bar falls outside the requested session")
                key = (instrument_id, session)
                if key in keys:
                    duplicate_keys += 1
                keys.add(key)
                instruments.add(instrument_id)

    if len(source_commits) != 1:
        raise ValueError("persisted daily intake contains multiple source commits")
    if bar_rows != manifest.records_received:
        raise ValueError("persisted daily row count conflicts with the full-market manifest")
    if duplicate_keys != 0 or manifest.duplicate_keys != 0:
        raise ValueError("persisted daily intake contains duplicate instrument-session keys")
    publication_receipts = root / "catalog" / "publication_receipts"
    if publication_receipts.exists() and any(publication_receipts.iterdir()):
        raise ValueError("isolated daily intake unexpectedly contains publication receipts")

    return {
        "schema_version": "1.0.0",
        "session_date": manifest.session_date.isoformat(),
        "manifest_archives": len(manifest.entries),
        "verified_receipts": len(expected_bundles),
        "verified_artifacts": verified_artifacts,
        "persisted_bar_rows": bar_rows,
        "distinct_instruments": len(instruments),
        "duplicate_keys": duplicate_keys,
        "source_commit": next(iter(source_commits)),
        "catalog_published": False,
        "production_pointer_updated": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Independently verify one NAS daily intake")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = run(manifest_path=args.manifest, data_root=args.data_root)
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
