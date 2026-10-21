from __future__ import annotations

import argparse
import json
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from quant_stack.data.models import DatasetLayer
from quant_stack.data.publication import PublicationBundle
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import import_worker_archive
from scripts.run_us_equity_isolated_scale import _key

DATASET_ID = "yfinance_us_equity_daily"


def _archives_from_shards(shard_root: Path, scratch_root: Path) -> list[Path]:
    archives: list[Path] = []
    names: set[str] = set()
    for shard in sorted(shard_root.glob("*.tar")):
        with tarfile.open(shard, mode="r") as bundle:
            for member in bundle.getmembers():
                if (
                    not member.isfile()
                    or Path(member.name).name != member.name
                    or not member.name.endswith(".tar")
                    or member.name in names
                ):
                    raise ValueError("transfer shard contains an unsafe or duplicate member")
                source = bundle.extractfile(member)
                if source is None:
                    raise ValueError("transfer shard member is unreadable")
                path = scratch_root / member.name
                atomic_replace_bytes(path, source.read())
                archives.append(path)
                names.add(member.name)
    if not archives:
        raise ValueError("transfer shard root contains no worker archives")
    return sorted(archives)


def _archives_from_roots(archive_roots: list[Path]) -> list[Path]:
    archives = [archive for root in archive_roots for archive in sorted(root.glob("*.tar"))]
    if not archives:
        raise ValueError("archive roots contain no worker archives")
    return archives


def _import_archives(archives: list[Path], *, data_root: Path, hmac_key: bytes, workers: int):
    def import_one(path: Path):
        return import_worker_archive(archive_path=path, data_root=data_root, hmac_key=hmac_key)

    if workers == 1:
        return [import_one(path) for path in archives]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(import_one, archives))


def run(args: argparse.Namespace) -> dict[str, object]:
    key = _key(args.hmac_key_file)
    workers = int(getattr(args, "local_import_workers", 1))
    if not 1 <= workers <= 8:
        raise ValueError("local import workers must be between 1 and 8")
    verification_mode = getattr(args, "verification_mode", "full_replay")
    if verification_mode not in {"readback_only", "full_replay"}:
        raise ValueError("unsupported isolated import verification mode")
    archive_roots = list(getattr(args, "archive_root", []) or [])
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if archive_roots:
        archives = _archives_from_roots(archive_roots)
        transfer_shards = 0
    else:
        args.scratch_parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="us-core-import-", dir=args.scratch_parent)
        archives = _archives_from_shards(args.shard_root, Path(temporary.name))
        transfer_shards = len(list(args.shard_root.glob("*.tar")))
    try:
        first = _import_archives(
            archives, data_root=args.isolated_root, hmac_key=key, workers=workers
        )
        replay = (
            _import_archives(archives, data_root=args.isolated_root, hmac_key=key, workers=workers)
            if verification_mode == "full_replay"
            else []
        )
    finally:
        if temporary is not None:
            temporary.cleanup()
    if replay and any(not item.idempotent_replay for item in replay):
        raise RuntimeError("US core isolated replay was not idempotent")
    storage = LocalFileSystemStorage(args.isolated_root)
    row_count = 0
    raw_rows = 0
    symbols: set[str] = set()
    instruments: set[str] = set()
    sessions: set[object] = set()
    for imported in first:
        manifest = next(
            item
            for item in imported.envelope.proposed_manifests
            if item.dataset_id == DATASET_ID and item.layer is DatasetLayer.NORMALIZED
        )
        normalized_rows = storage.read_dataset(manifest.storage_uri)
        keys = [(str(row["instrument_id"]), row["ts_event"].date()) for row in normalized_rows]
        local_instruments = {key[0] for key in keys}
        if (
            len(keys) != len(set(keys))
            or len(local_instruments) != 1
            or not instruments.isdisjoint(local_instruments)
        ):
            raise RuntimeError("US core isolated readback contains duplicate keys")
        instruments.update(local_instruments)
        sessions.update(key[1] for key in keys)
        row_count += len(keys)
        bundle_path = args.isolated_root / imported.envelope.publication_bundle_relative_path
        publication = PublicationBundle.model_validate_json(bundle_path.read_text(encoding="utf-8"))
        raw_artifact = next(item for item in publication.artifacts if item.kind == "raw")
        payload = json.loads(
            (args.isolated_root / raw_artifact.uri.removeprefix("quant-data://")).read_text(
                encoding="utf-8"
            )
        )
        if any("Close" not in row or "Adj Close" not in row for row in payload["records"]):
            raise RuntimeError("US core Raw readback lost Close or Adj Close")
        if len(payload["records"]) != len(normalized_rows):
            raise RuntimeError("US core isolated Raw and normalized row counts differ")
        raw_rows += len(payload["records"])
        symbols.update(str(row["ticker"]) for row in payload["records"])
    report = {
        "schema_version": "1.0.0",
        "status": "passed",
        "source_mode": "archive_roots" if archive_roots else "transfer_shards",
        "local_import_workers": workers,
        "transfer_shards": transfer_shards,
        "archives": len(first),
        "first_imports": len(first),
        "new_imports": sum(not item.idempotent_replay for item in first),
        "resumed_imports": sum(item.idempotent_replay for item in first),
        "replay_imports": len(replay),
        "verification_mode": verification_mode,
        "idempotent_replay_deferred": verification_mode == "readback_only",
        "receipts": len(
            list((args.isolated_root / "staging/worker_import_receipts").glob("*.json"))
        ),
        "rows": row_count,
        "raw_rows": raw_rows,
        "instruments": len(instruments),
        "provider_symbols": len(symbols),
        "sessions": len(sessions),
        "first_session": min(sessions).isoformat(),
        "last_session": max(sessions).isoformat(),
        "duplicate_keys": 0,
        "price_semantics": "unadjusted_ohlcv; adjusted_close_raw_only",
        "catalog_published": (args.isolated_root / "catalog/quant.duckdb").exists(),
        "current_pointer_updated": (
            args.isolated_root / "catalog/current-manifest-set.json"
        ).exists(),
    }
    if report["catalog_published"] or report["current_pointer_updated"]:
        raise RuntimeError("US core isolated import unexpectedly touched Catalog/current")
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Import US Core transfer shards into NAS isolated")
    parser.add_argument("--shard-root", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--local-import-workers", type=int, default=1)
    parser.add_argument(
        "--verification-mode",
        choices=("readback_only", "full_replay"),
        default="full_replay",
    )
    args = parser.parse_args()
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
