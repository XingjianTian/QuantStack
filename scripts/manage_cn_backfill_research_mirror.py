#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb

MANIFEST_NAME = "mirror-manifest.json"


def _query_runtime_prefix(dataset_version: str) -> str:
    return f"pilot/query/version={dataset_version}/"


def _is_allowed_runtime_file(relative: str, dataset_version: str) -> bool:
    """Return whether a non-manifest file may exist outside manifest entries.

    Query indexes/proofs are derived artifacts and are intentionally excluded
    from the content manifest.  Lock/temporary files are also allowed so an
    interrupted atomic build can be diagnosed without being mistaken for
    unlisted research data.  Everything else must be listed and hash-bound.
    """

    path = Path(relative)
    if ".locks" in path.parts:
        return True
    if path.name.endswith((".write.lock", ".tmp")):
        return True
    query_prefix = _query_runtime_prefix(dataset_version)
    return relative in {
        f"{query_prefix}backfill-research.duckdb",
        f"{query_prefix}nas-query-proof.json",
    }


def _actual_relative_files(root: Path) -> set[str]:
    return {
        item.relative_to(root).as_posix()
        for item in root.rglob("*")
        if item.is_file()
    }


def _assert_exact_file_inventory(
    root: Path, manifest: dict[str, object]
) -> set[str]:
    dataset_version = str(manifest["dataset_version"])
    listed = {str(item["path"]) for item in manifest["files"]}
    actual = _actual_relative_files(root)
    unexpected = sorted(
        relative
        for relative in actual
        if relative != MANIFEST_NAME
        and not _is_allowed_runtime_file(relative, dataset_version)
        and relative not in listed
    )
    if unexpected:
        raise ValueError(
            "mirror contains unlisted files: " + ", ".join(unexpected[:10])
        )
    missing = sorted(relative for relative in listed if relative not in actual)
    if missing:
        raise ValueError("mirror manifest files are missing: " + ", ".join(missing[:10]))
    return listed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_value = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(temporary_value)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _resolve(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    if root != candidate and root not in candidate.parents:
        raise ValueError("manifest path escapes mirror root")
    return candidate


def _uri_path(root: Path, uri: str) -> Path:
    if not uri.startswith("quant-data://"):
        raise ValueError("inventory URI must use quant-data://")
    return _resolve(root, uri.removeprefix("quant-data://"))


def build_manifest(root: Path, dataset_version: str, logical_namespace: str) -> dict[str, object]:
    if (root / "catalog/current-manifest-set.json").exists() or (root / "catalog").exists():
        raise ValueError("research mirror must not contain canonical catalog artifacts")
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if relative == MANIFEST_NAME or _is_allowed_runtime_file(relative, dataset_version):
            continue
        if relative.startswith("pilot/query/"):
            raise ValueError(f"unexpected query runtime file: {relative}")
        files.append(
            {
                "path": relative,
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
        )
    inventory_relative = (
        Path("pilot/inventory")
        / f"version={dataset_version}"
        / "run-inventory.json"
    )
    report_paths = sorted(
        (root / "pilot/reports" / f"version={dataset_version}").glob("*.json")
    )
    if not (root / inventory_relative).is_file() or not report_paths:
        raise ValueError("completed inventory and run report are required")
    payload = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_equity_daily_backfill_pilot",
        "dataset_version": dataset_version,
        "logical_namespace": logical_namespace,
        "inventory_path": inventory_relative.as_posix(),
        "run_report_paths": [item.relative_to(root).as_posix() for item in report_paths],
        "created_at": datetime.now(UTC).isoformat(),
        "files": files,
        "file_count": len(files),
        "total_bytes": sum(int(item["bytes"]) for item in files),
        "canonical_eligible": False,
        "ss_candidate": False,
        "production_pointer_updated": False,
        "webdav_active_writes": False,
    }
    _atomic_replace(root / MANIFEST_NAME, _json_bytes(payload))
    return payload


def verify_mirror(root: Path) -> dict[str, object]:
    started = time.perf_counter()
    manifest_path = root / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    listed = _assert_exact_file_inventory(root, manifest)
    if int(manifest["file_count"]) != len(listed):
        raise ValueError("mirror manifest file_count does not match file entries")
    if int(manifest["total_bytes"]) != sum(int(item["bytes"]) for item in manifest["files"]):
        raise ValueError("mirror manifest total_bytes does not match file entries")
    for item in manifest["files"]:
        path = _resolve(root, item["path"])
        if (
            path.stat().st_size != int(item["bytes"])
            or _sha256(path) != item["sha256"]
        ):
            raise ValueError(f"mirror file verification failed: {item['path']}")
    inventory_path = _resolve(root, manifest["inventory_path"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if inventory["dataset_version"] != manifest["dataset_version"]:
        raise ValueError("mirror and inventory dataset versions differ")
    artifacts = [inventory["universe_artifact"]]
    for shard in inventory["shards"]:
        if shard["dataset_version"] != manifest["dataset_version"]:
            raise ValueError("inventory mixes dataset versions")
        artifacts.extend(shard["raw_artifacts"])
        artifacts.extend(
            item
            for item in (
                shard["parquet_artifact"],
                shard["quality_artifact"],
                shard["task_state_artifact"],
            )
            if item is not None
        )
    for artifact in artifacts:
        path = _uri_path(root, artifact["uri"])
        if path.stat().st_size != artifact["bytes"] or _sha256(path) != artifact["sha256"]:
            raise ValueError(f"inventory artifact verification failed: {artifact['uri']}")
    universe = json.loads(
        _uri_path(root, inventory["universe_artifact"]["uri"]).read_text(encoding="utf-8")
    )
    for source in universe.get("source_refs", []):
        path = _uri_path(root, source["raw_uri"])
        if _sha256(path) != source["raw_sha256"]:
            raise ValueError("historical universe snapshot hash mismatch")
    for report_relative in manifest["run_report_paths"]:
        report = json.loads(_resolve(root, report_relative).read_text(encoding="utf-8"))
        if report["inventory_uri"] != f"quant-data://{manifest['inventory_path']}":
            raise ValueError("run report does not bind mirror inventory")
        if report["inventory_sha256"] != _sha256(inventory_path):
            raise ValueError("run report inventory hash mismatch")
        if report["canonical_eligible"] or report["ss_candidate"]:
            raise ValueError("research report has canonical or SS eligibility")
    return {
        "status": "verified",
        "dataset_version": manifest["dataset_version"],
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
        "inventory_shards": len(inventory["shards"]),
        "inventory_rows": sum(int(item["row_count"]) for item in inventory["shards"]),
        "universe_sources": len(universe.get("source_refs", [])),
        "verification_ms": round((time.perf_counter() - started) * 1000, 3),
        "canonical_catalog_accessed": False,
        "ss_database_accessed": False,
    }


def _quoted_parquet_relation(paths: list[Path]) -> str:
    quoted = ",".join("'" + str(item).replace("'", "''") + "'" for item in paths)
    return f"read_parquet([{quoted}], union_by_name=true)"


def _query_bar_metrics(connection: duckdb.DuckDBPyConnection, relation: str) -> dict[str, object]:
    total, instruments, first_date, last_date = connection.execute(
        f"SELECT count(*), count(DISTINCT instrument_id), min(trading_date), "
        f"max(trading_date) FROM {relation}"
    ).fetchone()
    duplicate_keys = connection.execute(
        f"SELECT count(*) FROM (SELECT instrument_id, trading_date FROM {relation} "
        "GROUP BY 1, 2 HAVING count(*) > 1)"
    ).fetchone()[0]
    return {
        "rows": int(total),
        "instruments": int(instruments),
        "first_date": first_date.isoformat() if first_date is not None else None,
        "last_date": last_date.isoformat() if last_date is not None else None,
        "duplicate_instrument_date_keys": int(duplicate_keys),
    }


def _validate_materialized_index(
    connection: duckdb.DuckDBPyConnection,
    expected: dict[str, object],
    *,
    inventory_uri: str,
    inventory_sha256: str,
    dataset_version: str,
) -> dict[str, object]:
    actual = _query_bar_metrics(connection, "pilot_bars")
    for key in (
        "rows",
        "instruments",
        "first_date",
        "last_date",
        "duplicate_instrument_date_keys",
    ):
        if actual[key] != expected[key]:
            raise ValueError(
                f"temporary query index {key} mismatch: "
                f"expected {expected[key]!r}, got {actual[key]!r}"
            )
    metadata = dict(
        connection.execute(
            "SELECT key, value FROM backfill_index_metadata"
        ).fetchall()
    )
    expected_metadata = {
        "dataset_version": dataset_version,
        "inventory_uri": inventory_uri,
        "inventory_file_sha256": inventory_sha256,
    }
    for key, value in expected_metadata.items():
        if metadata.get(key) != value:
            raise ValueError(
                f"temporary query index metadata mismatch for {key}: "
                f"expected {value!r}, got {metadata.get(key)!r}"
            )
    return actual


def rebuild_index(root: Path) -> dict[str, object]:
    verification = verify_mirror(root)
    manifest = json.loads((root / MANIFEST_NAME).read_text(encoding="utf-8"))
    inventory_path = _resolve(root, manifest["inventory_path"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    parquet_paths = [
        _uri_path(root, shard["parquet_artifact"]["uri"])
        for shard in inventory["shards"]
        if shard["parquet_artifact"] is not None
    ]
    if not parquet_paths:
        raise ValueError("completed inventory has no parquet artifacts")
    inventory_sha256 = _sha256(inventory_path)
    inventory_uri = f"quant-data://{manifest['inventory_path']}"
    relation = _quoted_parquet_relation(parquet_paths)
    source_connection = duckdb.connect()
    try:
        source_metrics = _query_bar_metrics(source_connection, relation)
    finally:
        source_connection.close()
    expected_rows = sum(int(item["row_count"]) for item in inventory["shards"])
    if source_metrics["rows"] != expected_rows:
        raise ValueError(
            "inventory parquet row count mismatch: "
            f"expected {expected_rows}, got {source_metrics['rows']}"
        )
    index_relative = (
        Path("pilot/query")
        / f"version={manifest['dataset_version']}"
        / "backfill-research.duckdb"
    )
    index_path = root / index_relative
    index_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_value = tempfile.mkstemp(
        dir=index_path.parent, prefix=f".{index_path.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(temporary_value)
    temporary.unlink()
    try:
        connection = duckdb.connect(str(temporary))
        try:
            connection.execute(
                "CREATE TABLE pilot_bars AS SELECT * "
                f"FROM {relation}"
            )
            connection.execute(
                "CREATE INDEX pilot_bars_instrument_date ON "
                "pilot_bars(instrument_id, trading_date)"
            )
            connection.execute(
                "CREATE TABLE backfill_index_metadata(key VARCHAR PRIMARY KEY, value VARCHAR)"
            )
            connection.executemany(
                "INSERT INTO backfill_index_metadata VALUES (?, ?)",
                [
                    ("dataset_version", manifest["dataset_version"]),
                    ("inventory_uri", inventory_uri),
                    ("inventory_file_sha256", inventory_sha256),
                    ("table_materialization", "physical_from_authoritative_parquet"),
                ],
            )
            connection.execute("CHECKPOINT")
            _validate_materialized_index(
                connection,
                source_metrics,
                inventory_uri=inventory_uri,
                inventory_sha256=inventory_sha256,
                dataset_version=manifest["dataset_version"],
            )
        finally:
            connection.close()
        os.replace(temporary, index_path)
    finally:
        temporary.unlink(missing_ok=True)

    connection = duckdb.connect(str(index_path), read_only=True)
    try:
        query_started = time.perf_counter()
        metrics = _validate_materialized_index(
            connection,
            source_metrics,
            inventory_uri=inventory_uri,
            inventory_sha256=inventory_sha256,
            dataset_version=manifest["dataset_version"],
        )
        annual = connection.execute(
            "SELECT year(trading_date), count(*) FROM pilot_bars GROUP BY 1 ORDER BY 1"
        ).fetchall()
        venues = connection.execute(
            "SELECT venue, count(*), count(DISTINCT instrument_id) "
            "FROM pilot_bars GROUP BY 1 ORDER BY 1"
        ).fetchall()
        duplicate_keys = connection.execute(
            "SELECT count(*) FROM (SELECT instrument_id, trading_date FROM pilot_bars "
            "GROUP BY 1, 2 HAVING count(*) > 1)"
        ).fetchone()[0]
        sample_sql = (
            "SELECT instrument_id, trading_date, close FROM pilot_bars "
            "ORDER BY md5(instrument_id), trading_date LIMIT 50"
        )
        sample_a = connection.execute(sample_sql).fetchall()
        sample_b = connection.execute(sample_sql).fetchall()
        range_rows = connection.execute(
            "SELECT count(*) FROM pilot_bars WHERE trading_date BETWEEN ? AND ?",
            [metrics["first_date"], metrics["last_date"]],
        ).fetchone()[0]
        query_ms = (time.perf_counter() - query_started) * 1000
    finally:
        connection.close()
    proof = {
        **verification,
        "index_uri": f"quant-data://{index_relative.as_posix()}",
        "index_bytes": index_path.stat().st_size,
        **metrics,
        "annual_rows": [{"year": year, "rows": rows} for year, rows in annual],
        "venue_rows": [
            {"venue": venue, "rows": rows, "instruments": count}
            for venue, rows, count in venues
        ],
        "range_query_rows": range_rows,
        "duplicate_instrument_date_keys": duplicate_keys,
        "repeat_query_equal": sample_a == sample_b,
        "query_ms": round(query_ms, 3),
        "query_index_rebuildable": True,
        "per_symbol_http_queries": 0,
        "mac_worker_required_for_query": False,
    }
    _atomic_replace(index_path.parent / "nas-query-proof.json", _json_bytes(proof))
    return proof


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build, verify, or index a CN research mirror")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build-manifest")
    build.add_argument("--root", required=True, type=Path)
    build.add_argument("--dataset-version", required=True)
    build.add_argument("--logical-namespace", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", required=True, type=Path)
    rebuild = subparsers.add_parser("rebuild-index")
    rebuild.add_argument("--root", required=True, type=Path)
    return parser


def main() -> None:
    args = _parser().parse_args()
    root = args.root.expanduser().resolve()
    if args.command == "build-manifest":
        result = build_manifest(root, args.dataset_version, args.logical_namespace)
    elif args.command == "verify":
        result = verify_mirror(root)
    else:
        result = rebuild_index(root)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
