#!/usr/bin/env python3
"""Materialize the actual T-094 wheel selection from its retained uv cache.

This helper is deliberately offline.  It enumerates the retained wheels-v5
HTTP sidecars to discover what T-094 selected, then cross-binds those observed
URL/SHA pairs to the reviewed T-093 artifact metadata.  The six build-system
artifacts are additionally proven against their retained simple-v19 records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

HEX64 = re.compile(rb"[0-9a-f]{64}")
WHEEL_URL = re.compile(rb"https://files\.pythonhosted\.org/[^\x00\"\s]+\.whl")
STATIC_NAMES = {"hatchling", "packaging", "pathspec", "pluggy", "trove-classifiers"}
DYNAMIC_NAMESPACE = "build-system:dynamic:build_editable:editables"


class LedgerError(RuntimeError):
    """Raised when retained-cache evidence cannot prove a mandatory fact."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def only_match(pattern: re.Pattern[bytes], value: bytes, label: str) -> bytes:
    matches = list(dict.fromkeys(pattern.findall(value)))
    if len(matches) != 1:
        raise LedgerError(f"{label}: expected one distinct match, found {len(matches)}")
    return matches[0]


def sidecar_selection(path: Path) -> dict[str, object]:
    value = path.read_bytes()
    url = only_match(WHEEL_URL, value, f"{path}: wheel URL").decode()
    filename = unquote(Path(urlsplit(url).path).name)
    hash_match = re.search(rb"Sha256[^0-9a-f]+([0-9a-f]{64})", value)
    return {
        "url": url,
        "filename": filename,
        "embedded_sha256": hash_match.group(1).decode() if hash_match else None,
        "sidecar_sha256": sha256_bytes(value),
    }


def simple_record(
    path: Path,
    *,
    filename: str,
    url: str,
    expected_size: int,
) -> dict[str, object]:
    value = path.read_bytes()
    filename_bytes = filename.encode()
    filename_offsets = [match.start() for match in re.finditer(re.escape(filename_bytes), value)]
    records: list[tuple[int, int, str]] = []
    for filename_offset in filename_offsets:
        sha_offset = filename_offset + len(filename_bytes)
        candidate = value[sha_offset : sha_offset + 64]
        if HEX64.fullmatch(candidate):
            records.append((filename_offset, sha_offset, candidate.decode()))
    if len(records) != 1:
        raise LedgerError(
            f"{path}: expected one filename-plus-SHA record for {filename}, found {len(records)}"
        )
    filename_offset, sha_offset, sha256 = records[0]
    url_bytes = url.encode()
    url_offset = value.find(url_bytes, sha_offset + 64)
    if url_offset < 0:
        raise LedgerError(f"{path}: selected URL is absent after the artifact hash")

    size_bytes = expected_size.to_bytes(8, "little")
    size_search_start = url_offset + len(url_bytes)
    size_search_end = min(len(value), size_search_start + 256)
    size_offsets = [
        match.start()
        for match in re.finditer(re.escape(size_bytes), value[size_search_start:size_search_end])
    ]
    if len(size_offsets) != 1:
        raise LedgerError(
            f"{path}: expected one retained size field for {filename}, found {len(size_offsets)}"
        )
    size_value_offset = size_search_start + size_offsets[0]
    return {
        "sha256": sha256,
        "cache_file_sha256": sha256_bytes(value),
        "filename_offset": filename_offset,
        "sha256_offset": sha_offset,
        "url_offset": url_offset,
        "size_value_offset": size_value_offset,
    }


def reviewed_artifacts(t093: dict) -> dict[tuple[str, str], dict[str, object]]:
    artifacts: dict[tuple[str, str], dict[str, object]] = {}
    for package in t093["selected_packages"]:
        namespace = package["name"]
        for artifact in package["allowed_artifacts"]:
            artifacts[(artifact["url"], artifact["sha256"])] = {
                "namespace": namespace,
                "name": package["name"],
                "version": package["version"],
                "size": artifact["size"],
                "class": "runtime",
            }
    for package in t093["root_build_system_supply"]["resolved"]:
        namespace = f"build-system:{package['name']}"
        for artifact in package["allowed_artifacts"]:
            artifacts[(artifact["url"], artifact["sha256"])] = {
                "namespace": namespace,
                "name": package["name"],
                "version": package["version"],
                "size": artifact["size"],
                "class": "static_build_system",
            }
    return artifacts


def dynamic_evidence(t095: dict) -> dict[str, object]:
    selected = t095["dynamic_build_system_supply"]["operations"][0]["selected"]
    artifact = selected["allowed_artifacts"][0]
    return {
        "namespace": selected["namespace"],
        "name": selected["name"],
        "version": selected["version"],
        "url": artifact["url"],
        "sha256": artifact["sha256"],
        "size": artifact["size"],
        "provenance": selected["offline_metadata_provenance"],
    }


def materialize(cache_root: Path, t093_path: Path, t095_path: Path) -> dict:
    t093_bytes = t093_path.read_bytes()
    t095_bytes = t095_path.read_bytes()
    t093 = json.loads(t093_bytes)
    t095 = json.loads(t095_bytes)
    reviewed = reviewed_artifacts(t093)
    dynamic = dynamic_evidence(t095)

    sidecars = sorted((cache_root / "wheels-v5" / "pypi").glob("*/*.http"))
    if len(sidecars) != 55:
        raise LedgerError(f"expected 55 retained wheel sidecars, found {len(sidecars)}")

    entries: list[dict[str, object]] = []
    for sidecar in sidecars:
        observed = sidecar_selection(sidecar)
        package_dir = sidecar.parent.name
        simple: dict[str, object] | None = None

        if observed["embedded_sha256"] is None:
            simple_path = cache_root / "simple-v19" / "pypi" / f"{package_dir}.rkyv"
            if package_dir == "editables":
                expected_size = int(dynamic["size"])
            elif package_dir in STATIC_NAMES:
                matches = [
                    value
                    for (url, _), value in reviewed.items()
                    if url == observed["url"] and value["name"] == package_dir
                ]
                if len(matches) != 1:
                    raise LedgerError(f"{package_dir}: reviewed size is not uniquely bound")
                expected_size = int(matches[0]["size"])
            else:
                raise LedgerError(f"{package_dir}: selected hash absent from wheel sidecar")
            simple = simple_record(
                simple_path,
                filename=str(observed["filename"]),
                url=str(observed["url"]),
                expected_size=expected_size,
            )
            observed_sha256 = str(simple["sha256"])
        else:
            observed_sha256 = str(observed["embedded_sha256"])

        key = (str(observed["url"]), observed_sha256)
        historical = reviewed.get(key)
        if historical is not None:
            namespace = str(historical["namespace"])
            name = str(historical["name"])
            version = str(historical["version"])
            size = int(historical["size"])
            artifact_class = str(historical["class"])
            size_basis = "reviewed T-093 artifact exact-matched by observed URL and SHA-256"
        elif key == (dynamic["url"], dynamic["sha256"]):
            namespace = str(dynamic["namespace"])
            name = str(dynamic["name"])
            version = str(dynamic["version"])
            size = int(dynamic["size"])
            artifact_class = "dynamic_build_system"
            size_basis = "retained simple-v19 size field published by T-095"
        else:
            raise LedgerError(
                f"{package_dir}: observed URL/SHA is not explained by reviewed evidence"
            )

        if artifact_class != "runtime":
            if (
                simple is None
                or int.from_bytes(
                    (cache_root / "simple-v19" / "pypi" / f"{package_dir}.rkyv").read_bytes()[
                        int(simple["size_value_offset"]) : int(simple["size_value_offset"]) + 8
                    ],
                    "little",
                )
                != size
            ):
                raise LedgerError(f"{package_dir}: retained size field did not decode exactly")

        provenance: dict[str, object] = {
            "selection_source": "retained T-094 wheels-v5 HTTP sidecar",
            "sidecar_path": str(sidecar.relative_to(cache_root)),
            "sidecar_sha256": observed["sidecar_sha256"],
            "hash_source": (
                "wheels-v5 embedded Sha256"
                if observed["embedded_sha256"] is not None
                else "retained simple-v19 selected record"
            ),
            "size_source": size_basis,
        }
        if simple is not None:
            provenance["simple_cache"] = {
                key: simple[key]
                for key in (
                    "cache_file_sha256",
                    "filename_offset",
                    "sha256_offset",
                    "url_offset",
                    "size_value_offset",
                )
            }
        entries.append(
            {
                "package": namespace,
                "name": name,
                "version": version,
                "filename": observed["filename"],
                "url": observed["url"],
                "sha256": observed_sha256,
                "size": size,
                "artifact_class": artifact_class,
                "provenance": provenance,
            }
        )

    namespaces = [str(entry["package"]) for entry in entries]
    if len(set(namespaces)) != 55:
        raise LedgerError("retained selections do not form 55 distinct namespaces")
    class_counts = {
        key: sum(entry["artifact_class"] == key for entry in entries)
        for key in ("runtime", "static_build_system", "dynamic_build_system")
    }
    if class_counts != {
        "runtime": 49,
        "static_build_system": 5,
        "dynamic_build_system": 1,
    }:
        raise LedgerError(f"unexpected artifact class counts: {class_counts}")
    if dynamic["namespace"] != DYNAMIC_NAMESPACE:
        raise LedgerError("dynamic namespace drifted from the reviewed operation identity")

    return {
        "schema_version": "quantstack.t097-t094-frozen-artifact-ledger.v1",
        "workflow_id": "WF-20260805-001",
        "target_id": "T-20260811-097",
        "source_target_id": "T-20260811-094",
        "result": "PASS_FROZEN_LEDGER_COMPLETE",
        "entry_count": len(entries),
        "distinct_namespace_count": len(set(namespaces)),
        "class_counts": class_counts,
        "evidence_method": {
            "selection": "enumerate retained wheels-v5 HTTP sidecars",
            "runtime_identity": "embedded sidecar URL/SHA exact-match to reviewed T-093 artifact",
            "build_identity": "sidecar URL plus retained simple-v19 filename/SHA record",
            "runtime_size": "reviewed T-093 size after exact observed URL/SHA match",
            "build_size": "retained simple-v19 little-endian size value",
            "wheel_bytes_retained": False,
            "network_calls": 0,
            "docker_calls": 0,
        },
        "source_files": {
            "t093_plan_sha256": sha256_bytes(t093_bytes),
            "t095_plan_sha256": sha256_bytes(t095_bytes),
        },
        "artifact_ledger": sorted(entries, key=lambda entry: str(entry["package"])),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--t093-plan", type=Path, required=True)
    parser.add_argument("--t095-plan", type=Path, required=True)
    args = parser.parse_args()
    result = materialize(args.cache_root, args.t093_plan, args.t095_plan)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
