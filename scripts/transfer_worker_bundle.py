from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import time
from pathlib import Path, PurePosixPath

from quant_stack.data.storage.atomic import atomic_replace_bytes

_HOST_ALIAS = re.compile(r"^[A-Za-z0-9_.-]+$")


def _remote_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if not path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("remote staging root must be a normalized absolute path")
    return path


def _run(args: list[str], *, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    failure: subprocess.SubprocessError | None = None
    for attempt in range(1, 4):
        try:
            return subprocess.run(
                args,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.SubprocessError as exc:
            failure = exc
            if attempt < 3:
                time.sleep(attempt * 2)
    assert failure is not None
    raise failure


def _archive_identity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size_bytes += len(chunk)
    return digest.hexdigest(), size_bytes


def transfer(
    *,
    archive: Path,
    remote_host: str,
    remote_staging_root: str,
) -> dict[str, object]:
    if not _HOST_ALIAS.fullmatch(remote_host):
        raise ValueError("remote host must be a configured SSH alias")
    root = _remote_path(remote_staging_root)
    archive_sha256, size_bytes = _archive_identity(archive)
    incoming = root / ".incoming"
    inbox = root / "inbox"
    partial = incoming / f"{archive_sha256}.partial"
    final = inbox / f"{archive_sha256}.tar"
    _run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            remote_host,
            f"mkdir -p {shlex.quote(str(incoming))} {shlex.quote(str(inbox))}",
        ]
    )
    existing = _run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            remote_host,
            f"if test -f {shlex.quote(str(final))}; then "
            f"sha256sum {shlex.quote(str(final))}; fi",
        ]
    ).stdout.split()
    if existing:
        if existing[0] != archive_sha256:
            raise ValueError("existing remote worker archive hash mismatch")
        _run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                remote_host,
                f"test ! -e {shlex.quote(str(partial))}",
            ]
        )
        return {
            "schema_version": "1.0.0",
            "transport": "ssh-public-key",
            "archive_sha256": archive_sha256,
            "size_bytes": size_bytes,
            "remote_hash_verified": True,
            "atomic_inbox_install": True,
            "partial_path_absent": True,
            "remote_host_recorded": False,
            "remote_path_recorded": False,
        }
    _run(["scp", "-q", str(archive), f"{remote_host}:{partial}"], timeout=7_200)
    observed = _run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            remote_host,
            f"sha256sum {shlex.quote(str(partial))}",
        ]
    ).stdout.split()[0]
    if observed != archive_sha256:
        raise ValueError("remote worker archive hash mismatch")
    command = (
        f"if test -f {shlex.quote(str(final))}; then "
        f"test \"$(sha256sum {shlex.quote(str(final))} | awk '{{print $1}}')\" = "
        f"{shlex.quote(archive_sha256)} && rm -f {shlex.quote(str(partial))}; "
        f"else mv {shlex.quote(str(partial))} {shlex.quote(str(final))}; fi"
    )
    _run(["ssh", "-o", "BatchMode=yes", remote_host, command])
    final_observed = _run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            remote_host,
            f"sha256sum {shlex.quote(str(final))}",
        ]
    ).stdout.split()[0]
    if final_observed != archive_sha256:
        raise ValueError("atomically installed worker archive hash mismatch")
    return {
        "schema_version": "1.0.0",
        "transport": "ssh-public-key",
        "archive_sha256": archive_sha256,
        "size_bytes": size_bytes,
        "remote_hash_verified": True,
        "atomic_inbox_install": True,
        "partial_path_absent": True,
        "remote_host_recorded": False,
        "remote_path_recorded": False,
    }


def install_local(*, archive: Path, staging_root: Path) -> dict[str, object]:
    archive_sha256, size_bytes = _archive_identity(archive)
    incoming = staging_root / ".incoming"
    inbox = staging_root / "inbox"
    partial = incoming / f"{archive_sha256}.partial"
    final = inbox / f"{archive_sha256}.tar"
    incoming.mkdir(parents=True, exist_ok=True)
    inbox.mkdir(parents=True, exist_ok=True)
    if final.is_file():
        observed, _ = _archive_identity(final)
        if observed != archive_sha256:
            raise ValueError("existing local worker archive hash mismatch")
        if partial.exists():
            raise ValueError("partial local worker archive remains beside completed archive")
    else:
        shutil.copyfile(archive, partial)
        observed, _ = _archive_identity(partial)
        if observed != archive_sha256:
            raise ValueError("local worker archive hash mismatch")
        os.replace(partial, final)
    return {
        "schema_version": "1.0.0",
        "transport": "local-atomic-install",
        "archive_sha256": archive_sha256,
        "size_bytes": size_bytes,
        "remote_hash_verified": True,
        "atomic_inbox_install": True,
        "partial_path_absent": not partial.exists(),
        "remote_host_recorded": False,
        "remote_path_recorded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Authenticated atomic worker bundle transfer")
    parser.add_argument("--archive", type=Path, required=True)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--remote-host")
    destination.add_argument("--local-staging-root", type=Path)
    parser.add_argument("--remote-staging-root")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.local_staging_root is not None:
        if args.remote_staging_root is not None:
            parser.error("--remote-staging-root is invalid with --local-staging-root")
        report = install_local(
            archive=args.archive,
            staging_root=args.local_staging_root,
        )
    else:
        if args.remote_staging_root is None:
            parser.error("--remote-staging-root is required with --remote-host")
        report = transfer(
            archive=args.archive,
            remote_host=args.remote_host,
            remote_staging_root=args.remote_staging_root,
        )
    atomic_replace_bytes(
        args.report,
        (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
