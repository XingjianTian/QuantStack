#!/usr/bin/env python3
"""Check the complete Git index without reading unstaged file contents."""
from __future__ import annotations

import fnmatch
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args])


def private_path(name: str) -> bool:
    path = Path(name)
    if set(path.parts) & {".ai-bridge", ".venv", "node_modules", "secrets", "credentials"}:
        return True
    if name.startswith(("docs/evidence/", "docker/generated/")):
        return True
    if path.name.startswith(".env") and not path.name.endswith(".example"):
        return True
    return path.name.startswith("._") or any(fnmatch.fnmatch(path.name, pattern) for pattern in (
        "*.private.*", "*.local.*", "*.pem", "*.key", "*.p12", "*.pfx", "*.keystore",
        "*.jks", "known_hosts*", "*.db", "*.sqlite*", "*.dump", "*.sql.gz",
    ))


def local_literals(root: Path) -> list[bytes]:
    policy = root / ".privacy.local.json"
    literals = json.loads(policy.read_text())["forbidden_literals"] if policy.exists() else []
    env_file = root / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            key, sep, value = line.partition("=")
            if not sep or key.lstrip().startswith("#"):
                continue
            value = value.strip().strip("\"'")
            if re.search(r"(?i)(api.?key|token|password|passwd|secret)$", key.strip()):
                if len(value) >= 8 and "${" not in value:
                    literals.append(value)
    return [value.encode() for value in literals if value]


def main() -> int:
    root = Path(git("rev-parse", "--show-toplevel").decode().strip())
    os.chdir(root)
    for identity in ("GIT_AUTHOR_IDENT", "GIT_COMMITTER_IDENT"):
        if not re.search(rb"<[^<>]+@users\.noreply\.github\.com>", git("var", identity)):
            print("Privacy check blocked: configure a GitHub noreply author and committer email.")
            return 1
    if shutil.which("gitleaks") is None:
        print("Privacy check blocked: install Gitleaks 8.25+ (brew install gitleaks).")
        return 1
    literals = local_literals(root)
    entries = [row.split(b"\t", 1) for row in git("ls-files", "--stage", "-z").split(b"\0") if row]
    oids = [metadata.split()[1] for metadata, _ in entries]
    result = subprocess.run(["git", "cat-file", "--batch"], input=b"\n".join(oids) + b"\n",
                            capture_output=True, check=True)
    objects = io.BytesIO(result.stdout)
    with tempfile.TemporaryDirectory(prefix="quantstack-privacy-") as folder:
        snapshot = Path(folder)
        for metadata, raw_name in entries:
            mode, _, stage = metadata.split()
            name = os.fsdecode(raw_name)
            if stage != b"0" or mode not in (b"100644", b"100755") or private_path(name):
                print(f"Privacy check blocked: private or unsupported index path: {name}")
                return 1
            header = objects.readline().split()
            content = objects.read(int(header[2]))
            objects.read(1)
            if any(value.lower() in content.lower() for value in literals):
                print(f"Privacy check blocked: local private value in staged file: {name}")
                return 1
            target = snapshot / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        config = snapshot / ".gitleaks.toml"
        if not config.is_file():
            print("Privacy check blocked: stage the repository .gitleaks.toml configuration.")
            return 1
        report = snapshot / "scan-result.json"
        scan = subprocess.run([
            "gitleaks", "dir", str(snapshot), "--config", str(config), "--redact",
            "--no-banner", "--no-color", "--report-format", "json", "--report-path", str(report),
        ], capture_output=True)
        if scan.returncode:
            if report.exists():
                for finding in json.loads(report.read_text()):
                    name = Path(finding["File"]).relative_to(snapshot)
                    print(f"Privacy check blocked: {finding['RuleID']} in {name}")
            print("Privacy check failed; no secret values are printed.")
            return 1
    print(f"Privacy check passed: {len(entries)} staged files; local values and Gitleaks checked.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError):
        print("Privacy check could not complete; commit blocked.", file=sys.stderr)
        raise SystemExit(1) from None
