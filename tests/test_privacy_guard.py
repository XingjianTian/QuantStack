from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "scripts/check_privacy.py"


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "user.email", "test@users.noreply.github.com")
    shutil.copyfile(ROOT / ".gitleaks.toml", tmp_path / ".gitleaks.toml")
    (tmp_path / "README.md").write_text("Public example\n")
    git(tmp_path, "add", ".gitleaks.toml", "README.md")
    return tmp_path


def check(repo: Path, **extra_env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT)], cwd=repo, capture_output=True,
                          text=True, env={**os.environ, **extra_env})


def test_clean_staged_source_passes(repo: Path) -> None:
    result = check(repo)
    assert result.returncode == 0, result.stdout + result.stderr


def test_force_added_ignored_env_is_blocked(repo: Path) -> None:
    (repo / ".gitignore").write_text(".env\n")
    (repo / ".env").write_text("EXAMPLE=private\n")
    git(repo, "add", "-f", ".env")
    assert check(repo).returncode == 1


def test_checks_staged_contents_even_after_worktree_is_cleaned(repo: Path) -> None:
    marker = "fictional-confidential-marker"
    (repo / ".privacy.local.json").write_text(json.dumps({"forbidden_literals": [marker]}))
    (repo / "README.md").write_text(marker)
    git(repo, "add", "README.md")
    (repo / "README.md").write_text("No confidential text in working copy\n")
    result = check(repo)
    assert result.returncode == 1 and "local private value" in result.stdout
    assert marker not in result.stdout + result.stderr


def test_local_env_credential_value_is_blocked_in_other_file(repo: Path) -> None:
    value = "fictional-local-credential-value"
    (repo / ".env").write_text("QUANT_API_KEY=" + value + "\n")
    (repo / "README.md").write_text(value)
    git(repo, "add", "README.md")
    assert check(repo).returncode == 1


def test_scanner_detects_token_without_local_policy(repo: Path) -> None:
    token = "ghp_" + "Ab3dE5gH7jK9mN2pQ4rS6tU8vW0xY1zB3cD5"
    (repo / "README.md").write_text(token)
    git(repo, "add", "README.md")
    result = check(repo)
    assert result.returncode == 1 and "github-pat" in result.stdout
    assert token not in result.stdout + result.stderr


def test_non_noreply_author_is_blocked(repo: Path) -> None:
    result = check(repo, GIT_AUTHOR_EMAIL="test@example.net")
    assert result.returncode == 1 and "noreply" in result.stdout


def test_missing_scanner_blocks_commit(repo: Path) -> None:
    bin_dir = repo / "tools"
    bin_dir.mkdir()
    (bin_dir / "git").symlink_to(shutil.which("git"))
    result = check(repo, PATH=str(bin_dir))
    assert result.returncode == 1 and "install Gitleaks" in result.stdout


def test_fixture_exception_does_not_allow_other_passwords(repo: Path) -> None:
    script = repo / "scripts/verify_openresty_gateway.sh"
    script.parent.mkdir()
    content = (ROOT / "scripts/verify_openresty_gateway.sh").read_text()
    script.write_text(content)
    git(repo, "add", "scripts")
    assert check(repo).returncode == 0
    script.write_text(content.replace(":gateway-test-password ", ":unexpected-password "))
    git(repo, "add", "scripts")
    result = check(repo)
    assert result.returncode == 1 and "curl-auth-user" in result.stdout
