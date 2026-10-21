from pathlib import Path

from scripts.transfer_worker_bundle import install_local


def test_local_install_is_atomic_and_idempotent(tmp_path: Path) -> None:
    archive = tmp_path / "archive.tar"
    archive.write_bytes(b"signed-worker-archive")
    staging = tmp_path / "staging"

    first = install_local(archive=archive, staging_root=staging)
    replay = install_local(archive=archive, staging_root=staging)

    installed = staging / "inbox" / f"{first['archive_sha256']}.tar"
    assert installed.read_bytes() == archive.read_bytes()
    assert first["transport"] == "local-atomic-install"
    assert replay == first
    assert not list((staging / ".incoming").iterdir())
