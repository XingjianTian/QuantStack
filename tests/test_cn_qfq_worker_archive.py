from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.worker_bundle import import_worker_archive
from scripts.build_cn_qfq_worker_archive import build_archive
from scripts.package_cn_qfq_release import package_existing_release
from scripts.transfer_worker_bundle import transfer


def test_qfq_candidate_uses_existing_signed_import_protocol(tmp_path: Path) -> None:
    qfq = tmp_path / "qfq.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 'XSHG.600000' AS instrument_id, '600000' AS symbol, "
            "'XSHG' AS venue, DATE '2026-08-14' AS trading_date, "
            "TIMESTAMPTZ '2026-08-14 00:00:00+08' AS ts_event, "
            "10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS close, "
            "100.0 AS volume, 1050.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := 'qfq') AS lineage) "
            f"TO '{qfq}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()
    factors = tmp_path / "factors.json"
    factors.write_text(
        json.dumps(
            {
                "complete": True,
                "universe_count": 1,
                "request_end": "2026-08-14",
                "request_policy": {
                    "concurrency": 1,
                    "request_interval_ms": 50,
                    "batch_size": 500,
                    "batch_interval_seconds": 5,
                },
                "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
            }
        ),
        encoding="utf-8",
    )
    key = b"k" * 32
    archive = tmp_path / "candidate.tar"

    report = build_archive(
        qfq_path=qfq,
        factor_path=factors,
        staging_root=tmp_path / "staging-root",
        dataset_version="daily-through-20260814.qfq-isolated-v1",
        source_commit="a" * 40,
        hmac_key=key,
        output_path=archive,
    )
    first = import_worker_archive(
        archive_path=archive,
        data_root=tmp_path / "nas-isolated",
        hmac_key=key,
    )
    replay = import_worker_archive(
        archive_path=archive,
        data_root=tmp_path / "nas-isolated",
        hmac_key=key,
    )

    assert report["row_count"] == 1
    assert report["catalog_published"] is False
    assert first.idempotent_replay is False
    assert replay.idempotent_replay is True
    audit = first.envelope.provider_transport_audit
    assert audit["request_successful_source_fingerprints"] == [
        "baostock:query_adjust_factor:XSHG.600000"
    ]

    prepared = build_archive(
        qfq_path=qfq,
        factor_path=factors,
        staging_root=tmp_path / "prepared-only",
        dataset_version="daily-through-20260814.qfq-prepared-v1",
        source_commit="a" * 40,
        hmac_key=key,
        output_path=None,
    )
    assert prepared["prepared"] is True
    assert "archive_size_bytes" not in prepared
    prepared_replay = build_archive(
        qfq_path=qfq,
        factor_path=factors,
        staging_root=tmp_path / "prepared-only",
        dataset_version="daily-through-20260814.qfq-prepared-v1",
        source_commit="a" * 40,
        hmac_key=key,
        output_path=None,
    )
    assert prepared_replay["publication_bundle_id"] == prepared["publication_bundle_id"]

    repackaged_path = tmp_path / "candidate-repackaged.tar"
    repackaged = package_existing_release(
        staging_root=tmp_path / "staging-root",
        dataset_version="daily-through-20260814.qfq-isolated-v1",
        source_commit="b" * 40,
        hmac_key=key,
        output_path=repackaged_path,
    )
    repackaged_import = import_worker_archive(
        archive_path=repackaged_path,
        data_root=tmp_path / "nas-repackaged",
        hmac_key=key,
    )
    assert repackaged["source_commit"] == "b" * 40
    assert repackaged_import.envelope.dataset_version == (
        "daily-through-20260814.qfq-isolated-v1"
    )


def test_transfer_hashes_large_archive_without_reading_it_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "candidate.tar"
    archive.write_bytes(b"candidate")
    commands: list[list[str]] = []

    class Result:
        stdout = ""

    def fake_run(command: list[str], *, timeout: int = 120):
        assert timeout == (7_200 if command[0] == "scp" else 120)
        commands.append(command)
        result = Result()
        if command[0] == "ssh" and command[-1].startswith("sha256sum"):
            import hashlib

            result.stdout = hashlib.sha256(b"candidate").hexdigest() + "  remote\n"
        return result

    monkeypatch.setattr("scripts.transfer_worker_bundle._run", fake_run)
    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _self: (_ for _ in ()).throw(AssertionError("must stream archive")),
    )

    report = transfer(
        archive=archive,
        remote_host="nas",
        remote_staging_root="/isolated/inbox-root",
    )

    assert report["size_bytes"] == len(b"candidate")
    assert any(command[0] == "scp" for command in commands)


def test_transfer_reuses_an_exact_remote_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib

    archive = tmp_path / "candidate.tar"
    archive.write_bytes(b"candidate")
    digest = hashlib.sha256(b"candidate").hexdigest()
    commands: list[list[str]] = []

    class Result:
        stdout = ""

    def fake_run(command: list[str], *, timeout: int = 120):
        commands.append(command)
        result = Result()
        if command[0] == "ssh" and command[-1].startswith("if test -f"):
            result.stdout = f"{digest}  remote\n"
        return result

    monkeypatch.setattr("scripts.transfer_worker_bundle._run", fake_run)

    report = transfer(
        archive=archive,
        remote_host="nas",
        remote_staging_root="/isolated/inbox-root",
    )

    assert report["remote_hash_verified"] is True
    assert not any(command[0] == "scp" for command in commands)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"request_end": "2026-08-13"}, "watermark"),
        (
            {
                "request_policy": {
                    "concurrency": 2,
                    "request_interval_ms": 50,
                    "batch_size": 500,
                    "batch_interval_seconds": 5,
                }
            },
            "serial factor request policy",
        ),
        ({"requests": [{"instrument_id": "XSHG.600000", "status": "failed"}]}, "successful"),
        ({"requests": [{"instrument_id": "XSHE.000001", "status": "ok"}]}, "instruments"),
    ],
)
def test_qfq_candidate_rejects_invalid_factor_semantics(
    tmp_path: Path, override: dict[str, object], message: str
) -> None:
    qfq = tmp_path / "qfq.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT 'XSHG.600000' AS instrument_id, DATE '2026-08-14' AS trading_date, "
            "struct_pack(adjustment := 'qfq') AS lineage) "
            f"TO '{qfq}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()
    payload: dict[str, object] = {
        "complete": True,
        "universe_count": 1,
        "request_end": "2026-08-14",
        "request_policy": {
            "concurrency": 1,
            "request_interval_ms": 50,
            "batch_size": 500,
            "batch_interval_seconds": 5,
        },
        "requests": [{"instrument_id": "XSHG.600000", "status": "ok"}],
    }
    payload.update(override)
    factors = tmp_path / "factors.json"
    factors.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        build_archive(
            qfq_path=qfq,
            factor_path=factors,
            staging_root=tmp_path / "staging-root",
            dataset_version="daily-through-20260814.qfq-isolated-v1",
            source_commit="a" * 40,
            hmac_key=b"k" * 32,
            output_path=tmp_path / "candidate.tar",
        )
