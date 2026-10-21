from __future__ import annotations

import json
import stat
from datetime import date
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.qfq_release import promote_isolated_qfq_release, restore_qfq_release
from quant_stack.data.storage import atomic as atomic_storage
from quant_stack.data.worker_bundle import import_worker_archive, publish_imported_worker_bundle
from scripts.build_cn_qfq_worker_archive import build_archive


def _candidate(
    root: Path,
    *,
    version: str,
    sessions: tuple[date, ...],
    source_commit: str,
) -> tuple[Path, Path]:
    qfq = root / f"{version}.parquet"
    values = ", ".join(
        f"('XSHG.600000', DATE '{session.isoformat()}')" for session in sessions
    )
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id, '600000' AS symbol, 'XSHG' AS venue, "
            "trading_date, trading_date::TIMESTAMP AT TIME ZONE 'Asia/Shanghai' AS ts_event, "
            "10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS close, "
            "100.0 AS volume, 1050.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := 'qfq') AS lineage FROM (VALUES "
            f"{values}) AS source(instrument_id, trading_date)) "
            f"TO '{qfq}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()
    factors = root / f"{version}.factors.json"
    factors.write_text(
        json.dumps(
            {
                "complete": True,
                "universe_count": 1,
                "request_end": sessions[-1].isoformat(),
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
    archive = root / f"{version}.tar"
    staging = root / f"{version}.staging"
    build_archive(
        qfq_path=qfq,
        factor_path=factors,
        staging_root=staging,
        dataset_version=version,
        source_commit=source_commit,
        hmac_key=b"k" * 32,
        output_path=archive,
    )
    return archive, staging


def _published_baseline(tmp_path: Path) -> tuple[Path, str]:
    central = tmp_path / "central"
    source_commit = "a" * 40
    archive, _ = _candidate(
        tmp_path,
        version="baseline-qfq-v1",
        sessions=(date(2026, 8, 13),),
        source_commit=source_commit,
    )
    imported = import_worker_archive(
        archive_path=archive,
        data_root=central,
        hmac_key=b"k" * 32,
    )
    receipt = publish_imported_worker_bundle(
        data_root=central,
        import_receipt_path=imported.receipt_path,
        allowed_source_commits={source_commit},
    )
    return central, receipt.bundle_id


def test_isolated_qfq_release_pins_exact_ss_version_and_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    central, baseline_bundle_id = _published_baseline(tmp_path)
    source_commit = "b" * 40
    archive, _ = _candidate(
        tmp_path,
        version="daily-through-20260814.qfq-isolated-v1",
        sessions=(date(2026, 8, 13), date(2026, 8, 14)),
        source_commit=source_commit,
    )
    isolated = tmp_path / "isolated"
    imported = import_worker_archive(
        archive_path=archive,
        data_root=isolated,
        hmac_key=b"k" * 32,
    )
    ss_environment = tmp_path / ".env.docker"
    ss_environment.write_text(
        "QUANT_WORKSPACE_BACKEND_IMAGE_REF=personal/backend:slim.2\n"
        "SS_QUANTSTACK_READ_MODE=quantstack_primary\n"
        "SS_QUANTSTACK_PRIMARY_ENABLED=true\n"
        "SS_QUANTSTACK_SHADOW_ENABLED=false\n"
        "SS_QUANTSTACK_BARS_DATASET_ID=cn_equity_daily\n"
        "SS_QUANTSTACK_DATASET_VERSION=baseline-qfq-v1\n"
        "SS_QUANTSTACK_ALLOW_CANDIDATE=false\n",
        encoding="utf-8",
    )
    snapshot = central / "catalog/rollback_snapshots/daily-through-20260814"

    reader_gid = (central / "catalog").stat().st_gid
    exposed_catalog_paths = {
        central / "catalog/quant.duckdb",
        central / "catalog/current-manifest-set.json",
    }
    replace_events: list[tuple[str, str]] = []
    operation = "promote"
    original_replace = atomic_storage.os.replace

    def checked_replace(source: str | Path, destination: str | Path) -> None:
        destination_path = Path(destination)
        if destination_path in exposed_catalog_paths:
            source_stat = Path(source).stat()
            assert source_stat.st_gid == reader_gid
            assert stat.S_IMODE(source_stat.st_mode) == 0o640
            replace_events.append((operation, destination_path.name))
        original_replace(source, destination)

    monkeypatch.setattr(atomic_storage.os, "replace", checked_replace)

    result = promote_isolated_qfq_release(
        source_data_root=isolated,
        central_data_root=central,
        import_receipt_path=imported.receipt_path,
        allowed_source_commits={source_commit},
        snapshot_dir=snapshot,
        expected_dataset_id="cn_equity_daily",
        expected_dataset_version="daily-through-20260814.qfq-isolated-v1",
        expected_row_count=2,
        expected_instrument_count=1,
        expected_watermark=date(2026, 8, 14),
        ss_environment_path=ss_environment,
    )
    replay = promote_isolated_qfq_release(
        source_data_root=isolated,
        central_data_root=central,
        import_receipt_path=imported.receipt_path,
        allowed_source_commits={source_commit},
        snapshot_dir=snapshot,
        expected_dataset_id="cn_equity_daily",
        expected_dataset_version="daily-through-20260814.qfq-isolated-v1",
        expected_row_count=2,
        expected_instrument_count=1,
        expected_watermark=date(2026, 8, 14),
        ss_environment_path=ss_environment,
    )

    pointer = json.loads(
        (central / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    assert result.previous_bundle_id == baseline_bundle_id
    assert result.last_date == date(2026, 8, 14)
    assert replay.receipt.idempotent_replay is True
    assert pointer["bundle_id"] == result.receipt.bundle_id
    assert stat.S_IMODE((central / "catalog/current-manifest-set.json").stat().st_mode) == 0o640
    assert stat.S_IMODE(
        (
            central
            / "normalized/bars_1d/market=CN"
            / "version=daily-through-20260814.qfq-isolated-v1/bars.parquet"
        ).stat().st_mode
    ) == 0o640
    assert "SS_QUANTSTACK_DATASET_VERSION=daily-through-20260814.qfq-isolated-v1" in (
        ss_environment.read_text(encoding="utf-8")
    )

    ss_environment.write_text(
        ss_environment.read_text(encoding="utf-8").replace(
            "personal/backend:slim.2", "personal/backend:slim.3"
        ),
        encoding="utf-8",
    )

    operation = "rollback"
    rollback = restore_qfq_release(
        central_data_root=central,
        snapshot_dir=snapshot,
        ss_environment_path=ss_environment,
    )
    restored_pointer = json.loads(
        (central / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    assert rollback["restored_bundle_id"] == baseline_bundle_id
    assert restored_pointer["bundle_id"] == baseline_bundle_id
    assert stat.S_IMODE((central / "catalog/current-manifest-set.json").stat().st_mode) == 0o640
    assert "SS_QUANTSTACK_DATASET_VERSION=baseline-qfq-v1" in ss_environment.read_text(
        encoding="utf-8"
    )
    assert "QUANT_WORKSPACE_BACKEND_IMAGE_REF=personal/backend:slim.3" in (
        ss_environment.read_text(encoding="utf-8")
    )
    assert ("promote", "current-manifest-set.json") in replace_events
    assert ("rollback", "quant.duckdb") in replace_events
    assert ("rollback", "current-manifest-set.json") in replace_events


def test_failed_qfq_profile_restores_catalog_without_touching_ss(tmp_path: Path) -> None:
    central, baseline_bundle_id = _published_baseline(tmp_path)
    source_commit = "c" * 40
    archive, _ = _candidate(
        tmp_path,
        version="bad-expected-count-v1",
        sessions=(date(2026, 8, 14),),
        source_commit=source_commit,
    )
    isolated = tmp_path / "isolated"
    imported = import_worker_archive(
        archive_path=archive,
        data_root=isolated,
        hmac_key=b"k" * 32,
    )

    with pytest.raises(ValueError, match="approved release"):
        promote_isolated_qfq_release(
            source_data_root=isolated,
            central_data_root=central,
            import_receipt_path=imported.receipt_path,
            allowed_source_commits={source_commit},
            snapshot_dir=central / "catalog/rollback_snapshots/bad-release",
            expected_dataset_id="cn_equity_daily",
            expected_dataset_version="bad-expected-count-v1",
            expected_row_count=99,
            expected_instrument_count=1,
            expected_watermark=date(2026, 8, 14),
        )

    pointer = json.loads(
        (central / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    assert pointer["bundle_id"] == baseline_bundle_id
