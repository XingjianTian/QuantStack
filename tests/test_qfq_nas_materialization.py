from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from quant_stack.data.ingestion import IngestionService
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    normalize_akshare_replay_bars,
)
from quant_stack.data.providers.cn.replay.providers import AKSHARE_REPLAY_RUNTIME_CONTRACT
from quant_stack.data.publication import load_publication_bundle
from quant_stack.data.qfq_input_bundle import build_qfq_factor_input_archive
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.tasks import Task, TaskRuntime
from quant_stack.data.worker_bundle import (
    build_worker_envelope,
    create_worker_archive,
    import_worker_archive,
)
from scripts.materialize_cn_qfq_release import materialize

_AUDIT = {
    "schema_version": "1.0.0",
    "endpoint_host": "push2his.eastmoney.com",
    "tls_verification": True,
    "candidate_source_fingerprints": ["a" * 64],
    "attempted_source_fingerprints": ["a" * 64],
    "successful_source_fingerprint": "a" * 64,
    "failure_classes": [],
    "failure_codes": [],
    "failure_stages": [],
    "completed_requests": 2,
    "request_attempted_source_fingerprints": [["a" * 64]] * 2,
    "request_successful_source_fingerprints": ["a" * 64] * 2,
    "request_failure_codes": [[]] * 2,
    "request_failure_stages": [[]] * 2,
    "all_requests_succeeded": True,
}


class _Provider(AKShareReplayProvider):
    def transport_audit(self) -> dict[str, object]:
        return dict(_AUDIT)


def _baseline(path: Path, *, adjustment: str) -> None:
    connection = duckdb.connect()
    try:
        output = "'" + str(path).replace("'", "''") + "'"
        connection.execute(
            "COPY (SELECT instrument_id, split_part(instrument_id, '.', 2) AS symbol, "
            "split_part(instrument_id, '.', 1) AS venue, DATE '2026-07-27' AS trading_date, "
            "TIMESTAMPTZ '2026-07-27 15:00:00+08' AS ts_event, 10.0 AS open, "
            "11.0 AS high, 9.0 AS low, 10.5 AS close, 100.0 AS volume, "
            "1050.0 AS amount, 'CNY' AS currency, "
            "struct_pack(adjustment := ?) AS lineage FROM (VALUES ('XSHG.600000'), "
            f"('XSHE.000001')) symbols(instrument_id)) TO {output} (FORMAT PARQUET)",
            [adjustment],
        )
    finally:
        connection.close()


def _daily_import(root: Path, key: bytes) -> tuple[dict[str, object], str]:
    producer = root / "producer"
    provider = _Provider()
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(producer),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    task = Task(
        dataset_id="cn_equity_daily",
        dataset_version="daily-2026-07-28.fixture-v1",
        provider=AKSHARE_REPLAY_RUNTIME_CONTRACT.provider,
        provider_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.provider_version,
        endpoint=AKSHARE_REPLAY_RUNTIME_CONTRACT.endpoint,
        adapter_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.adapter_version,
        native_schema_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.native_schema_version,
        market="CN",
        instrument_scope=("XSHG.600000", "XSHE.000001"),
        start_date=date(2026, 7, 28),
        end_date=date(2026, 7, 28),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    execution = TaskRuntime().execute(task, service)
    assert execution.bundle is not None
    bundle_path = producer / "catalog/publication_bundles" / f"{execution.bundle.bundle_id}.json"
    assert load_publication_bundle(bundle_path).dataset_id == "cn_equity_daily"
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="d" * 40,
        environment_id="daily-fixture",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=dict(_AUDIT),
        created_at=datetime(2026, 7, 28, tzinfo=UTC),
    )
    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=root / "daily.tar",
        hmac_key=key,
    )
    data_root = root / "nas-imported"
    imported = import_worker_archive(
        archive_path=archive.path,
        data_root=data_root,
        hmac_key=key,
    )
    report = {
        "session_date": "2026-07-28",
        "source_commit": "d" * 40,
        "imported_archives": 1,
        "catalog_published": False,
        "production_pointer_updated": False,
        "entries": [
            {
                "worker_bundle_id": imported.envelope.bundle_id,
                "receipt_relative_path": imported.receipt_path.relative_to(data_root).as_posix(),
            }
        ],
    }
    return report, imported.envelope.bundle_id


def test_nas_materializer_builds_qfq_without_wan_parquet(tmp_path: Path) -> None:
    key = b"k" * 32
    report, _bundle_id = _daily_import(tmp_path, key)
    data_root = tmp_path / "nas-imported"
    reports = tmp_path / "reports"
    reports.mkdir()
    daily_report = reports / "20260728-first-import.json"
    daily_report.write_text(json.dumps(report), encoding="utf-8")
    factor = tmp_path / "factor.json"
    factor.write_text(
        json.dumps(
            {
                "complete": True,
                "universe_count": 2,
                "request_start": "2015-01-01",
                "request_end": "2026-07-28",
                "request_policy": {
                    "concurrency": 1,
                    "request_interval_ms": 50,
                    "batch_size": 500,
                    "batch_interval_seconds": 5,
                },
                "requests": [
                    {
                        "instrument_id": instrument_id,
                        "status": "ok_no_adjustments",
                        "rows": [],
                        "empty_confirmations": 2,
                    }
                    for instrument_id in ("XSHG.600000", "XSHE.000001")
                ],
            }
        ),
        encoding="utf-8",
    )
    factor_input = tmp_path / "factor-input.tar"
    build_qfq_factor_input_archive(
        factor_path=factor,
        daily_import_report_path=daily_report,
        session=date(2026, 7, 28),
        dataset_version="daily-through-20260728.qfq-release-v1",
        source_commit="c" * 40,
        hmac_key=key,
        output_path=factor_input,
    )
    baseline = tmp_path / "raw-baseline.parquet"
    read_only_release = tmp_path / "read-only-release"
    read_only_release.mkdir()
    current_qfq = read_only_release / "current-qfq.parquet"
    _baseline(baseline, adjustment="none")
    _baseline(current_qfq, adjustment="qfq")
    read_only_release.chmod(0o555)
    calendar = tmp_path / "calendar.json"
    calendar.write_text(
        json.dumps(
            {
                "days": [
                    {"trading_date": "2026-07-27", "state": "session"},
                    {"trading_date": "2026-07-28", "state": "session"},
                ]
            }
        ),
        encoding="utf-8",
    )
    key_file = tmp_path / "hmac.key"
    key_file.write_bytes(key)
    try:
        result = materialize(
            argparse.Namespace(
                input_archive=factor_input,
                data_root=data_root,
                raw_baseline=baseline,
                current_qfq=current_qfq,
                calendar_normalized=calendar,
                daily_reports_root=reports,
                work_root=tmp_path / "materialized",
                hmac_key_file=key_file,
                source_commit="c" * 40,
                dataset_version="daily-through-20260728.qfq-release-v1",
            )
        )
    finally:
        read_only_release.chmod(0o755)

    assert result["status"] == "completed"
    assert result["factor_input_archive_size_bytes"] < 10 * 1024 * 1024
    assert result["wan_contains_raw_parquet"] is False
    assert result["wan_contains_qfq_parquet"] is False
    assert result["qfq_release_archive_location"] == "nas-local"
    assert result["raw"]["rows"] == result["qfq"]["rows"] == 4
    assert result["qfq"]["adjustments"] == ["qfq"]
    assert result["first_import_idempotent_replay"] is False
    assert result["replay_import_idempotent_replay"] is True
    assert result["import_resumed"] is False

    resumed = materialize(
        argparse.Namespace(
            input_archive=factor_input,
            data_root=data_root,
            raw_baseline=baseline,
            current_qfq=current_qfq,
            calendar_normalized=calendar,
            daily_reports_root=reports,
            work_root=tmp_path / "materialized",
            hmac_key_file=key_file,
            source_commit="c" * 40,
            dataset_version="daily-through-20260728.qfq-release-v1",
        )
    )
    assert resumed["first_import_idempotent_replay"] is True
    assert resumed["replay_import_idempotent_replay"] is True
    assert resumed["import_resumed"] is True
