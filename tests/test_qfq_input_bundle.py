from __future__ import annotations

import io
import json
import tarfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.qfq_input_bundle import (
    build_qfq_factor_input_archive,
    read_qfq_factor_input_archive,
)


def _factor(path: Path, *, complete: bool = True) -> None:
    path.write_text(
        json.dumps(
            {
                "complete": complete,
                "universe_count": 2,
                "request_start": "2015-01-01",
                "request_end": "2026-08-19",
                "request_policy": {
                    "concurrency": 1,
                    "request_interval_ms": 50,
                    "batch_size": 500,
                    "batch_interval_seconds": 5,
                },
                "requests": [
                    {"instrument_id": "XSHG.600000", "status": "ok", "rows": []},
                    {
                        "instrument_id": "XSHE.000001",
                        "status": "ok_no_adjustments",
                        "rows": [],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def _daily_report(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "session_date": "2026-08-19",
                "source_commit": "d" * 40,
                "imported_archives": 1,
                "catalog_published": False,
                "production_pointer_updated": False,
                "entries": [
                    {
                        "worker_bundle_id": "worker-" + "a" * 32,
                        "receipt_relative_path": (
                            "staging/worker_import_receipts/worker-" + "a" * 32 + ".json"
                        ),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_qfq_factor_input_is_small_signed_and_contains_no_parquet(tmp_path: Path) -> None:
    factor = tmp_path / "factor.json"
    daily = tmp_path / "daily.json"
    archive = tmp_path / "factor-input.tar"
    _factor(factor)
    _daily_report(daily)
    key = b"k" * 32
    envelope = build_qfq_factor_input_archive(
        factor_path=factor,
        daily_import_report_path=daily,
        session=date(2026, 8, 19),
        dataset_version="daily-through-20260819.qfq-release-v1",
        source_commit="c" * 40,
        hmac_key=key,
        output_path=archive,
        created_at=datetime(2026, 8, 19, tzinfo=UTC),
    )
    verified = read_qfq_factor_input_archive(archive, hmac_key=key)

    assert archive.stat().st_size < 10 * 1024 * 1024
    assert envelope.contains_raw_parquet is False
    assert envelope.contains_qfq_parquet is False
    assert verified.factor["complete"] is True
    with tarfile.open(archive) as payload:
        names = {item.name for item in payload.getmembers()}
    assert names == {
        "envelope.json",
        "envelope.hmac",
        "factor-acquisition.json",
        "daily-import-report.json",
    }
    assert not any(name.endswith(".parquet") for name in names)


def test_qfq_factor_input_rejects_tamper_and_incomplete_checkpoint(tmp_path: Path) -> None:
    factor = tmp_path / "factor.json"
    daily = tmp_path / "daily.json"
    archive = tmp_path / "factor-input.tar"
    _factor(factor)
    _daily_report(daily)
    key = b"k" * 32
    build_qfq_factor_input_archive(
        factor_path=factor,
        daily_import_report_path=daily,
        session=date(2026, 8, 19),
        dataset_version="daily-through-20260819.qfq-release-v1",
        source_commit="c" * 40,
        hmac_key=key,
        output_path=archive,
    )
    with tarfile.open(archive) as source:
        payloads = {
            item.name: source.extractfile(item).read()  # type: ignore[union-attr]
            for item in source.getmembers()
        }
    payloads["factor-acquisition.json"] += b" "
    tampered = tmp_path / "tampered.tar"
    with tarfile.open(tampered, "w") as output:
        for name, payload in payloads.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            output.addfile(info, io.BytesIO(payload))
    with pytest.raises(ValueError, match="payload"):
        read_qfq_factor_input_archive(tampered, hmac_key=key)

    _factor(factor, complete=False)
    with pytest.raises(ValueError, match="incomplete"):
        build_qfq_factor_input_archive(
            factor_path=factor,
            daily_import_report_path=daily,
            session=date(2026, 8, 19),
            dataset_version="daily-through-20260819.qfq-release-v1",
            source_commit="c" * 40,
            hmac_key=key,
            output_path=tmp_path / "incomplete.tar",
        )
