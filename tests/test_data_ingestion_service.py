import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.models import DatasetManifest
from quant_stack.data.providers import SyntheticProvider
from quant_stack.data.storage import LocalFileSystemStorage


def _job(**overrides) -> DataJob:
    values = {
        "dataset": "synthetic_us_equity_daily",
        "provider": "synthetic",
        "market": "US",
        "frequency": "1d",
        "execution_time": datetime(2026, 7, 28, 1, 30, tzinfo=UTC),
        "retry_policy": RetryPolicy(max_attempts=3, backoff_seconds=0),
        "symbols": ["AAPL"],
        "start": date(2026, 1, 1),
        "end": date(2026, 1, 7),
    }
    values.update(overrides)
    return DataJob(**values)


def test_local_storage_raw_parquet_and_manifest_contract(tmp_path: Path) -> None:
    storage = LocalFileSystemStorage(tmp_path)
    raw = storage.write_raw_object("raw/US/test/payload.json", b'{"ok":true}')
    assert raw.uri == "quant-data://raw/US/test/payload.json"
    assert storage.write_raw_object("raw/US/test/payload.json", b'{"ok":true}') == raw
    with pytest.raises(ValueError, match="Immutable raw object conflict"):
        storage.write_raw_object("raw/US/test/payload.json", b'{"ok":false}')

    quality = storage.write_quality_report("quality/test/report.json", b'{"status":"passed"}')
    assert (
        storage.write_quality_report("quality/test/report.json", b'{"status":"passed"}') == quality
    )
    with pytest.raises(ValueError, match="Immutable quality report conflict"):
        storage.write_quality_report("quality/test/report.json", b'{"status":"failed"}')

    parquet = storage.write_parquet_dataset(
        "normalized/test/records.parquet",
        [{"instrument_id": "XNAS.AAPL", "value": 1.0}],
    )
    assert (
        storage.write_parquet_dataset(
            "normalized/test/records.parquet",
            [{"instrument_id": "XNAS.AAPL", "value": 1.0}],
        )
        == parquet
    )
    with pytest.raises(ValueError, match="Immutable parquet dataset conflict"):
        storage.write_parquet_dataset(
            "normalized/test/records.parquet",
            [{"instrument_id": "XNAS.AAPL", "value": 2.0}],
        )
    assert storage.read_dataset(parquet.uri) == [{"instrument_id": "XNAS.AAPL", "value": 1.0}]
    manifest = DatasetManifest(
        dataset_id="storage_test",
        version="2026-07-28.1",
        layer="normalized",
        kind="factor",
        market="US",
        provider="synthetic",
        frequency="1d",
        schema_ref="schemas/data/test.schema.json",
        storage_uri=parquet.uri,
        available_at_policy="test_only",
        revision_policy="immutable",
        row_count=1,
        sha256=parquet.sha256,
        quality_status="passed",
        license_scope="synthetic",
    )
    path = storage.register_manifest(manifest)
    assert path.is_file()
    assert storage.list_manifests()[0] == manifest


def test_ingestion_service_persists_raw_normalized_and_lineage(tmp_path: Path) -> None:
    storage = LocalFileSystemStorage(tmp_path)
    provider = SyntheticProvider()
    service = IngestionService(providers={provider.name: provider}, storage=storage)

    result = service.run(_job())

    assert result.status == "completed"
    assert result.attempts == 1
    assert result.raw_artifact.path.is_file()
    assert result.instrument_artifact.path.is_file()
    assert result.bar_artifact.path.is_file()
    assert len(result.manifest_paths) == 3
    raw_payload = json.loads(result.raw_artifact.path.read_text(encoding="utf-8"))
    assert raw_payload["job"]["provider"] == "synthetic"
    assert len(raw_payload["records"]) == 5
    raw_version = f"{result.raw_manifest.dataset_id}@{result.raw_manifest.version}"
    assert result.bar_manifest.input_dataset_versions == [raw_version]
    assert result.bar_manifest.raw_storage_uri == result.raw_artifact.uri
    assert result.bar_manifest.row_count == 5
    assert {item.layer.value for item in storage.list_manifests()} == {
        "raw",
        "normalized",
    }
    repeated = service.run(_job())
    assert repeated.bar_manifest == result.bar_manifest
    assert len(storage.list_manifests()) == 3


def test_ingestion_service_applies_scheduler_neutral_retry_policy(tmp_path: Path) -> None:
    class FlakySyntheticProvider(SyntheticProvider):
        def __init__(self) -> None:
            self.calls = 0

        def fetch_bars(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("synthetic timeout")
            return super().fetch_bars(*args, **kwargs)

    provider = FlakySyntheticProvider()
    service = IngestionService(
        providers={provider.name: provider}, storage=LocalFileSystemStorage(tmp_path)
    )
    result = service.run(_job())
    assert result.attempts == 2
    assert provider.calls == 2
