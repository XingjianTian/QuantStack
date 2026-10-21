from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from quant_stack.api import create_app
from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchDatasetNotFoundError,
    BatchSnapshotError,
    CanonicalBatchRepository,
)
from quant_stack.data.ingestion import (
    DataJob,
    IngestionService,
    ProviderRuntimeContract,
    RetryPolicy,
)
from quant_stack.data.models import DatasetKind
from quant_stack.data.providers import SyntheticProvider
from quant_stack.data.publication import CatalogPublisher, build_publication_bundle
from quant_stack.data.semantics import (
    CN_EQUITY_DAILY_SEMANTICS,
    DatasetSemanticsRegistry,
)
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.platform import PlatformRepository

ROOT = Path(__file__).parents[1]
AUTH_HEADER = {"X-Quant-API-Key": "batch-secret"}


class PublicationFixtureProvider(SyntheticProvider):
    runtime_contract = ProviderRuntimeContract(
        provider="synthetic",
        endpoint="synthetic_fixture_v1",
        provider_version="fixture-1.0.0",
        adapter_version="fixture-1.0.0",
        native_schema_version="fixture-1.0.0",
        supported_markets=("US",),
        supported_frequencies=("1d",),
    )


@pytest.fixture(autouse=True)
def _configured_batch_auth(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_API_KEY", AUTH_HEADER["X-Quant-API-Key"])


def _publish(
    data_root: Path,
    *,
    version: str = "1.0.0",
    execution_time: datetime = datetime(2026, 7, 31, tzinfo=UTC),
) -> None:
    storage = StagingFileSystemStorage(data_root)
    provider = PublicationFixtureProvider()
    result = IngestionService(providers={provider.name: provider}, storage=storage).run(
        DataJob(
            dataset="synthetic_us_equity_daily",
            dataset_version=version,
            provider="synthetic",
            provider_version=provider.runtime_contract.provider_version,
            provider_endpoint=provider.runtime_contract.endpoint,
            adapter_version=provider.runtime_contract.adapter_version,
            native_schema_version=provider.runtime_contract.native_schema_version,
            market="US",
            frequency="1d",
            execution_time=execution_time,
            retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=["AAPL", "MSFT"],
            start=date(2026, 7, 27),
            end=date(2026, 7, 30),
        )
    )
    bundle = build_publication_bundle(
        result=result,
        task_id=f"batch-fixture-{version}",
        attempt_id="attempt-1",
        endpoint=provider.runtime_contract.endpoint,
        created_at=execution_time,
        environment_id="test",
    )
    CatalogPublisher(data_root).publish(bundle)


def _client(tmp_path: Path) -> TestClient:
    _publish(tmp_path)
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    return TestClient(
        create_app(repository, data_storage=StagingFileSystemStorage(tmp_path)),
        headers=AUTH_HEADER,
    )


def test_us_index_snapshot_reads_local_history(tmp_path: Path, monkeypatch) -> None:
    from quant_stack.data.us_market_indexes import install_index_history

    index_root = tmp_path / "indexes"
    rows = [
        {
            "ticker": symbol,
            "Date": "2026-08-28",
            "Open": 99,
            "High": 102,
            "Low": 98,
            "Close": 101,
            "Volume": 1000,
        }
        for symbol in ("^GSPC", "^IXIC", "^DJI", "^RUT")
    ]
    install_index_history(rows, index_root)
    monkeypatch.setenv("QUANT_US_INDEX_ROOT", str(index_root))

    response = _client(tmp_path).get(
        "/market/index-snapshot?market=US&as_of_date=2026-08-28"
    )

    assert response.status_code == 200
    assert len(response.json()["key_markets"]) == 4


def _normalized_manifests(tmp_path: Path):
    _publish(tmp_path)
    manifests = StagingFileSystemStorage(tmp_path).list_manifests(layer="normalized")
    bars = next(item for item in manifests if item.kind == DatasetKind.MARKET_BARS)
    instruments = next(item for item in manifests if item.kind == DatasetKind.INSTRUMENTS)
    return bars, instruments


def _select_latest(repository: CanonicalBatchRepository, manifests, kind: DatasetKind):
    return repository._select_snapshot(  # noqa: SLF001
        manifests,
        kind=kind,
        dataset_id=None,
        dataset_version="latest",
        market="US",
        frequency="1d" if kind == DatasetKind.MARKET_BARS else None,
    )


def test_batch_bars_rejects_newer_instrument_dataset(tmp_path: Path) -> None:
    _bars, instruments = _normalized_manifests(tmp_path)
    newer = instruments.model_copy(update={"created_at": datetime(2026, 8, 2, tzinfo=UTC)})
    with pytest.raises(BatchDatasetNotFoundError):
        _select_latest(CanonicalBatchRepository(tmp_path), [newer], DatasetKind.MARKET_BARS)


def test_batch_bars_rejects_fundamental_dataset(tmp_path: Path) -> None:
    _bars, instruments = _normalized_manifests(tmp_path)
    fundamentals = instruments.model_copy(
        update={
            "kind": DatasetKind.FUNDAMENTALS,
            "created_at": datetime(2026, 8, 2, tzinfo=UTC),
        }
    )
    with pytest.raises(BatchDatasetNotFoundError):
        _select_latest(
            CanonicalBatchRepository(tmp_path),
            [fundamentals],
            DatasetKind.MARKET_BARS,
        )


def test_batch_bars_latest_selects_only_market_bars(tmp_path: Path) -> None:
    bars, instruments = _normalized_manifests(tmp_path)
    newer_instruments = instruments.model_copy(
        update={"created_at": datetime(2026, 8, 2, tzinfo=UTC)}
    )
    snapshot = _select_latest(
        CanonicalBatchRepository(tmp_path),
        [bars, newer_instruments],
        DatasetKind.MARKET_BARS,
    )
    assert snapshot.manifest == bars


def test_batch_instruments_selects_only_instrument_dataset(tmp_path: Path) -> None:
    bars, instruments = _normalized_manifests(tmp_path)
    newer_bars = bars.model_copy(update={"created_at": datetime(2026, 8, 2, tzinfo=UTC)})
    snapshot = _select_latest(
        CanonicalBatchRepository(tmp_path),
        [newer_bars, instruments],
        DatasetKind.INSTRUMENTS,
    )
    assert snapshot.manifest == instruments


def test_metadata_kind_contract_is_explicit(tmp_path: Path, monkeypatch) -> None:
    repository = CanonicalBatchRepository(tmp_path)
    captured: dict[str, object] = {}

    def capture(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(repository, "_prepare", capture)
    repository.prepare_metadata("bars-us")
    assert captured["kind"] == DatasetKind.MARKET_BARS

    captured.clear()
    repository.prepare_bars(
        BatchBarsQuery(
            instrument_ids=("XNAS.AAPL",),
            start=date(2026, 7, 27),
            end=date(2026, 7, 30),
        )
    )
    assert captured["kind"] == DatasetKind.MARKET_BARS


def test_exact_version_reads_only_its_selected_manifest(tmp_path: Path) -> None:
    _publish(tmp_path, version="1.0.0")
    _publish(
        tmp_path,
        version="2.0.0",
        execution_time=datetime(2026, 8, 1, tzinfo=UTC),
    )
    repository = CanonicalBatchRepository(tmp_path)
    pointer = json.loads(
        (tmp_path / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    old_manifest = next(
        tmp_path / relative
        for relative in pointer["manifests"]
        if Path(relative).name == "synthetic_us_equity_daily@1.0.0.json"
    )
    old_receipt = (
        tmp_path
        / "catalog/publication_receipts"
        / f"{old_manifest.parent.name}.json"
    )
    old_manifest.write_text("not-json", encoding="utf-8")
    old_receipt.write_text("not-json", encoding="utf-8")
    (tmp_path / "catalog/quant.duckdb").write_text("not-a-database", encoding="utf-8")

    plan = repository.prepare_bars(
        BatchBarsQuery(
            instrument_ids=("XNAS.AAPL",),
            dataset_id="synthetic_us_equity_daily",
            dataset_version="2.0.0",
            start=date(2026, 7, 27),
            end=date(2026, 7, 30),
        )
    )
    try:
        assert plan.snapshot.manifest.version == "2.0.0"
        assert plan.metrics.catalog_queries == 0
        assert plan.metrics.duckdb_connections == 0
    finally:
        plan.close()


def test_exact_version_rejects_tampered_publication_bundle(tmp_path: Path) -> None:
    _publish(tmp_path, version="2.0.0")
    pointer = json.loads(
        (tmp_path / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    manifest_relative = next(
        relative
        for relative in pointer["manifests"]
        if Path(relative).name == "synthetic_us_equity_daily@2.0.0.json"
    )
    bundle_id = Path(manifest_relative).parent.name
    (tmp_path / "catalog/publication_bundles" / f"{bundle_id}.json").write_text(
        "tampered", encoding="utf-8"
    )

    with pytest.raises(BatchSnapshotError):
        CanonicalBatchRepository(tmp_path).prepare_bars(
            BatchBarsQuery(
                instrument_ids=("XNAS.AAPL",),
                dataset_id="synthetic_us_equity_daily",
                dataset_version="2.0.0",
                start=date(2026, 7, 27),
                end=date(2026, 7, 30),
            )
        )


def test_batch_instruments_and_bars_are_stable_paginated_and_sanitized(
    tmp_path: Path,
) -> None:
    client = _client(tmp_path)
    first = client.post(
        "/market/batch/instruments",
        json={"market": "US", "page_size": 1},
    )
    assert first.status_code == 200
    first_payload = first.json()
    assert len(first_payload["items"]) == 1
    assert first_payload["next_cursor"]
    assert "metrics" not in first_payload
    assert first.headers["x-quant-catalog-queries"] == "1"
    assert first.headers["x-quant-payload-queries"] == "1"
    assert first.headers["x-quant-duckdb-connections"] == "1"
    assert first.headers["x-quant-parquet-scans"] == "1"
    assert "prepare;dur=" in first.headers["server-timing"]
    assert "storage_uri" not in first.text
    assert ".parquet" not in first.text.lower()

    second = client.post(
        "/market/batch/instruments",
        json={
            "market": "US",
            "page_size": 1,
            "cursor": first_payload["next_cursor"],
        },
    )
    assert second.status_code == 200
    ids = [first_payload["items"][0]["instrument_id"], second.json()["items"][0]["instrument_id"]]
    assert ids == sorted(ids)

    bars = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.MSFT", "XNAS.AAPL"],
            "market": "US",
            "start": "2026-07-27",
            "end": "2026-07-30",
            "frequency": "1d",
            "page_size": 100,
        },
    )
    assert bars.status_code == 200, bars.text
    payload = bars.json()
    keys = [(item["instrument_id"], item["ts_event"]) for item in payload["items"]]
    assert keys == sorted(keys)
    assert {item["volume_unit"] for item in payload["items"]} == {"shares"}
    assert {item["quality_status"] for item in payload["items"]} == {"passed"}
    assert payload["dataset_id"] == "synthetic_us_equity_daily"
    assert payload["dataset_version"] == "1.0.0"


def test_batch_etag_304_metadata_and_manifest_change_invalidation(tmp_path: Path) -> None:
    client = _client(tmp_path)
    request = {
        "instrument_ids": ["XNAS.AAPL", "XNAS.MSFT"],
        "market": "US",
        "frequency": "1d",
        "start": "2026-07-27",
        "end": "2026-07-30",
        "page_size": 2,
    }
    first = client.post("/market/batch/bars", json=request)
    assert first.status_code == 200
    etag = first.headers["etag"]
    assert etag == first.json()["etag"]
    assert first.json()["snapshot_token"]
    assert "metrics" not in first.json()
    cached = client.post("/market/batch/bars", json=request, headers={"If-None-Match": etag})
    assert cached.status_code == 304
    assert cached.content == b""
    assert cached.headers["x-quant-payload-queries"] == "0"
    assert cached.headers["x-quant-parquet-scans"] == "0"

    second_request = {**request, "cursor": first.json()["next_cursor"]}
    second = client.post(
        "/market/batch/bars",
        json=second_request,
        headers={"If-None-Match": etag},
    )
    assert second.status_code == 200
    assert second.headers["etag"] != etag
    assert second.json()["snapshot_token"] == first.json()["snapshot_token"]

    resized = client.post("/market/batch/bars", json={**request, "page_size": 3})
    assert resized.status_code == 200
    assert resized.headers["etag"] != etag

    metadata = client.get(
        "/market/datasets/synthetic_us_equity_daily/metadata",
    )
    assert metadata.status_code == 200
    assert "storage_uri" not in metadata.text
    metadata_cached = client.get(
        "/market/datasets/synthetic_us_equity_daily/metadata",
        headers={"If-None-Match": metadata.headers["etag"]},
    )
    assert metadata_cached.status_code == 304

    _publish(
        tmp_path,
        version="1.0.1",
        execution_time=datetime(2026, 8, 1, tzinfo=UTC),
    )
    changed = client.post("/market/batch/bars", json=request)
    assert changed.status_code == 200
    assert changed.headers["etag"] != etag
    assert changed.json()["dataset_version"] == "1.0.1"


def _synthetic_semantics(*, extra_blocked_operation: str | None = None):
    research = CN_EQUITY_DAILY_SEMANTICS.research.model_copy(
        update={
            "dataset_id": "synthetic_us_equity_daily",
            "blocked_operations": (
                *CN_EQUITY_DAILY_SEMANTICS.research.blocked_operations,
                *((extra_blocked_operation,) if extra_blocked_operation else ()),
            ),
        }
    )
    return CN_EQUITY_DAILY_SEMANTICS.model_copy(
        update={
            "dataset_id": "synthetic_us_equity_daily",
            "market": "US",
            "research": research,
            "eligible_consumers": ("batch_api",),
        }
    )


def test_metadata_etag_includes_semantics_but_bars_etag_remains_snapshot_scoped(
    tmp_path: Path,
) -> None:
    _publish(tmp_path)
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    first_semantics = _synthetic_semantics()
    changed_semantics = _synthetic_semantics(extra_blocked_operation="new_research_gate")
    first_registry = DatasetSemanticsRegistry((first_semantics,))
    changed_registry = DatasetSemanticsRegistry((changed_semantics,))
    first_client = TestClient(
        create_app(
            repository,
            data_storage=StagingFileSystemStorage(tmp_path),
            dataset_semantics=first_registry,
        ),
        headers=AUTH_HEADER,
    )
    changed_client = TestClient(
        create_app(
            repository,
            data_storage=StagingFileSystemStorage(tmp_path),
            dataset_semantics=changed_registry,
        ),
        headers=AUTH_HEADER,
    )

    url = "/market/datasets/synthetic_us_equity_daily/metadata"
    first = first_client.get(url)
    assert first.status_code == 200
    assert first.json()["semantics_schema_version"] == "1.3.0"
    assert first.json()["semantics_fingerprint"] == first_semantics.fingerprint()
    assert "docs/" not in first.text
    assert (
        first_client.get(url, headers={"If-None-Match": first.headers["etag"]}).status_code == 304
    )

    changed = changed_client.get(url, headers={"If-None-Match": first.headers["etag"]})
    assert changed.status_code == 200
    assert changed.headers["etag"] != first.headers["etag"]
    assert changed.json()["manifest_fingerprint"] == first.json()["manifest_fingerprint"]

    request = {
        "instrument_ids": ["XNAS.AAPL"],
        "market": "US",
        "frequency": "1d",
        "start": "2026-07-27",
        "end": "2026-07-30",
        "page_size": 2,
    }
    first_bars = first_client.post("/market/batch/bars", json=request)
    changed_bars = changed_client.post("/market/batch/bars", json=request)
    assert first_bars.status_code == changed_bars.status_code == 200
    assert first_bars.headers["etag"] == changed_bars.headers["etag"]


def test_batch_fails_closed_on_pointer_mismatch_and_rejects_bypasses(tmp_path: Path) -> None:
    client = _client(tmp_path)
    bypass = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.AAPL"],
            "include_quarantined": True,
            "start": "2026-07-27",
            "end": "2026-07-30",
        },
    )
    assert bypass.status_code == 422
    role_bypass = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.AAPL"],
            "consumer_access": "shadow",
            "start": "2026-07-27",
            "end": "2026-07-30",
        },
    )
    assert role_bypass.status_code == 422

    missing = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.AAPL"],
            "dataset_id": "synthetic_us_equity_daily",
            "dataset_version": "does-not-exist",
            "start": "2026-07-27",
            "end": "2026-07-30",
        },
    )
    assert missing.status_code == 404

    pointer_path = tmp_path / "catalog/current-manifest-set.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["manifests"] = pointer["manifests"][:-1]
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    mismatch = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.AAPL"],
            "start": "2026-07-27",
            "end": "2026-07-30",
        },
    )
    assert mismatch.status_code == 409
    assert mismatch.json()["detail"] == "canonical dataset snapshot is unavailable"
    assert str(tmp_path) not in mismatch.text


def test_batch_inherits_api_key_auth_and_enforces_request_limits(
    tmp_path: Path, monkeypatch
) -> None:
    _publish(tmp_path)
    monkeypatch.setenv("QUANT_API_KEY", "batch-secret")
    monkeypatch.setenv("QUANT_BATCH_MAX_INSTRUMENTS", "1")
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    client = TestClient(create_app(repository, data_storage=StagingFileSystemStorage(tmp_path)))
    request = {
        "instrument_ids": ["XNAS.AAPL", "XNAS.MSFT"],
        "start": "2026-07-27",
        "end": "2026-07-30",
    }
    assert client.post("/market/batch/bars", json=request).status_code == 401
    limited = client.post(
        "/market/batch/bars",
        json=request,
        headers={"X-Quant-API-Key": "batch-secret"},
    )
    assert limited.status_code == 422
    assert limited.json()["detail"] == "batch instrument limit exceeded"


def _uncredentialed_client(tmp_path: Path) -> TestClient:
    _publish(tmp_path)
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    return TestClient(create_app(repository, data_storage=StagingFileSystemStorage(tmp_path)))


def _bars_request() -> dict[str, object]:
    return {
        "instrument_ids": ["XNAS.AAPL"],
        "start": "2026-07-27",
        "end": "2026-07-30",
    }


def _batch_route_responses(client: TestClient, headers=None):
    return (
        client.post("/market/batch/instruments", json={}, headers=headers),
        client.post("/market/batch/bars", json=_bars_request(), headers=headers),
        client.get(
            "/market/datasets/synthetic_us_equity_daily/metadata",
            headers=headers,
        ),
    )


def test_batch_auth_missing_server_key_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("QUANT_API_KEY")
    responses = _batch_route_responses(_uncredentialed_client(tmp_path))
    assert [response.status_code for response in responses] == [503, 503, 503]
    assert all(
        response.json() == {"detail": "API authentication is not configured"}
        for response in responses
    )


def test_batch_auth_missing_client_key_returns_401(tmp_path: Path) -> None:
    responses = _batch_route_responses(_uncredentialed_client(tmp_path))
    assert [response.status_code for response in responses] == [401, 401, 401]
    assert all(response.json() == {"detail": "Invalid API key"} for response in responses)


def test_batch_auth_wrong_key_returns_401(tmp_path: Path) -> None:
    responses = _batch_route_responses(
        _uncredentialed_client(tmp_path), {"X-Quant-API-Key": "wrong"}
    )
    assert [response.status_code for response in responses] == [401, 401, 401]
    assert all(response.json() == {"detail": "Invalid API key"} for response in responses)


def test_batch_auth_correct_key_passes(tmp_path: Path) -> None:
    responses = _batch_route_responses(_uncredentialed_client(tmp_path), AUTH_HEADER)
    assert [response.status_code for response in responses] == [200, 200, 200]


def test_batch_invalid_server_consumer_access_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("QUANT_BATCH_CONSUMER_ACCESS", "request_override")
    responses = _batch_route_responses(_uncredentialed_client(tmp_path), AUTH_HEADER)
    assert [response.status_code for response in responses] == [503, 503, 503]
    assert all(
        response.json() == {"detail": "Batch consumer access is not configured"}
        for response in responses
    )


def test_batch_auth_header_when_server_key_missing_does_not_500(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("QUANT_API_KEY")
    response = _uncredentialed_client(tmp_path).post(
        "/market/batch/bars",
        json=_bars_request(),
        headers={"X-Quant-API-Key": "client-only-secret"},
    )
    assert response.status_code == 503


def test_auth_error_never_echoes_key(tmp_path: Path) -> None:
    secret = "do-not-echo-this-api-key"
    response = _uncredentialed_client(tmp_path).get(
        "/market/datasets/synthetic_us_equity_daily/metadata",
        headers={"X-Quant-API-Key": secret},
    )
    assert response.status_code == 401
    assert secret not in response.text


def test_batch_rejects_oversized_request_before_repository_query(
    tmp_path: Path, monkeypatch
) -> None:
    _publish(tmp_path)
    monkeypatch.setenv("QUANT_BATCH_MAX_REQUEST_BYTES", "64")
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    client = TestClient(create_app(repository, data_storage=StagingFileSystemStorage(tmp_path)))
    response = client.post(
        "/market/batch/bars",
        json={"instrument_ids": ["XNAS.AAPL"], "frequency": "1d", "padding": "x" * 100},
    )
    assert response.status_code == 413
    assert response.json()["detail"] == "batch request size limit exceeded"


def test_batch_requires_bounded_paired_dates(tmp_path: Path) -> None:
    client = _client(tmp_path)
    unbounded = client.post("/market/batch/bars", json={"instrument_ids": ["XNAS.AAPL"]})
    assert unbounded.status_code == 422
    one_sided = client.post(
        "/market/batch/bars",
        json={"instrument_ids": ["XNAS.AAPL"], "start": "2026-07-27"},
    )
    assert one_sided.status_code == 422


def test_actual_body_size_limit_handles_missing_or_false_content_length(
    tmp_path: Path, monkeypatch
) -> None:
    _publish(tmp_path)
    monkeypatch.setenv("QUANT_BATCH_MAX_REQUEST_BYTES", "64")
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    client = TestClient(create_app(repository, data_storage=StagingFileSystemStorage(tmp_path)))
    body = json.dumps(
        {
            "instrument_ids": ["XNAS.AAPL"],
            "start": "2026-07-27",
            "end": "2026-07-30",
            "padding": "x" * 100,
        }
    ).encode()
    missing_length = client.post(
        "/market/batch/bars",
        content=(chunk for chunk in (body[:40], body[40:])),
        headers={"Content-Type": "application/json"},
    )
    assert missing_length.status_code == 413
    false_length = client.post(
        "/market/batch/bars",
        content=body,
        headers={"Content-Type": "application/json", "Content-Length": "1"},
    )
    assert false_length.status_code == 413


def test_payload_failure_is_sanitized_and_does_not_expose_sql_or_path(
    tmp_path: Path,
) -> None:
    client = _client(tmp_path)
    parquet_path = next((tmp_path / "normalized/bars_1d").rglob("bars.parquet"))
    parquet_path.write_bytes(b"not a parquet file")
    response = client.post(
        "/market/batch/bars",
        json={
            "instrument_ids": ["XNAS.AAPL"],
            "start": "2026-07-27",
            "end": "2026-07-30",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "canonical dataset payload query failed"
    assert str(tmp_path) not in response.text
    assert "read_parquet" not in response.text
