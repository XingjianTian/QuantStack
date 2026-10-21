from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from quant_stack.api import create_app
from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.providers import SyntheticProvider
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.platform import PlatformRepository

ROOT = Path(__file__).parents[1]


def _client_with_synthetic_data(tmp_path: Path) -> TestClient:
    storage = LocalFileSystemStorage(tmp_path)
    provider = SyntheticProvider()
    IngestionService(providers={provider.name: provider}, storage=storage).run(
        DataJob(
            dataset="synthetic_us_equity_daily",
            provider="synthetic",
            market="US",
            frequency="1d",
            execution_time=datetime(2026, 7, 28, 2, 0, tzinfo=UTC),
            retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=["AAPL"],
            start=date(2026, 1, 1),
            end=date(2026, 1, 9),
        )
    )
    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    return TestClient(create_app(repository, data_storage=storage))


def test_market_data_api_reads_only_through_storage_contract(tmp_path: Path) -> None:
    client = _client_with_synthetic_data(tmp_path)

    instruments = client.get("/market/instruments", params={"market": "US"})
    assert instruments.status_code == 200
    assert instruments.json()[0]["symbol"] == "AAPL"

    bars = client.get(
        "/market/AAPL/bars",
        params={"market": "US", "frequency": "1d", "start": "2026-01-05", "end": "2026-01-07"},
    )
    assert bars.status_code == 200
    assert len(bars.json()) == 3
    assert {item["instrument_id"] for item in bars.json()} == {"XNAS.AAPL"}

    datasets = client.get("/datasets")
    assert datasets.status_code == 200
    assert len(datasets.json()) == 3
    assert {item["layer"] for item in datasets.json()} == {"raw", "normalized"}
    bar_dataset = next(
        item
        for item in datasets.json()
        if item["layer"] == "normalized" and item["kind"] == "market_bars"
    )
    assert bar_dataset["quality"]["status"] == "passed"
    assert bar_dataset["quality"]["report_uri"].startswith("quant-data://quality/")
    assert bar_dataset["source_timestamp"] is not None
    assert bar_dataset["available_at"] is not None


def test_market_data_api_infers_instrument_market_from_manifest(tmp_path: Path) -> None:
    client = _client_with_synthetic_data(tmp_path)
    instruments_path = next(tmp_path.rglob("*instruments*.parquet"))
    records = [
        {key: value for key, value in record.items() if key != "market"}
        for record in pq.ParquetFile(instruments_path).read().to_pylist()
    ]
    pq.write_table(pa.Table.from_pylist(records), instruments_path)

    response = client.get("/market/AAPL/bars", params={"market": "US"})

    assert response.status_code == 200
    assert response.json()[0]["instrument_id"] == "XNAS.AAPL"


def test_market_data_api_contract_is_read_only_and_reports_missing_symbols(
    tmp_path: Path,
) -> None:
    client = _client_with_synthetic_data(tmp_path)
    response = client.get("/market/MSFT/bars", params={"market": "US"})
    assert response.status_code == 404

    operations = client.get("/openapi.json").json()["paths"]
    for path in ("/market/instruments", "/market/{symbol}/bars", "/datasets"):
        assert set(operations[path]) == {"get"}
