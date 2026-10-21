import json
from datetime import UTC, date, datetime, time
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from quant_stack.api import create_app
from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.providers.yfinance import (
    ReplayYFinanceTransport,
    YFinanceProvider,
    normalize_yfinance_bars,
    run_yfinance_daily_pipeline,
)
from quant_stack.data.quality import ProviderSchemaDriftError
from quant_stack.data.storage import LocalFileSystemStorage
from quant_stack.platform import PlatformRepository

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/providers/yfinance/us_daily_bars_2026-07-20_2026-07-24.json"


def _provider() -> YFinanceProvider:
    return YFinanceProvider(ReplayYFinanceTransport(FIXTURE))


def _job() -> DataJob:
    return DataJob(
        dataset="yfinance_us_equity_daily",
        provider="yfinance",
        market="US",
        frequency="1d",
        execution_time=datetime(2026, 7, 28, 3, 0, tzinfo=UTC),
        retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2026, 7, 20),
        end=date(2026, 7, 24),
    )


def test_yfinance_provider_replays_native_instruments_and_daily_bars_offline() -> None:
    provider = _provider()
    instruments = list(provider.list_instruments("US", ["AAPL", "MSFT", "NVDA"]))
    records = list(
        provider.fetch_bars("US", ["AAPL", "MSFT", "NVDA"], date(2026, 7, 20), date(2026, 7, 24))
    )
    assert [item.symbol for item in instruments] == ["AAPL", "MSFT", "NVDA"]
    assert {item.instrument_id for item in instruments} == {
        "XNAS.AAPL",
        "XNAS.MSFT",
        "XNAS.NVDA",
    }
    assert len(records) == 15
    assert {record["ticker"] for record in records} == {"AAPL", "MSFT", "NVDA"}
    assert {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"} <= set(records[0])


def test_yfinance_native_response_is_registered_before_normalization_failure(
    tmp_path: Path,
) -> None:
    storage = LocalFileSystemStorage(tmp_path)
    provider = _provider()

    def broken_normalizer(*args, **kwargs):
        raise ValueError("normalization rejected fixture")

    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: broken_normalizer},
    )
    with pytest.raises(ValueError, match="normalization rejected fixture"):
        service.run(_job())
    manifests = storage.list_manifests()
    assert len(manifests) == 1
    assert manifests[0].layer == "raw"
    raw_payload = json.loads(
        (tmp_path / manifests[0].storage_uri.removeprefix("quant-data://")).read_text(
            encoding="utf-8"
        )
    )
    assert "Adj Close" in raw_payload["records"][0]
    assert "open" not in raw_payload["records"][0]


def test_yfinance_replay_runs_raw_to_api_query_closed_loop(tmp_path: Path) -> None:
    storage = LocalFileSystemStorage(tmp_path)
    result = run_yfinance_daily_pipeline(
        tmp_path,
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2026, 7, 20),
        end=date(2026, 7, 24),
        execution_time=datetime(2026, 7, 28, 3, 0, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
        retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
    )

    assert result.raw_manifest.provider == "yfinance"
    assert result.raw_manifest.source_url == "https://finance.yahoo.com"
    assert result.bar_manifest.dataset_id == "yfinance_us_equity_daily"
    assert result.bar_manifest.row_count == 15
    assert result.bar_manifest.raw_storage_uri == result.raw_artifact.uri
    assert result.quality_report.quality_status == "passed"
    assert result.bar_manifest.quality.status == "passed"
    assert result.bar_manifest.quality.report_uri == result.quality_artifact.uri
    assert result.bar_manifest.source_timestamp is not None
    assert result.bar_manifest.available_at == datetime(2026, 7, 28, 3, 0, tzinfo=UTC)
    assert all(path.is_file() for path in result.manifest_paths)

    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    catalog_row = connection.execute(
        """SELECT provider, row_count, quality_status, quality_report_uri,
                  source_timestamp, available_at
           FROM datasets
           WHERE dataset_id = 'yfinance_us_equity_daily'"""
    ).fetchone()
    connection.close()
    assert catalog_row[:3] == ("yfinance", 15, "passed")
    assert catalog_row[3] == result.quality_artifact.uri
    assert catalog_row[4] is not None
    assert catalog_row[5] == datetime(2026, 7, 28, 3, 0, tzinfo=UTC)

    repository = PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)
    client = TestClient(create_app(repository, data_storage=storage))
    response = client.get(
        "/market/MSFT/bars",
        params={"market": "US", "frequency": "1d"},
    )
    assert response.status_code == 200
    assert len(response.json()) == 5
    assert response.json()[0]["instrument_id"] == "XNAS.MSFT"
    assert client.get("/datasets").status_code == 200


def test_yfinance_pipeline_uses_injected_calendar_for_session_time(tmp_path: Path) -> None:
    class InjectedCalendar:
        def __init__(self) -> None:
            self.closed_dates: list[date] = []

        def sessions(self, market: str, start: date, end: date):
            return []

        def is_trading_day(self, market: str, trading_date: date) -> bool:
            return True

        def session_open(self, market: str, trading_date: date) -> datetime:
            return datetime.combine(trading_date, time(13, 31), tzinfo=UTC)

        def session_close(self, market: str, trading_date: date) -> datetime:
            assert market == "US"
            self.closed_dates.append(trading_date)
            return datetime.combine(trading_date, time(21, 17), tzinfo=UTC)

    calendar = InjectedCalendar()
    result = run_yfinance_daily_pipeline(
        tmp_path,
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2026, 7, 20),
        end=date(2026, 7, 24),
        execution_time=datetime(2026, 7, 29, tzinfo=UTC),
        transport=ReplayYFinanceTransport(FIXTURE),
        retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
        calendar=calendar,
    )

    records = LocalFileSystemStorage(tmp_path).read_dataset(result.bar_artifact.uri)
    assert len(calendar.closed_dates) == 15
    assert all(record["ts_event"].time() == time(21, 17) for record in records)


@pytest.mark.parametrize("drift", ["missing", "wrong_type"])
def test_yfinance_provider_schema_drift_fails_after_raw_before_normalization(
    tmp_path: Path, drift: str
) -> None:
    fixture_payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    record = dict(fixture_payload["records"][0])
    if drift == "missing":
        record.pop("Close")
    else:
        record["Open"] = str(record["Open"])

    class DriftTransport:
        def fetch_daily_bars(self, symbols, start, end):
            return [record]

    provider = YFinanceProvider(DriftTransport())
    storage = LocalFileSystemStorage(tmp_path)
    service = IngestionService(
        providers={provider.name: provider},
        storage=storage,
        bar_normalizers={provider.name: normalize_yfinance_bars},
    )
    with pytest.raises(ProviderSchemaDriftError) as exc_info:
        service.run(_job())

    assert exc_info.value.report.checked_rows == 1
    assert len(storage.list_manifests()) == 1
    assert storage.list_manifests()[0].layer.value == "raw"
    assert not (tmp_path / "normalized").exists()
    assert not (tmp_path / "quality").exists()
