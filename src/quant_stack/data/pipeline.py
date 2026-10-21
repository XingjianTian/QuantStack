from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from .ingestion import DataJob, IngestionService, RetryPolicy
from .interfaces import MarketCalendarProvider
from .providers import SyntheticProvider
from .storage import LocalFileSystemStorage


@dataclass(frozen=True)
class SyntheticPipelineResult:
    manifest_path: Path
    parquet_path: Path
    catalog_path: Path
    row_count: int
    raw_path: Path
    instrument_path: Path


def run_synthetic_pipeline(
    output_root: str | Path,
    *,
    market: str = "US",
    symbols: list[str] | None = None,
    start: date = date(2026, 1, 1),
    end: date = date(2026, 1, 15),
    execution_time: datetime | None = None,
    calendar: MarketCalendarProvider | None = None,
) -> SyntheticPipelineResult:
    """Compatibility entry point backed by the production-style ingestion service."""

    provider = SyntheticProvider()
    resolved_symbols = (
        symbols
        or {
            "CN": ["600000"],
            "HK": ["0700"],
            "US": ["AAPL"],
        }[market]
    )
    storage = LocalFileSystemStorage(output_root)
    service = IngestionService(
        providers={provider.name: provider}, storage=storage, calendar=calendar
    )
    result = service.run(
        DataJob(
            dataset=f"synthetic_{market.lower()}_equity_daily",
            provider=provider.name,
            market=market,
            frequency="1d",
            execution_time=execution_time or datetime.now(UTC),
            retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=resolved_symbols,
            start=start,
            end=end,
        )
    )
    return SyntheticPipelineResult(
        manifest_path=result.manifest_paths[2],
        parquet_path=result.bar_artifact.path,
        catalog_path=storage.data_root / "catalog/quant.duckdb",
        row_count=result.bar_manifest.row_count,
        raw_path=result.raw_artifact.path,
        instrument_path=result.instrument_artifact.path,
    )
