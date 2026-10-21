from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from ...calendars import USExchangeCalendar
from ...ingestion import DataJob, IngestionResult, IngestionService, RetryPolicy
from ...interfaces import MarketCalendarProvider
from ...storage import LocalFileSystemStorage, StagingFileSystemStorage
from ...tasks import FileTaskStore, Task, TaskExecution, TaskRuntime
from .instruments import USInstrumentSnapshot
from .normalizer import normalize_yfinance_bars
from .provider import YFinanceProvider
from .transport import YFinanceTransport


def run_yfinance_daily_pipeline(
    data_root: str | Path,
    *,
    symbols: list[str],
    start: date,
    end: date,
    execution_time: datetime | None = None,
    transport: YFinanceTransport | None = None,
    retry_policy: RetryPolicy | None = None,
    calendar: MarketCalendarProvider | None = None,
    instrument_snapshot: USInstrumentSnapshot | None = None,
    include_actions: bool = False,
) -> IngestionResult:
    """Run the real-provider pipeline; inject ReplayYFinanceTransport for offline use."""

    provider = YFinanceProvider(
        transport, instrument_snapshot, include_actions=include_actions
    )
    service = IngestionService(
        providers={provider.name: provider},
        storage=LocalFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalize_yfinance_bars},
        calendar=calendar or USExchangeCalendar("XNYS"),
    )
    return service.run(
        DataJob(
            dataset="yfinance_us_equity_daily",
            provider=provider.name,
            market="US",
            frequency="1d",
            execution_time=execution_time or datetime.now(UTC),
            retry_policy=retry_policy or RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=symbols,
            start=start,
            end=end,
        )
    )


def run_yfinance_daily_task(
    data_root: str | Path,
    task_state_root: str | Path,
    *,
    symbols: list[str],
    start: date,
    end: date,
    dataset_version: str,
    execution_time: datetime | None = None,
    transport: YFinanceTransport | None = None,
    calendar: MarketCalendarProvider | None = None,
    checkpoint: dict[str, object] | None = None,
    cursor: str | None = None,
    instrument_snapshot: USInstrumentSnapshot | None = None,
    max_attempts: int = 3,
    include_actions: bool = False,
    durable_task_state: bool = True,
) -> TaskExecution:
    """Execute one pinned, recoverable yfinance task without catalog publication."""

    provider = YFinanceProvider(
        transport, instrument_snapshot, include_actions=include_actions
    )
    contract = provider.runtime_contract
    created_at = execution_time or datetime.now(UTC)
    task = Task(
        dataset_id="yfinance_us_equity_daily",
        dataset_version=dataset_version,
        provider=contract.provider,
        provider_version=contract.provider_version,
        endpoint=contract.endpoint,
        adapter_version=contract.adapter_version,
        native_schema_version=contract.native_schema_version,
        market="US",
        frequency="1d",
        instrument_scope=tuple(symbols),
        start_date=start,
        end_date=end,
        created_at=created_at,
        max_attempts=max_attempts,
    )
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalize_yfinance_bars},
        calendar=calendar or USExchangeCalendar("XNYS"),
    )
    return TaskRuntime(FileTaskStore(task_state_root, durable=durable_task_state)).execute(
        task,
        service,
        now=created_at,
        checkpoint=checkpoint,
        cursor=cursor,
    )
