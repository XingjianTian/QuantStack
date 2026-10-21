from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal

from pydantic import Field, model_validator

from ..ingestion import IngestionService
from ..models import DataModel
from ..tasks import Task, TaskRuntime, TaskState

CNDAILY_DATASET_ID = "cn_equity_daily"


class VerifiedTradingSession(DataModel):
    """An explicitly verified CN trading session supplied by calendar evidence."""

    market: Literal["CN"] = "CN"
    session_date: date
    calendar_source: str = Field(min_length=1)
    calendar_version: str = Field(min_length=1)
    evidence_ref: str = Field(min_length=1)
    verified: Literal[True] = True


class DailyUniverse(DataModel):
    """Versioned point-in-time input universe; membership is not inferred by the worker."""

    universe_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    version: str = Field(min_length=1)
    instrument_ids: tuple[str, ...] = Field(min_length=1, max_length=20_000)
    evidence_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_instruments(self) -> DailyUniverse:
        if len(self.instrument_ids) != len(set(self.instrument_ids)):
            raise ValueError("instrument_ids must not contain duplicates")
        return self


class DailyProviderContract(DataModel):
    provider: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    provider_version: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    batch_size: int = Field(default=25, ge=1, le=500)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=0, ge=0, le=3600)


class DailyRunPlan(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: Literal["cn_equity_daily"] = CNDAILY_DATASET_ID
    dataset_version: str
    session: VerifiedTradingSession
    universe: DailyUniverse
    provider_contract: DailyProviderContract
    created_at: datetime
    tasks: tuple[Task, ...] = Field(min_length=1)
    canonical_eligible: Literal[False] = False
    production_pointer_updated: Literal[False] = False

    @model_validator(mode="after")
    def validate_tasks(self) -> DailyRunPlan:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        planned = tuple(instrument for task in self.tasks for instrument in task.instrument_scope)
        if planned != tuple(sorted(self.universe.instrument_ids)):
            raise ValueError("tasks must cover the sorted universe exactly once")
        expected_versions = (
            (self.dataset_version,)
            if len(self.tasks) == 1
            else tuple(
                f"{self.dataset_version}.batch-{index:04d}"
                for index in range(1, len(self.tasks) + 1)
            )
        )
        if any(
            task.start_date != self.session.session_date
            or task.end_date != self.session.session_date
            or task.dataset_version != expected_version
            for task, expected_version in zip(self.tasks, expected_versions, strict=True)
        ):
            raise ValueError("daily tasks must bind the declared session and dataset version")
        provider_fields = (
            "provider",
            "provider_version",
            "endpoint",
            "adapter_version",
            "native_schema_version",
        )
        if any(
            any(
                getattr(task, field) != getattr(self.provider_contract, field)
                for field in provider_fields
            )
            for task in self.tasks
        ):
            raise ValueError("daily tasks must bind the declared provider contract")
        return self


class DailyBatchResult(DataModel):
    task_id: str
    instrument_ids: tuple[str, ...]
    state: TaskState
    deduplicated: bool
    attempts: int = Field(ge=0)
    records_received: int = Field(ge=0)
    publication_bundle_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    error_type: str | None = None
    error_stage: str | None = None


class DailyRunReport(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: Literal["cn_equity_daily"] = CNDAILY_DATASET_ID
    dataset_version: str
    session_date: date
    universe_version: str
    started_at: datetime
    completed_at: datetime
    batches: tuple[DailyBatchResult, ...]
    succeeded_batches: int = Field(ge=0)
    deduplicated_batches: int = Field(ge=0)
    retryable_failed_batches: int = Field(ge=0)
    permanently_failed_batches: int = Field(ge=0)
    quarantined_batches: int = Field(ge=0)
    cancelled_batches: int = Field(ge=0)
    publication_bundle_uris: tuple[str, ...]
    canonical_eligible: Literal[False] = False
    production_pointer_updated: Literal[False] = False


def build_daily_run_plan(
    *,
    session: VerifiedTradingSession,
    universe: DailyUniverse,
    provider_contract: DailyProviderContract,
    created_at: datetime | None = None,
) -> DailyRunPlan:
    """Create deterministic same-session tasks without contacting a provider."""

    planned_at = (created_at or datetime.now(UTC)).astimezone(UTC)
    instruments = tuple(sorted(universe.instrument_ids))
    dataset_version = f"daily-{session.session_date.isoformat()}-{universe.version}"
    batches = tuple(
        instruments[offset : offset + provider_contract.batch_size]
        for offset in range(0, len(instruments), provider_contract.batch_size)
    )
    tasks = tuple(
        Task(
            dataset_id=CNDAILY_DATASET_ID,
            dataset_version=(
                dataset_version if len(batches) == 1 else f"{dataset_version}.batch-{index:04d}"
            ),
            provider=provider_contract.provider,
            provider_version=provider_contract.provider_version,
            endpoint=provider_contract.endpoint,
            adapter_version=provider_contract.adapter_version,
            native_schema_version=provider_contract.native_schema_version,
            market="CN",
            instrument_scope=batch,
            start_date=session.session_date,
            end_date=session.session_date,
            max_attempts=provider_contract.max_attempts,
            retry_backoff_seconds=provider_contract.retry_backoff_seconds,
            created_at=planned_at,
        )
        for index, batch in enumerate(batches, start=1)
    )
    return DailyRunPlan(
        dataset_version=dataset_version,
        session=session,
        universe=universe,
        provider_contract=provider_contract,
        created_at=planned_at,
        tasks=tasks,
    )


class DailyWorker:
    """Execute a daily plan sequentially; scheduling and catalog publication stay external."""

    def __init__(self, runtime: TaskRuntime, service: IngestionService) -> None:
        self.runtime = runtime
        self.service = service

    def run(self, plan: DailyRunPlan, *, now: datetime | None = None) -> DailyRunReport:
        started_at = (now or datetime.now(UTC)).astimezone(UTC)
        results: list[DailyBatchResult] = []
        for task in plan.tasks:
            submitted = self.runtime.submit(task)
            terminal_failure = submitted.state in {
                TaskState.PERMANENTLY_FAILED,
                TaskState.QUARANTINED,
                TaskState.CANCELLED,
            }
            execution = (
                None
                if terminal_failure
                else self.runtime.execute(task, self.service, now=started_at)
            )
            attempts = self.runtime.attempts(task.task_id)
            latest = attempts[-1] if attempts else None
            persisted = self.runtime.store.load(task.task_id) if self.runtime.store else None
            bundle_uri = (
                latest.publication_bundle_uri
                if latest and latest.publication_bundle_uri
                else persisted.publication_bundle_uri
                if persisted
                else None
            )
            results.append(
                DailyBatchResult(
                    task_id=task.task_id,
                    instrument_ids=task.instrument_scope,
                    state=submitted.state if execution is None else execution.task.state,
                    deduplicated=False if execution is None else execution.deduplicated,
                    attempts=len(attempts),
                    records_received=latest.records_received if latest else 0,
                    publication_bundle_uri=bundle_uri,
                    error_type=(latest.retry.error_type if latest else None),
                    error_stage=(
                        latest.failure_context.failure_stage
                        if latest and latest.failure_context
                        else None
                    ),
                )
            )

        completed_at = max(datetime.now(UTC), started_at)
        batches = tuple(results)
        return DailyRunReport(
            dataset_version=plan.dataset_version,
            session_date=plan.session.session_date,
            universe_version=plan.universe.version,
            started_at=started_at,
            completed_at=completed_at,
            batches=batches,
            succeeded_batches=sum(item.state is TaskState.SUCCEEDED for item in batches),
            deduplicated_batches=sum(item.deduplicated for item in batches),
            retryable_failed_batches=sum(
                item.state is TaskState.RETRYABLE_FAILED for item in batches
            ),
            permanently_failed_batches=sum(
                item.state is TaskState.PERMANENTLY_FAILED for item in batches
            ),
            quarantined_batches=sum(item.state is TaskState.QUARANTINED for item in batches),
            cancelled_batches=sum(item.state is TaskState.CANCELLED for item in batches),
            publication_bundle_uris=tuple(
                item.publication_bundle_uri
                for item in batches
                if item.publication_bundle_uri is not None
            ),
        )
