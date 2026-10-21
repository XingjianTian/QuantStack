from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, JsonValue, computed_field, model_validator

from ..models import DataModel, Market


class TaskState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE_FAILED = "retryable_failed"
    PERMANENTLY_FAILED = "permanently_failed"
    QUARANTINED = "quarantined"
    CANCELLED = "cancelled"


def make_idempotency_key(
    *,
    task_schema_version: str,
    dataset_id: str,
    dataset_version: str,
    provider: str,
    provider_version: str,
    endpoint: str,
    adapter_version: str,
    native_schema_version: str,
    market: Market,
    frequency: str,
    instrument_scope: tuple[str, ...],
    start_date: date,
    end_date: date,
) -> str:
    """Hash the stable business identity; runtime timestamps are deliberately excluded."""

    identity = {
        "task_schema_version": task_schema_version,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "provider": provider,
        "provider_version": provider_version,
        "endpoint": endpoint,
        "adapter_version": adapter_version,
        "native_schema_version": native_schema_version,
        "market": market,
        "frequency": frequency,
        "instrument_scope": sorted(instrument_scope),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    payload = json.dumps(identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


class Task(DataModel):
    """Business task identity and aggregate state, independent from execution attempts."""

    schema_version: Literal["1.1.0"] = "1.1.0"
    dataset_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    dataset_version: str = Field(min_length=1)
    provider: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    provider_version: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    market: Market
    instrument_scope: tuple[str, ...] = Field(min_length=1)
    start_date: date
    end_date: date
    frequency: str = Field(default="1d", pattern=r"^[a-z0-9][a-z0-9_-]*$")
    state: TaskState = TaskState.PENDING
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=0, ge=0, le=3600)
    created_at: datetime

    @model_validator(mode="after")
    def validate_task(self) -> Task:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.end_date < self.start_date:
            raise ValueError("end_date cannot be earlier than start_date")
        if len(self.instrument_scope) != len(set(self.instrument_scope)):
            raise ValueError("instrument_scope must not contain duplicates")
        return self

    @computed_field
    @property
    def idempotency_key(self) -> str:
        return make_idempotency_key(
            task_schema_version=self.schema_version,
            dataset_id=self.dataset_id,
            dataset_version=self.dataset_version,
            provider=self.provider,
            provider_version=self.provider_version,
            endpoint=self.endpoint,
            adapter_version=self.adapter_version,
            native_schema_version=self.native_schema_version,
            market=self.market,
            frequency=self.frequency,
            instrument_scope=self.instrument_scope,
            start_date=self.start_date,
            end_date=self.end_date,
        )

    @computed_field
    @property
    def task_id(self) -> str:
        return f"task-{self.idempotency_key[:24]}"


class RetryMetadata(DataModel):
    max_attempts: int = Field(ge=1, le=10)
    retryable: bool = False
    backoff_seconds: float = Field(default=0, ge=0, le=3600)
    next_retry_at: datetime | None = None
    error_type: str | None = None
    error_message: str | None = None

    @model_validator(mode="after")
    def validate_retry(self) -> RetryMetadata:
        if self.next_retry_at is not None and self.next_retry_at.tzinfo is None:
            raise ValueError("next_retry_at must be timezone-aware")
        if self.retryable and self.next_retry_at is None:
            raise ValueError("retryable metadata requires next_retry_at")
        return self


FailureStage = Literal[
    "provider_contract_validation",
    "calendar_validation",
    "provider_fetch",
    "raw_persist",
    "provider_schema_validation",
    "normalization",
    "quality_gate",
    "parquet_write",
    "manifest_stage",
    "bundle_build",
    "catalog_publish",
]


class TaskFailureContext(DataModel):
    """Durable evidence for diagnosing or resuming a failed attempt."""

    failure_stage: FailureStage
    raw_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    raw_manifest_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    quality_report_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    proposed_manifest_uris: tuple[str, ...] = ()
    completed_artifact_uris: tuple[str, ...] = ()
    provider: str
    endpoint: str
    job_id: str


class TaskAttempt(DataModel):
    """One immutable execution observation; retries append a new attempt."""

    schema_version: Literal["1.1.0"] = "1.1.0"
    attempt_id: str = Field(pattern=r"^attempt-[a-f0-9]{24}-[0-9]+$")
    task_id: str = Field(pattern=r"^task-[a-f0-9]{24}$")
    attempt_number: int = Field(ge=1)
    state: TaskState
    checkpoint: dict[str, JsonValue] = Field(default_factory=dict)
    cursor: str | None = None
    retry: RetryMetadata
    started_at: datetime
    completed_at: datetime | None = None
    records_received: int = Field(default=0, ge=0)
    raw_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    output_manifest_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    publication_bundle_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    failure_context: TaskFailureContext | None = None

    @model_validator(mode="after")
    def validate_attempt(self) -> TaskAttempt:
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")
        if self.completed_at is not None and self.completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")
        if self.completed_at and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be earlier than started_at")
        if self.state is TaskState.RUNNING and self.completed_at is not None:
            raise ValueError("running attempts cannot have completed_at")
        if self.state is not TaskState.RUNNING and self.completed_at is None:
            raise ValueError("finished attempts require completed_at")
        if self.state is TaskState.RETRYABLE_FAILED and not self.retry.retryable:
            raise ValueError("retryable_failed attempts require retryable metadata")
        return self


class TaskStateFile(DataModel):
    """Crash-readable task aggregate with append-only attempt history."""

    schema_version: Literal["1.1.0"] = "1.1.0"
    task: Task
    attempts: tuple[TaskAttempt, ...] = ()
    publication_bundle_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    updated_at: datetime

    @model_validator(mode="after")
    def validate_state_file(self) -> TaskStateFile:
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")
        if any(attempt.task_id != self.task.task_id for attempt in self.attempts):
            raise ValueError("all attempts must belong to the task")
        expected = tuple(range(1, len(self.attempts) + 1))
        if tuple(item.attempt_number for item in self.attempts) != expected:
            raise ValueError("attempt history must be ordered and contiguous")
        return self
