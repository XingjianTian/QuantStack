from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from enum import StrEnum

from pydantic import Field, model_validator

from ..models import DataModel, Market


class IngestionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RetryPolicy(DataModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_seconds: float = Field(default=1.0, ge=0, le=3600)


class DataJob(DataModel):
    """Scheduler-neutral declaration; Celery, APScheduler or Airflow may execute it."""

    dataset: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    dataset_version: str | None = Field(default=None, min_length=1)
    idempotency_key: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    provider: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    provider_version: str | None = Field(default=None, min_length=1)
    provider_endpoint: str | None = Field(default=None, min_length=1)
    adapter_version: str | None = Field(default=None, min_length=1)
    native_schema_version: str | None = Field(default=None, min_length=1)
    market: Market
    frequency: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    execution_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    symbols: list[str] = Field(min_length=1)
    start: date
    end: date

    @model_validator(mode="after")
    def validate_window(self) -> DataJob:
        if self.execution_time.tzinfo is None:
            raise ValueError("execution_time must be timezone-aware")
        if self.end < self.start:
            raise ValueError("end must be on or after start")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("symbols must be unique")
        return self

    @property
    def job_id(self) -> str:
        if self.idempotency_key is not None:
            return self.idempotency_key[:16]
        identity = "|".join(
            [
                self.dataset,
                self.provider,
                self.market,
                self.frequency,
                self.execution_time.astimezone(UTC).isoformat(),
                self.start.isoformat(),
                self.end.isoformat(),
                ",".join(self.symbols),
            ]
        )
        return hashlib.sha256(identity.encode()).hexdigest()[:16]

    @property
    def version(self) -> str:
        if self.dataset_version is not None:
            return self.dataset_version
        timestamp = self.execution_time.astimezone(UTC).strftime("%Y-%m-%d.%H%M%S")
        return f"{timestamp}-{self.job_id[:8]}"
