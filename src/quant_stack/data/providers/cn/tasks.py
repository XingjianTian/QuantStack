from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, JsonValue, computed_field, model_validator

from ...markets import CNExchange
from ...models import DataModel


class CNTaskType(StrEnum):
    INSTRUMENT_UNIVERSE_UPDATE = "instrument-universe-update"
    HISTORICAL_BOOTSTRAP = "historical-bootstrap"
    DAILY_INCREMENT = "daily-increment"
    GAP_REPAIR = "gap-repair"
    CROSS_VALIDATION = "cross-validation"
    CATALOG_PROMOTION = "catalog-promotion"


class CNTaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE_FAILED = "retryable_failed"
    PERMANENTLY_FAILED = "permanently_failed"
    QUARANTINED = "quarantined"
    CANCELLED = "cancelled"


class CNDataTask(DataModel):
    """Scheduler-neutral task identity and current aggregate state."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    task_type: CNTaskType
    provider: str = Field(min_length=1)
    provider_endpoint: str = Field(min_length=1)
    provider_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    market: Literal["CN"] = "CN"
    exchange: CNExchange | None = None
    instrument_scope: tuple[str, ...] = ()
    start_date: date | None = None
    end_date: date | None = None
    status: CNTaskStatus = CNTaskStatus.PENDING
    created_at: datetime

    @model_validator(mode="after")
    def validate_task(self) -> CNDataTask:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date cannot be earlier than start_date")
        if len(self.instrument_scope) != len(set(self.instrument_scope)):
            raise ValueError("instrument_scope must not contain duplicates")
        return self

    @computed_field
    @property
    def idempotency_key(self) -> str:
        identity = {
            "schema_version": self.schema_version,
            "task_type": self.task_type.value,
            "provider": self.provider,
            "provider_endpoint": self.provider_endpoint,
            "provider_version": self.provider_version,
            "native_schema_version": self.native_schema_version,
            "market": self.market,
            "exchange": self.exchange,
            "instrument_scope": sorted(self.instrument_scope),
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
        }
        payload = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    @computed_field
    @property
    def task_id(self) -> str:
        return f"cn-{self.idempotency_key[:24]}"


class CNDataTaskAttempt(DataModel):
    """One execution attempt; retries append records instead of mutating prior attempts."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    attempt_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    task_id: str = Field(pattern=r"^cn-[a-f0-9]{24}$")
    attempt_count: int = Field(ge=1)
    status: CNTaskStatus
    checkpoint: dict[str, JsonValue] = Field(default_factory=dict)
    cursor: str | None = None
    records_received: int = Field(default=0, ge=0)
    raw_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    output_manifest_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    error_summary: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    next_retry_at: datetime | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None

    @model_validator(mode="after")
    def validate_attempt(self) -> CNDataTaskAttempt:
        timestamps = (
            self.started_at,
            self.completed_at,
            self.next_retry_at,
            self.lease_expires_at,
        )
        if any(item is not None and item.tzinfo is None for item in timestamps):
            raise ValueError("attempt timestamps must be timezone-aware")
        if self.completed_at and self.started_at and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be earlier than started_at")
        if self.status is CNTaskStatus.RUNNING and self.started_at is None:
            raise ValueError("running attempts require started_at")
        if self.status is CNTaskStatus.RETRYABLE_FAILED and self.next_retry_at is None:
            raise ValueError("retryable failures require next_retry_at")
        return self


class CNDailyIncrementPolicy(DataModel):
    lookback_trading_days: int = Field(default=20, ge=1, le=60)
