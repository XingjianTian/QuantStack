from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from ...markets import CNExchange
from ...models import DataModel
from .capabilities import CNSecurityType


class CNInstrumentStatus(StrEnum):
    LISTED = "listed"
    SUSPENDED = "suspended"
    DELISTED = "delisted"
    UNKNOWN = "unknown"


class CNInstrumentRecord(DataModel):
    """Point-in-time instrument-master record retained in a versioned snapshot."""

    instrument_id: str = Field(min_length=3)
    symbol: str = Field(min_length=1)
    canonical_venue: CNExchange
    provider_symbol: str = Field(min_length=1)
    provider_exchange_code: str = Field(min_length=1)
    security_type: CNSecurityType
    listed_from: date | None = None
    listed_to: date | None = None
    status: CNInstrumentStatus
    currency: Literal["CNY"] = "CNY"
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    source_provider: str = Field(min_length=1)
    source_endpoint: str = Field(min_length=1)
    source_timestamp: datetime

    @model_validator(mode="after")
    def validate_identity_and_dates(self) -> CNInstrumentRecord:
        if self.instrument_id != f"{self.canonical_venue}.{self.symbol}":
            raise ValueError("instrument_id must use <canonical MIC>.<symbol>")
        if self.listed_from and self.listed_to and self.listed_to < self.listed_from:
            raise ValueError("listed_to cannot be earlier than listed_from")
        if self.source_timestamp.tzinfo is None:
            raise ValueError("source_timestamp must be timezone-aware")
        return self


class CNInstrumentSnapshot(DataModel):
    """Immutable universe snapshot; a new observation creates a new snapshot id."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    snapshot_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    as_of: datetime
    source_provider: str = Field(min_length=1)
    source_endpoint: str = Field(min_length=1)
    instruments: tuple[CNInstrumentRecord, ...]

    @model_validator(mode="after")
    def validate_snapshot(self) -> CNInstrumentSnapshot:
        if self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        identities = [item.instrument_id for item in self.instruments]
        if len(identities) != len(set(identities)):
            raise ValueError("instrument_id must be unique within a snapshot")
        return self
