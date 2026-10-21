from __future__ import annotations

import json
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from ...models import DataModel, Instrument


class USInstrumentStatus(StrEnum):
    LISTED = "listed"
    DELISTED = "delisted"
    UNKNOWN = "unknown"


class USInstrumentRecord(DataModel):
    instrument_id: str = Field(min_length=3)
    symbol: str = Field(min_length=1)
    canonical_venue: Literal["XNYS", "XNAS"]
    provider_symbol: str = Field(min_length=1)
    source_symbol: str | None = None
    name: str | None = None
    asset_class: Literal["equity", "etf"] = "equity"
    listed_from: date | None = None
    listed_to: date | None = None
    status: USInstrumentStatus
    currency: Literal["USD"] = "USD"
    timezone: Literal["America/New_York"] = "America/New_York"

    @model_validator(mode="after")
    def validate_identity_and_lifecycle(self) -> USInstrumentRecord:
        if self.instrument_id != f"{self.canonical_venue}.{self.symbol}":
            raise ValueError("instrument_id must use <canonical MIC>.<symbol>")
        if self.provider_symbol.upper() != self.symbol:
            raise ValueError("provider_symbol must match the canonical symbol for this snapshot")
        if self.listed_from and self.listed_to and self.listed_to < self.listed_from:
            raise ValueError("listed_to cannot be earlier than listed_from")
        if self.status is USInstrumentStatus.DELISTED and self.listed_to is None:
            raise ValueError("delisted instruments require listed_to")
        return self

    def to_instrument(self, snapshot_id: str) -> Instrument:
        return Instrument(
            instrument_id=self.instrument_id,
            symbol=self.symbol,
            market="US",
            asset_class=self.asset_class,
            venue=self.canonical_venue,
            provider_symbol=self.provider_symbol,
            provider_exchange_alias=self.canonical_venue,
            currency=self.currency,
            name=self.name,
            timezone=self.timezone,
            instrument_snapshot_id=snapshot_id,
            active=self.status is USInstrumentStatus.LISTED,
        )


class USInstrumentSnapshot(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    snapshot_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    as_of: datetime
    source: Literal["curated_research_snapshot"] = "curated_research_snapshot"
    update_policy: Literal["new_version"] = "new_version"
    membership_basis: Literal[
        "curated_research",
        "current_sp500_constituents_not_point_in_time",
        "current_xnas_xnys_listings_not_point_in_time",
        "recovery_subset_of_versioned_snapshot",
    ] = "curated_research"
    source_refs: tuple[str, ...] = ()
    excluded_source_symbols: tuple[str, ...] = ()
    instruments: tuple[USInstrumentRecord, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_snapshot(self) -> USInstrumentSnapshot:
        if self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        instrument_ids = [item.instrument_id for item in self.instruments]
        provider_symbols = [item.provider_symbol for item in self.instruments]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("instrument_id must be unique within a snapshot")
        if len(provider_symbols) != len(set(provider_symbols)):
            raise ValueError("provider_symbol must be unique within a snapshot")
        return self

    def select(self, symbols: list[str] | tuple[str, ...] | None = None) -> tuple[Instrument, ...]:
        requested = (
            [symbol.upper() for symbol in symbols]
            if symbols is not None
            else [
                item.provider_symbol
                for item in self.instruments
                if item.status is USInstrumentStatus.LISTED
            ]
        )
        if len(requested) != len(set(requested)):
            raise ValueError("symbols must be unique")
        indexed = {item.provider_symbol: item for item in self.instruments}
        unknown = sorted(set(requested) - indexed.keys())
        if unknown:
            raise ValueError(f"Symbols are absent from instrument snapshot: {', '.join(unknown)}")
        return tuple(
            indexed[symbol].to_instrument(self.snapshot_id)
            for symbol in requested
            if indexed[symbol].status is USInstrumentStatus.LISTED
        )


DEFAULT_SNAPSHOT_PATH = Path(__file__).with_name("us_instruments_research_v1.json")


def load_us_instrument_snapshot(path: str | Path = DEFAULT_SNAPSHOT_PATH) -> USInstrumentSnapshot:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return USInstrumentSnapshot.model_validate(payload)


def default_snapshot_observed_at() -> datetime:
    """Stable timestamp exposed for callers constructing offline task metadata."""

    return datetime(2026, 8, 16, tzinfo=UTC)
