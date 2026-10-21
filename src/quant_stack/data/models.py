from __future__ import annotations

from datetime import UTC, date, datetime, time
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Market = Literal["CN", "HK", "US"]
Frequency = Literal["1d", "1m", "tick"]


class DatasetLayer(StrEnum):
    RAW = "raw"
    NORMALIZED = "normalized"
    FEATURE = "feature"


class DatasetKind(StrEnum):
    MARKET_BARS = "market_bars"
    INSTRUMENTS = "instruments"
    FUNDAMENTALS = "fundamentals"
    CORPORATE_ACTIONS = "corporate_actions"
    FACTOR = "factor"


class EvidenceStatus(StrEnum):
    VERIFIED = "verified"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    BLOCKED = "blocked"


class CalendarDayState(StrEnum):
    SESSION = "session"
    WEEKEND = "weekend"
    HOLIDAY = "holiday"
    TEMPORARY_CLOSURE = "temporary_closure"
    UNKNOWN = "unknown"


class DataModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Instrument(DataModel):
    instrument_id: str = Field(min_length=3)
    symbol: str = Field(min_length=1)
    market: Market
    asset_class: Literal["equity", "etf"] = "equity"
    venue: str = Field(min_length=1)
    provider_symbol: str | None = None
    provider_exchange_alias: str | None = None
    currency: Literal["CNY", "HKD", "USD"]
    name: str | None = None
    timezone: str
    instrument_snapshot_id: str | None = None
    active: bool = True


class BarLineage(DataModel):
    provider: str | None = None
    endpoint: str | None = None
    provider_version: str | None = None
    adapter_version: str | None = None
    native_schema_version: str | None = None
    provider_symbol: str | None = None
    provider_exchange_alias: str | None = None
    canonical_mic: str | None = None
    instrument_snapshot_id: str | None = None
    source_timestamp: datetime | None = None
    retrieval_timestamp: datetime | None = None
    raw_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    raw_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    original_volume_unit: str | None = None
    applied_volume_multiplier: float | None = None
    adjustment: str | None = None

    @model_validator(mode="after")
    def validate_timestamps(self) -> BarLineage:
        for field_name in ("source_timestamp", "retrieval_timestamp"):
            timestamp = getattr(self, field_name)
            if timestamp is not None and timestamp.tzinfo is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if (
            self.source_timestamp is not None
            and self.retrieval_timestamp is not None
            and self.retrieval_timestamp < self.source_timestamp
        ):
            raise ValueError("retrieval_timestamp cannot be earlier than source_timestamp")
        return self


class Bar(DataModel):
    instrument_id: str
    ts_event: datetime
    open: float = Field(ge=0)
    high: float = Field(ge=0)
    low: float = Field(ge=0)
    close: float = Field(ge=0)
    volume: float = Field(ge=0)
    amount: float | None = Field(default=None, ge=0)
    vwap: float | None = Field(default=None, ge=0)
    currency: Literal["CNY", "HKD", "USD"]
    lineage: BarLineage = Field(default_factory=BarLineage)

    @model_validator(mode="after")
    def validate_ohlc(self) -> Bar:
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high must be greater than or equal to OHLC values")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low must be less than or equal to OHLC values")
        return self

    def normalized(self) -> Bar:
        return self.model_copy(update={"ts_event": self.ts_event.astimezone(UTC)})


class DatasetQualityMetadata(DataModel):
    status: Literal["passed", "warning", "failed"]
    checks: list[str] = Field(default_factory=list)
    failed_checks: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    report_uri: str | None = Field(default=None, pattern=r"^quant-data://")


class CalendarDayEvidence(DataModel):
    trading_date: date
    state: CalendarDayState
    source_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_weekend(self) -> CalendarDayEvidence:
        if self.state is CalendarDayState.WEEKEND and self.trading_date.weekday() < 5:
            raise ValueError("weekend classification requires Saturday or Sunday")
        if self.state is CalendarDayState.SESSION and self.trading_date.weekday() >= 5:
            raise ValueError("weekend cannot be an exchange session")
        return self


class CalendarEvidence(DataModel):
    """Bounded, auditable session evidence used for live canonical bars."""

    schema_version: Literal["1.0.0", "1.1.0"] = "1.0.0"
    source: str = Field(min_length=1)
    source_kind: Literal["exchange_announcement", "provider_observed"]
    version: str = Field(min_length=1)
    market: Market
    timezone: str = Field(min_length=1)
    open_time: time
    close_time: time
    supported_start: date
    supported_end: date
    confirmed_sessions: tuple[date, ...] = Field(min_length=1)
    confirmed_non_sessions: tuple[date, ...] = ()
    observed_at: datetime
    exchanges: tuple[str, ...] = ()
    day_evidence: tuple[CalendarDayEvidence, ...] = ()
    raw_evidence_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    raw_evidence_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    normalized_evidence_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    normalized_evidence_sha256: str | None = Field(
        default=None, pattern=r"^[a-f0-9]{64}$"
    )
    update_policy: Literal["new_version"] = "new_version"

    @model_validator(mode="after")
    def validate_scope(self) -> CalendarEvidence:
        if self.observed_at.tzinfo is None:
            raise ValueError("calendar observed_at must be timezone-aware")
        if self.supported_end < self.supported_start:
            raise ValueError("calendar support end must not precede start")
        sessions = set(self.confirmed_sessions)
        non_sessions = set(self.confirmed_non_sessions)
        if len(sessions) != len(self.confirmed_sessions):
            raise ValueError("calendar confirmed sessions must be unique")
        if len(non_sessions) != len(self.confirmed_non_sessions):
            raise ValueError("calendar confirmed non-sessions must be unique")
        if sessions & non_sessions:
            raise ValueError("calendar session and non-session evidence cannot overlap")
        for day in sessions | non_sessions:
            if not self.supported_start <= day <= self.supported_end:
                raise ValueError("calendar evidence date is outside supported range")
        if any(day.weekday() >= 5 for day in sessions):
            raise ValueError("weekend cannot be a confirmed exchange session")
        if self.close_time <= self.open_time:
            raise ValueError("calendar close time must follow open time")
        if self.schema_version == "1.1.0":
            if self.source_kind != "exchange_announcement":
                raise ValueError("official calendar evidence requires exchange announcements")
            if not self.exchanges:
                raise ValueError("official calendar evidence must name covered exchanges")
            required_artifacts = {
                "raw_evidence_uri": self.raw_evidence_uri,
                "raw_evidence_sha256": self.raw_evidence_sha256,
                "normalized_evidence_uri": self.normalized_evidence_uri,
                "normalized_evidence_sha256": self.normalized_evidence_sha256,
            }
            missing = sorted(name for name, value in required_artifacts.items() if value is None)
            if missing:
                raise ValueError("official calendar evidence is missing: " + ", ".join(missing))
            indexed = {item.trading_date: item for item in self.day_evidence}
            if len(indexed) != len(self.day_evidence):
                raise ValueError("calendar day evidence dates must be unique")
            expected_days = {
                self.supported_start.fromordinal(ordinal)
                for ordinal in range(
                    self.supported_start.toordinal(), self.supported_end.toordinal() + 1
                )
            }
            if set(indexed) != expected_days:
                raise ValueError("official calendar must classify every supported date")
            classified_sessions = {
                day
                for day, item in indexed.items()
                if item.state is CalendarDayState.SESSION
            }
            if classified_sessions != sessions:
                raise ValueError("official session evidence conflicts with day classifications")
            classified_non_sessions = {
                day
                for day, item in indexed.items()
                if item.state
                in {
                    CalendarDayState.WEEKEND,
                    CalendarDayState.HOLIDAY,
                    CalendarDayState.TEMPORARY_CLOSURE,
                }
            }
            if classified_non_sessions != non_sessions:
                raise ValueError("official non-session evidence conflicts with day classifications")
            if any(item.state is CalendarDayState.UNKNOWN for item in self.day_evidence):
                raise ValueError("official supported range cannot contain unknown dates")
        return self


class MarketStateEvidence(DataModel):
    evidence_id: str = Field(pattern=r"^[a-zA-Z0-9._-]+$")
    instrument_id: str = Field(min_length=3)
    exchange: str = Field(min_length=1)
    state: Literal[
        "normal",
        "risk_warning",
        "suspended",
        "zero_volume",
        "recent_listing",
        "short_history",
        "delisted",
        "inactive",
        "code_change",
    ]
    status: EvidenceStatus
    effective_from: date | None = None
    effective_to: date | None = None
    observed_at: datetime
    source_kind: Literal["exchange_announcement", "provider_observed", "blocked_assessment"]
    source_url: str
    source_title: str = Field(min_length=1)
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_effective_evidence(self) -> MarketStateEvidence:
        if self.observed_at.tzinfo is None:
            raise ValueError("market-state observed_at must be timezone-aware")
        if self.effective_from and self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("market-state effective_to cannot precede effective_from")
        if self.status is EvidenceStatus.VERIFIED:
            if self.source_kind != "exchange_announcement":
                raise ValueError("verified market state requires exchange evidence")
            if self.effective_from is None:
                raise ValueError("verified market state requires effective_from")
        if self.source_kind == "blocked_assessment" and self.status is not EvidenceStatus.BLOCKED:
            raise ValueError("blocked assessment must remain blocked")
        return self


class MarketStateSample(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    version: str = Field(min_length=1)
    finite_sample: Literal[True] = True
    as_of: datetime
    evidence: tuple[MarketStateEvidence, ...] = Field(min_length=1)
    raw_evidence_uri: str = Field(pattern=r"^quant-data://")
    raw_evidence_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    update_policy: Literal["new_version"] = "new_version"

    @model_validator(mode="after")
    def validate_sample(self) -> MarketStateSample:
        if self.as_of.tzinfo is None:
            raise ValueError("market-state sample as_of must be timezone-aware")
        ids = [item.evidence_id for item in self.evidence]
        if len(ids) != len(set(ids)):
            raise ValueError("market-state evidence IDs must be unique")
        return self


class DatasetManifest(DataModel):
    manifest_schema_version: Literal["1.0.0", "1.1.0", "1.2.0", "1.3.0"] = "1.2.0"
    dataset_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    version: str = Field(min_length=1)
    layer: DatasetLayer
    kind: DatasetKind
    market: Market
    provider: str = Field(min_length=1)
    provider_version: str | None = Field(default=None, min_length=1)
    adapter_version: str | None = Field(default=None, min_length=1)
    native_schema_version: str | None = Field(default=None, min_length=1)
    frequency: str = Field(min_length=1)
    schema_ref: str = Field(min_length=1)
    storage_uri: str = Field(pattern=r"^quant-data://")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    available_at_policy: str = Field(min_length=1)
    revision_policy: Literal["immutable", "append_only", "rebuildable"]
    row_count: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    quality_status: Literal["passed", "warning", "failed"]
    quality: DatasetQualityMetadata = Field(
        default_factory=lambda: DatasetQualityMetadata(status="passed")
    )
    license_scope: str = Field(min_length=1)
    source_url: str = "synthetic://quant-stack"
    input_dataset_versions: list[str] = Field(default_factory=list)
    raw_storage_uri: str | None = Field(default=None, pattern=r"^quant-data://")
    job_id: str | None = None
    source_timestamp: datetime | None = None
    available_at: datetime | None = None
    calendar_evidence: CalendarEvidence | None = None
    market_state_sample: MarketStateSample | None = None

    @model_validator(mode="before")
    @classmethod
    def populate_legacy_quality_metadata(cls, value: object) -> object:
        if isinstance(value, dict) and "quality" not in value and value.get("quality_status"):
            value = {
                **value,
                "quality": {"status": value["quality_status"]},
            }
        return value

    @model_validator(mode="after")
    def quality_status_matches_metadata(self) -> DatasetManifest:
        if self.quality_status != self.quality.status:
            raise ValueError("quality_status must match quality.status")
        for field_name in ("created_at", "source_timestamp", "available_at"):
            timestamp = getattr(self, field_name)
            if timestamp is not None and timestamp.tzinfo is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if (
            self.source_timestamp is not None
            and self.available_at is not None
            and self.available_at < self.source_timestamp
        ):
            raise ValueError("available_at cannot be earlier than source_timestamp")
        return self
