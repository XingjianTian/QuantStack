"""Provider-neutral, exact-value contracts for read-only brokerage data.

These models deliberately contain no Schwab SDK types or field names.  Missing
values stay ``None`` and unknown enumerations use an explicit ``UNKNOWN`` value;
neither is silently converted to zero or a successful state.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _decimal(value: Any) -> Decimal:
    """Parse JSON decimal values without accepting lossy binary floats."""

    if isinstance(value, bool) or value is None:
        raise TypeError("a decimal value must be an integer, string, or Decimal")
    if isinstance(value, float):
        raise TypeError("floating-point values are not accepted for money or quantity")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"invalid decimal value: {value!r}") from exc
    else:
        raise TypeError(f"unsupported decimal value type: {type(value).__name__}")
    if not result.is_finite():
        raise ValueError("decimal values must be finite")
    return result


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


class BrokerageModel(BaseModel):
    """Strict immutable model shared by contracts and persisted snapshots."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, payload: str) -> Any:
        return cls.model_validate_json(payload)


class UnknownValue(StrEnum):
    UNKNOWN = "unknown"


class AccountType(StrEnum):
    CASH = "cash"
    MARGIN = "margin"
    IRA = "ira"
    ROTH_IRA = "roth_ira"
    BROKERAGE = "brokerage"
    UNKNOWN = "unknown"


class AssetType(StrEnum):
    EQUITY = "equity"
    ETF = "etf"
    OPTION = "option"
    CASH = "cash"
    MUTUAL_FUND = "mutual_fund"
    FIXED_INCOME = "fixed_income"
    UNKNOWN = "unknown"


class PositionSide(StrEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"
    UNKNOWN = "unknown"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    BUY_TO_COVER = "buy_to_cover"
    SELL_SHORT = "sell_short"
    UNKNOWN = "unknown"


class OrderStatus(StrEnum):
    NEW = "new"
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    REPLACED = "replaced"
    PENDING = "pending"
    UNKNOWN = "unknown"


class SyncStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class CurrencyAmount(BrokerageModel):
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    amount: Decimal

    _parse_amount = field_validator("amount", mode="before")(_decimal)


class CashBalances(BrokerageModel):
    """Account funding values with independent unknown and semantic flags."""

    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    balance: Decimal | None = None
    cash: Decimal | None = None
    settled_cash: Decimal | None = None
    available_cash: Decimal | None = None
    buying_power: Decimal | None = None
    withdrawable_cash: Decimal | None = None
    open_order_reservation: Decimal | None = None
    order_reservation_included: bool | None = None
    margin_enabled: bool | None = None
    source_fields: tuple[str, ...] = ()
    unknown_fields: tuple[str, ...] = ()

    _parse_decimals = field_validator(
        "balance",
        "cash",
        "settled_cash",
        "available_cash",
        "buying_power",
        "withdrawable_cash",
        "open_order_reservation",
        mode="before",
    )(_optional_decimal)


class OptionContract(BrokerageModel):
    underlying_symbol: str = Field(min_length=1)
    expiration_date: date | None = None
    put_call: str | None = None
    strike_price: Decimal | None = None
    contract_multiplier: Decimal | None = None
    deliverable: str | None = None
    contract_symbol: str | None = None
    unknown_fields: tuple[str, ...] = ()

    _parse_decimals = field_validator("strike_price", "contract_multiplier", mode="before")(
        _optional_decimal
    )

    @field_validator("contract_multiplier")
    @classmethod
    def _positive_multiplier(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value <= 0:
            raise ValueError("option contract multiplier must be positive")
        return value


class Instrument(BrokerageModel):
    symbol: str = Field(min_length=1)
    asset_type: AssetType = AssetType.UNKNOWN
    description: str | None = None
    currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    cusip: str | None = None
    option: OptionContract | None = None
    unknown_fields: tuple[str, ...] = ()

    @field_validator("asset_type", mode="before")
    @classmethod
    def _unknown_asset_type(cls, value: Any) -> Any:
        if value is None:
            return AssetType.UNKNOWN
        value = str(value).strip().lower()
        return {
            "equity": AssetType.EQUITY,
            "etf": AssetType.ETF,
            "option": AssetType.OPTION,
            "cash": AssetType.CASH,
            "mutual fund": AssetType.MUTUAL_FUND,
            "mutual_fund": AssetType.MUTUAL_FUND,
            "fixed income": AssetType.FIXED_INCOME,
            "fixed_income": AssetType.FIXED_INCOME,
        }.get(value, AssetType.UNKNOWN)


class Account(BrokerageModel):
    account_id: str = Field(min_length=1)
    alias: str = Field(min_length=1)
    account_type: AccountType = AccountType.UNKNOWN
    base_currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    balances: tuple[CashBalances, ...] = ()
    source_account_type: str | None = None
    unknown_fields: tuple[str, ...] = ()


class Position(BrokerageModel):
    position_id: str
    account_id: str
    instrument: Instrument
    quantity: Decimal
    side: PositionSide = PositionSide.UNKNOWN
    average_price: CurrencyAmount | None = None
    market_value: CurrencyAmount | None = None
    cost_basis: CurrencyAmount | None = None
    as_of: datetime | None = None
    source_position_id: str | None = None
    unknown_fields: tuple[str, ...] = ()

    _parse_quantity = field_validator("quantity", mode="before")(_decimal)
    _parse_as_of = field_validator("as_of")(_aware_datetime)


class Fill(BrokerageModel):
    execution_id: str
    account_id: str
    order_id: str | None = None
    instrument: Instrument | None = None
    side: OrderSide = OrderSide.UNKNOWN
    quantity: Decimal
    price: CurrencyAmount | None = None
    amount: CurrencyAmount | None = None
    fee: CurrencyAmount | None = None
    executed_at: datetime | None = None
    attribution_status: str = "unattributed"
    source_transaction_id: str | None = None
    unknown_fields: tuple[str, ...] = ()

    _parse_quantity = field_validator("quantity", mode="before")(_decimal)
    _parse_executed_at = field_validator("executed_at")(_aware_datetime)


class Transaction(BrokerageModel):
    """A broker transaction preserved independently from order evidence."""

    transaction_id: str
    account_id: str
    transaction_type: str = "unknown"
    transaction_subtype: str | None = None
    instrument: Instrument | None = None
    quantity: Decimal | None = None
    price: CurrencyAmount | None = None
    net_amount: CurrencyAmount | None = None
    transaction_time: datetime | None = None
    order_id: str | None = None
    related_transaction_id: str | None = None
    attribution_status: str = "unattributed"
    unknown_fields: tuple[str, ...] = ()

    _parse_quantity = field_validator("quantity", mode="before")(_optional_decimal)
    _parse_transaction_time = field_validator("transaction_time")(_aware_datetime)


class Order(BrokerageModel):
    order_id: str
    account_id: str
    instrument: Instrument | None = None
    side: OrderSide = OrderSide.UNKNOWN
    order_type: str | None = None
    status: OrderStatus = OrderStatus.UNKNOWN
    raw_status: str | None = None
    requested_quantity: Decimal | None = None
    filled_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    limit_price: CurrencyAmount | None = None
    entered_at: datetime | None = None
    updated_at: datetime | None = None
    replaces_order_id: str | None = None
    replaced_by_order_id: str | None = None
    child_order_ids: tuple[str, ...] = ()
    is_open: bool = False
    unknown_fields: tuple[str, ...] = ()

    _parse_decimals = field_validator(
        "requested_quantity", "filled_quantity", "remaining_quantity", mode="before"
    )(_optional_decimal)
    _parse_entered_at = field_validator("entered_at", "updated_at")(_aware_datetime)


class Quote(BrokerageModel):
    instrument: Instrument
    bid: CurrencyAmount | None = None
    ask: CurrencyAmount | None = None
    last: CurrencyAmount | None = None
    quote_time: datetime | None = None
    stale_after_seconds: int = Field(default=900, ge=0)
    is_stale: bool = False
    unknown_fields: tuple[str, ...] = ()

    _parse_quote_time = field_validator("quote_time")(_aware_datetime)


class Coverage(BrokerageModel):
    sections: tuple[str, ...] = ()
    start: datetime | None = None
    end: datetime | None = None
    complete: bool = False
    truncated: bool = False
    next_cursor: str | None = None
    source_as_of: datetime | None = None

    _parse_dates = field_validator("start", "end", "source_as_of")(_aware_datetime)


class SyncAttempt(BrokerageModel):
    attempt_id: str
    account_id: str
    status: SyncStatus
    started_at: datetime
    finished_at: datetime
    failure_sections: tuple[str, ...] = ()
    error_code: str | None = None
    error_message: str | None = None
    batch_id: str | None = None

    _parse_dates = field_validator("started_at", "finished_at")(_aware_datetime)


class SyncState(BrokerageModel):
    account_id: str
    last_success_snapshot_id: str | None = None
    last_success_at: datetime | None = None
    last_attempt_id: str | None = None
    last_attempt_status: SyncStatus | None = None
    last_attempt_at: datetime | None = None
    last_failure_sections: tuple[str, ...] = ()
    last_error_code: str | None = None
    last_error_message: str | None = None
    cursor: str | None = None
    # Immutable transaction/fill facts discovered outside the current
    # published observation.  A non-empty set keeps downstream budget and
    # proposal generation blocked until a newer complete snapshot includes
    # every item and can be rebuilt from the complete evidence set.
    pending_fact_ids: tuple[str, ...] = ()
    complete: bool = False

    _parse_dates = field_validator("last_success_at", "last_attempt_at")(_aware_datetime)


class AccountSnapshot(BrokerageModel):
    snapshot_id: str
    account: Account
    positions: tuple[Position, ...] = ()
    orders: tuple[Order, ...] = ()
    transactions: tuple[Transaction, ...] = ()
    fills: tuple[Fill, ...] = ()
    quotes: tuple[Quote, ...] = ()
    business_time: datetime | None = None
    received_at: datetime
    sync_batch_id: str
    mapping_version: str
    coverage: Coverage
    cursor: str | None = None
    complete: bool = False
    conflicts: tuple[str, ...] = ()
    non_executable: Literal[True] = True

    _parse_dates = field_validator("business_time", "received_at")(_aware_datetime)

    REQUIRED_SECTIONS: ClassVar[tuple[str, ...]] = (
        "account",
        "positions",
        "orders",
        "transactions",
    )


class AccountData(BrokerageModel):
    """Normalized, pre-publication data returned by ``SchwabAdapter``."""

    account: Account
    positions: tuple[Position, ...] = ()
    orders: tuple[Order, ...] = ()
    transactions: tuple[Transaction, ...] = ()
    fills: tuple[Fill, ...] = ()
    quotes: tuple[Quote, ...] = ()
    business_time: datetime | None = None
    received_at: datetime
    mapping_version: str
    coverage: Coverage
    conflicts: tuple[str, ...] = ()
    cursor: str | None = None

    _parse_dates = field_validator("business_time", "received_at")(_aware_datetime)


__all__ = [
    "Account",
    "AccountData",
    "AccountSnapshot",
    "AccountType",
    "AssetType",
    "BrokerageModel",
    "CashBalances",
    "Coverage",
    "CurrencyAmount",
    "Fill",
    "Instrument",
    "OptionContract",
    "Order",
    "OrderSide",
    "OrderStatus",
    "Position",
    "PositionSide",
    "Quote",
    "SyncAttempt",
    "SyncState",
    "SyncStatus",
    "Transaction",
    "UnknownValue",
]
