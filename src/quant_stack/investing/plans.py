"""Provider-neutral investment plan contracts.

The investing package deliberately knows nothing about a broker SDK.  Plans are
small, immutable value objects and all money-like values are represented by
``Decimal``.  The schedule helpers use an explicit IANA timezone so that a
period is decided from the local calendar rather than from the machine's
timezone.
"""

from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, ClassVar, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _decimal(value: Any) -> Decimal:
    """Parse exact decimal input and reject binary floats."""

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


def _money(value: Any, *, allow_zero: bool = True) -> Decimal:
    result = _decimal(value)
    if result < 0 or (not allow_zero and result == 0):
        raise ValueError("money values must be non-negative")
    return result


class PlanModel(BaseModel):
    """Strict immutable model used for persisted plan configuration."""

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


class Period(StrEnum):
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class CatchUpPolicy(StrEnum):
    ROLL = "roll"
    SKIP = "skip"


class UnsupportedScheduleError(ValueError):
    """A schedule mode whose cross-period allowance is not yet evidenced."""


class ETFPlan(PlanModel):
    """A calendar-driven whole-share ETF budget.

    ``execution_day`` is a weekday number (Monday=0) for weekly plans and a
    day-of-month for monthly plans.  Monthly day 31 uses the final day of a
    shorter month.  The annual cap is applied independently to each calendar
    year; it is never inferred from account cash or buying power.
    """

    plan_id: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)
    account_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    amount_per_period: Decimal
    period: Period = Period.MONTHLY
    start_date: date
    timezone: str
    execution_day: int = Field(ge=0, le=31)
    annual_cap: Decimal | None = None
    whole_shares: Literal[True] = True
    catch_up: CatchUpPolicy = CatchUpPolicy.ROLL
    fee_per_order: Decimal = Decimal("0")
    cash_reserve: Decimal = Decimal("0")
    limit_price: Decimal | None = None
    priority: int = 1000

    _decimal_fields: ClassVar[tuple[str, ...]] = (
        "amount_per_period",
        "annual_cap",
        "fee_per_order",
        "cash_reserve",
        "limit_price",
    )

    @model_validator(mode="before")
    @classmethod
    def _parse_decimals(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        for name in cls._decimal_fields:
            if values.get(name) is not None:
                values[name] = _decimal(values[name])
        return values

    @model_validator(mode="after")
    def _validate_schedule(self) -> ETFPlan:
        if self.amount_per_period <= 0:
            raise ValueError("amount_per_period must be positive")
        if self.annual_cap is not None and self.annual_cap <= 0:
            raise ValueError("annual_cap must be positive when provided")
        if self.fee_per_order < 0 or self.cash_reserve < 0:
            raise ValueError("fees and cash reserve must be non-negative")
        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError("limit_price must be positive")
        if self.whole_shares is not True:
            raise ValueError("ETF proposals are whole-share only")
        if self.period is Period.WEEKLY and self.execution_day > 6:
            raise ValueError("weekly execution_day must be 0 (Monday) through 6 (Sunday)")
        if self.period is Period.MONTHLY and not 1 <= self.execution_day <= 31:
            raise ValueError("monthly execution_day must be 1 through 31")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown IANA timezone: {self.timezone}") from exc
        return self

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def local_date(self, at: datetime) -> date:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("schedule timestamps must include a timezone")
        return at.astimezone(ZoneInfo(self.timezone)).date()

    def _period_date(self, year: int, month: int | None = None) -> date:
        if self.period is Period.WEEKLY:
            raise ValueError("weekly schedules do not have a month period date")
        assert month is not None
        return date(year, month, min(self.execution_day, monthrange(year, month)[1]))

    def due_period_dates(
        self,
        at: datetime,
        *,
        catch_up: CatchUpPolicy | None = None,
    ) -> tuple[date, ...]:
        """Return execution dates through ``at`` in the configured timezone.

        The result starts at the first calendar period containing ``start_date``
        and only includes periods whose execution date has occurred.  ``SKIP``
        is explicitly unsupported until a persisted period-allowance watermark
        can distinguish an intentionally skipped period from a missing run.
        """

        selected_policy = catch_up or self.catch_up
        if selected_policy is CatchUpPolicy.SKIP:
            raise UnsupportedScheduleError(
                "ETF catch_up=skip is unsupported until a persisted period-allowance "
                "watermark is available"
            )
        local = self.local_date(at)
        if local < self.start_date:
            return ()
        dates: list[date] = []
        if self.period is Period.MONTHLY:
            cursor = date(self.start_date.year, self.start_date.month, 1)
            while cursor <= local:
                scheduled = self._period_date(cursor.year, cursor.month)
                if scheduled >= self.start_date and scheduled <= local:
                    dates.append(scheduled)
                if cursor.month == 12:
                    cursor = date(cursor.year + 1, 1, 1)
                else:
                    cursor = date(cursor.year, cursor.month + 1, 1)
        else:
            first_monday = self.start_date - timedelta(days=self.start_date.weekday())
            cursor = first_monday
            while cursor <= local:
                scheduled = cursor + timedelta(days=self.execution_day)
                if scheduled >= self.start_date and scheduled <= local:
                    dates.append(scheduled)
                cursor += timedelta(days=7)
        return tuple(dates)

    def due_since(
        self,
        at: datetime,
        *,
        after: date | None = None,
        catch_up: CatchUpPolicy | None = None,
    ) -> tuple[date, ...]:
        dates = self.due_period_dates(at, catch_up=catch_up)
        if after is None:
            return dates
        return tuple(item for item in dates if item > after)

    def accrued_amount(self, at: datetime, *, after: date | None = None) -> Decimal:
        """Calculate accrued budget without mutating a ledger or account."""

        dates = self.due_since(at, after=after)
        if not dates:
            return Decimal("0")
        total = Decimal("0")
        by_year: dict[int, Decimal] = {}
        for period_date in dates:
            by_year[period_date.year] = (
                by_year.get(period_date.year, Decimal("0")) + self.amount_per_period
            )
        for year in sorted(by_year):
            amount = by_year[year]
            if self.annual_cap is not None:
                amount = min(amount, self.annual_cap)
            total += amount
        return total

    @property
    def accrued_budget(self) -> Any:
        """Backward-friendly callable property marker.

        The actual calculation requires a clock, so use ``accrued_amount(at)``.
        This property exists only to make accidental mutation of a plan obvious
        rather than silently returning an account-dependent number.
        """

        return self.accrued_amount


class EquityLayer(PlanModel):
    """A one-time, price-triggered budget layer for a fictional equity."""

    layer_id: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)
    account_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    budget: Decimal
    trigger_price: Decimal
    total_position_limit: Decimal
    sequence: int = Field(default=1, ge=0)
    require_review: bool = True
    fee_per_order: Decimal = Decimal("0")
    cash_reserve: Decimal = Decimal("0")
    limit_price: Decimal | None = None
    enabled: bool = True

    @model_validator(mode="before")
    @classmethod
    def _parse_decimals(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        for name in (
            "budget",
            "trigger_price",
            "total_position_limit",
            "fee_per_order",
            "cash_reserve",
            "limit_price",
        ):
            if values.get(name) is not None:
                values[name] = _decimal(values[name])
        return values

    @model_validator(mode="after")
    def _validate_layer(self) -> EquityLayer:
        if self.budget <= 0:
            raise ValueError("equity layer budget must be positive")
        if self.trigger_price <= 0 or (self.limit_price is not None and self.limit_price <= 0):
            raise ValueError("equity layer prices must be positive")
        if self.total_position_limit <= 0:
            raise ValueError("total_position_limit must be positive")
        if self.fee_per_order < 0 or self.cash_reserve < 0:
            raise ValueError("fees and cash reserve must be non-negative")
        return self

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class InvestmentPlan(PlanModel):
    """Versioned account plan containing ETF schedules and equity layers."""

    account_id: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)
    etf_plans: tuple[ETFPlan, ...] = ()
    equity_layers: tuple[EquityLayer, ...] = ()
    created_at: datetime | None = None
    enabled: bool = True
    notes: str | None = None

    @model_validator(mode="after")
    def _validate_children(self) -> InvestmentPlan:
        if not self.etf_plans and not self.equity_layers:
            raise ValueError("investment plan must contain at least one rule")
        ids: set[str] = set()
        for item in (*self.etf_plans, *self.equity_layers):
            if item.account_id != self.account_id:
                raise ValueError(f"rule {self.rule_id(item)} belongs to another account")
            rule_id = self.rule_id(item)
            if rule_id in ids:
                raise ValueError(f"duplicate investment rule id: {rule_id}")
            ids.add(rule_id)
        if self.created_at is not None and (
            self.created_at.tzinfo is None or self.created_at.utcoffset() is None
        ):
            raise ValueError("created_at must include a timezone")
        return self

    @staticmethod
    def rule_id(rule: ETFPlan | EquityLayer) -> str:
        return rule.plan_id if isinstance(rule, ETFPlan) else rule.layer_id

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def rule(self, rule_id: str) -> ETFPlan | EquityLayer:
        for item in (*self.etf_plans, *self.equity_layers):
            if self.rule_id(item) == rule_id:
                return item
        raise KeyError(rule_id)


def plan_fingerprint(plan: InvestmentPlan | ETFPlan | EquityLayer) -> str:
    """Return a stable version-independent content fingerprint."""

    return hashlib.sha256(plan.canonical_json().encode("utf-8")).hexdigest()


__all__ = [
    "CatchUpPolicy",
    "ETFPlan",
    "EquityLayer",
    "InvestmentPlan",
    "Period",
    "PlanModel",
    "plan_fingerprint",
    "UnsupportedScheduleError",
]
