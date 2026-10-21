from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time
from typing import Protocol

from pydantic import Field

from ..models import DataModel, Market


class MarketSession(DataModel):
    trading_date: date
    market: Market
    open_time: time
    close_time: time
    timezone: str = Field(min_length=1)


class MarketCalendarProvider(Protocol):
    """Exchange-independent source of market-session rules."""

    def sessions(self, market: Market, start: date, end: date) -> Iterable[MarketSession]: ...

    def is_trading_day(self, market: Market, trading_date: date) -> bool: ...

    def session_open(self, market: Market, trading_date: date) -> datetime | None: ...

    def session_close(self, market: Market, trading_date: date) -> datetime | None: ...


# Compatibility alias for callers written against the Phase 1.6 contract.
MarketCalendar = MarketCalendarProvider
