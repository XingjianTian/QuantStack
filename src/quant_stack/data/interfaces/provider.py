from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Protocol, TypedDict

from ..models import Instrument, Market


class RawBar(TypedDict, total=False):
    """Provider-native bar boundary; adapters may add fields, normalizers may not."""

    symbol: str
    instrument_id: str
    date: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    currency: str


class DataProvider(Protocol):
    """Read-only provider contract shared by tushare, akshare, yfinance, polygon and IBKR."""

    @property
    def name(self) -> str: ...

    @property
    def source_url(self) -> str: ...

    @property
    def license_scope(self) -> str: ...

    @property
    def available_at_policy(self) -> str: ...

    def list_instruments(
        self, market: Market, symbols: Sequence[str] | None = None
    ) -> Iterable[Instrument]: ...

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]: ...
