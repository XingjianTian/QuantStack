from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

from ..interfaces import MarketSession
from ..models import Market

USVenue = Literal["XNYS", "XNAS"]


class USExchangeCalendar:
    """XNYS/XNAS sessions backed by the pinned ``exchange_calendars`` release."""

    timezone = "America/New_York"

    def __init__(self, venue: USVenue) -> None:
        if venue not in {"XNYS", "XNAS"}:
            raise ValueError(f"Unsupported US venue: {venue}")
        try:
            import exchange_calendars as xcals
        except ImportError as exc:
            raise RuntimeError(
                "Install QuantStack with the research extra for US exchange calendars"
            ) from exc
        self.venue = venue
        self._calendar = xcals.get_calendar(venue)
        self._timezone = ZoneInfo(self.timezone)

    @staticmethod
    def _require_market(market: Market) -> None:
        if market != "US":
            raise ValueError("USExchangeCalendar supports only the US market")

    def sessions(self, market: Market, start: date, end: date) -> Iterable[MarketSession]:
        self._require_market(market)
        if end < start:
            raise ValueError("end must be on or after start")
        for label in self._calendar.sessions_in_range(start.isoformat(), end.isoformat()):
            trading_date = label.date()
            yield MarketSession(
                trading_date=trading_date,
                market="US",
                open_time=self._local_datetime("open", trading_date).time(),
                close_time=self._local_datetime("close", trading_date).time(),
                timezone=self.timezone,
            )

    def is_trading_day(self, market: Market, trading_date: date) -> bool:
        self._require_market(market)
        return bool(self._calendar.is_session(trading_date.isoformat()))

    def session_open(self, market: Market, trading_date: date) -> datetime | None:
        self._require_market(market)
        if not self.is_trading_day(market, trading_date):
            return None
        return self._local_datetime("open", trading_date)

    def session_close(self, market: Market, trading_date: date) -> datetime | None:
        self._require_market(market)
        if not self.is_trading_day(market, trading_date):
            return None
        return self._local_datetime("close", trading_date)

    def _local_datetime(self, boundary: Literal["open", "close"], trading_date: date) -> datetime:
        method = self._calendar.session_open if boundary == "open" else self._calendar.session_close
        timestamp = method(trading_date.isoformat())
        return timestamp.to_pydatetime().astimezone(self._timezone)
