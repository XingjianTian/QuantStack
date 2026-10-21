from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from ..interfaces import MarketSession
from ..models import Market

_SESSIONS = {
    "CN": (time(9, 30), time(15, 0), "Asia/Shanghai"),
    "HK": (time(9, 30), time(16, 0), "Asia/Hong_Kong"),
    "US": (time(9, 30), time(16, 0), "America/New_York"),
}


class SyntheticMarketCalendar:
    """Deterministic weekday calendar; it intentionally models no exchange holidays."""

    def sessions(self, market: Market, start: date, end: date) -> Iterable[MarketSession]:
        if end < start:
            raise ValueError("end must be on or after start")
        open_time, close_time, timezone = _SESSIONS[market]
        current = start
        while current <= end:
            if self.is_trading_day(market, current):
                yield MarketSession(
                    trading_date=current,
                    market=market,
                    open_time=open_time,
                    close_time=close_time,
                    timezone=timezone,
                )
            current += timedelta(days=1)

    def is_trading_day(self, market: Market, trading_date: date) -> bool:
        _SESSIONS[market]
        return trading_date.weekday() < 5

    def session_open(self, market: Market, trading_date: date) -> datetime | None:
        session = self._session(market, trading_date)
        if session is None:
            return None
        return datetime.combine(
            session.trading_date,
            session.open_time,
            tzinfo=ZoneInfo(session.timezone),
        )

    def session_close(self, market: Market, trading_date: date) -> datetime | None:
        session = self._session(market, trading_date)
        if session is None:
            return None
        return datetime.combine(
            session.trading_date,
            session.close_time,
            tzinfo=ZoneInfo(session.timezone),
        )

    def _session(self, market: Market, trading_date: date) -> MarketSession | None:
        if not self.is_trading_day(market, trading_date):
            return None
        open_time, close_time, timezone = _SESSIONS[market]
        return MarketSession(
            trading_date=trading_date,
            market=market,
            open_time=open_time,
            close_time=close_time,
            timezone=timezone,
        )
