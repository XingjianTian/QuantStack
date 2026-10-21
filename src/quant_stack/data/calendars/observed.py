from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from zoneinfo import ZoneInfo

from ..interfaces import MarketSession
from ..models import CalendarEvidence, Market


class CalendarEvidenceError(ValueError):
    """Raised when a date is outside explicit live-session evidence."""


class UnknownMarketSessionError(CalendarEvidenceError):
    """The bounded evidence neither confirms a session nor a non-session."""


class PartialMarketSessionError(CalendarEvidenceError):
    """A confirmed session has not closed at the evidence observation time."""


class ObservedSessionCalendar:
    """Fail-closed calendar for a small range backed by explicit evidence."""

    def __init__(self, evidence: CalendarEvidence) -> None:
        self.evidence = evidence
        self._sessions = frozenset(evidence.confirmed_sessions)
        self._non_sessions = frozenset(evidence.confirmed_non_sessions)
        self._timezone = ZoneInfo(evidence.timezone)

    @property
    def verified_for_live_publication(self) -> bool:
        """Provider-observed rows never substitute for an official exchange calendar."""

        return (
            self.evidence.schema_version == "1.1.0"
            and self.evidence.source_kind == "exchange_announcement"
            and bool(self.evidence.exchanges)
        )

    def _require_market(self, market: Market) -> None:
        if market != self.evidence.market:
            raise CalendarEvidenceError("calendar evidence does not cover this market")

    def _require_supported(self, market: Market, trading_date: date) -> None:
        self._require_market(market)
        if not self.evidence.supported_start <= trading_date <= self.evidence.supported_end:
            raise UnknownMarketSessionError("date is outside calendar evidence support")

    def sessions(self, market: Market, start: date, end: date) -> Iterable[MarketSession]:
        self._require_market(market)
        if end < start:
            raise ValueError("end must be on or after start")
        self._require_supported(market, start)
        self._require_supported(market, end)
        unknown = {
            day
            for day in _date_range(start, end)
            if day not in self._sessions and day not in self._non_sessions
        }
        if unknown:
            raise UnknownMarketSessionError("calendar evidence has unknown dates in range")
        for day in _date_range(start, end):
            if day in self._sessions:
                yield self._session(market, day)

    def is_trading_day(self, market: Market, trading_date: date) -> bool:
        self._require_supported(market, trading_date)
        if trading_date in self._sessions:
            return True
        if trading_date in self._non_sessions:
            return False
        raise UnknownMarketSessionError("calendar evidence does not classify this date")

    def session_open(self, market: Market, trading_date: date) -> datetime | None:
        if not self.is_trading_day(market, trading_date):
            return None
        return datetime.combine(
            trading_date, self.evidence.open_time, tzinfo=self._timezone
        )

    def session_close(self, market: Market, trading_date: date) -> datetime | None:
        if not self.is_trading_day(market, trading_date):
            return None
        close = datetime.combine(
            trading_date, self.evidence.close_time, tzinfo=self._timezone
        )
        if self.evidence.observed_at.astimezone(self._timezone) < close:
            raise PartialMarketSessionError("confirmed session has not closed")
        return close

    def _session(self, market: Market, trading_date: date) -> MarketSession:
        close = self.session_close(market, trading_date)
        assert close is not None
        return MarketSession(
            trading_date=trading_date,
            market=market,
            open_time=self.evidence.open_time,
            close_time=self.evidence.close_time,
            timezone=self.evidence.timezone,
        )


def _date_range(start: date, end: date) -> Iterable[date]:
    from datetime import timedelta

    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
