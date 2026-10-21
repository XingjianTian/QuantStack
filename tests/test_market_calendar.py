from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from quant_stack.data.calendars import SyntheticMarketCalendar


@pytest.mark.parametrize(
    ("market", "open_time", "close_time", "timezone"),
    [
        ("CN", time(9, 30), time(15, 0), "Asia/Shanghai"),
        ("HK", time(9, 30), time(16, 0), "Asia/Hong_Kong"),
        ("US", time(9, 30), time(16, 0), "America/New_York"),
    ],
)
def test_synthetic_calendar_exposes_canonical_market_sessions(
    market: str,
    open_time: time,
    close_time: time,
    timezone: str,
) -> None:
    calendar = SyntheticMarketCalendar()
    sessions = list(calendar.sessions(market, date(2026, 7, 24), date(2026, 7, 27)))
    assert [session.trading_date for session in sessions] == [
        date(2026, 7, 24),
        date(2026, 7, 27),
    ]
    assert all(session.market == market for session in sessions)
    assert all(session.open_time == open_time for session in sessions)
    assert all(session.close_time == close_time for session in sessions)
    assert all(session.timezone == timezone for session in sessions)


def test_synthetic_calendar_rejects_reversed_window() -> None:
    with pytest.raises(ValueError, match="end must be on or after start"):
        list(SyntheticMarketCalendar().sessions("US", date(2026, 7, 28), date(2026, 7, 27)))


@pytest.mark.parametrize("market", ["CN", "HK", "US"])
def test_synthetic_calendar_exposes_timezone_aware_open_and_close(market: str) -> None:
    calendar = SyntheticMarketCalendar()
    trading_date = date(2026, 7, 27)
    session = next(calendar.sessions(market, trading_date, trading_date))

    assert calendar.session_open(market, trading_date) == datetime.combine(
        trading_date,
        session.open_time,
        tzinfo=ZoneInfo(session.timezone),
    )
    assert calendar.session_close(market, trading_date) == datetime.combine(
        trading_date,
        session.close_time,
        tzinfo=ZoneInfo(session.timezone),
    )
    assert calendar.is_trading_day(market, trading_date)
    assert calendar.session_open(market, date(2026, 7, 26)) is None
    assert calendar.session_close(market, date(2026, 7, 26)) is None
