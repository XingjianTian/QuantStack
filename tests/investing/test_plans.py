from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from quant_stack.investing.plans import (
    CatchUpPolicy,
    ETFPlan,
    Period,
    UnsupportedScheduleError,
)


def test_monthly_schedule_uses_iana_timezone_and_year_cap() -> None:
    plan = ETFPlan(
        plan_id="fictional-etf",
        account_id="fictional-account",
        symbol="QXEN",
        amount_per_period="100.00",
        period=Period.MONTHLY,
        start_date=date(2026, 1, 31),
        timezone="America/New_York",
        execution_day=31,
        annual_cap="250.00",
    )

    as_of = datetime(2026, 4, 1, 1, 0, tzinfo=UTC)
    assert plan.due_period_dates(as_of) == (date(2026, 1, 31), date(2026, 2, 28), date(2026, 3, 31))
    assert plan.accrued_amount(as_of) == Decimal("250.00")


def test_start_boundary_and_skip_catch_up_are_explicit() -> None:
    plan = ETFPlan(
        plan_id="fictional-weekly",
        account_id="fictional-account",
        symbol="QXEN",
        amount_per_period="25",
        period=Period.WEEKLY,
        start_date=date(2026, 10, 14),
        timezone="UTC",
        execution_day=0,
        catch_up=CatchUpPolicy.ROLL,
    )
    assert plan.due_period_dates(datetime(2026, 10, 13, tzinfo=UTC)) == ()
    with pytest.raises(UnsupportedScheduleError, match="catch_up=skip"):
        plan.due_period_dates(datetime(2026, 11, 1, tzinfo=UTC), catch_up=CatchUpPolicy.SKIP)

    skip_plan = plan.model_copy(update={"catch_up": CatchUpPolicy.SKIP})
    with pytest.raises(UnsupportedScheduleError, match="watermark"):
        skip_plan.accrued_amount(datetime(2026, 11, 1, tzinfo=UTC))


def test_float_money_is_rejected() -> None:
    with pytest.raises(TypeError):
        ETFPlan(
            plan_id="fictional",
            account_id="fictional-account",
            symbol="QXEN",
            amount_per_period=1.1,
            start_date=date(2026, 1, 1),
            timezone="UTC",
            execution_day=1,
        )
