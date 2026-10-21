from __future__ import annotations

from decimal import Decimal

import pytest

from quant_stack.brokerage.contracts import (
    AssetType,
    Instrument,
    OptionContract,
    Position,
    PositionSide,
    SyncState,
    SyncStatus,
)
from quant_stack.investing.budgets import build_ledger
from quant_stack.investing.plans import EquityLayer, InvestmentPlan
from quant_stack.investing.proposals import (
    ProposalStatus,
    ReviewStatus,
    generate_proposals,
    review_proposal,
)

from .conftest import NOW, make_open_order, make_snapshot


def _plan(
    *,
    symbol: str = "QXEN",
    budget: str = "2000.00",
    total_position_limit: str = "100.00",
    version: int = 1,
    require_review: bool = False,
) -> InvestmentPlan:
    return InvestmentPlan(
        account_id="fictional-account",
        version=version,
        equity_layers=(
            EquityLayer(
                layer_id="layer-1",
                account_id="fictional-account",
                symbol=symbol,
                budget=budget,
                trigger_price="101.00",
                total_position_limit=total_position_limit,
                sequence=1,
                require_review=require_review,
            ),
        ),
    )


def _quote(symbol: str = "QXEN") -> object:
    # Keep the fixture construction aligned with ``make_snapshot`` while
    # making the intended proposal price explicit in these safety cases.
    return make_snapshot(symbol=symbol).quotes[0]


def test_fractional_position_and_known_short_put_share_position_cap() -> None:
    equity = Instrument(symbol="QXEN", asset_type=AssetType.EQUITY, currency="USD")
    short_put = Instrument(
        symbol="QXEN  261016P00100000",
        asset_type=AssetType.OPTION,
        currency="USD",
        option=OptionContract(
            underlying_symbol="QXEN",
            put_call="PUT",
            strike_price="100.00",
            contract_multiplier="100",
            deliverable="100 shares of QXEN",
            contract_symbol="QXEN  261016P00100000",
        ),
    )
    snapshot = make_snapshot(
        cash="5000.00",
        positions=(
            Position(
                position_id="fractional-qxen",
                account_id="fictional-account",
                instrument=equity,
                quantity="0.5",
                side=PositionSide.LONG,
                as_of=NOW,
            ),
            Position(
                position_id="short-put-qxen",
                account_id="fictional-account",
                instrument=short_put,
                quantity="1",
                side=PositionSide.SHORT,
                as_of=NOW,
            ),
        ),
        quotes=(_quote(),),
    )
    plan = _plan(total_position_limit="100.00")

    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    item = batch.items[0]
    assert item.quantity == Decimal("0")
    assert item.status is ProposalStatus.INSUFFICIENT_BUDGET
    assert "OPTION_DELIVERY_CAP" in item.reasons


@pytest.mark.parametrize(
    ("unknown_fields", "put_call"),
    [
        (("putCall",), None),
        (("underlyingSymbol",), "PUT"),
    ],
)
def test_unknown_option_assignment_fields_block_fail_closed(
    unknown_fields: tuple[str, ...], put_call: str | None
) -> None:
    option = Instrument(
        symbol="QXEN  261016P00100000",
        asset_type=AssetType.OPTION,
        currency="USD",
        option=OptionContract(
            # The required field remains populated so this is a normalized
            # contract; ``unknown_fields`` records that the source did not
            # provide a trustworthy value for the corresponding field.
            underlying_symbol="QXEN",
            put_call=put_call,
            contract_multiplier="100",
            deliverable="100 shares of QXEN",
            unknown_fields=unknown_fields,
        ),
    )
    position = Position(
        position_id="uncertain-short-put",
        account_id="fictional-account",
        instrument=option,
        quantity="1",
        side=PositionSide.SHORT,
        as_of=NOW,
    )
    snapshot = make_snapshot(cash="5000.00", positions=(position,))
    plan = _plan(total_position_limit="1000.00")

    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert "UNKNOWN_OPTION_OBLIGATION" in batch.blocking_reasons
    assert batch.items[0].quantity == Decimal("0")


def test_missing_option_deliverable_blocks_fail_closed() -> None:
    option = Instrument(
        symbol="QXEN  261016P00100000",
        asset_type=AssetType.OPTION,
        currency="USD",
        option=OptionContract(
            underlying_symbol="QXEN",
            put_call="PUT",
            strike_price="100.00",
            contract_multiplier="100",
            contract_symbol="QXEN  261016P00100000",
        ),
    )
    position = Position(
        position_id="short-put-without-deliverable",
        account_id="fictional-account",
        instrument=option,
        quantity="1",
        side=PositionSide.SHORT,
        as_of=NOW,
    )
    snapshot = make_snapshot(cash="5000.00", positions=(position,))
    plan = _plan(total_position_limit="1000.00")

    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert "UNKNOWN_OPTION_OBLIGATION" in batch.blocking_reasons
    assert batch.items[0].quantity == Decimal("0")


def test_available_cash_is_used_and_open_order_occupation_is_subtracted_once() -> None:
    snapshot = make_snapshot(
        cash="1000.00",
        reservation_included=False,
        orders=(make_open_order(symbol="ZETA", remaining="2", price="101.00"),),
    )
    balance = snapshot.account.balances[0].model_copy(update={"buying_power": Decimal("5000.00")})
    snapshot = snapshot.model_copy(
        update={"account": snapshot.account.model_copy(update={"balances": (balance,)})}
    )
    plan = _plan(budget="2000.00")

    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    item = batch.items[0]
    # Available cash is $1,000.  The unrelated open BUY order occupies $202,
    # leaving $798, which buys seven shares at the $101 assumed limit.
    assert batch.available_cash == Decimal("1000.00")
    assert batch.open_order_occupation == Decimal("202.00")
    assert item.quantity == Decimal("7")
    assert item.maximum_cash_use == Decimal("707.00")
    assert item.quantity != Decimal("9")  # buying power must not be treated as cash


def test_plan_change_and_failed_sync_reset_reviewability() -> None:
    snapshot = make_snapshot(cash="5000.00")
    plan = _plan(require_review=True)
    ledger = build_ledger(snapshot, plan)
    good_sync = SyncState(
        account_id=snapshot.account.account_id,
        last_success_snapshot_id=snapshot.snapshot_id,
        last_success_at=NOW,
        last_attempt_status=SyncStatus.SUCCEEDED,
        last_attempt_at=NOW,
        complete=True,
    )
    first = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=good_sync,
    )
    reviewed = review_proposal(
        first,
        reviewer="fixture-reviewer",
        expected_input_fingerprint=first.input_fingerprint,
        at=NOW,
    )
    assert reviewed.review_status is ReviewStatus.REVIEWED

    changed_plan = plan.model_copy(update={"version": 2})
    changed_ledger = build_ledger(snapshot, changed_plan)
    after_plan_change = generate_proposals(
        snapshot,
        changed_plan,
        changed_ledger,
        as_of=NOW,
        sync_state=good_sync,
        previous=reviewed,
    )
    assert after_plan_change.batch_id != reviewed.batch_id
    assert after_plan_change.replaced_batch_id == reviewed.batch_id
    assert after_plan_change.review_status is ReviewStatus.PENDING

    failed_sync = good_sync.model_copy(
        update={
            "complete": False,
            "last_attempt_status": SyncStatus.FAILED,
            "last_error_code": "fixture-sync-failed",
        }
    )
    after_sync_failure = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=failed_sync,
        previous=reviewed,
    )
    assert after_sync_failure.status is ProposalStatus.BLOCKED
    assert "SYNC_FAILED" in after_sync_failure.blocking_reasons
    assert after_sync_failure.review_status is ReviewStatus.PENDING
