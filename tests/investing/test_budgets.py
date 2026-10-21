import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from quant_stack.brokerage.contracts import (
    AssetType,
    CurrencyAmount,
    Instrument,
    OrderSide,
    OrderStatus,
    Position,
    PositionSide,
)
from quant_stack.brokerage.schwab_adapter import SchwabAdapter
from quant_stack.investing.budgets import (
    Attribution,
    AttributionStatus,
    EntryKind,
    LedgerEntry,
    budget_summaries,
    build_ledger,
    order_occupation,
)
from quant_stack.investing.plans import EquityLayer, ETFPlan, InvestmentPlan
from quant_stack.investing.proposals import ProposalStatus, generate_proposals

from .conftest import NOW, make_snapshot, make_trade

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "brokerage" / "ready"


def _plan(account_id: str = "fictional-account") -> InvestmentPlan:
    return InvestmentPlan(
        account_id=account_id,
        etf_plans=(
            ETFPlan(
                plan_id="fictional-etf",
                account_id=account_id,
                symbol="QXEN",
                amount_per_period="100.00",
                start_date="2026-01-01",
                timezone="UTC",
                execution_day=1,
                annual_cap="1000.00",
            ),
        ),
    )


def _two_symbol_etf_plan() -> InvestmentPlan:
    account_id = "fictional-account"
    return InvestmentPlan(
        account_id=account_id,
        etf_plans=(
            _plan().etf_plans[0],
            ETFPlan(
                plan_id="fictional-qzet-etf",
                account_id=account_id,
                symbol="QZET",
                amount_per_period="100.00",
                start_date="2026-01-01",
                timezone="UTC",
                execution_day=1,
                annual_cap="1000.00",
            ),
        ),
    )


def _equity_plan(*, same_symbol: bool = False) -> InvestmentPlan:
    account_id = "fictional-account"
    second_symbol = "QXEN" if same_symbol else "QZET"
    return InvestmentPlan(
        account_id=account_id,
        equity_layers=(
            EquityLayer(
                layer_id="fictional-qxen-layer",
                account_id=account_id,
                symbol="QXEN",
                budget="100.00",
                trigger_price="100.00",
                total_position_limit="10",
            ),
            EquityLayer(
                layer_id="fictional-second-layer",
                account_id=account_id,
                symbol=second_symbol,
                budget="100.00",
                trigger_price="100.00",
                total_position_limit="10",
            ),
        ),
    )


def _position(*, position_id: str, quantity: str, as_of=None, market_value=None) -> Position:
    return Position(
        position_id=position_id,
        account_id="fictional-account",
        instrument=Instrument(symbol="QXEN", asset_type=AssetType.EQUITY, currency="USD"),
        quantity=Decimal(quantity),
        side=PositionSide.LONG,
        as_of=as_of,
        market_value=(
            CurrencyAmount(currency="USD", amount=Decimal(market_value))
            if market_value is not None
            else None
        ),
    )


def _fixture_orders(mutator) -> tuple:
    """Read the fictional raw order fixture through the adapter contract."""

    orders = json.loads((FIXTURE_ROOT / "normal_orders.json").read_text(encoding="utf-8"))
    mutator(orders)
    payloads = {
        "account": json.loads((FIXTURE_ROOT / "normal_account.json").read_text(encoding="utf-8")),
        "orders": orders,
        "transactions": json.loads(
            (FIXTURE_ROOT / "normal_transactions.json").read_text(encoding="utf-8")
        ),
        "coverage": json.loads((FIXTURE_ROOT / "normal_coverage.json").read_text(encoding="utf-8")),
    }
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads=payloads,
        clock=lambda: datetime(2026, 10, 16, tzinfo=UTC),
    )
    try:
        return adapter.read_orders(
            "fictional-account-hash",
            account_id="fictional-account",
            from_entered_datetime=datetime(2026, 10, 1, tzinfo=UTC),
            to_entered_datetime=datetime(2026, 10, 17, tzinfo=UTC),
            currency="USD",
        )
    finally:
        adapter.close()


def _correction(
    transaction,
    *,
    transaction_id: str,
    related_transaction_id: str,
    amount: str = "10.00",
    account_id: str | None = None,
    currency: str = "USD",
):
    return transaction.model_copy(
        update={
            "transaction_id": transaction_id,
            "account_id": account_id or transaction.account_id,
            "transaction_type": "adjustment",
            "transaction_subtype": "FEE_ADJUSTMENT",
            "related_transaction_id": related_transaction_id,
            "instrument": None,
            "quantity": None,
            "price": None,
            "net_amount": CurrencyAmount(currency=currency, amount=Decimal(amount)),
        }
    )


def test_same_execution_in_transaction_and_fill_consumes_once() -> None:
    transaction, fill = make_trade(amount="60.00")
    snapshot = make_snapshot(transactions=(transaction,), fills=(fill,))
    ledger = build_ledger(
        snapshot,
        _plan(),
        attributions={transaction.transaction_id: "fictional-etf"},
    )

    assert ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")
    assert len(ledger.entries) == 1


def test_two_distinct_transactions_same_amount_are_not_merged() -> None:
    first_transaction, first_fill = make_trade(transaction_id="transaction-1", amount="30.00")
    second_transaction, second_fill = make_trade(transaction_id="transaction-2", amount="30.00")
    snapshot = make_snapshot(
        transactions=(first_transaction, second_transaction),
        fills=(first_fill, second_fill),
    )
    ledger = build_ledger(
        snapshot,
        _plan(),
        attributions={
            first_transaction.transaction_id: "fictional-etf",
            second_transaction.transaction_id: "fictional-etf",
        },
    )

    assert ledger.spent_for("fictional-etf") == Decimal("60.00")
    assert len(ledger.entries) == 2


def test_unattributed_manual_trade_is_retained_and_blocks_spend() -> None:
    transaction, fill = make_trade(amount="60.00")
    ledger = build_ledger(make_snapshot(transactions=(transaction,), fills=(fill,)), _plan())

    assert ledger.unattributed_evidence
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("0")
    assert ledger.entries[0].status is AttributionStatus.UNATTRIBUTED


def test_correction_entry_reduces_cost_without_touching_positions() -> None:
    transaction, fill = make_trade(amount="60.00")
    correction = LedgerEntry(
        entry_id="correction-1",
        account_id="fictional-account",
        symbol="QXEN",
        kind=EntryKind.CORRECTION,
        cost="-10.00",
        plan_id="fictional-etf",
        status=AttributionStatus.CORRECTED,
        evidence_id="correction-1",
        correction_of="execution:transaction-1:execution",
        note="fictional fee correction",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _plan(),
        attributions={transaction.transaction_id: "fictional-etf"},
        initial_entries=(correction,),
    )

    assert ledger.spent_for("fictional-etf") == Decimal("50.00")


def test_manual_attribution_is_explicit_and_clock_deterministic() -> None:
    transaction, fill = make_trade(amount="60.00")
    snapshot = make_snapshot(transactions=(transaction,), fills=(fill,))
    recorded_at = snapshot.received_at
    attribution = Attribution(
        evidence_id=f"execution:{fill.execution_id}",
        plan_id="fictional-etf",
        reason="fictional manual trade review",
        recorded_at=recorded_at,
        recorded_by="fixture-reviewer",
    )

    first = build_ledger(
        snapshot,
        _plan(),
        attributions={transaction.transaction_id: attribution},
        clock=lambda: recorded_at,
    )
    second = build_ledger(
        snapshot,
        _plan(),
        attributions={transaction.transaction_id: "fictional-etf"},
        clock=lambda: recorded_at,
    )

    assert first.attributions[0].reason == "fictional manual trade review"
    assert first.attributions[0].recorded_by == "fixture-reviewer"
    assert first.spent_for("fictional-etf") == Decimal("60.00")
    assert (
        second.ledger_version
        == build_ledger(
            snapshot,
            _plan(),
            attributions={transaction.transaction_id: "fictional-etf"},
            clock=lambda: recorded_at,
        ).ledger_version
    )


def test_rolling_window_keeps_confirmed_spend_and_deduplicates_overlap() -> None:
    plan = ETFPlan(
        plan_id="fictional-etf",
        account_id="fictional-account",
        symbol="QXEN",
        amount_per_period="100.00",
        start_date="2026-02-01",
        timezone="UTC",
        execution_day=1,
        annual_cap="1000.00",
    )
    rolling_plan = InvestmentPlan(account_id="fictional-account", etf_plans=(plan,))
    first_at = datetime(2026, 2, 15, tzinfo=UTC)
    second_at = datetime(2026, 3, 2, tzinfo=UTC)
    transaction, fill = make_trade(amount="60.00")
    first = make_snapshot(
        snapshot_id="window-1",
        transactions=(transaction,),
        fills=(fill,),
        business_time=first_at,
    )
    first_ledger = build_ledger(
        first,
        rolling_plan,
        attributions={transaction.transaction_id: "fictional-etf"},
        clock=lambda: first_at,
    )

    # The second broker window overlaps the same execution. Carrying the
    # prior ledger keeps the confirmed $60 spend once while two periods accrue
    # $200, leaving exactly $140 for the plan.
    second = make_snapshot(
        snapshot_id="window-2",
        transactions=(transaction,),
        fills=(fill,),
        business_time=second_at,
    )
    second_ledger = build_ledger(
        second,
        rolling_plan,
        prior=first_ledger,
        attributions={transaction.transaction_id: "fictional-etf"},
        clock=lambda: second_at,
    )

    summary = budget_summaries(second_ledger, rolling_plan, as_of=second_at)[0]
    assert len(second_ledger.entries) == 1
    assert second_ledger.spent_for("fictional-etf") == Decimal("60.00")
    assert summary.accrued == Decimal("200.00")
    assert summary.remaining == Decimal("140.00")


def test_position_quantity_change_without_trade_is_blocked() -> None:
    prior_snapshot = make_snapshot(
        snapshot_id="position-baseline",
        positions=(_position(position_id="position-1", quantity="1"),),
    )
    prior = build_ledger(prior_snapshot, _plan())
    current_snapshot = make_snapshot(
        snapshot_id="position-change",
        positions=(_position(position_id="position-1", quantity="2"),),
    )

    ledger = build_ledger(current_snapshot, _plan(), prior=prior)

    assert "UNEXPLAINED_POSITION_CHANGE:QXEN" in ledger.blocking_reasons
    assert not ledger.complete


def test_empty_position_baseline_still_detects_a_later_unexplained_holding() -> None:
    prior_snapshot = make_snapshot(snapshot_id="flat-baseline")
    prior = build_ledger(prior_snapshot, _plan())
    current_snapshot = make_snapshot(
        snapshot_id="unexpected-holding",
        positions=(_position(position_id="position-1", quantity="1"),),
    )

    ledger = build_ledger(current_snapshot, _plan(), prior=prior)

    assert "UNEXPLAINED_POSITION_CHANGE:QXEN" in ledger.blocking_reasons
    assert not ledger.complete


def test_position_market_value_and_as_of_changes_do_not_block() -> None:
    prior_snapshot = make_snapshot(
        snapshot_id="position-baseline",
        positions=(
            _position(
                position_id="position-1",
                quantity="1",
                market_value="100.00",
            ),
        ),
    )
    prior = build_ledger(prior_snapshot, _plan())
    current_snapshot = make_snapshot(
        snapshot_id="position-repriced",
        positions=(
            _position(
                position_id="position-1",
                quantity="1",
                as_of=datetime(2026, 10, 16, 13, 0, tzinfo=UTC),
                market_value="120.00",
            ),
        ),
    )

    ledger = build_ledger(current_snapshot, _plan(), prior=prior)

    assert not any(
        reason.startswith("UNEXPLAINED_POSITION_CHANGE:") for reason in ledger.blocking_reasons
    )
    assert ledger.complete


def test_position_quantity_change_is_explained_by_trade_evidence() -> None:
    prior_snapshot = make_snapshot(
        snapshot_id="position-baseline",
        positions=(_position(position_id="position-1", quantity="1"),),
    )
    prior = build_ledger(prior_snapshot, _plan())
    transaction, fill = make_trade(amount="60.00", quantity="1")
    current_snapshot = make_snapshot(
        snapshot_id="position-trade",
        positions=(_position(position_id="position-1", quantity="2"),),
        transactions=(transaction,),
        fills=(fill,),
    )

    ledger = build_ledger(
        current_snapshot,
        _plan(),
        prior=prior,
        attributions={transaction.transaction_id: "fictional-etf"},
    )

    assert ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_position_reconciliation_uses_fact_watermark_for_cumulative_windows() -> None:
    baseline_transaction, baseline_fill = make_trade(
        transaction_id="baseline-trade", amount="60.00"
    )
    prior_snapshot = make_snapshot(
        snapshot_id="position-baseline-trade",
        positions=(_position(position_id="position-1", quantity="1"),),
        transactions=(baseline_transaction,),
        fills=(baseline_fill,),
    )
    prior = build_ledger(
        prior_snapshot,
        _plan(),
        attributions={baseline_transaction.transaction_id: "fictional-etf"},
    )
    transaction, fill = make_trade(
        transaction_id="increment-trade", execution_id="increment-fill", amount="60.00"
    )
    current_snapshot = make_snapshot(
        snapshot_id="position-cumulative-trades",
        positions=(_position(position_id="position-1", quantity="2"),),
        transactions=(baseline_transaction, transaction),
        fills=(baseline_fill, fill),
    )

    ledger = build_ledger(
        current_snapshot,
        _plan(),
        prior=prior,
        attributions={
            baseline_transaction.transaction_id: "fictional-etf",
            transaction.transaction_id: "fictional-etf",
        },
    )

    assert ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("120.00")


def test_unknown_corporate_action_blocks_position_reconciliation() -> None:
    prior_snapshot = make_snapshot(
        snapshot_id="position-baseline",
        positions=(_position(position_id="position-1", quantity="1"),),
    )
    prior = build_ledger(prior_snapshot, _plan())
    transaction = make_trade(transaction_id="corporate-action")[0].model_copy(
        update={
            "transaction_type": "corporate_action",
            "transaction_subtype": "POSITION_ADJUSTMENT",
            "instrument": None,
            "quantity": None,
            "price": None,
            "net_amount": CurrencyAmount(currency="USD", amount=Decimal("0")),
        }
    )
    current_snapshot = make_snapshot(
        snapshot_id="position-structural-event",
        positions=(_position(position_id="position-1", quantity="2"),),
        transactions=(transaction,),
    )

    ledger = build_ledger(current_snapshot, _plan(), prior=prior)

    assert "UNKNOWN_STRUCTURAL_EVENT:corporate-action" in ledger.blocking_reasons
    assert not ledger.complete


def test_late_execution_and_manual_reassignment_update_one_entry() -> None:
    plan = _plan()
    first_at = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    second_at = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
    transaction, fill = make_trade(amount="60.00")
    first = make_snapshot(snapshot_id="late-window-1", business_time=first_at)
    first_ledger = build_ledger(first, plan, clock=lambda: first_at)
    second = make_snapshot(
        snapshot_id="late-window-2",
        transactions=(transaction, transaction),
        fills=(fill, fill),
        business_time=second_at,
    )
    attribution = Attribution(
        evidence_id=f"execution:{fill.execution_id}",
        plan_id="fictional-etf",
        reason="late fictional execution matched manually",
        recorded_at=second_at,
    )
    second_ledger = build_ledger(
        second,
        plan,
        prior=first_ledger,
        attributions={transaction.transaction_id: attribution},
        clock=lambda: second_at,
    )

    assert len(second_ledger.entries) == 1
    assert second_ledger.spent_for("fictional-etf") == Decimal("60.00")
    assert second_ledger.attributions[0].reason == "late fictional execution matched manually"
    assert not second_ledger.unattributed_evidence


def test_fee_reversal_and_correction_are_explicit_and_deduplicated() -> None:
    transaction, fill = make_trade(amount="60.00")
    fee = transaction.model_copy(
        update={
            "transaction_id": "fee-1",
            "transaction_type": "fee",
            "transaction_subtype": "COMMISSION",
            "instrument": transaction.instrument,
            "quantity": None,
            "price": None,
            "net_amount": transaction.net_amount.model_copy(update={"amount": Decimal("2.00")}),
        }
    )
    reversal = transaction.model_copy(
        update={
            "transaction_id": "reversal-1",
            "transaction_type": "reversal",
            "transaction_subtype": "TRADE_REVERSAL",
            "related_transaction_id": transaction.transaction_id,
            "net_amount": transaction.net_amount.model_copy(update={"amount": Decimal("60.00")}),
        }
    )
    correction = fee.model_copy(
        update={
            "transaction_id": "correction-1",
            "transaction_type": "adjustment",
            "transaction_subtype": "FEE_ADJUSTMENT",
            "related_transaction_id": fee.transaction_id,
            "net_amount": fee.net_amount.model_copy(update={"amount": Decimal("1.00")}),
        }
    )
    snapshot = make_snapshot(
        transactions=(transaction, fee, reversal, correction, fee),
        fills=(fill,),
    )
    ledger = build_ledger(
        snapshot,
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            fee.transaction_id: "fictional-etf",
            reversal.transaction_id: "fictional-etf",
            correction.transaction_id: "fictional-etf",
        },
    )

    assert len(ledger.entries) == 4
    assert ledger.spent_for("fictional-etf") == Decimal("1.00")
    assert {entry.kind for entry in ledger.entries} == {
        EntryKind.BUY,
        EntryKind.FEE,
        EntryKind.REVERSAL,
        EntryKind.CORRECTION,
    }


def test_fee_only_transaction_without_instrument_is_accounted() -> None:
    fee = make_trade(amount="2.00")[0].model_copy(
        update={
            "transaction_id": "fee-only-1",
            "transaction_type": "fee",
            "transaction_subtype": "COMMISSION",
            "instrument": None,
            "quantity": None,
            "price": None,
            "net_amount": CurrencyAmount(currency="USD", amount=Decimal("2.00")),
        }
    )
    ledger = build_ledger(
        make_snapshot(transactions=(fee,)),
        _plan(),
        attributions={fee.transaction_id: "fictional-etf"},
    )

    assert ledger.complete
    assert ledger.entries[0].kind is EntryKind.FEE
    assert ledger.spent_for("fictional-etf") == Decimal("2.00")
    assert not any("ENTRY_RULE_SYMBOL" in reason for reason in ledger.blocking_reasons)


def test_fee_with_explicit_wrong_symbol_is_retained_but_unattributed() -> None:
    fee = make_trade(symbol="QXEN", amount="2.00")[0].model_copy(
        update={
            "transaction_id": "fee-wrong-symbol",
            "transaction_type": "fee",
            "transaction_subtype": "COMMISSION",
            "quantity": None,
            "price": None,
            "net_amount": CurrencyAmount(currency="USD", amount=Decimal("2.00")),
        }
    )
    ledger = build_ledger(
        make_snapshot(transactions=(fee,)),
        _two_symbol_etf_plan(),
        attributions={fee.transaction_id: "fictional-qzet-etf"},
    )

    assert ledger.entries[0].kind is EntryKind.FEE
    assert ledger.entries[0].symbol == "QXEN"
    assert ledger.entries[0].status is AttributionStatus.UNATTRIBUTED
    assert "ENTRY_RULE_SYMBOL_MISMATCH:transaction:fee-wrong-symbol" in ledger.blocking_reasons
    assert ledger.spent_for("fictional-qzet-etf") == Decimal("0")
    assert not ledger.complete


def test_attributed_buy_with_wrong_etf_symbol_is_retained_but_unattributed() -> None:
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _two_symbol_etf_plan(),
        attributions={transaction.transaction_id: "fictional-qzet-etf"},
    )

    entry = ledger.entries[0]
    assert entry.symbol == "QXEN"
    assert entry.plan_id == "fictional-qzet-etf"
    assert entry.status is AttributionStatus.UNATTRIBUTED
    assert entry.evidence_id in ledger.unattributed_evidence
    assert "ENTRY_RULE_SYMBOL_MISMATCH:execution:transaction-1:execution" in ledger.blocking_reasons
    assert ledger.spent_for("fictional-qzet-etf") == Decimal("0")
    assert ledger.spent_for("fictional-etf") == Decimal("0")
    assert ledger.attributions[0].plan_id == "fictional-qzet-etf"
    assert not ledger.complete


def test_attributed_buy_with_wrong_equity_layer_symbol_is_retained_but_unattributed() -> None:
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _equity_plan(),
        attributions={transaction.transaction_id: "fictional-second-layer"},
    )

    entry = ledger.entries[0]
    assert entry.symbol == "QXEN"
    assert entry.layer_id == "fictional-second-layer"
    assert entry.status is AttributionStatus.UNATTRIBUTED
    assert "ENTRY_RULE_SYMBOL_MISMATCH:execution:transaction-1:execution" in ledger.blocking_reasons
    assert ledger.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "0"
    )
    assert ledger.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")
    assert not ledger.complete


def test_conflicting_equity_plan_and_layer_tuple_is_unattributed() -> None:
    transaction, fill = make_trade(symbol="QZET", amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _equity_plan(),
        attributions={
            transaction.transaction_id: ("fictional-qxen-layer", "fictional-second-layer")
        },
    )

    entry = ledger.entries[0]
    assert entry.symbol == "QZET"
    assert entry.plan_id == "fictional-qxen-layer"
    assert entry.layer_id == "fictional-second-layer"
    assert entry.status is AttributionStatus.UNATTRIBUTED
    assert "ENTRY_RULE_IDENTITY_MISMATCH:execution:transaction-1:execution" in (
        ledger.blocking_reasons
    )
    assert ledger.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")
    assert ledger.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "0"
    )
    assert ledger.attributions[0].plan_id == "fictional-qxen-layer"
    assert ledger.attributions[0].layer_id == "fictional-second-layer"
    assert not ledger.complete


def test_conflicting_equity_plan_and_layer_attribution_object_is_unattributed() -> None:
    transaction, fill = make_trade(symbol="QZET", amount="60.00")
    attribution = Attribution(
        evidence_id=f"execution:{fill.execution_id}",
        plan_id="fictional-qxen-layer",
        layer_id="fictional-second-layer",
        reason="fictional conflicting layer review",
        recorded_at=NOW,
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _equity_plan(),
        attributions={transaction.transaction_id: attribution},
    )

    assert ledger.entries[0].status is AttributionStatus.UNATTRIBUTED
    assert "ENTRY_RULE_IDENTITY_MISMATCH:execution:transaction-1:execution" in (
        ledger.blocking_reasons
    )
    assert ledger.attributions[0].plan_id == "fictional-qxen-layer"
    assert ledger.attributions[0].layer_id == "fictional-second-layer"
    assert ledger.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")
    assert ledger.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "0"
    )
    assert not ledger.complete


def test_etf_attribution_cannot_carry_another_etf_as_layer_id() -> None:
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _two_symbol_etf_plan(),
        attributions={transaction.transaction_id: ("fictional-etf", "fictional-qzet-etf")},
    )

    assert ledger.entries[0].status is AttributionStatus.UNATTRIBUTED
    assert "ENTRY_RULE_IDENTITY_MISMATCH:execution:transaction-1:execution" in (
        ledger.blocking_reasons
    )
    assert ledger.spent_for("fictional-etf") == Decimal("0")
    assert ledger.spent_for("fictional-qzet-etf") == Decimal("0")
    assert not ledger.complete


def test_same_security_across_equity_layers_is_a_valid_attribution() -> None:
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _equity_plan(same_symbol=True),
        attributions={transaction.transaction_id: "fictional-second-layer"},
    )

    assert ledger.complete
    assert ledger.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "60.00"
    )
    assert ledger.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")


def test_same_security_across_equity_layers_accepts_matching_explicit_ids() -> None:
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    attribution = Attribution(
        evidence_id=f"execution:{fill.execution_id}",
        plan_id="fictional-second-layer",
        layer_id="fictional-second-layer",
        reason="fictional same-security layer review",
        recorded_at=NOW,
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _equity_plan(same_symbol=True),
        attributions={transaction.transaction_id: attribution},
    )

    assert ledger.complete
    assert ledger.entries[0].layer_id == "fictional-second-layer"
    assert ledger.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "60.00"
    )


def test_wrong_symbol_attribution_remains_blocked_across_prior_overlap() -> None:
    plan = _two_symbol_etf_plan()
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    attribution = {transaction.transaction_id: "fictional-qzet-etf"}
    first = build_ledger(
        make_snapshot(
            snapshot_id="wrong-symbol-window-1", transactions=(transaction,), fills=(fill,)
        ),
        plan,
        attributions=attribution,
    )
    second = build_ledger(
        make_snapshot(
            snapshot_id="wrong-symbol-window-2", transactions=(transaction,), fills=(fill,)
        ),
        plan,
        prior=first,
        attributions=attribution,
    )

    assert not first.complete
    assert not second.complete
    assert len(second.entries) == 1
    assert second.entries[0].status is AttributionStatus.UNATTRIBUTED
    assert second.entries[0].evidence_id in second.unattributed_evidence
    assert "ENTRY_RULE_SYMBOL_MISMATCH:execution:transaction-1:execution" in second.blocking_reasons
    assert second.spent_for("fictional-qzet-etf") == Decimal("0")
    assert second.attributions[0].plan_id == "fictional-qzet-etf"


def test_wrong_symbol_buy_correction_cannot_release_spend_before_target_check() -> None:
    plan = _two_symbol_etf_plan()
    transaction, fill = make_trade(symbol="QXEN", amount="60.00")
    correction = _correction(
        transaction,
        transaction_id="wrong-symbol-reversal",
        related_transaction_id=transaction.transaction_id,
        amount="60.00",
    )
    attribution = {
        transaction.transaction_id: "fictional-qzet-etf",
        correction.transaction_id: "fictional-qzet-etf",
    }
    snapshot = make_snapshot(
        snapshot_id="wrong-symbol-correction",
        transactions=(correction, transaction),
        fills=(fill,),
    )
    first = build_ledger(snapshot, plan, attributions=attribution)
    second = build_ledger(
        snapshot.model_copy(update={"snapshot_id": "wrong-symbol-correction-overlap"}),
        plan,
        prior=first,
        attributions=attribution,
    )

    assert not first.complete
    assert not second.complete
    assert "ENTRY_RULE_SYMBOL_MISMATCH:execution:transaction-1:execution" in first.blocking_reasons
    assert "CORRECTION_TARGET_RULE_SYMBOL_MISMATCH:transaction:wrong-symbol-reversal" in (
        first.blocking_reasons
    )
    assert first.spent_for("fictional-qzet-etf") == Decimal("0")
    assert second.spent_for("fictional-qzet-etf") == Decimal("0")
    assert all(entry.status is AttributionStatus.UNATTRIBUTED for entry in second.entries)


def test_conflicting_ids_stay_blocked_through_correction_and_prior_overlap() -> None:
    plan = _equity_plan()
    transaction, fill = make_trade(symbol="QZET", amount="60.00")
    correction = _correction(
        transaction,
        transaction_id="conflicting-ids-reversal",
        related_transaction_id=transaction.transaction_id,
        amount="60.00",
    )
    attribution = {
        transaction.transaction_id: ("fictional-qxen-layer", "fictional-second-layer"),
        correction.transaction_id: ("fictional-qxen-layer", "fictional-second-layer"),
    }
    snapshot = make_snapshot(
        snapshot_id="conflicting-ids-correction",
        transactions=(correction, transaction),
        fills=(fill,),
    )
    first = build_ledger(snapshot, plan, attributions=attribution)
    second = build_ledger(
        snapshot.model_copy(update={"snapshot_id": "conflicting-ids-overlap"}),
        plan,
        prior=first,
        attributions=attribution,
    )

    assert not first.complete
    assert not second.complete
    assert "ENTRY_RULE_IDENTITY_MISMATCH:execution:transaction-1:execution" in (
        first.blocking_reasons
    )
    assert "CORRECTION_TARGET_RULE_IDENTITY_MISMATCH:transaction:conflicting-ids-reversal" in (
        first.blocking_reasons
    )
    assert first.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")
    assert first.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "0"
    )
    assert second.spent_for("fictional-qxen-layer", layer_id="fictional-qxen-layer") == Decimal("0")
    assert second.spent_for("fictional-second-layer", layer_id="fictional-second-layer") == Decimal(
        "0"
    )
    assert all(entry.status is AttributionStatus.UNATTRIBUTED for entry in second.entries)


def test_conflicting_rule_identity_blocks_proposals() -> None:
    transaction, fill = make_trade(symbol="QZET", amount="60.00")
    snapshot = make_snapshot(transactions=(transaction,), fills=(fill,))
    plan = _equity_plan()
    ledger = build_ledger(
        snapshot,
        plan,
        attributions={
            transaction.transaction_id: ("fictional-qxen-layer", "fictional-second-layer")
        },
    )
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items
    assert all(item.status is ProposalStatus.BLOCKED for item in batch.items)
    assert "ENTRY_RULE_IDENTITY_MISMATCH:execution:transaction-1:execution" in (
        batch.blocking_reasons
    )


def test_buy_without_a_symbol_is_blocked_defensively() -> None:
    entry = LedgerEntry(
        entry_id="missing-symbol-entry",
        account_id="fictional-account",
        symbol="QXEN",
        kind=EntryKind.BUY,
        cost="60.00",
        quantity="1",
        plan_id="fictional-etf",
        status=AttributionStatus.ATTRIBUTED,
        evidence_id="missing-symbol-entry",
    ).model_copy(update={"symbol": ""})

    ledger = build_ledger(make_snapshot(), _plan(), initial_entries=(entry,))

    assert "ENTRY_RULE_SYMBOL_MISSING:missing-symbol-entry" in ledger.blocking_reasons
    assert ledger.entries[0].status is AttributionStatus.UNATTRIBUTED
    assert ledger.spent_for("fictional-etf") == Decimal("0")
    assert not ledger.complete


def test_missing_trade_details_block_instead_of_being_treated_as_cash() -> None:
    transaction, _ = make_trade(amount="60.00")
    missing_details = transaction.model_copy(
        update={
            "transaction_id": "trade-without-details",
            "instrument": None,
            "quantity": None,
            "price": None,
        }
    )

    ledger = build_ledger(
        make_snapshot(transactions=(missing_details,)),
        _plan(),
        attributions={missing_details.transaction_id: "fictional-etf"},
    )

    assert "UNKNOWN_TRANSACTION_SHAPE:trade-without-details" in ledger.blocking_reasons
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("0")


def test_known_cash_event_without_instrument_is_ignored() -> None:
    transaction, _ = make_trade(amount="100.00")
    cash_events = tuple(
        transaction.model_copy(
            update={
                "transaction_id": f"cash-{transaction_type}",
                "transaction_type": transaction_type,
                "transaction_subtype": None,
                "instrument": None,
                "quantity": None,
                "price": None,
                "net_amount": CurrencyAmount(currency="USD", amount=Decimal("100.00")),
            }
        )
        for transaction_type in (
            "cash_receipt",
            "cash_disbursement",
            "cash_transfer",
            "cash_deposit",
            "cash_withdrawal",
        )
    )

    ledger = build_ledger(make_snapshot(transactions=cash_events), _plan())

    assert ledger.complete
    assert ledger.entries == ()
    assert ledger.spent_for("fictional-etf") == Decimal("0")

    unknown_cash = cash_events[0].model_copy(update={"unknown_fields": ("transaction_shape",)})
    blocked = build_ledger(make_snapshot(transactions=(unknown_cash,)), _plan())
    assert "UNKNOWN_TRANSACTION_SHAPE:cash-cash_receipt" in blocked.blocking_reasons
    assert not blocked.complete


def test_unknown_cash_shape_with_zero_value_still_blocks() -> None:
    transaction, _ = make_trade(amount="0.00")
    unknown = transaction.model_copy(
        update={
            "transaction_id": "unknown-cash-event",
            "transaction_type": "cash_mystery",
            "transaction_subtype": None,
            "instrument": None,
            "quantity": None,
            "price": None,
            "net_amount": CurrencyAmount(currency="USD", amount=Decimal("0")),
        }
    )

    ledger = build_ledger(make_snapshot(transactions=(unknown,)), _plan())

    assert "UNKNOWN_TRANSACTION_SHAPE:unknown-cash-event" in ledger.blocking_reasons
    assert not ledger.complete


def test_unknown_plan_attribution_blocks_even_without_layer() -> None:
    transaction, fill = make_trade(amount="60.00")
    ledger = build_ledger(
        make_snapshot(transactions=(transaction,), fills=(fill,)),
        _plan(),
        attributions={transaction.transaction_id: "unknown-etf-plan"},
    )

    assert "UNKNOWN_RULE:execution:transaction-1:execution" in ledger.blocking_reasons
    assert not ledger.complete


def test_missing_correction_target_cannot_reduce_spent() -> None:
    transaction, _ = make_trade(amount="60.00")
    correction = transaction.model_copy(
        update={
            "transaction_id": "correction-without-target",
            "transaction_type": "adjustment",
            "transaction_subtype": "FEE_ADJUSTMENT",
            "instrument": None,
            "quantity": None,
            "price": None,
            "related_transaction_id": "does-not-exist",
            "net_amount": CurrencyAmount(currency="USD", amount=Decimal("60.00")),
        }
    )
    ledger = build_ledger(
        make_snapshot(transactions=(correction,)),
        _plan(),
        attributions={correction.transaction_id: "fictional-etf"},
    )

    assert (
        "UNKNOWN_CORRECTION_TARGET:transaction:correction-without-target" in ledger.blocking_reasons
    )
    assert ledger.unattributed_evidence
    assert ledger.spent_for("fictional-etf") == Decimal("0")


def test_self_referencing_correction_cannot_reduce_spent() -> None:
    transaction, fill = make_trade(amount="60.00")
    correction = _correction(
        transaction,
        transaction_id="self-correction",
        related_transaction_id="self-correction",
        amount="60.00",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, correction), fills=(fill,)),
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            correction.transaction_id: "fictional-etf",
        },
    )

    assert "CORRECTION_SELF_REFERENCE:transaction:self-correction" in ledger.blocking_reasons
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_correction_cycle_and_correction_of_correction_are_blocked() -> None:
    transaction, fill = make_trade(amount="60.00")
    first = _correction(
        transaction,
        transaction_id="correction-a",
        related_transaction_id="correction-b",
    )
    second = _correction(
        transaction,
        transaction_id="correction-b",
        related_transaction_id="correction-a",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, first, second), fills=(fill,)),
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            first.transaction_id: "fictional-etf",
            second.transaction_id: "fictional-etf",
        },
    )

    assert any(reason.startswith("CORRECTION_TARGET_CYCLE:") for reason in ledger.blocking_reasons)
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_correction_targeting_another_correction_is_blocked() -> None:
    transaction, fill = make_trade(amount="60.00")
    first = _correction(
        transaction,
        transaction_id="base-correction",
        related_transaction_id=transaction.transaction_id,
        amount="10.00",
    )
    second = _correction(
        transaction,
        transaction_id="nested-correction",
        related_transaction_id=first.transaction_id,
        amount="5.00",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, first, second), fills=(fill,)),
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            first.transaction_id: "fictional-etf",
            second.transaction_id: "fictional-etf",
        },
    )

    assert "CORRECTION_TARGET_NOT_BASE:transaction:nested-correction" in ledger.blocking_reasons
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("50.00")


def test_correction_cannot_cross_rule_or_currency() -> None:
    transaction, fill = make_trade(amount="60.00")
    other_plan = ETFPlan(
        plan_id="other-etf",
        account_id="fictional-account",
        symbol="QZET",
        amount_per_period="100.00",
        start_date="2026-01-01",
        timezone="UTC",
        execution_day=1,
    )
    plan = InvestmentPlan(
        account_id="fictional-account",
        etf_plans=(*_plan().etf_plans, other_plan),
    )
    cross_rule = _correction(
        transaction,
        transaction_id="cross-rule-correction",
        related_transaction_id=transaction.transaction_id,
    )
    currency = _correction(
        transaction,
        transaction_id="currency-correction",
        related_transaction_id=transaction.transaction_id,
        currency="EUR",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, cross_rule, currency), fills=(fill,)),
        plan,
        attributions={
            transaction.transaction_id: "fictional-etf",
            cross_rule.transaction_id: "other-etf",
            currency.transaction_id: "fictional-etf",
        },
    )

    assert (
        "CORRECTION_TARGET_RULE_MISMATCH:transaction:cross-rule-correction"
        in ledger.blocking_reasons
    )
    assert (
        "CORRECTION_TARGET_CURRENCY_MISMATCH:transaction:currency-correction"
        in ledger.blocking_reasons
    )
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_correction_cannot_cross_account_or_exceed_original_cost() -> None:
    transaction, fill = make_trade(amount="60.00")
    cross_account = _correction(
        transaction,
        transaction_id="cross-account-correction",
        related_transaction_id=transaction.transaction_id,
        account_id="other-account",
    )
    excessive = _correction(
        transaction,
        transaction_id="excessive-correction",
        related_transaction_id=transaction.transaction_id,
        amount="100.00",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, cross_account, excessive), fills=(fill,)),
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            cross_account.transaction_id: "fictional-etf",
            excessive.transaction_id: "fictional-etf",
        },
    )

    assert (
        "CORRECTION_TARGET_ACCOUNT_MISMATCH:transaction:cross-account-correction"
        in ledger.blocking_reasons
    )
    assert "CORRECTION_EXCEEDS_TARGET:transaction:excessive-correction" in ledger.blocking_reasons
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_duplicate_corrections_cannot_reduce_the_same_target_twice() -> None:
    transaction, fill = make_trade(amount="60.00")
    first = _correction(
        transaction,
        transaction_id="duplicate-correction-1",
        related_transaction_id=transaction.transaction_id,
        amount="10.00",
    )
    second = _correction(
        transaction,
        transaction_id="duplicate-correction-2",
        related_transaction_id=transaction.transaction_id,
        amount="10.00",
    )
    ledger = build_ledger(
        make_snapshot(transactions=(transaction, first, second), fills=(fill,)),
        _plan(),
        attributions={
            transaction.transaction_id: "fictional-etf",
            first.transaction_id: "fictional-etf",
            second.transaction_id: "fictional-etf",
        },
    )

    assert "DUPLICATE_CORRECTION_TARGET:transaction:transaction-1" in ledger.blocking_reasons
    assert not ledger.complete
    assert ledger.spent_for("fictional-etf") == Decimal("50.00")


def test_transaction_only_then_fill_replaces_provisional_entry_once() -> None:
    transaction, fill = make_trade(amount="60.00")
    first = make_snapshot(
        snapshot_id="transaction-only-window",
        transactions=(transaction,),
    )
    first_ledger = build_ledger(
        first,
        _plan(),
        attributions={transaction.transaction_id: "fictional-etf"},
    )

    second = make_snapshot(
        snapshot_id="execution-window",
        transactions=(transaction,),
        fills=(fill,),
    )
    second_ledger = build_ledger(
        second,
        _plan(),
        prior=first_ledger,
        attributions={transaction.transaction_id: "fictional-etf"},
    )

    assert len(second_ledger.entries) == 1
    assert second_ledger.entries[0].source_execution_id == fill.execution_id
    assert second_ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_transaction_then_changed_fill_value_blocks_without_double_spend() -> None:
    transaction, fill = make_trade(amount="60.00")
    first = make_snapshot(
        snapshot_id="transaction-only-mismatch-window",
        transactions=(transaction,),
    )
    first_ledger = build_ledger(
        first,
        _plan(),
        attributions={transaction.transaction_id: "fictional-etf"},
    )
    changed_fill = fill.model_copy(
        update={"amount": CurrencyAmount(currency="USD", amount=Decimal("61.00"))}
    )
    second = make_snapshot(
        snapshot_id="execution-mismatch-window",
        transactions=(transaction,),
        fills=(changed_fill,),
    )
    second_ledger = build_ledger(
        second,
        _plan(),
        prior=first_ledger,
        attributions={transaction.transaction_id: "fictional-etf"},
    )

    assert "TRANSACTION_FILL_VALUE_MISMATCH:transaction-1" in second_ledger.blocking_reasons
    assert second_ledger.spent_for("fictional-etf") == Decimal("60.00")


def test_raw_fixture_canceled_remainder_blocks_proposals() -> None:
    def mutate(orders) -> None:
        orders[0].update(
            status="CANCELED",
            filledQuantity="4",
            remainingQuantity="1",
        )

    orders = _fixture_orders(mutate)
    assert orders[0].status is OrderStatus.CANCELED
    assert orders[0].remaining_quantity == Decimal("1")

    snapshot = make_snapshot(orders=orders, reservation_included=False)
    occupation = order_occupation(snapshot)
    ledger = build_ledger(snapshot, _plan())
    batch = generate_proposals(snapshot, _plan(), ledger, as_of=NOW)

    assert occupation.amount == Decimal("35.00")
    assert "TERMINAL_ORDER_REMAINDER:SAMPLE-ORDER-001" in occupation.blocking_reasons
    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED


def test_raw_fixture_replaced_without_successor_blocks_proposals() -> None:
    def mutate(orders) -> None:
        orders[0].update(
            status="REPLACED",
            remainingQuantity="0",
            replacedByOrderId="MISSING-REPLACEMENT",
        )

    orders = _fixture_orders(mutate)
    assert orders[0].status is OrderStatus.REPLACED
    assert orders[0].replaced_by_order_id == "MISSING-REPLACEMENT"

    snapshot = make_snapshot(orders=orders, reservation_included=False)
    occupation = order_occupation(snapshot)
    ledger = build_ledger(snapshot, _plan())
    batch = generate_proposals(snapshot, _plan(), ledger, as_of=NOW)

    assert occupation.amount == Decimal("0")
    assert "UNKNOWN_ORDER_REPLACEMENT:SAMPLE-ORDER-001" in occupation.blocking_reasons
    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED


def test_raw_fixture_buy_to_cover_is_cash_occupation_in_proposals() -> None:
    def mutate(orders) -> None:
        orders[0].update(
            status="WORKING",
            filledQuantity="0",
            remainingQuantity="2",
        )
        orders[0]["orderLegCollection"][0]["instruction"] = "BUY_TO_COVER"

    orders = _fixture_orders(mutate)
    assert orders[0].side is OrderSide.BUY_TO_COVER
    assert orders[0].is_open is True

    snapshot = make_snapshot(orders=orders, reservation_included=False)
    occupation = order_occupation(snapshot)
    ledger = build_ledger(snapshot, _plan())
    batch = generate_proposals(snapshot, _plan(), ledger, as_of=NOW)

    assert occupation.amount == Decimal("70.00")
    assert occupation.order_ids == ("SAMPLE-ORDER-001",)
    assert batch.open_order_occupation == Decimal("70.00")
    assert batch.items[0].maximum_cash_use <= Decimal("930.00")


def test_raw_fixture_complete_replacement_chain_remains_known() -> None:
    def mutate(orders) -> None:
        orders[0].update(
            status="REPLACED",
            remainingQuantity="0",
            replacedByOrderId="SAMPLE-ORDER-003",
        )
        orders.append(
            {
                "orderId": "SAMPLE-ORDER-003",
                "status": "WORKING",
                "enteredTime": "2026-10-16T00:00:00+00:00",
                "lastUpdatedTime": "2026-10-16T00:00:00+00:00",
                "orderType": "LIMIT",
                "price": "35.00",
                "quantity": "2",
                "filledQuantity": "0",
                "remainingQuantity": "2",
                "replacingOrderId": "SAMPLE-ORDER-001",
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": "2",
                        "instrument": {
                            "assetType": "EQUITY",
                            "symbol": "QXEN",
                            "currency": "USD",
                        },
                    }
                ],
            }
        )

    orders = _fixture_orders(mutate)
    snapshot = make_snapshot(orders=orders, reservation_included=False)
    occupation = order_occupation(snapshot)

    assert occupation.amount == Decimal("70.00")
    assert occupation.order_ids == ("SAMPLE-ORDER-003",)
    assert occupation.blocking_reasons == ()
