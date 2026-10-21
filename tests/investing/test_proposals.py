import json
from collections.abc import Callable
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from quant_stack.brokerage.contracts import (
    AccountSnapshot,
    AssetType,
    CurrencyAmount,
    Instrument,
    OptionContract,
    OrderStatus,
    Position,
    PositionSide,
    Quote,
    SyncState,
    SyncStatus,
)
from quant_stack.brokerage.schwab_adapter import SchwabAdapter
from quant_stack.brokerage.sync import PrivateSnapshotStore, SyncService
from quant_stack.investing.budgets import build_ledger
from quant_stack.investing.plans import EquityLayer, InvestmentPlan
from quant_stack.investing.proposals import (
    BlockCode,
    ProposalStatus,
    ReviewStatus,
    StaleProposalError,
    generate_proposals,
    review_proposal,
)

from .conftest import NOW, make_open_order, make_snapshot

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "brokerage" / "ready"


def _ready_payloads() -> dict[str, Any]:
    payloads = {
        name.removeprefix("normal_").removesuffix(".json"): json.loads(
            (FIXTURE_ROOT / name).read_text(encoding="utf-8")
        )
        for name in (
            "normal_account.json",
            "normal_orders.json",
            "normal_transactions.json",
            "normal_quotes.json",
            "normal_coverage.json",
        )
    }
    # The checked-in sample quotes are at the beginning of the fixture day.
    # Refresh only this in-memory transport metadata so proposal tests exercise
    # option classification rather than an unrelated stale-quote block.
    for quote in payloads["quotes"].values():
        quote["quote"]["quoteTime"] = NOW.isoformat()
    return payloads


def _sync_ready_payloads(
    tmp_path: Path, mutate: Callable[[dict[str, Any]], None]
) -> tuple[AccountSnapshot, SyncState]:
    payloads = _ready_payloads()
    mutate(payloads)
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads=payloads,
        clock=lambda: NOW,
    )
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    try:
        result = SyncService(adapter, store, clock=lambda: NOW).sync_account(
            "fictional-account", "fictional-account-hash"
        )
    finally:
        adapter.close()
    assert result.snapshot is not None
    assert result.attempt.status is SyncStatus.SUCCEEDED
    assert result.state.complete is True
    return result.snapshot, result.state


def _option_payload(payloads: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    account = payloads["account"]["securitiesAccount"]
    position = next(
        item
        for item in account["positions"]
        if item["instrument"].get("assetType") == "OPTION"
    )
    return position, position["instrument"]


def _plan(*layers: EquityLayer, account_id: str = "fictional-account") -> InvestmentPlan:
    return InvestmentPlan(account_id=account_id, equity_layers=tuple(layers))


def _layer(
    layer_id: str = "layer-1",
    *,
    symbol: str = "QXEN",
    budget: str = "1000.00",
    sequence: int = 1,
    require_review: bool = True,
) -> EquityLayer:
    return EquityLayer(
        layer_id=layer_id,
        account_id="fictional-account",
        symbol=symbol,
        budget=budget,
        trigger_price="101.00",
        total_position_limit="100.00",
        sequence=sequence,
        require_review=require_review,
        fee_per_order="1.00",
        cash_reserve="10.00",
    )


def test_integer_shares_respect_fee_reserve_and_shared_cash_pool() -> None:
    plan = _plan(_layer(), _layer("layer-2", symbol="ZETA", sequence=2))
    snapshot = make_snapshot(
        quotes=(
            Quote(
                instrument=Instrument(symbol="QXEN", asset_type=AssetType.EQUITY, currency="USD"),
                ask=CurrencyAmount(currency="USD", amount="101.00"),
                last=CurrencyAmount(currency="USD", amount="101.00"),
                quote_time=NOW,
                stale_after_seconds=900,
            ),
            Quote(
                instrument=Instrument(symbol="ZETA", asset_type=AssetType.EQUITY, currency="USD"),
                ask=CurrencyAmount(currency="USD", amount="101.00"),
                last=CurrencyAmount(currency="USD", amount="101.00"),
                quote_time=NOW,
                stale_after_seconds=900,
            ),
        ),
    )
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    first, second = batch.items
    assert first.quantity == Decimal("9")
    assert first.maximum_cash_use == Decimal("920.00")
    assert first.status is ProposalStatus.WAITING_REVIEW
    assert second.quantity == Decimal("0")
    assert second.status is ProposalStatus.INSUFFICIENT_CASH
    assert batch.status is ProposalStatus.READY
    assert batch.non_executable is True


def test_unknown_reservation_semantics_and_options_block_before_review() -> None:
    option = Instrument(
        symbol="XYZ  261016C00200000",
        asset_type=AssetType.OPTION,
        currency="USD",
        option=OptionContract(
            underlying_symbol="XYZ",
            deliverable="100 shares of XYZ",
        ),
    )
    position = Position(
        position_id="option-position",
        account_id="fictional-account",
        instrument=option,
        quantity="1",
        side=PositionSide.LONG,
        as_of=NOW,
    )
    snapshot = make_snapshot(
        reservation_included=None,
        positions=(position,),
    )
    plan = _plan(_layer())
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert "UNKNOWN_RESERVATION_SEMANTICS" in batch.blocking_reasons
    assert "UNKNOWN_OPTION_OBLIGATION" in batch.blocking_reasons
    with pytest.raises(StaleProposalError):
        review_proposal(
            batch,
            reviewer="fictional-reviewer",
            expected_input_fingerprint=batch.input_fingerprint,
            at=NOW,
        )


def test_raw_schwab_missing_option_underlying_blocks_proposals(tmp_path: Path) -> None:
    def remove_underlying(payloads: dict[str, Any]) -> None:
        _, option = _option_payload(payloads)
        option.pop("underlyingSymbol")

    snapshot, sync_state = _sync_ready_payloads(tmp_path, remove_underlying)
    option_position = next(
        position
        for position in snapshot.positions
        if position.instrument.asset_type is AssetType.OPTION
    )
    assert option_position.instrument.option is not None
    assert option_position.instrument.option.underlying_symbol == "<unknown>"
    assert "underlyingSymbol" in option_position.instrument.option.unknown_fields

    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(
        snapshot,
        plan,
        attributions={"SAMPLE-TRANSACTION-001": "layer-1"},
    )
    batch = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=sync_state,
    )

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED
    assert batch.items[0].quantity == Decimal("0")
    assert BlockCode.UNKNOWN_OPTION_OBLIGATION.value in batch.blocking_reasons


def test_raw_schwab_nonstandard_deliverable_blocks_proposals(tmp_path: Path) -> None:
    def set_nonstandard_deliverable(payloads: dict[str, Any]) -> None:
        _, option = _option_payload(payloads)
        option["deliverable"] = "cash settled at provider discretion"

    snapshot, sync_state = _sync_ready_payloads(tmp_path, set_nonstandard_deliverable)
    option_position = next(
        position
        for position in snapshot.positions
        if position.instrument.asset_type is AssetType.OPTION
    )
    assert option_position.instrument.option is not None
    assert "deliverable" in option_position.instrument.option.unknown_fields

    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(
        snapshot,
        plan,
        attributions={"SAMPLE-TRANSACTION-001": "layer-1"},
    )
    batch = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=sync_state,
    )

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED
    assert batch.items[0].quantity == Decimal("0")
    assert BlockCode.UNKNOWN_OPTION_OBLIGATION.value in batch.blocking_reasons


def test_raw_schwab_standard_short_put_counts_delivery_against_position_cap(
    tmp_path: Path,
) -> None:
    def make_short_put(payloads: dict[str, Any]) -> None:
        position, option = _option_payload(payloads)
        position["longQuantity"] = "0"
        position["shortQuantity"] = "1"
        option.update(
            {
                "symbol": "QXEN  261016P00200000",
                "underlyingSymbol": "QXEN",
                "putCall": "PUT",
                "deliverable": "100 shares of QXEN",
            }
        )

    snapshot, sync_state = _sync_ready_payloads(tmp_path, make_short_put)
    option_position = next(
        position
        for position in snapshot.positions
        if position.instrument.asset_type is AssetType.OPTION
    )
    assert option_position.side is PositionSide.SHORT
    assert option_position.instrument.option is not None
    assert option_position.instrument.option.unknown_fields == ()

    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(
        snapshot,
        plan,
        attributions={"SAMPLE-TRANSACTION-001": "layer-1"},
    )
    batch = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=sync_state,
    )

    assert BlockCode.UNKNOWN_OPTION_OBLIGATION.value not in batch.blocking_reasons
    assert batch.items[0].quantity == Decimal("0")
    assert batch.items[0].status is ProposalStatus.INSUFFICIENT_BUDGET
    assert "OPTION_DELIVERY_CAP" in batch.items[0].reasons


def test_raw_schwab_short_put_missing_deliverable_blocks_proposals(tmp_path: Path) -> None:
    def remove_deliverable(payloads: dict[str, Any]) -> None:
        position, option = _option_payload(payloads)
        position["longQuantity"] = "0"
        position["shortQuantity"] = "1"
        option.update(
            {
                "symbol": "QXEN  261016P00200000",
                "underlyingSymbol": "QXEN",
                "putCall": "PUT",
            }
        )
        option.pop("deliverable")

    snapshot, sync_state = _sync_ready_payloads(tmp_path, remove_deliverable)
    option_position = next(
        position
        for position in snapshot.positions
        if position.instrument.asset_type is AssetType.OPTION
    )
    assert option_position.instrument.option is not None
    assert "deliverable" in option_position.instrument.option.unknown_fields

    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(
        snapshot,
        plan,
        attributions={"SAMPLE-TRANSACTION-001": "layer-1"},
    )
    batch = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_state=sync_state,
    )

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED
    assert batch.items[0].quantity == Decimal("0")
    assert BlockCode.UNKNOWN_OPTION_OBLIGATION.value in batch.blocking_reasons


def test_open_order_is_not_proposed_again_and_known_reservation_is_subtracted() -> None:
    snapshot = make_snapshot(
        cash="1000.00",
        reservation_included=False,
        orders=(make_open_order(remaining="2"),),
    )
    plan = _plan(_layer())
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    item = batch.items[0]
    assert item.status is ProposalStatus.ALREADY_RESERVED
    assert item.quantity == Decimal("0")
    assert batch.open_order_occupation == Decimal("202.00")


def test_confirmed_cancellation_releases_occupation_but_pending_cancel_blocks() -> None:
    confirmed = make_open_order(remaining="2").model_copy(
        update={
            "status": OrderStatus.CANCELED,
            "is_open": False,
            "remaining_quantity": Decimal("0"),
        }
    )
    plan = _plan(_layer(require_review=False))
    confirmed_snapshot = make_snapshot(
        cash="1000.00",
        reservation_included=False,
        orders=(confirmed,),
    )
    confirmed_batch = generate_proposals(
        confirmed_snapshot,
        plan,
        build_ledger(confirmed_snapshot, plan),
        as_of=NOW,
    )
    assert confirmed_batch.open_order_occupation == Decimal("0")
    assert confirmed_batch.items[0].status is ProposalStatus.READY

    pending_cancel = make_open_order(remaining="2").model_copy(
        update={"status": OrderStatus.PENDING, "raw_status": "PENDING_CANCEL"}
    )
    pending_snapshot = make_snapshot(
        cash="1000.00",
        reservation_included=False,
        orders=(pending_cancel,),
    )
    pending_batch = generate_proposals(
        pending_snapshot,
        plan,
        build_ledger(pending_snapshot, plan),
        as_of=NOW,
    )
    assert pending_batch.status is ProposalStatus.BLOCKED
    assert any(
        reason.startswith("UNCONFIRMED_ORDER_CANCELLATION:")
        for reason in pending_batch.blocking_reasons
    )


def test_same_symbol_rules_share_the_smallest_position_cap() -> None:
    plan = _plan(
        EquityLayer(
            layer_id="layer-1",
            account_id="fictional-account",
            symbol="QXEN",
            budget="1000.00",
            trigger_price="101.00",
            total_position_limit="10.00",
            sequence=1,
            require_review=False,
        ),
        EquityLayer(
            layer_id="layer-2",
            account_id="fictional-account",
            symbol="QXEN",
            budget="1000.00",
            trigger_price="101.00",
            total_position_limit="10.00",
            sequence=2,
            require_review=False,
        ),
    )
    snapshot = make_snapshot(cash="5000.00")
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    assert batch.items[0].quantity == Decimal("9")
    assert batch.items[1].quantity == Decimal("1")


def test_same_input_is_idempotent_and_changed_snapshot_replaces_review() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(snapshot, plan)
    first = generate_proposals(snapshot, plan, ledger, as_of=NOW)
    reviewed = review_proposal(
        first,
        reviewer="fictional-reviewer",
        expected_input_fingerprint=first.input_fingerprint,
        at=NOW,
    )
    repeated = generate_proposals(snapshot, plan, ledger, as_of=NOW, previous=reviewed)
    assert repeated.batch_id == reviewed.batch_id
    assert repeated.review_status is ReviewStatus.REVIEWED
    changed_snapshot = snapshot.model_copy(update={"snapshot_id": "snapshot-2"})
    changed_ledger = build_ledger(changed_snapshot, plan)
    replacement = generate_proposals(
        changed_snapshot,
        plan,
        changed_ledger,
        as_of=NOW,
        previous=reviewed,
    )
    assert replacement.batch_id != reviewed.batch_id
    assert replacement.replaced_batch_id == reviewed.batch_id
    assert replacement.review_status is ReviewStatus.PENDING


def test_sync_failure_generation_invalidates_review_but_repeats_are_idempotent() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(snapshot, plan)
    first = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        sync_failure_generation=0,
    )
    reviewed = review_proposal(
        first,
        reviewer="fixture-reviewer",
        expected_input_fingerprint=first.input_fingerprint,
        at=NOW,
    )

    recovered = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        previous=reviewed,
        sync_state=SyncState(
            account_id=snapshot.account.account_id,
            last_success_snapshot_id=snapshot.snapshot_id,
            last_attempt_status=SyncStatus.SUCCEEDED,
            complete=True,
        ),
        sync_failure_generation=1,
    )
    assert recovered.batch_id != reviewed.batch_id
    assert recovered.review_status is ReviewStatus.PENDING
    assert recovered.sync_failure_generation == 1

    repeated = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=NOW,
        previous=recovered,
        sync_failure_generation=1,
    )
    assert repeated.batch_id == recovered.batch_id
    assert repeated.review_status is ReviewStatus.PENDING


def test_disabled_plan_has_no_buy_quantity_and_preserves_other_blocks() -> None:
    snapshot = make_snapshot(reservation_included=None)
    plan = _plan(_layer(require_review=False)).model_copy(update={"enabled": False})
    failed_sync = SyncState(
        account_id=snapshot.account.account_id,
        last_success_snapshot_id=snapshot.snapshot_id,
        last_attempt_status=SyncStatus.FAILED,
        complete=False,
    )

    batch = generate_proposals(
        snapshot,
        plan,
        build_ledger(snapshot, plan),
        as_of=NOW,
        sync_state=failed_sync,
    )

    assert batch.status is ProposalStatus.BLOCKED
    assert all(item.quantity == Decimal("0") for item in batch.items)
    assert BlockCode.PLAN_DISABLED.value in batch.blocking_reasons
    assert BlockCode.SYNC_FAILED.value in batch.blocking_reasons
    assert BlockCode.UNKNOWN_RESERVATION_SEMANTICS.value in batch.blocking_reasons


def test_plan_switch_invalidates_review_and_layer_switch_remains_independent() -> None:
    snapshot = make_snapshot(cash="5000.00")
    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(snapshot, plan)
    initial = generate_proposals(snapshot, plan, ledger, as_of=NOW)
    reviewed = review_proposal(
        initial,
        reviewer="fixture-reviewer",
        expected_input_fingerprint=initial.input_fingerprint,
        at=NOW,
    )

    disabled = plan.model_copy(update={"enabled": False})
    disabled_batch = generate_proposals(
        snapshot,
        disabled,
        build_ledger(snapshot, disabled),
        as_of=NOW,
        previous=reviewed,
    )
    assert disabled_batch.review_status is ReviewStatus.PENDING
    assert all(item.quantity == Decimal("0") for item in disabled_batch.items)
    assert BlockCode.PLAN_DISABLED.value in disabled_batch.blocking_reasons

    independent = _plan(
        _layer("disabled-layer", sequence=1).model_copy(update={"enabled": False}),
        _layer("enabled-layer", sequence=2, require_review=False),
    )
    independent_batch = generate_proposals(
        snapshot,
        independent,
        build_ledger(snapshot, independent),
        as_of=NOW,
    )
    assert {item.layer_id for item in independent_batch.items} == {"enabled-layer"}
    assert independent_batch.items[0].quantity > Decimal("0")


def test_failed_sync_state_invalidates_even_when_snapshot_exists() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    ledger = build_ledger(snapshot, plan)
    state = SyncState(
        account_id=snapshot.account.account_id,
        last_success_snapshot_id=snapshot.snapshot_id,
        complete=False,
    )
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW, sync_state=state)
    assert batch.status is ProposalStatus.BLOCKED
    assert "SYNC_FAILED" in batch.blocking_reasons


def test_foreign_ledger_account_blocks_before_proposals() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    foreign_ledger = build_ledger(snapshot, plan).model_copy(
        update={"account_id": "another-fictional-account"}
    )

    batch = generate_proposals(snapshot, plan, foreign_ledger, as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert BlockCode.LEDGER_ACCOUNT_MISMATCH.value in batch.blocking_reasons


def test_foreign_ledger_cannot_hide_behind_unmet_trigger() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    foreign_ledger = build_ledger(snapshot, plan).model_copy(
        update={"account_id": "another-fictional-account"}
    )

    batch = generate_proposals(snapshot, plan, foreign_ledger, as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED


def test_old_snapshot_blocks_even_when_other_inputs_are_fresh() -> None:
    snapshot = make_snapshot(business_time=NOW - timedelta(days=365))
    plan = _plan(_layer(require_review=False))
    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert BlockCode.SNAPSHOT_STALE.value in batch.blocking_reasons


def test_stale_quote_blocks_the_batch_and_surfaces_item_reason() -> None:
    fresh = make_snapshot()
    stale_quote = fresh.quotes[0].model_copy(
        update={"quote_time": NOW - timedelta(hours=1)}
    )
    snapshot = fresh.model_copy(update={"quotes": (stale_quote,)})
    plan = _plan(_layer(require_review=False))

    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)

    assert batch.status is ProposalStatus.BLOCKED
    assert batch.items[0].status is ProposalStatus.BLOCKED
    assert BlockCode.QUOTE_STALE.value in batch.blocking_reasons
    assert BlockCode.QUOTE_STALE.value in batch.items[0].reasons


def test_invalidated_proposal_cannot_be_reviewed_again() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)
    invalidated = batch.model_copy(update={"status": ProposalStatus.INVALIDATED})

    with pytest.raises(StaleProposalError):
        review_proposal(
            invalidated,
            reviewer="fictional-reviewer",
            expected_input_fingerprint=batch.input_fingerprint,
            at=NOW,
        )


def test_old_proposal_cannot_be_reviewed() -> None:
    snapshot = make_snapshot()
    plan = _plan(_layer(require_review=False))
    batch = generate_proposals(snapshot, plan, build_ledger(snapshot, plan), as_of=NOW)
    old_batch = batch.model_copy(update={"generated_at": NOW - timedelta(days=30)})

    with pytest.raises(StaleProposalError):
        review_proposal(
            old_batch,
            reviewer="fictional-reviewer",
            expected_input_fingerprint=batch.input_fingerprint,
            at=NOW,
        )
