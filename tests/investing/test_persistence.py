from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal

import pytest

from quant_stack.brokerage.contracts import SyncAttempt, SyncStatus
from quant_stack.brokerage.sync import PrivateSnapshotStore
from quant_stack.brokerage_cli import _current_sample_view
from quant_stack.investing.budgets import (
    Attribution,
    build_ledger,
    load_ledger,
    persist_ledger,
)
from quant_stack.investing.plans import InvestmentPlan
from quant_stack.investing.proposals import (
    StaleProposalError,
    generate_proposals,
    load_current_proposal,
    persist_ledger_and_proposal,
    persist_plan,
    persist_proposal,
    review_proposal,
)

from .conftest import NOW, make_snapshot, make_trade


def test_ledger_and_current_proposal_survive_restart(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-1",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "100.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    snapshot = make_snapshot()
    ledger = build_ledger(snapshot, plan)
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id="attempt:restart",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=snapshot.received_at,
            finished_at=snapshot.received_at,
            batch_id=snapshot.sync_batch_id,
        ),
    )
    persist_ledger(store, ledger)
    persist_plan(store, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=snapshot.business_time)
    persist_proposal(store, batch, commit_at=snapshot.business_time)

    restarted = PrivateSnapshotStore(runtime, source_root=source_root)
    assert load_ledger(restarted, "fictional-account").ledger_version == ledger.ledger_version
    assert load_current_proposal(restarted, "fictional-account").batch_id == batch.batch_id


def test_manual_attribution_revision_replaces_persisted_ledger_entry(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        etf_plans=(
            {
                "plan_id": "fictional-etf",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "amount_per_period": "100.00",
                "start_date": "2026-01-01",
                "timezone": "UTC",
                "execution_day": 1,
            },
        ),
    )
    transaction, fill = make_trade(amount="60.00")
    snapshot = make_snapshot(transactions=(transaction,), fills=(fill,))
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    _publish_success(store, snapshot, attempt_id="attempt:manual-attribution")
    first = build_ledger(snapshot, plan)
    persist_ledger(store, first)
    persist_plan(store, plan)
    first_batch = generate_proposals(snapshot, plan, first, as_of=snapshot.business_time)
    persist_proposal(store, first_batch, commit_at=snapshot.business_time)
    assert load_current_proposal(store, "fictional-account").batch_id == first_batch.batch_id
    assert load_ledger(store, "fictional-account").unattributed_evidence

    attribution = Attribution(
        evidence_id=f"execution:{fill.execution_id}",
        plan_id="fictional-etf",
        reason="fictional manual attribution correction",
        recorded_at=snapshot.received_at,
        recorded_by="fictional-reviewer",
        supersedes=None,
    )
    revised = build_ledger(
        snapshot,
        plan,
        prior=first,
        attributions={transaction.transaction_id: attribution},
    )
    persist_ledger(store, revised)
    restarted = PrivateSnapshotStore(runtime, source_root=source_root)
    loaded = load_ledger(restarted, "fictional-account")
    assert loaded.ledger_version == revised.ledger_version
    assert loaded.spent_for("fictional-etf") == Decimal("60.00")
    assert loaded.unattributed_evidence == ()
    assert loaded.attributions[0].recorded_by == "fictional-reviewer"
    assert load_current_proposal(restarted, "fictional-account") is None


def test_ledger_and_proposal_commit_rolls_back_together(tmp_path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-1",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "100.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    snapshot = make_snapshot()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id="attempt:rollback",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=snapshot.received_at,
            finished_at=snapshot.received_at,
            batch_id=snapshot.sync_batch_id,
        ),
    )
    ledger = build_ledger(snapshot, plan)
    persist_plan(store, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=snapshot.business_time)
    original_transaction = store.transaction

    @contextmanager
    def crashing_transaction():
        with original_transaction() as transaction:
            yield transaction
            raise RuntimeError("simulated commit failure")

    monkeypatch.setattr(store, "transaction", crashing_transaction)
    with pytest.raises(RuntimeError, match="simulated commit failure"):
        persist_ledger_and_proposal(
            store,
            ledger,
            batch,
            plan=plan,
            commit_at=snapshot.business_time,
        )

    assert load_ledger(store, "fictional-account") is None
    assert load_current_proposal(store, "fictional-account") is None


def _publish_success(store, snapshot, *, attempt_id: str, finished_at=None) -> None:
    finished_at = finished_at or snapshot.received_at
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id=attempt_id,
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=snapshot.received_at,
            finished_at=finished_at,
            batch_id=snapshot.sync_batch_id,
        ),
    )


def test_cross_snapshot_recompute_uses_explicit_prior_ledger_cas(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-1",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "100.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    snapshot_a = make_snapshot(snapshot_id="snapshot-a")
    snapshot_b = make_snapshot(snapshot_id="snapshot-b")
    snapshot_c = make_snapshot(snapshot_id="snapshot-c")
    store = PrivateSnapshotStore(runtime, source_root=source_root)

    _publish_success(store, snapshot_a, attempt_id="attempt:a")
    ledger_a = build_ledger(snapshot_a, plan, clock=lambda: NOW)
    batch_a = generate_proposals(snapshot_a, plan, ledger_a, as_of=NOW)
    persist_ledger_and_proposal(
        store,
        ledger_a,
        batch_a,
        plan=plan,
        commit_at=NOW,
    )

    _publish_success(store, snapshot_b, attempt_id="attempt:b")
    prior = load_ledger(store, "fictional-account")
    assert prior is not None
    ledger_b = build_ledger(snapshot_b, plan, prior=prior, clock=lambda: NOW)
    batch_b = generate_proposals(snapshot_b, plan, ledger_b, as_of=NOW)
    persist_ledger_and_proposal(
        store,
        ledger_b,
        batch_b,
        plan=plan,
        expected_prior_ledger=prior,
        commit_at=NOW,
    )
    assert load_ledger(store, "fictional-account").ledger_version == ledger_b.ledger_version
    assert load_current_proposal(store, "fictional-account").batch_id == batch_b.batch_id

    # A caller that calculated against stale ledger A cannot overwrite the
    # current ledger B, even after the synchronized snapshot has advanced.
    _publish_success(store, snapshot_c, attempt_id="attempt:c")
    ledger_c = build_ledger(snapshot_c, plan, prior=ledger_b, clock=lambda: NOW)
    batch_c = generate_proposals(snapshot_c, plan, ledger_c, as_of=NOW)
    with pytest.raises(StaleProposalError, match="current ledger changed"):
        persist_ledger_and_proposal(
            store,
            ledger_c,
            batch_c,
            plan=plan,
            expected_prior_ledger=prior,
            commit_at=NOW,
        )


def test_persisted_review_cannot_cross_a_failed_sync_generation(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-generation",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "100.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    snapshot = make_snapshot()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    _publish_success(store, snapshot, attempt_id="attempt:generation-success")
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)
    persist_ledger_and_proposal(store, ledger, batch, plan=plan, commit_at=NOW)
    reviewed = review_proposal(
        batch,
        reviewer="fixture-reviewer",
        expected_input_fingerprint=batch.input_fingerprint,
        at=NOW,
    )
    persist_proposal(store, reviewed, commit_at=NOW)

    failure_at = NOW + timedelta(minutes=1)
    store.record_failure(
        SyncAttempt(
            attempt_id="attempt:generation-failure",
            account_id=snapshot.account.account_id,
            status=SyncStatus.FAILED,
            started_at=failure_at,
            finished_at=failure_at,
            failure_sections=("quotes",),
            error_code="FIXTURE_FAILURE",
            error_message="fixture quote section unavailable",
        )
    )
    recovery_at = NOW + timedelta(minutes=2)
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id="attempt:generation-recovery",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=recovery_at,
            finished_at=recovery_at,
            batch_id=snapshot.sync_batch_id,
        ),
    )

    with pytest.raises(StaleProposalError, match="sync failure generation"):
        persist_proposal(store, reviewed, commit_at=recovery_at)


def test_commit_quote_age_uses_injected_commit_clock(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-1",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "1000.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    snapshot = make_snapshot()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    _publish_success(store, snapshot, attempt_id="attempt:quote-clock")
    ledger = build_ledger(snapshot, plan, clock=lambda: NOW)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)

    with pytest.raises(StaleProposalError, match="quote is no longer usable"):
        persist_ledger_and_proposal(
            store,
            ledger,
            batch,
            plan=plan,
            commit_at=NOW + timedelta(minutes=30),
        )


def test_late_window_facts_are_included_before_ledger_recompute(tmp_path) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            {
                "layer_id": "layer-1",
                "account_id": "fictional-account",
                "symbol": "QXEN",
                "budget": "1000.00",
                "trigger_price": "101.00",
                "total_position_limit": "10",
                "require_review": False,
            },
        ),
    )
    current = make_snapshot(snapshot_id="current-window", business_time=NOW)
    transaction, fill = make_trade(amount="60.00")
    late = make_snapshot(
        snapshot_id="late-window",
        business_time=NOW - timedelta(hours=1),
        transactions=(transaction,),
        fills=(fill,),
    )
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    _publish_success(store, current, attempt_id="attempt:current")
    _publish_success(
        store,
        late,
        attempt_id="attempt:late",
        finished_at=NOW - timedelta(hours=1),
    )

    cumulative, state = _current_sample_view(store, "fictional-account")
    assert cumulative is not None
    assert cumulative.snapshot_id == current.snapshot_id
    assert state.last_success_snapshot_id == current.snapshot_id
    assert [item.transaction_id for item in cumulative.transactions] == [transaction.transaction_id]

    ledger = build_ledger(
        cumulative,
        plan,
        attributions={transaction.transaction_id: "layer-1"},
        clock=lambda: NOW,
    )
    assert ledger.spent_for("layer-1", layer_id="layer-1") == Decimal("60.00")
    batch = generate_proposals(cumulative, plan, ledger, as_of=NOW, sync_state=state)
    assert batch.status.value == "blocked"
    with pytest.raises(StaleProposalError, match="complete current sync"):
        persist_ledger_and_proposal(store, ledger, batch, plan=plan, commit_at=NOW)

    # A newer complete provider window that includes every pending immutable
    # fact clears the conservative rebuild gate.  The ledger can then consume
    # the same transaction/fill exactly once.
    rebuild_at = NOW + timedelta(minutes=1)
    rebuild = make_snapshot(
        snapshot_id="complete-rebuild",
        business_time=rebuild_at,
        transactions=(transaction,),
        fills=(fill,),
    )
    _publish_success(
        store,
        rebuild,
        attempt_id="attempt:complete-rebuild",
        finished_at=rebuild_at,
    )
    cumulative, state = _current_sample_view(store, "fictional-account")
    assert cumulative is not None
    assert cumulative.snapshot_id == rebuild.snapshot_id
    assert state.complete
    ledger = build_ledger(
        cumulative,
        plan,
        attributions={transaction.transaction_id: "layer-1"},
        clock=lambda: rebuild_at,
    )
    batch = generate_proposals(cumulative, plan, ledger, as_of=rebuild_at, sync_state=state)
    persist_ledger_and_proposal(store, ledger, batch, plan=plan, commit_at=rebuild_at)
    assert load_ledger(store, "fictional-account").facts_fingerprint == ledger.facts_fingerprint
