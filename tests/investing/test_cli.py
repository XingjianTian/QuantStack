import json
from datetime import timedelta

import pytest

from quant_stack.brokerage.contracts import SyncAttempt, SyncStatus
from quant_stack.brokerage.sync import PrivateSnapshotStore
from quant_stack.brokerage_cli import main
from quant_stack.investing.budgets import build_ledger
from quant_stack.investing.plans import EquityLayer, InvestmentPlan
from quant_stack.investing.proposals import (
    generate_proposals,
    persist_ledger_and_proposal,
    persist_plan,
    persist_proposal,
    review_proposal,
)

from .conftest import NOW, make_snapshot


def test_fixture_cli_sync_and_proposal_are_machine_readable(tmp_path, capsys) -> None:
    state_root = str(tmp_path / "runtime")
    assert (
        main(
            [
                "--state-root",
                state_root,
                "sync-sample",
                "--at",
                "2026-10-16T00:00:00+00:00",
            ]
        )
        == 0
    )
    sync_output = json.loads(capsys.readouterr().out)
    assert sync_output["snapshot"]["complete"] is True
    assert sync_output["snapshot"]["non_executable"] is True

    assert (
        main(
            [
                "--state-root",
                state_root,
                "propose-sample",
                "--at",
                "2026-10-16T00:00:00+00:00",
            ]
        )
        == 2
    )
    proposal_output = json.loads(capsys.readouterr().out)
    assert proposal_output["non_executable"] is True
    assert proposal_output["status"] == "blocked"


def test_fixture_cli_has_no_live_switch() -> None:
    with pytest.raises(SystemExit):
        main(["--live", "sync-sample"])


def test_ready_profile_produces_a_reviewable_but_non_executable_batch(
    tmp_path, capsys
) -> None:
    state_root = str(tmp_path / "ready-runtime")
    args = [
        "--profile",
        "ready",
        "--state-root",
        state_root,
        "propose-sample",
        "--at",
        "2026-10-16T00:00:00+00:00",
    ]
    assert main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "ready"
    assert first["blocking_reasons"] == []
    assert first["non_executable"] is True
    assert first["fictional"] is True
    assert any(item["quantity"] != "0" for item in first["items"])

    assert main(args) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["batch_id"] == first["batch_id"]
    assert second["ledger_version"] == first["ledger_version"]


def test_ready_profile_with_stale_quote_blocks_the_whole_batch(tmp_path, capsys) -> None:
    state_root = str(tmp_path / "ready-stale-runtime")
    ready_args = ["--profile", "ready", "--state-root", state_root]

    assert (
        main(
            [
                *ready_args,
                "propose-sample",
                "--at",
                "2026-10-16T00:00:00+00:00",
            ]
        )
        == 0
    )
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "ready"

    assert (
        main(
            [
                *ready_args,
                "propose-sample",
                "--at",
                "2026-10-16T01:00:00+00:00",
            ]
        )
        == 2
    )
    second = json.loads(capsys.readouterr().out)
    assert second["status"] == "blocked"
    assert "QUOTE_STALE" in second["blocking_reasons"]
    assert any(
        item["status"] == "blocked" and "QUOTE_STALE" in item["reasons"]
        for item in second["items"]
    )


def test_show_state_hides_proposal_after_sync_snapshot_moves(tmp_path, capsys) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            EquityLayer(
                layer_id="layer-1",
                account_id="fictional-account",
                symbol="QXEN",
                budget="100.00",
                trigger_price="101.00",
                total_position_limit="10",
                require_review=False,
            ),
        ),
    )
    first = make_snapshot(snapshot_id="cli-state-first", business_time=NOW)
    store.publish(
        first,
        SyncAttempt(
            attempt_id="attempt:cli-state-first",
            account_id=first.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=first.received_at,
            finished_at=first.received_at,
            batch_id=first.sync_batch_id,
        ),
    )
    ledger = build_ledger(first, plan, clock=lambda: NOW)
    batch = generate_proposals(first, plan, ledger, as_of=NOW)
    persist_ledger_and_proposal(store, ledger, batch, plan=plan, commit_at=NOW)

    second_at = NOW + timedelta(minutes=1)
    second = make_snapshot(snapshot_id="cli-state-second", business_time=second_at)
    store.publish(
        second,
        SyncAttempt(
            attempt_id="attempt:cli-state-second",
            account_id=second.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=second.received_at,
            finished_at=second_at,
            batch_id=second.sync_batch_id,
        ),
    )

    assert (
        main(
            [
                "--state-root",
                str(runtime),
                "show-state",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["snapshot_id"] == second.snapshot_id
    assert output["current_proposal"] is None
    assert output["current_proposal_state"] == "stale"
    assert output["stale_proposal_batch_id"] == batch.batch_id

    assert (
        main(
            [
                "--state-root",
                str(runtime),
                "show-state",
                "--at",
                "2026-10-17T00:00:00+00:00",
            ]
        )
        == 0
    )
    stale_output = json.loads(capsys.readouterr().out)
    assert stale_output["current_proposal"] is None
    assert stale_output["current_proposal_state"] == "stale"
    assert any(
        reason.startswith("QUOTE_STALE:")
        for reason in stale_output["proposal_freshness_reasons"]
    )

    assert (
        main(
            [
                "--state-root",
                str(runtime),
                "show-state",
                "--at",
                "2026-10-18T00:00:00+00:00",
            ]
        )
        == 0
    )
    old_output = json.loads(capsys.readouterr().out)
    assert old_output["current_proposal"] is None
    assert old_output["current_proposal_state"] == "stale"
    assert "SNAPSHOT_STALE" in old_output["proposal_freshness_reasons"]


def test_show_state_does_not_restore_review_after_failed_same_snapshot_recovery(
    tmp_path, capsys
) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    snapshot = make_snapshot()
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id="attempt:review-generation-success",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=NOW,
            finished_at=NOW,
            batch_id=snapshot.sync_batch_id,
        ),
    )
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            EquityLayer(
                layer_id="layer-generation",
                account_id="fictional-account",
                symbol="QXEN",
                budget="100.00",
                trigger_price="101.00",
                total_position_limit="10",
                require_review=False,
            ),
        ),
    )
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
            attempt_id="attempt:review-generation-failure",
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
            attempt_id="attempt:review-generation-recovery",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=recovery_at,
            finished_at=recovery_at,
            batch_id=snapshot.sync_batch_id,
        ),
    )
    assert store.sync_failure_generation("fictional-account") == 1

    assert (
        main(
            [
                "--state-root",
                str(runtime),
                "show-state",
                "--at",
                recovery_at.isoformat(),
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["current_proposal"] is None
    assert output["current_proposal_state"] == "stale"
    assert output["stale_proposal_batch_id"] == reviewed.batch_id
    assert "SYNC_FAILURE_GENERATION_CHANGED" in output["proposal_freshness_reasons"]


def test_show_state_hides_proposal_after_persisted_plan_change(tmp_path, capsys) -> None:
    source_root = tmp_path / "source"
    runtime = tmp_path / "runtime"
    source_root.mkdir()
    store = PrivateSnapshotStore(runtime, source_root=source_root)
    snapshot = make_snapshot()
    store.publish(
        snapshot,
        SyncAttempt(
            attempt_id="attempt:plan-change",
            account_id=snapshot.account.account_id,
            status=SyncStatus.SUCCEEDED,
            started_at=NOW,
            finished_at=NOW,
            batch_id=snapshot.sync_batch_id,
        ),
    )
    plan = InvestmentPlan(
        account_id="fictional-account",
        equity_layers=(
            EquityLayer(
                layer_id="layer-plan-change",
                account_id="fictional-account",
                symbol="QXEN",
                budget="100.00",
                trigger_price="101.00",
                total_position_limit="10",
                require_review=False,
            ),
        ),
    )
    ledger = build_ledger(snapshot, plan)
    batch = generate_proposals(snapshot, plan, ledger, as_of=NOW)
    persist_ledger_and_proposal(store, ledger, batch, plan=plan, commit_at=NOW)
    persist_plan(store, plan.model_copy(update={"notes": "changed without recompute"}))

    assert (
        main(
            [
                "--state-root",
                str(runtime),
                "show-state",
                "--at",
                NOW.isoformat(),
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["current_proposal"] is None
    assert output["current_proposal_state"] == "unavailable"
