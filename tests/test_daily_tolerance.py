import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from quant_stack.data.daily.tolerance import (
    BackfillBudget,
    BackfillCandidate,
    GateStatus,
    ProviderOutcome,
    QualityStatus,
    RepairStatus,
    assess_daily_tolerance,
    build_weekend_backfill_plan,
    minimum_returned_count,
)

SESSION = date(2026, 9, 18)
SNAPSHOT = "us-equity-xnas-xnys-2026-08-27.v1"
ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "daily_tolerance"
SCHEMA_ROOT = ROOT / "schemas" / "data"


def _ids(count: int) -> list[str]:
    return [f"XNAS.TEST{index:04d}" for index in range(count)]


def _outcomes(
    count: int,
    *,
    returned: int,
    unresolved: ProviderOutcome = ProviderOutcome.PROVIDER_ERROR,
) -> tuple[list[str], dict[str, ProviderOutcome]]:
    ids = _ids(count)
    outcomes = {
        identity: ProviderOutcome.RETURNED if index < returned else unresolved
        for index, identity in enumerate(ids)
    }
    return ids, outcomes


def _candidate(
    instrument_id: str,
    *,
    session: date = SESSION,
    outcome: ProviderOutcome = ProviderOutcome.PROVIDER_ERROR,
    critical: bool = False,
    consecutive: int = 0,
    attempts: int = 0,
    priority: int = 0,
) -> BackfillCandidate:
    return BackfillCandidate(
        market="US",
        session=session,
        instrument_id=instrument_id,
        snapshot_id=SNAPSHOT,
        dataset="us_equity_daily_raw",
        price_basis="raw",
        outcome=outcome,
        reason="bounded_provider_failure",
        critical=critical,
        consecutive_unresolved=consecutive,
        attempts=attempts,
        priority=priority,
    )


def test_threshold_counts_match_fixed_5079_snapshot() -> None:
    assert minimum_returned_count(5_079, 0.98) == 4_978
    assert minimum_returned_count(5_079, 0.95) == 4_826


def test_exact_closure_is_complete_even_when_two_symbols_are_explicit_no_bar() -> None:
    ids, outcomes = _outcomes(10, returned=8, unresolved=ProviderOutcome.NO_BAR)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
        no_bar_evidence_ids=ids[8:],
    )

    assert assessment.quality_status is QualityStatus.COMPLETE
    assert assessment.repair_status is RepairStatus.NONE
    assert assessment.gate_status is GateStatus.READY
    assert assessment.metrics.exact_closure == 1.0
    assert assessment.metrics.bar_coverage == 0.8


def test_98_percent_provider_gaps_are_usable_partial_but_backfill_pending() -> None:
    ids, outcomes = _outcomes(100, returned=98)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
    )

    assert assessment.quality_status is QualityStatus.USABLE_PARTIAL
    assert assessment.repair_status is RepairStatus.BACKFILL_PENDING
    assert assessment.gate_status is GateStatus.READY
    assert assessment.metrics.bar_coverage == 0.98
    assert set(assessment.unresolved_outcomes.values()) == {"provider_error"}


def test_95_to_98_percent_requires_explicit_exception() -> None:
    ids, outcomes = _outcomes(100, returned=95, unresolved=ProviderOutcome.PROVIDER_EMPTY)
    blocked = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
    )
    approved = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
        partial_exception_approved=True,
    )

    assert blocked.quality_status is QualityStatus.BLOCKED
    assert "partial_exception_not_approved" in blocked.hard_failures
    assert approved.quality_status is QualityStatus.USABLE_PARTIAL_EXCEPTION
    assert approved.gate_status is GateStatus.READY


def test_provider_gap_in_critical_dependency_blocks_even_above_auto_threshold() -> None:
    ids, outcomes = _outcomes(100, returned=98)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
        critical_ids={ids[-1]},
    )

    assert assessment.quality_status is QualityStatus.BLOCKED
    assert assessment.gate_status is GateStatus.BLOCKED
    assert assessment.repair_status is RepairStatus.BACKFILL_PENDING
    assert "critical_dependency_unresolved" in assessment.hard_failures


def test_identity_and_readiness_failures_are_fail_closed() -> None:
    ids, outcomes = _outcomes(10, returned=9, unresolved=ProviderOutcome.PROVIDER_EMPTY)
    outcomes[ids[-1]] = "not_a_provider_outcome"  # type: ignore[assignment]
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids + [ids[0]],
        outcomes=outcomes,
        ready_age_minutes=60,
        session_closed=False,
        manifest_valid=False,
    )

    assert assessment.quality_status is QualityStatus.BLOCKED
    assert assessment.repair_status is RepairStatus.QUARANTINED
    assert "duplicate_universe_id" in assessment.hard_failures
    assert "invalid_provider_outcome" in assessment.hard_failures
    assert "session_not_closed" in assessment.hard_failures
    assert "provider_finalization_wait_incomplete" in assessment.hard_failures
    assert "manifest_or_readback_invalid" in assessment.hard_failures


def test_unproven_no_bar_never_creates_explicit_O() -> None:
    ids, outcomes = _outcomes(2, returned=1, unresolved=ProviderOutcome.NO_BAR)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
    )

    assert assessment.quality_status is QualityStatus.BLOCKED
    assert assessment.repair_status is RepairStatus.QUARANTINED
    assert assessment.explicit_omission_ids == ()
    assert assessment.unresolved_ids == (ids[-1],)
    assert assessment.unresolved_outcomes == {ids[-1]: "no_bar"}
    assert assessment.metrics.no_bar_count == 0
    assert assessment.metrics.unresolved_count == 1
    assert assessment.metrics.exact_closure == 0.5
    assert assessment.metrics.ledger_closure == 1.0
    assert "no_bar_without_explicit_evidence" in assessment.hard_failures


def test_no_bar_evidence_identity_conflicts_fail_closed() -> None:
    ids, outcomes = _outcomes(2, returned=2)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
        no_bar_evidence_ids=[ids[0], "XNAS.OUTSIDE"],
    )

    assert assessment.quality_status is QualityStatus.BLOCKED
    assert "no_bar_evidence_conflict" in assessment.hard_failures
    assert "no_bar_evidence_outside_universe" in assessment.hard_failures


def test_planner_excludes_no_bar_and_orders_critical_then_consecutive() -> None:
    planned_at = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    candidates = [
        _candidate("XNAS.NORMAL", priority=100),
        _candidate("XNAS.CRITICAL", critical=True),
        _candidate("XNAS.REPEATED", consecutive=2),
        _candidate("XNAS.NO_BAR", outcome=ProviderOutcome.NO_BAR),
        _candidate("XNAS.EXHAUSTED", attempts=3),
    ]
    plan = build_weekend_backfill_plan(
        candidates,
        planned_at=planned_at,
        budget=BackfillBudget(
            max_symbols=2,
            max_symbol_sessions=2,
            max_http_requests=2,
            max_attempts_per_item=3,
        ),
    )

    assert plan.planner_only is True
    assert [item.instrument_id for item in plan.items] == [
        "XNAS.CRITICAL",
        "XNAS.REPEATED",
    ]
    excluded = dict(plan.excluded)
    no_bar_key = next(
        item.idempotency_key for item in candidates if item.instrument_id == "XNAS.NO_BAR"
    )
    assert excluded[no_bar_key] == "legal_no_bar_not_backfillable"
    assert "attempt_budget_exhausted" in dict(plan.excluded).values()
    assert plan.estimated_http_requests == 2


def test_planner_is_idempotent_and_expires_before_next_open() -> None:
    planned_at = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    next_open = planned_at + timedelta(hours=10)
    candidates = [_candidate("XNAS.TEST")]
    first = build_weekend_backfill_plan(
        candidates,
        planned_at=planned_at,
        next_regular_open=next_open,
    )
    second = build_weekend_backfill_plan(
        candidates,
        planned_at=planned_at,
        next_regular_open=next_open,
    )

    assert first.plan_id == second.plan_id
    assert first.items[0].idempotency_key == second.items[0].idempotency_key
    assert first.expires_at == next_open - timedelta(hours=2)
    payload = first.to_dict()
    assert payload["schema_version"] == "quantstack.daily-backfill-plan.v1"
    assert payload["planner_only"] is True


def test_planner_conflicting_outcomes_rejected_in_all_input_orders() -> None:
    planned_at = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    unresolved = _candidate("XNAS.CONFLICT")
    no_bar = _candidate("XNAS.CONFLICT", outcome=ProviderOutcome.NO_BAR)

    for ordered_candidates in ([unresolved, no_bar], [no_bar, unresolved]):
        with pytest.raises(ValueError, match="conflicting duplicate backfill candidate"):
            build_weekend_backfill_plan(
                ordered_candidates,
                planned_at=planned_at,
            )


def test_planner_exhausted_duplicate_cannot_reenter_items() -> None:
    planned_at = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    exhausted = _candidate("XNAS.EXHAUSTED", attempts=3)
    fresh = _candidate("XNAS.EXHAUSTED", attempts=0)

    for ordered_candidates in ([exhausted, fresh], [fresh, exhausted]):
        with pytest.raises(ValueError, match="conflicting duplicate backfill candidate"):
            build_weekend_backfill_plan(
                ordered_candidates,
                planned_at=planned_at,
                budget=BackfillBudget(max_attempts_per_item=3),
            )


def test_planner_identical_duplicates_have_one_disposition() -> None:
    planned_at = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    selected = _candidate("XNAS.SELECTED")
    excluded = _candidate("XNAS.NO_BAR", outcome=ProviderOutcome.NO_BAR)
    deferred = _candidate("XNAS.DEFERRED")
    plan = build_weekend_backfill_plan(
        [selected, selected, excluded, excluded, deferred, deferred],
        planned_at=planned_at,
        budget=BackfillBudget(
            max_symbols=1,
            max_symbol_sessions=1,
            max_http_requests=1,
        ),
    )

    dispositions = (
        [item.idempotency_key for item in plan.items]
        + [key for key, _reason in plan.deferred]
        + [key for key, _reason in plan.excluded]
    )
    assert len(dispositions) == 3
    assert len(dispositions) == len(set(dispositions))
    assert set(dispositions) == {
        selected.idempotency_key,
        excluded.idempotency_key,
        deferred.idempotency_key,
    }


def test_json_fixtures_cover_complete_and_98_percent_partial_contracts() -> None:
    complete = json.loads(
        (FIXTURE_ROOT / "complete_with_no_bar.json").read_text(encoding="utf-8")
    )
    complete_assessment = assess_daily_tolerance(
        market=complete["market"],
        session=date.fromisoformat(complete["session"]),
        snapshot_id=complete["snapshot_id"],
        universe_ids=complete["universe_ids"],
        outcomes=complete["outcomes"],
        no_bar_evidence_ids=complete["no_bar_evidence_ids"],
    )
    assert complete_assessment.quality_status is QualityStatus.COMPLETE

    partial = json.loads(
        (FIXTURE_ROOT / "usable_partial_98.json").read_text(encoding="utf-8")
    )
    unresolved_id = next(
        identity for identity in partial["universe_ids"] if identity not in partial["returned_ids"]
    )
    partial_assessment = assess_daily_tolerance(
        market=partial["market"],
        session=date.fromisoformat(partial["session"]),
        snapshot_id=partial["snapshot_id"],
        universe_ids=partial["universe_ids"],
        outcomes={
            identity: "returned" for identity in partial["returned_ids"]
        }
        | {unresolved_id: partial["unresolved_outcome"]},
    )
    assert partial_assessment.quality_status is QualityStatus.USABLE_PARTIAL
    assert partial_assessment.metrics.bar_coverage == 0.98


def test_serialized_metrics_assessment_and_plan_match_json_schemas() -> None:
    ids, outcomes = _outcomes(100, returned=98)
    assessment = assess_daily_tolerance(
        market="US",
        session=SESSION,
        snapshot_id=SNAPSHOT,
        universe_ids=ids,
        outcomes=outcomes,
    )
    plan = build_weekend_backfill_plan(
        [_candidate("XNAS.TEST")],
        planned_at=datetime(2026, 9, 26, 10, 0, tzinfo=UTC),
    )

    for payload, schema_name in (
        (assessment.to_dict(), "daily_tolerance_assessment.schema.json"),
        (assessment.metrics.to_dict(), "daily_tolerance_metrics.schema.json"),
        (plan.to_dict(), "daily_backfill_plan.schema.json"),
    ):
        schema = json.loads((SCHEMA_ROOT / schema_name).read_text(encoding="utf-8"))
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
