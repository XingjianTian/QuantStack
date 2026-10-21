"""Pure daily-data tolerance and planner-only backfill contracts.

This module deliberately has no provider, filesystem, database, scheduler, or publication
integration.  It is an offline decision surface for the proposed partial-data policy; the
production daily runner remains strict until a separate production Gate is consumed.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from math import ceil, isfinite

AUTO_PARTIAL_THRESHOLD = 0.98
HARD_FLOOR = 0.95
PARTIAL_TTL = timedelta(hours=24)
MIN_PROVIDER_FINALIZATION_AGE_MINUTES = 120.0
MAX_UNRESOLVED_CONCENTRATION = 0.05
MIN_SECTOR_SAMPLE_SIZE = 50
MAX_MISSING_WEIGHT_RATIO = 0.01


class ProviderOutcome(StrEnum):
    """The four outcomes that can be recorded for one requested identity."""

    RETURNED = "returned"
    NO_BAR = "no_bar"
    PROVIDER_EMPTY = "provider_empty"
    PROVIDER_ERROR = "provider_error"


class QualityStatus(StrEnum):
    COMPLETE = "complete"
    USABLE_PARTIAL = "usable_partial"
    USABLE_PARTIAL_EXCEPTION = "usable_partial_exception"
    BLOCKED = "blocked"


class RepairStatus(StrEnum):
    NONE = "none"
    BACKFILL_PENDING = "backfill_pending"
    QUARANTINED = "quarantined"


class GateStatus(StrEnum):
    READY = "ready"
    BLOCKED = "blocked"


class FreshnessStatus(StrEnum):
    FRESH = "fresh"
    STALE = "stale"


UNRESOLVED_OUTCOMES = frozenset(
    {
        ProviderOutcome.PROVIDER_EMPTY,
        ProviderOutcome.PROVIDER_ERROR,
    }
)


def minimum_returned_count(universe_size: int, threshold: float) -> int:
    """Return the smallest returned-row count that reaches ``threshold``."""

    if universe_size < 0:
        raise ValueError("universe_size cannot be negative")
    if not 0 < threshold <= 1:
        raise ValueError("threshold must be in the (0, 1] interval")
    return ceil(universe_size * threshold)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _sorted_ids(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(str(value) for value in values))


def _json_value(value: object) -> object:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        return _as_utc(value).isoformat().replace("+00:00", "Z")
    return value


@dataclass(frozen=True)
class DailyToleranceMetrics:
    """Machine-readable counters shared by assessment and reporting code."""

    market: str
    session: date
    snapshot_id: str
    requested_count: int
    returned_count: int
    no_bar_count: int
    unresolved_count: int
    bar_coverage: float
    exact_closure: float
    ledger_closure: float
    critical_unresolved_count: int
    max_unresolved_concentration: float
    missing_weight_ratio: float
    max_consecutive_unresolved: int
    schema_version: str = field(
        init=False, default="quantstack.daily-tolerance-metrics.v1"
    )

    def to_dict(self) -> dict[str, object]:
        return {
            key: _json_value(value)
            for key, value in {
                "schema_version": self.schema_version,
                "market": self.market,
                "session": self.session,
                "snapshot_id": self.snapshot_id,
                "requested_count": self.requested_count,
                "returned_count": self.returned_count,
                "no_bar_count": self.no_bar_count,
                "unresolved_count": self.unresolved_count,
                "bar_coverage": self.bar_coverage,
                "exact_closure": self.exact_closure,
                "ledger_closure": self.ledger_closure,
                "critical_unresolved_count": self.critical_unresolved_count,
                "max_unresolved_concentration": self.max_unresolved_concentration,
                "missing_weight_ratio": self.missing_weight_ratio,
                "max_consecutive_unresolved": self.max_consecutive_unresolved,
            }.items()
        }


@dataclass(frozen=True)
class DailyToleranceAssessment:
    """Orthogonal quality, repair, gate, and freshness states for one session."""

    market: str
    session: date
    snapshot_id: str
    quality_status: QualityStatus
    repair_status: RepairStatus
    gate_status: GateStatus
    freshness_status: FreshnessStatus
    returned_ids: tuple[str, ...]
    explicit_omission_ids: tuple[str, ...]
    unresolved_ids: tuple[str, ...]
    unresolved_outcomes: Mapping[str, str]
    alerts: tuple[str, ...]
    hard_failures: tuple[str, ...]
    metrics: DailyToleranceMetrics
    schema_version: str = field(
        init=False, default="quantstack.daily-tolerance-assessment.v1"
    )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "market": self.market,
            "session": self.session.isoformat(),
            "snapshot_id": self.snapshot_id,
            "quality_status": self.quality_status.value,
            "repair_status": self.repair_status.value,
            "gate_status": self.gate_status.value,
            "freshness_status": self.freshness_status.value,
            "returned_ids": list(self.returned_ids),
            "explicit_omission_ids": list(self.explicit_omission_ids),
            "unresolved_ids": list(self.unresolved_ids),
            "unresolved_outcomes": dict(sorted(self.unresolved_outcomes.items())),
            "alerts": list(self.alerts),
            "hard_failures": list(self.hard_failures),
            "metrics": self.metrics.to_dict(),
        }


def assess_daily_tolerance(
    *,
    market: str,
    session: date,
    snapshot_id: str,
    universe_ids: Iterable[str],
    outcomes: Mapping[str, ProviderOutcome | str],
    no_bar_evidence_ids: Iterable[str] = (),
    critical_ids: Iterable[str] = (),
    dependency_unresolved_ids: Iterable[str] = (),
    consecutive_unresolved: Mapping[str, int] | None = None,
    unresolved_concentration: Mapping[str, float] | None = None,
    sector_sample_sizes: Mapping[str, int] | None = None,
    missing_weight_ratio: float = 0.0,
    no_bar_anomaly: bool = False,
    session_closed: bool = True,
    ready_age_minutes: float = MIN_PROVIDER_FINALIZATION_AGE_MINUTES,
    manifest_valid: bool = True,
    partial_exception_approved: bool = False,
    stale: bool = False,
    quarantine: bool = False,
    validation_errors: Iterable[str] = (),
) -> DailyToleranceAssessment:
    """Assess a fixed-universe result without contacting a provider.

    ``outcomes`` is intentionally a per-identity mapping.  Missing identities remain unresolved
    and therefore cannot accidentally disappear through count-only arithmetic.  ``no_bar`` only
    becomes a legal omission when its identity is present in ``no_bar_evidence_ids``.
    """

    raw_universe = tuple(str(item) for item in universe_ids)
    universe = tuple(sorted(set(raw_universe)))
    universe_set = set(universe)
    duplicate_universe = len(raw_universe) != len(universe)
    normalized: dict[str, ProviderOutcome] = {}
    invalid_outcomes: dict[str, str] = {}
    for raw_id, raw_outcome in outcomes.items():
        identity = str(raw_id)
        try:
            normalized[identity] = ProviderOutcome(raw_outcome)
        except ValueError:
            invalid_outcomes[identity] = str(raw_outcome)

    outcome_ids = set(normalized) | set(invalid_outcomes)
    extra_ids = sorted(outcome_ids - universe_set)
    missing_ids = sorted(universe_set - outcome_ids)
    returned_ids = _sorted_ids(
        identity for identity in universe if normalized.get(identity) is ProviderOutcome.RETURNED
    )
    explicit_no_bar = set(str(item) for item in no_bar_evidence_ids)
    claimed_no_bar = {
        identity
        for identity in universe
        if normalized.get(identity) is ProviderOutcome.NO_BAR
    }
    omission_ids = _sorted_ids(claimed_no_bar & explicit_no_bar)
    no_bar_without_evidence = sorted(claimed_no_bar - explicit_no_bar)
    no_bar_evidence_outside_universe = sorted(explicit_no_bar - universe_set)
    no_bar_evidence_conflicts = sorted(explicit_no_bar & set(returned_ids))
    unresolved_ids = _sorted_ids(universe_set - set(returned_ids) - set(omission_ids))
    unresolved_outcomes = {
        identity: (
            normalized[identity].value
            if identity in normalized
            else "missing_outcome"
        )
        for identity in unresolved_ids
    }

    critical = set(str(item) for item in critical_ids)
    dependency_unresolved = set(str(item) for item in dependency_unresolved_ids)
    unresolved_set = set(unresolved_ids)
    critical_unresolved = sorted(critical & unresolved_set)
    dependency_gap = sorted(dependency_unresolved & unresolved_set)

    consecutive = {
        str(identity): int(count)
        for identity, count in (consecutive_unresolved or {}).items()
        if str(identity) in unresolved_set
    }
    max_consecutive = max(consecutive.values(), default=0)
    concentrations = {
        str(group): float(value)
        for group, value in (unresolved_concentration or {}).items()
    }
    max_concentration = max(concentrations.values(), default=0.0)
    invalid_concentration = any(
        not isfinite(value) or not 0 <= value <= 1
        for value in concentrations.values()
    )
    sector_sizes = {
        str(group): int(value) for group, value in (sector_sample_sizes or {}).items()
    }

    requested_count = len(universe)
    returned_count = len(returned_ids)
    no_bar_count = len(omission_ids)
    unresolved_count = len(unresolved_ids)
    bar_coverage = returned_count / requested_count if requested_count else 0.0
    exact_closure = (
        (returned_count + no_bar_count) / requested_count if requested_count else 0.0
    )
    ledger_closure = (
        (returned_count + no_bar_count + unresolved_count) / requested_count
        if requested_count
        else 0.0
    )

    alerts: list[str] = []
    hard_failures: list[str] = []
    if requested_count == 0:
        hard_failures.append("empty_universe")
    if duplicate_universe:
        hard_failures.append("duplicate_universe_id")
    if extra_ids:
        hard_failures.append("outcome_identity_outside_universe")
    if missing_ids:
        hard_failures.append("missing_outcome_identity")
    if invalid_outcomes:
        hard_failures.append("invalid_provider_outcome")
    if no_bar_without_evidence:
        hard_failures.append("no_bar_without_explicit_evidence")
    if no_bar_evidence_outside_universe:
        hard_failures.append("no_bar_evidence_outside_universe")
    if no_bar_evidence_conflicts:
        hard_failures.append("no_bar_evidence_conflict")
    if abs(ledger_closure - 1.0) > 1e-12:
        hard_failures.append("ledger_not_closed")
    if not session_closed:
        hard_failures.append("session_not_closed")
    if (
        not isfinite(ready_age_minutes)
        or ready_age_minutes < MIN_PROVIDER_FINALIZATION_AGE_MINUTES
    ):
        hard_failures.append("provider_finalization_wait_incomplete")
    if not manifest_valid:
        hard_failures.append("manifest_or_readback_invalid")
    if critical_unresolved:
        hard_failures.append("critical_dependency_unresolved")
    if dependency_gap:
        hard_failures.append("required_calculation_dependency_unresolved")
    if invalid_concentration:
        hard_failures.append("unresolved_concentration_invalid")
    elif max_concentration > MAX_UNRESOLVED_CONCENTRATION:
        hard_failures.append("unresolved_concentration_exceeded")
    if any(value < MIN_SECTOR_SAMPLE_SIZE for value in sector_sizes.values()):
        hard_failures.append("sector_sample_below_minimum")
    if not isfinite(missing_weight_ratio) or not 0 <= missing_weight_ratio <= 1:
        hard_failures.append("missing_weight_ratio_invalid")
    elif missing_weight_ratio > MAX_MISSING_WEIGHT_RATIO:
        hard_failures.append("missing_weight_ratio_exceeded")
    if no_bar_anomaly:
        hard_failures.append("no_bar_anomaly")
    if max_consecutive >= 3:
        hard_failures.append("three_consecutive_unresolved_sessions")
    elif max_consecutive >= 2:
        alerts.append("two_consecutive_unresolved_sessions")
    if validation_errors:
        hard_failures.extend(f"validation:{str(error)}" for error in validation_errors)
    if quarantine:
        hard_failures.append("quarantine_requested")
    if stale:
        hard_failures.append("stale_ttl_expired")

    if unresolved_count:
        alerts.append("backfill_pending")
    if unresolved_count and bar_coverage < AUTO_PARTIAL_THRESHOLD:
        alerts.append("below_auto_partial_threshold")
    if unresolved_count and bar_coverage < HARD_FLOOR:
        hard_failures.append("coverage_below_hard_floor")
    elif (
        unresolved_count
        and bar_coverage < AUTO_PARTIAL_THRESHOLD
        and not partial_exception_approved
    ):
        hard_failures.append("partial_exception_not_approved")

    if not hard_failures and unresolved_count == 0 and exact_closure == 1.0:
        quality_status = QualityStatus.COMPLETE
    elif not hard_failures and bar_coverage >= AUTO_PARTIAL_THRESHOLD:
        quality_status = QualityStatus.USABLE_PARTIAL
    elif (
        not hard_failures
        and bar_coverage >= HARD_FLOOR
        and partial_exception_approved
    ):
        quality_status = QualityStatus.USABLE_PARTIAL_EXCEPTION
    else:
        quality_status = QualityStatus.BLOCKED

    freshness_status = FreshnessStatus.STALE if stale else FreshnessStatus.FRESH
    repair_status = (
        RepairStatus.QUARANTINED
        if quarantine or hard_failures and any(
            failure
            in {
                "duplicate_universe_id",
                "outcome_identity_outside_universe",
                "missing_outcome_identity",
                "invalid_provider_outcome",
                "no_bar_without_explicit_evidence",
                "no_bar_evidence_outside_universe",
                "no_bar_evidence_conflict",
                "manifest_or_readback_invalid",
                "quarantine_requested",
            }
            for failure in hard_failures
        )
        else RepairStatus.BACKFILL_PENDING
        if unresolved_count
        else RepairStatus.NONE
    )
    gate_status = (
        GateStatus.READY
        if quality_status is not QualityStatus.BLOCKED and not hard_failures
        else GateStatus.BLOCKED
    )

    metrics = DailyToleranceMetrics(
        market=market,
        session=session,
        snapshot_id=snapshot_id,
        requested_count=requested_count,
        returned_count=returned_count,
        no_bar_count=no_bar_count,
        unresolved_count=unresolved_count,
        bar_coverage=bar_coverage,
        exact_closure=exact_closure,
        ledger_closure=ledger_closure,
        critical_unresolved_count=len(critical_unresolved),
        max_unresolved_concentration=max_concentration,
        missing_weight_ratio=missing_weight_ratio,
        max_consecutive_unresolved=max_consecutive,
    )
    return DailyToleranceAssessment(
        market=market,
        session=session,
        snapshot_id=snapshot_id,
        quality_status=quality_status,
        repair_status=repair_status,
        gate_status=gate_status,
        freshness_status=freshness_status,
        returned_ids=returned_ids,
        explicit_omission_ids=omission_ids,
        unresolved_ids=unresolved_ids,
        unresolved_outcomes=unresolved_outcomes,
        alerts=tuple(sorted(set(alerts))),
        hard_failures=tuple(sorted(set(hard_failures))),
        metrics=metrics,
    )


@dataclass(frozen=True)
class BackfillBudget:
    """Planner-only weekly limits; they are not provider or scheduler configuration."""

    max_symbols: int = 250
    max_symbol_sessions: int = 1000
    max_http_requests: int = 100
    max_attempts_per_item: int = 3

    def __post_init__(self) -> None:
        if any(
            value < 1
            for value in (
                self.max_symbols,
                self.max_symbol_sessions,
                self.max_http_requests,
                self.max_attempts_per_item,
            )
        ):
            raise ValueError("backfill budget values must be positive")

    def to_dict(self) -> dict[str, int]:
        return {
            "max_symbols": self.max_symbols,
            "max_symbol_sessions": self.max_symbol_sessions,
            "max_http_requests": self.max_http_requests,
            "max_attempts_per_item": self.max_attempts_per_item,
        }


@dataclass(frozen=True)
class BackfillCandidate:
    """One exact unresolved symbol-session candidate for the planner."""

    market: str
    session: date
    instrument_id: str
    snapshot_id: str
    dataset: str
    price_basis: str
    outcome: ProviderOutcome | str
    reason: str
    priority: int = 0
    consecutive_unresolved: int = 0
    critical: bool = False
    attempts: int = 0
    estimated_http_requests: int = 1

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value and "|" not in value
            for value in (
                self.market,
                self.instrument_id,
                self.snapshot_id,
                self.dataset,
                self.price_basis,
                self.reason,
            )
        ):
            raise ValueError("backfill candidate fields must be non-empty and pipe-safe")
        if self.attempts < 0 or self.consecutive_unresolved < 0:
            raise ValueError("backfill attempts and consecutive count cannot be negative")
        if self.estimated_http_requests < 1:
            raise ValueError("estimated_http_requests must be positive")
        try:
            ProviderOutcome(self.outcome)
        except ValueError as error:
            raise ValueError(f"unsupported backfill outcome: {self.outcome}") from error

    @property
    def normalized_outcome(self) -> ProviderOutcome:
        return ProviderOutcome(self.outcome)

    @property
    def idempotency_key(self) -> str:
        return (
            "backfill:v1|"
            f"market={self.market}|session={self.session.isoformat()}|"
            f"instrument={self.instrument_id}|snapshot={self.snapshot_id}|"
            f"dataset={self.dataset}|price_basis={self.price_basis}"
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "session": self.session.isoformat(),
            "instrument_id": self.instrument_id,
            "snapshot_id": self.snapshot_id,
            "dataset": self.dataset,
            "price_basis": self.price_basis,
            "outcome": self.normalized_outcome.value,
            "reason": self.reason,
            "priority": self.priority,
            "consecutive_unresolved": self.consecutive_unresolved,
            "critical": self.critical,
            "attempts": self.attempts,
            "attempt_no": self.attempts + 1,
            "estimated_http_requests": self.estimated_http_requests,
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class WeekendBackfillPlan:
    """Deterministic planner output; it has no execution or persistence behavior."""

    plan_id: str
    planned_at: datetime
    expires_at: datetime
    planner_only: bool
    plan_revision: str
    budget: BackfillBudget
    items: tuple[BackfillCandidate, ...]
    deferred: tuple[tuple[str, str], ...]
    excluded: tuple[tuple[str, str], ...]
    estimated_http_requests: int
    schema_version: str = field(init=False, default="quantstack.daily-backfill-plan.v1")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "planned_at": self.planned_at.isoformat().replace("+00:00", "Z"),
            "expires_at": self.expires_at.isoformat().replace("+00:00", "Z"),
            "planner_only": self.planner_only,
            "plan_revision": self.plan_revision,
            "budget": self.budget.to_dict(),
            "items": [item.to_dict() for item in self.items],
            "deferred": [
                {"idempotency_key": key, "reason": reason}
                for key, reason in self.deferred
            ],
            "excluded": [
                {"idempotency_key": key, "reason": reason}
                for key, reason in self.excluded
            ],
            "estimated_http_requests": self.estimated_http_requests,
        }


def _expiry_for_plan(planned_at: datetime, next_regular_open: datetime | None) -> datetime:
    default_expiry = planned_at + PARTIAL_TTL
    if next_regular_open is None:
        return default_expiry
    market_open_expiry = _as_utc(next_regular_open) - timedelta(hours=2)
    if market_open_expiry <= planned_at:
        raise ValueError("next_regular_open must leave a positive planning window")
    return min(default_expiry, market_open_expiry)


def build_weekend_backfill_plan(
    candidates: Iterable[BackfillCandidate],
    *,
    planned_at: datetime,
    next_regular_open: datetime | None = None,
    budget: BackfillBudget | None = None,
    plan_revision: str = "r8",
) -> WeekendBackfillPlan:
    """Build a deterministic, planner-only request from exact unresolved candidates.

    Legal ``no_bar`` observations and already returned identities are retained in ``excluded``
    evidence, never converted into request items.  Budget overflow is retained in ``deferred`` so
    the weekly report can distinguish it from a provider failure.
    """

    planned = _as_utc(planned_at)
    limits = budget or BackfillBudget()
    unique: dict[str, BackfillCandidate] = {}
    for candidate in candidates:
        key = candidate.idempotency_key
        previous = unique.get(key)
        if previous is not None:
            if previous != candidate:
                raise ValueError(f"conflicting duplicate backfill candidate: {key}")
            continue
        unique[key] = candidate

    eligible: list[BackfillCandidate] = []
    excluded: list[tuple[str, str]] = []
    for key, candidate in unique.items():
        outcome = candidate.normalized_outcome
        if outcome is ProviderOutcome.NO_BAR:
            excluded.append((key, "legal_no_bar_not_backfillable"))
        elif outcome is ProviderOutcome.RETURNED:
            excluded.append((key, "already_returned"))
        elif outcome not in UNRESOLVED_OUTCOMES:
            excluded.append((key, "unsupported_unresolved_outcome"))
        elif candidate.attempts >= limits.max_attempts_per_item:
            excluded.append((key, "attempt_budget_exhausted"))
        else:
            eligible.append(candidate)

    ordered = sorted(
        eligible,
        key=lambda item: (
            not item.critical,
            -item.consecutive_unresolved,
            item.session,
            -item.priority,
            item.instrument_id,
            item.idempotency_key,
        ),
    )
    selected: list[BackfillCandidate] = []
    deferred: list[tuple[str, str]] = []
    selected_symbols: set[str] = set()
    estimated_requests = 0
    for candidate in ordered:
        if len(selected) >= limits.max_symbol_sessions:
            deferred.append((candidate.idempotency_key, "symbol_session_budget"))
            continue
        new_symbol = candidate.instrument_id not in selected_symbols
        if new_symbol and len(selected_symbols) >= limits.max_symbols:
            deferred.append((candidate.idempotency_key, "symbol_budget"))
            continue
        if estimated_requests + candidate.estimated_http_requests > limits.max_http_requests:
            deferred.append((candidate.idempotency_key, "http_request_budget"))
            continue
        selected.append(candidate)
        selected_symbols.add(candidate.instrument_id)
        estimated_requests += candidate.estimated_http_requests

    selected_keys = [item.idempotency_key for item in selected]
    digest_payload = {
        "planned_date": planned.date().isoformat(),
        "plan_revision": plan_revision,
        "items": selected_keys,
    }
    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    return WeekendBackfillPlan(
        plan_id=f"daily-backfill-{planned.date().isoformat()}-{digest}",
        planned_at=planned,
        expires_at=_expiry_for_plan(planned, next_regular_open),
        planner_only=True,
        plan_revision=plan_revision,
        budget=limits,
        items=tuple(selected),
        deferred=tuple(sorted(deferred)),
        excluded=tuple(sorted(excluded)),
        estimated_http_requests=estimated_requests,
    )
