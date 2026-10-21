from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, date, datetime
from typing import Literal

from pydantic import Field, computed_field, model_validator

from ..models import DataModel
from ..providers.cn.live import (
    BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT,
    BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY,
    BaoStockAccessRejected,
    BaoStockDailyBatchLimitExceeded,
    BaoStockDailyQuotaExceeded,
    configured_batch_interval_seconds,
    max_batches_per_day_for_interval,
)
from ..tasks import TaskState
from .models import BackfillPilotReport, BackfillShard, BackfillShardResult, BackfillUniverse
from .runner import BackfillPilotRunner, build_annual_batch_shards


class BackfillCampaignPlan(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    campaign_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    dataset_version: str = Field(min_length=1)
    universe_version: str = Field(min_length=1)
    universe_content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    requested_start: date
    requested_end: date
    batch_size: int = Field(ge=1, le=50)
    created_at: datetime
    shards: tuple[BackfillShard, ...] = Field(min_length=1)
    canonical_eligible: Literal[False] = False
    ss_candidate: Literal[False] = False
    production_pointer_updated: Literal[False] = False

    @model_validator(mode="after")
    def validate_plan(self) -> BackfillCampaignPlan:
        if self.requested_end < self.requested_start:
            raise ValueError("campaign end cannot precede start")
        shard_ids = [item.shard_id for item in self.shards]
        if len(shard_ids) != len(set(shard_ids)):
            raise ValueError("campaign shard IDs must be unique")
        if any(
            item.dataset_version != self.dataset_version
            or item.universe_version != self.universe_version
            or item.start_date < self.requested_start
            or item.end_date > self.requested_end
            for item in self.shards
        ):
            raise ValueError("campaign shards must bind its dataset, universe, and date range")
        return self

    @computed_field
    @property
    def content_sha256(self) -> str:
        payload = self.model_dump(mode="json", exclude_computed_fields=True)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class BackfillCampaignShardProgress(DataModel):
    shard_id: str
    task_id: str
    status: Literal["pending", "running", "retryable", "succeeded", "empty", "failed"]
    attempts: int = Field(ge=0)
    row_count: int = Field(ge=0)
    error_class: str | None = None


class BackfillCampaignProgress(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    campaign_id: str
    dataset_version: str
    universe_version: str
    observed_at: datetime
    shards: tuple[BackfillCampaignShardProgress, ...]
    planned_shards: int = Field(ge=1)
    pending_shards: int = Field(ge=0)
    running_shards: int = Field(ge=0)
    retryable_shards: int = Field(ge=0)
    succeeded_shards: int = Field(ge=0)
    empty_shards: int = Field(ge=0)
    failed_shards: int = Field(ge=0)
    total_rows: int = Field(ge=0)
    finalizable: bool

    @model_validator(mode="after")
    def validate_counts(self) -> BackfillCampaignProgress:
        counted = {
            status: sum(item.status == status for item in self.shards)
            for status in (
                "pending",
                "running",
                "retryable",
                "succeeded",
                "empty",
                "failed",
            )
        }
        expected = {
            "pending": self.pending_shards,
            "running": self.running_shards,
            "retryable": self.retryable_shards,
            "succeeded": self.succeeded_shards,
            "empty": self.empty_shards,
            "failed": self.failed_shards,
        }
        if counted != expected or sum(counted.values()) != self.planned_shards:
            raise ValueError("campaign progress counters do not match shard states")
        can_finalize = not any(
            (self.pending_shards, self.running_shards, self.retryable_shards, self.failed_shards)
        )
        if self.finalizable != can_finalize:
            raise ValueError("campaign finalizable flag does not match shard states")
        return self


class BackfillCampaignWaveReport(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    campaign_id: str
    started_at: datetime
    completed_at: datetime
    max_shards: int = Field(ge=1)
    selected_shards: int = Field(ge=0)
    results: tuple[BackfillShardResult, ...]
    progress: BackfillCampaignProgress
    wall_seconds: float = Field(ge=0)
    estimated_request_count: int = Field(ge=0)
    estimated_batch_count: int = Field(ge=0)
    safety_limits: dict[str, int | float] = Field(default_factory=dict)


class BackfillCampaignRequeueEntry(DataModel):
    shard_id: str
    task_id: str
    error_class: str
    previous_max_attempts: int = Field(ge=1, le=10)
    new_max_attempts: int = Field(ge=1, le=10)


class BackfillCampaignRequeueReport(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    campaign_id: str
    requeued_at: datetime
    allowed_error_classes: tuple[str, ...] = Field(min_length=1)
    additional_attempts: int = Field(ge=1, le=9)
    requeued_shards: int = Field(ge=0)
    skipped_failed_shards: int = Field(ge=0)
    entries: tuple[BackfillCampaignRequeueEntry, ...]

    @model_validator(mode="after")
    def validate_counts(self) -> BackfillCampaignRequeueReport:
        if self.requeued_shards != len(self.entries):
            raise ValueError("campaign requeue count does not match entries")
        return self


def build_backfill_campaign_plan(
    universe: BackfillUniverse,
    *,
    campaign_id: str,
    dataset_version: str,
    start: date,
    end: date,
    batch_size: int = 50,
    created_at: datetime | None = None,
) -> BackfillCampaignPlan:
    return BackfillCampaignPlan(
        campaign_id=campaign_id,
        dataset_version=dataset_version,
        universe_version=universe.version,
        universe_content_sha256=universe.content_sha256,
        requested_start=start,
        requested_end=end,
        batch_size=batch_size,
        created_at=created_at or datetime.now(UTC),
        shards=build_annual_batch_shards(
            universe,
            dataset_version=dataset_version,
            start=start,
            end=end,
            batch_size=batch_size,
        ),
    )


def _validate_binding(
    runner: BackfillPilotRunner,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
) -> None:
    if runner.dataset_version != plan.dataset_version:
        raise ValueError("runner dataset version does not match campaign")
    if universe.version != plan.universe_version:
        raise ValueError("universe version does not match campaign")
    if universe.content_sha256 != plan.universe_content_sha256:
        raise ValueError("universe content does not match campaign")
    expected_shards = build_annual_batch_shards(
        universe,
        dataset_version=plan.dataset_version,
        start=plan.requested_start,
        end=plan.requested_end,
        batch_size=plan.batch_size,
    )
    if plan.shards != expected_shards:
        raise ValueError("campaign shard coverage does not match the complete deterministic plan")


def build_campaign_progress(
    runner: BackfillPilotRunner,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
    *,
    observed_at: datetime | None = None,
) -> BackfillCampaignProgress:
    _validate_binding(runner, universe, plan)
    entries: list[BackfillCampaignShardProgress] = []
    for shard in plan.shards:
        task = runner.task_for_shard(shard)
        state = runner.store.load(task.task_id)
        if state is None:
            entries.append(
                BackfillCampaignShardProgress(
                    shard_id=shard.shard_id,
                    task_id=task.task_id,
                    status="pending",
                    attempts=0,
                    row_count=0,
                )
            )
            continue
        last = state.attempts[-1] if state.attempts else None
        checkpoint = last.checkpoint if last else {}
        row_count = int(checkpoint.get("row_count", 0))
        if state.task.state is TaskState.SUCCEEDED:
            status = "succeeded" if row_count else "empty"
        elif state.task.state is TaskState.RUNNING:
            status = "running"
        elif state.task.state is TaskState.RETRYABLE_FAILED:
            status = "retryable"
        else:
            status = "failed"
        entries.append(
            BackfillCampaignShardProgress(
                shard_id=shard.shard_id,
                task_id=task.task_id,
                status=status,
                attempts=len(state.attempts),
                row_count=row_count,
                error_class=last.retry.error_type if last else None,
            )
        )
    counts = {
        status: sum(item.status == status for item in entries)
        for status in (
            "pending",
            "running",
            "retryable",
            "succeeded",
            "empty",
            "failed",
        )
    }
    return BackfillCampaignProgress(
        campaign_id=plan.campaign_id,
        dataset_version=plan.dataset_version,
        universe_version=plan.universe_version,
        observed_at=observed_at or datetime.now(UTC),
        shards=tuple(entries),
        planned_shards=len(entries),
        pending_shards=counts["pending"],
        running_shards=counts["running"],
        retryable_shards=counts["retryable"],
        succeeded_shards=counts["succeeded"],
        empty_shards=counts["empty"],
        failed_shards=counts["failed"],
        total_rows=sum(item.row_count for item in entries),
        finalizable=not any(
            (counts["pending"], counts["running"], counts["retryable"], counts["failed"])
        ),
    )


def run_campaign_wave(
    runner: BackfillPilotRunner,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
    *,
    max_shards: int,
    max_batches: int = BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY,
    max_requests: int = BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT,
    shard_id: str | None = None,
) -> BackfillCampaignWaveReport:
    if not 1 <= max_shards <= 500:
        raise ValueError("campaign wave max_shards must be from 1 to 500")
    if not 1 <= max_batches <= BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY:
        raise ValueError(
            "campaign wave max_batches must be from 1 to "
            f"{BAOSTOCK_DEFAULT_MAX_BATCHES_PER_DAY}"
        )
    if not 1 <= max_requests <= BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT:
        raise ValueError(
            "campaign wave max_requests must be from 1 to "
            f"{BAOSTOCK_DEFAULT_DAILY_REQUEST_LIMIT}"
        )
    before = build_campaign_progress(runner, universe, plan)
    runnable = {
        item.shard_id
        for item in before.shards
        if item.status in {"pending", "running", "retryable"}
    }
    if shard_id is not None:
        requested = next((item for item in plan.shards if item.shard_id == shard_id), None)
        if requested is None:
            raise ValueError(f"requested shard_id is not in campaign plan: {shard_id}")
        if shard_id not in runnable:
            observed = next(item for item in before.shards if item.shard_id == shard_id)
            raise ValueError(
                f"requested shard_id is not runnable: {shard_id} (status={observed.status})"
            )
        candidate_shards: tuple[BackfillShard, ...] = (requested,)
    else:
        candidate_shards = plan.shards
    selected_list: list[BackfillShard] = []
    estimated_requests = 0
    for item in candidate_shards:
        if item.shard_id not in runnable:
            continue
        if len(item.instrument_ids) > 50:
            raise ValueError("campaign shard exceeds BaoStock's 50-instrument batch limit")
        shard_requests = 1 + len(item.instrument_ids)  # one login + one query per instrument
        if shard_requests > max_requests:
            raise ValueError("campaign shard exceeds the configured request budget")
        if len(selected_list) >= max_shards or len(selected_list) >= max_batches:
            break
        if selected_list and estimated_requests + shard_requests > max_requests:
            break
        selected_list.append(item)
        estimated_requests += shard_requests
    selected = tuple(selected_list)
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    results_list: list[BackfillShardResult] = []
    for item in selected:
        result = runner.run_shard(item)
        results_list.append(result)
        # Never fan a provider rejection or local quota pause out into one
        # failed task per remaining shard.  Leave the rest pending so a safe
        # recovery can resume from the exact next shard.
        if result.error_class in {
            BaoStockAccessRejected.__name__,
            BaoStockDailyBatchLimitExceeded.__name__,
            BaoStockDailyQuotaExceeded.__name__,
        }:
            break
    results = tuple(results_list)
    completed_at = datetime.now(UTC)
    return BackfillCampaignWaveReport(
        campaign_id=plan.campaign_id,
        started_at=started_at,
        completed_at=completed_at,
        max_shards=max_shards,
        selected_shards=len(results),
        results=results,
        progress=build_campaign_progress(runner, universe, plan, observed_at=completed_at),
        wall_seconds=time.perf_counter() - started,
        estimated_request_count=estimated_requests,
        estimated_batch_count=len(selected),
        safety_limits={
            "daily_request_limit": max_requests,
            "batch_size_max": 50,
            "min_batch_interval_seconds": configured_batch_interval_seconds(),
            "max_batches_per_day": max_batches_per_day_for_interval(
                configured_batch_interval_seconds()
            ),
            "wave_max_batches": max_batches,
        },
    )


def requeue_campaign_failures(
    runner: BackfillPilotRunner,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
    *,
    allowed_error_classes: tuple[str, ...],
    additional_attempts: int = 3,
    requeued_at: datetime | None = None,
    shard_id: str | None = None,
) -> BackfillCampaignRequeueReport:
    """Re-open only explicitly allowed terminal failures while preserving attempt history."""

    _validate_binding(runner, universe, plan)
    if not allowed_error_classes or any(not item for item in allowed_error_classes):
        raise ValueError("campaign requeue requires explicit non-empty error classes")
    if not 1 <= additional_attempts <= 9:
        raise ValueError("campaign additional attempts must be from 1 to 9")
    allowed = set(allowed_error_classes)
    entries: list[BackfillCampaignRequeueEntry] = []
    skipped = 0
    if shard_id is not None:
        selected_shard = next((item for item in plan.shards if item.shard_id == shard_id), None)
        if selected_shard is None:
            raise ValueError(f"requested shard_id is not in campaign plan: {shard_id}")
        selected_shards: tuple[BackfillShard, ...] = (selected_shard,)
    else:
        selected_shards = plan.shards
    for shard in selected_shards:
        task = runner.task_for_shard(shard)
        state = runner.store.load(task.task_id)
        if state is None:
            if shard_id is not None:
                raise ValueError(f"requested shard has no persisted task state: {shard_id}")
            continue
        if state.task.task_id != task.task_id:
            raise ValueError(f"persisted task identity does not match shard: {shard.shard_id}")
        if any(attempt.task_id != task.task_id for attempt in state.attempts):
            raise ValueError(f"persisted attempt identity does not match shard: {shard.shard_id}")
        if state.task.state is not TaskState.PERMANENTLY_FAILED:
            if shard_id is not None:
                raise ValueError(f"requested shard is not terminally failed: {shard_id}")
            continue
        last = state.attempts[-1] if state.attempts else None
        error_class = last.retry.error_type if last else None
        if error_class not in allowed:
            if shard_id is not None:
                raise ValueError(
                    f"requested shard failure is not in allowed_error_classes: {error_class}"
                )
            skipped += 1
            continue
        previous_max = state.task.max_attempts
        new_max = min(10, max(previous_max + additional_attempts, len(state.attempts) + 1))
        if new_max <= len(state.attempts):
            if shard_id is not None:
                raise ValueError(f"requested shard has no remaining retry capacity: {shard_id}")
            skipped += 1
            continue
        reopened = state.task.model_copy(
            update={"state": TaskState.RETRYABLE_FAILED, "max_attempts": new_max}
        )
        runner.store.save(reopened, state.attempts)
        entries.append(
            BackfillCampaignRequeueEntry(
                shard_id=shard.shard_id,
                task_id=task.task_id,
                error_class=str(error_class),
                previous_max_attempts=previous_max,
                new_max_attempts=new_max,
            )
        )
    return BackfillCampaignRequeueReport(
        campaign_id=plan.campaign_id,
        requeued_at=requeued_at or datetime.now(UTC),
        allowed_error_classes=tuple(sorted(allowed)),
        additional_attempts=additional_attempts,
        requeued_shards=len(entries),
        skipped_failed_shards=skipped,
        entries=tuple(entries),
    )


def finalize_campaign(
    runner: BackfillPilotRunner,
    universe: BackfillUniverse,
    plan: BackfillCampaignPlan,
) -> BackfillPilotReport:
    progress = build_campaign_progress(runner, universe, plan)
    if not progress.finalizable:
        raise ValueError(
            "campaign cannot finalize until every shard succeeds or is explicitly empty; "
            f"pending={progress.pending_shards}, running={progress.running_shards}, "
            f"retryable={progress.retryable_shards}, failed={progress.failed_shards}"
        )
    return runner.run(plan.shards)
