from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ..ingestion import (
    DataJob,
    IngestionFailureContext,
    IngestionResult,
    IngestionService,
    IngestionStageError,
    RetryPolicy,
)
from ..publication import (
    PublicationBundle,
    build_publication_bundle,
    write_publication_bundle,
)
from ..quality import DataQualityError
from ..storage import StagingFileSystemStorage
from .models import RetryMetadata, Task, TaskAttempt, TaskFailureContext, TaskState
from .store import FileTaskStore

_ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.RETRYABLE_FAILED,
            TaskState.PERMANENTLY_FAILED,
            TaskState.QUARANTINED,
            TaskState.CANCELLED,
        }
    ),
    TaskState.RETRYABLE_FAILED: frozenset(
        {TaskState.RUNNING, TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED}
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.PERMANENTLY_FAILED: frozenset(),
    TaskState.QUARANTINED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


def _is_retryable_error(error: Exception) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, (TimeoutError, ConnectionError, OSError)):
            return True
        current = current.__cause__ or current.__context__
    return False


def _sanitized_error_message(error: Exception) -> str:
    message = str(error).replace("\n", " ")[:1000]
    message = re.sub(
        r"(?i)(token|password|secret|api[_-]?key)\s*[:=]\s*['\"]?([^&,'\"\s]+)",
        r"\1=<redacted>",
        message,
    )
    message = re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1<redacted>@", message)
    return message


def _original_error(error: Exception) -> Exception:
    current = error.original_error if isinstance(error, IngestionStageError) else error
    while isinstance(current.__cause__, Exception):
        current = current.__cause__
    return current


@dataclass(frozen=True)
class TaskExecution:
    task: Task
    attempt: TaskAttempt | None
    result: IngestionResult | None
    bundle: PublicationBundle | None = None
    deduplicated: bool = False


class TaskRuntime:
    """Small synchronous runtime for replay POCs; no scheduler, queue, or distributed lock."""

    def __init__(self, store: FileTaskStore | None = None) -> None:
        self._tasks: dict[str, Task] = {}
        self._attempts: dict[str, list[TaskAttempt]] = {}
        self._results: dict[str, IngestionResult] = {}
        self._bundles: dict[str, PublicationBundle] = {}
        self.store = store

    def _persist(self, task_id: str, bundle_uri: str | None = None) -> None:
        if self.store is None:
            return
        existing = self.store.load(task_id)
        self.store.save(
            self._tasks[task_id],
            tuple(self._attempts[task_id]),
            publication_bundle_uri=(
                bundle_uri if bundle_uri is not None else (
                    existing.publication_bundle_uri if existing else None
                )
            ),
        )

    def submit(self, task: Task) -> Task:
        existing = self._tasks.get(task.task_id)
        if existing is not None:
            return existing
        persisted = self.store.load(task.task_id) if self.store is not None else None
        if persisted is not None:
            self._tasks[task.task_id] = persisted.task
            self._attempts[task.task_id] = list(persisted.attempts)
            return persisted.task
        self._tasks[task.task_id] = task
        self._attempts[task.task_id] = []
        self._persist(task.task_id)
        return task

    def task(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError as exc:
            raise KeyError(f"Unknown task: {task_id}") from exc

    def attempts(self, task_id: str) -> tuple[TaskAttempt, ...]:
        self.task(task_id)
        return tuple(self._attempts[task_id])

    def transition(self, task_id: str, target: TaskState) -> Task:
        current = self.task(task_id)
        if target not in _ALLOWED_TRANSITIONS[current.state]:
            raise ValueError(f"Invalid task transition: {current.state} -> {target}")
        updated = current.model_copy(update={"state": target})
        self._tasks[task_id] = updated
        self._persist(task_id)
        return updated

    def _recover_interrupted_attempt(self, task: Task, *, now: datetime) -> Task:
        """Close a persisted RUNNING attempt after a process crash, then allow resume."""

        attempts = self._attempts[task.task_id]
        if not attempts or attempts[-1].state is not TaskState.RUNNING:
            raise ValueError(f"Task {task.task_id} is running without an active attempt")
        active = attempts[-1]
        retryable = active.attempt_number < task.max_attempts
        recovered_state = (
            TaskState.RETRYABLE_FAILED if retryable else TaskState.PERMANENTLY_FAILED
        )
        attempts[-1] = active.model_copy(
            update={
                "state": recovered_state,
                "completed_at": max(now, active.started_at),
                "retry": RetryMetadata(
                    max_attempts=task.max_attempts,
                    retryable=retryable,
                    backoff_seconds=task.retry_backoff_seconds,
                    next_retry_at=(
                        max(now, active.started_at)
                        + timedelta(seconds=task.retry_backoff_seconds)
                        if retryable
                        else None
                    ),
                    error_type="InterruptedExecution",
                    error_message="worker process ended before the attempt completed",
                ),
            }
        )
        recovered = task.model_copy(update={"state": recovered_state})
        self._tasks[task.task_id] = recovered
        self._persist(task.task_id)
        return recovered

    def _record_failure(
        self,
        *,
        running: Task,
        attempt_number: int,
        started_at: datetime,
        checkpoint: dict[str, object],
        cursor: str | None,
        error: Exception,
        failure_context: TaskFailureContext,
    ) -> TaskExecution:
        original = _original_error(error)
        completed_at = max(datetime.now(UTC), started_at)
        quarantined = isinstance(original, DataQualityError)
        retryable_error = _is_retryable_error(error)
        retryable = retryable_error and attempt_number < running.max_attempts
        failed_state = (
            TaskState.QUARANTINED
            if quarantined
            else TaskState.RETRYABLE_FAILED
            if retryable
            else TaskState.PERMANENTLY_FAILED
        )
        retry = RetryMetadata(
            max_attempts=running.max_attempts,
            retryable=retryable,
            backoff_seconds=running.retry_backoff_seconds,
            next_retry_at=(
                completed_at + timedelta(seconds=running.retry_backoff_seconds)
                if retryable
                else None
            ),
            error_type=type(original).__name__,
            error_message=_sanitized_error_message(original),
        )
        attempt = TaskAttempt(
            attempt_id=f"attempt-{running.idempotency_key[:24]}-{attempt_number}",
            task_id=running.task_id,
            attempt_number=attempt_number,
            state=failed_state,
            checkpoint=checkpoint,
            cursor=cursor,
            retry=retry,
            started_at=started_at,
            completed_at=completed_at,
            raw_uri=failure_context.raw_uri,
            output_manifest_uri=(
                failure_context.proposed_manifest_uris[-1]
                if failure_context.proposed_manifest_uris
                else None
            ),
            failure_context=failure_context,
        )
        self._attempts[running.task_id][-1] = attempt
        failed_task = self.transition(running.task_id, failed_state)
        return TaskExecution(task=failed_task, attempt=attempt, result=None)

    def execute(
        self,
        task: Task,
        service: IngestionService,
        *,
        now: datetime | None = None,
        checkpoint: dict[str, object] | None = None,
        cursor: str | None = None,
    ) -> TaskExecution:
        if self.store is None:
            return self._execute_locked(
                task,
                service,
                now=now,
                checkpoint=checkpoint,
                cursor=cursor,
            )
        with self.store.execution_lock(task.task_id):
            return self._execute_locked(
                task,
                service,
                now=now,
                checkpoint=checkpoint,
                cursor=cursor,
            )

    def _execute_locked(
        self,
        task: Task,
        service: IngestionService,
        *,
        now: datetime | None = None,
        checkpoint: dict[str, object] | None = None,
        cursor: str | None = None,
    ) -> TaskExecution:
        if not isinstance(service.storage, StagingFileSystemStorage):
            raise TypeError(
                "task workers require StagingFileSystemStorage; only CatalogPublisher writes DuckDB"
            )
        submitted = self.submit(task)
        execution_now = (now or datetime.now(UTC)).astimezone(UTC)
        if submitted.state is TaskState.RUNNING:
            submitted = self._recover_interrupted_attempt(submitted, now=execution_now)
        if submitted.state is TaskState.SUCCEEDED:
            result = self._results.get(submitted.idempotency_key)
            bundle = self._bundles.get(submitted.idempotency_key)
            return TaskExecution(
                task=submitted,
                attempt=None,
                result=result,
                bundle=bundle,
                deduplicated=True,
            )
        if submitted.state not in {TaskState.PENDING, TaskState.RETRYABLE_FAILED}:
            raise ValueError(f"Task {submitted.task_id} cannot execute from {submitted.state}")

        prior_attempts = self._attempts[submitted.task_id]
        attempt_number = len(prior_attempts) + 1
        if attempt_number > submitted.max_attempts:
            if submitted.state is TaskState.RETRYABLE_FAILED:
                submitted = self.transition(submitted.task_id, TaskState.PERMANENTLY_FAILED)
            raise ValueError(f"Task {submitted.task_id} exhausted retry attempts")

        started_at = execution_now
        inherited_checkpoint = dict(prior_attempts[-1].checkpoint) if prior_attempts else {}
        inherited_cursor = prior_attempts[-1].cursor if prior_attempts else None
        active_checkpoint = dict(checkpoint) if checkpoint is not None else inherited_checkpoint
        active_cursor = cursor if cursor is not None else inherited_cursor
        running = submitted.model_copy(update={"state": TaskState.RUNNING})
        self._tasks[submitted.task_id] = running
        attempt_id = f"attempt-{running.idempotency_key[:24]}-{attempt_number}"
        self._attempts[running.task_id].append(
            TaskAttempt(
                attempt_id=attempt_id,
                task_id=running.task_id,
                attempt_number=attempt_number,
                state=TaskState.RUNNING,
                checkpoint=active_checkpoint,
                cursor=active_cursor,
                retry=RetryMetadata(max_attempts=running.max_attempts),
                started_at=started_at,
            )
        )
        self._persist(running.task_id)

        job = DataJob(
            dataset=running.dataset_id,
            dataset_version=running.dataset_version,
            idempotency_key=running.idempotency_key,
            provider=running.provider,
            provider_version=running.provider_version,
            provider_endpoint=running.endpoint,
            adapter_version=running.adapter_version,
            native_schema_version=running.native_schema_version,
            market=running.market,
            frequency=running.frequency,
            execution_time=running.created_at,
            retry_policy=RetryPolicy(max_attempts=1, backoff_seconds=0),
            symbols=list(running.instrument_scope),
            start=running.start_date,
            end=running.end_date,
        )

        try:
            result = service.run(job)
        except Exception as exc:  # task-level retry boundary; details stay on the attempt
            context = getattr(exc, "failure_context", None)
            if context is None:
                context = IngestionFailureContext(
                    failure_stage="provider_fetch",
                    provider=running.provider,
                    endpoint=running.endpoint,
                    job_id=job.job_id,
                )
            return self._record_failure(
                running=running,
                attempt_number=attempt_number,
                started_at=started_at,
                checkpoint=active_checkpoint,
                cursor=active_cursor,
                error=exc,
                failure_context=TaskFailureContext(**context.__dict__),
            )

        completed_at = max(datetime.now(UTC), started_at)
        retry = RetryMetadata(max_attempts=running.max_attempts)
        try:
            bundle = build_publication_bundle(
                result=result,
                task_id=running.task_id,
                attempt_id=attempt_id,
                endpoint=running.endpoint,
                created_at=running.created_at,
            )
            bundle_path = write_publication_bundle(service.storage, bundle)
        except Exception as exc:
            proposed_uris = tuple(
                f"quant-data://{path.relative_to(service.storage.data_root).as_posix()}"
                for path in result.manifest_paths
            )
            return self._record_failure(
                running=running,
                attempt_number=attempt_number,
                started_at=started_at,
                checkpoint=active_checkpoint,
                cursor=active_cursor,
                error=exc,
                failure_context=TaskFailureContext(
                    failure_stage="bundle_build",
                    raw_uri=result.raw_artifact.uri,
                    raw_manifest_uri=proposed_uris[0],
                    quality_report_uri=result.quality_artifact.uri,
                    proposed_manifest_uris=proposed_uris,
                    completed_artifact_uris=(
                        result.raw_artifact.uri,
                        result.quality_artifact.uri,
                        result.instrument_artifact.uri,
                        result.bar_artifact.uri,
                    ),
                    provider=running.provider,
                    endpoint=running.endpoint,
                    job_id=result.job_id,
                ),
            )
        bundle_uri = f"quant-data://{bundle_path.relative_to(service.storage.data_root).as_posix()}"
        attempt = TaskAttempt(
            attempt_id=attempt_id,
            task_id=running.task_id,
            attempt_number=attempt_number,
            state=TaskState.SUCCEEDED,
            checkpoint=active_checkpoint,
            cursor=active_cursor,
            retry=retry,
            started_at=started_at,
            completed_at=completed_at,
            records_received=result.bar_manifest.row_count,
            raw_uri=result.raw_artifact.uri,
            output_manifest_uri=(
                f"quant-data://{result.manifest_paths[-1].relative_to(service.storage.data_root).as_posix()}"
            ),
            publication_bundle_uri=bundle_uri,
        )
        self._attempts[running.task_id][-1] = attempt
        succeeded = self.transition(running.task_id, TaskState.SUCCEEDED)
        self._results[succeeded.idempotency_key] = result
        self._bundles[succeeded.idempotency_key] = bundle
        self._persist(succeeded.task_id, bundle_uri=bundle_uri)
        return TaskExecution(task=succeeded, attempt=attempt, result=result, bundle=bundle)
