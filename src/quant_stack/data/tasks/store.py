from __future__ import annotations

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from ..storage.atomic import atomic_replace_bytes
from .models import Task, TaskAttempt, TaskStateFile


class TaskAlreadyRunningError(RuntimeError):
    """Raised when another local process owns the same task execution lock."""


class FileTaskStore:
    """Atomic JSON task-state persistence; no database or background service required."""

    def __init__(self, root: str | Path, *, durable: bool = True) -> None:
        self.root = Path(root).expanduser().resolve()
        self.durable = durable
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, task_id: str) -> Path:
        return self.root / f"{task_id}.json"

    def lock_path_for(self, task_id: str) -> Path:
        return self.root / ".locks" / f"{task_id}.lock"

    @contextmanager
    def execution_lock(self, task_id: str):
        """Hold a non-blocking per-task Mac/Linux lock for an entire execution."""

        path = self.lock_path_for(task_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+b") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise TaskAlreadyRunningError(
                    f"Task {task_id} is already running in another local process"
                ) from exc
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def load(self, task_id: str) -> TaskStateFile | None:
        path = self.path_for(task_id)
        if not path.exists():
            return None
        return TaskStateFile.model_validate_json(path.read_text(encoding="utf-8"))

    def save(
        self,
        task: Task,
        attempts: tuple[TaskAttempt, ...],
        *,
        publication_bundle_uri: str | None = None,
    ) -> TaskStateFile:
        state = TaskStateFile(
            task=task,
            attempts=attempts,
            publication_bundle_uri=publication_bundle_uri,
            updated_at=datetime.now(UTC),
        )
        path = self.path_for(task.task_id)
        payload = (
            json.dumps(
                state.model_dump(mode="json", exclude_computed_fields=True), indent=2
            )
            + "\n"
        ).encode()
        if self.durable:
            atomic_replace_bytes(
                path,
                payload,
                validate=lambda temporary: TaskStateFile.model_validate_json(
                    temporary.read_text(encoding="utf-8")
                ),
            )
        else:
            # Batch daily collection already has a durable provider-batch cache.
            # Keep each mutable task transition atomic, but avoid two fsyncs per
            # transition on the NAS; a crash can rebuild these states from that
            # session cache without another Provider request.
            descriptor, temporary_value = tempfile.mkstemp(
                dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
            )
            temporary = Path(temporary_value)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                TaskStateFile.model_validate_json(
                    temporary.read_text(encoding="utf-8")
                )
                os.chmod(temporary, 0o600)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        return state
