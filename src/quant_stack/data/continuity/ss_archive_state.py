from __future__ import annotations

import fcntl
import json
import os
import secrets
from contextlib import contextmanager
from datetime import UTC, datetime

from ..storage.atomic import atomic_write_immutable_bytes
from .ss_archive_models import ArchiveRunState, SSArchiveEvent
from .ss_archive_safety import ArchiveLayout

_TRANSITIONS: dict[ArchiveRunState, set[ArchiveRunState]] = {
    ArchiveRunState.PLANNED: {ArchiveRunState.SNAPSHOT_OPEN},
    ArchiveRunState.SNAPSHOT_OPEN: {
        ArchiveRunState.INVENTORY_COMPLETE,
        ArchiveRunState.BACKUP_STAGED,
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
    },
    ArchiveRunState.INVENTORY_COMPLETE: {
        ArchiveRunState.BACKUP_STAGED,
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
    },
    ArchiveRunState.BACKUP_STAGED: {
        ArchiveRunState.BACKUP_ENCRYPTED,
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
    },
    ArchiveRunState.BACKUP_ENCRYPTED: {
        ArchiveRunState.EXPORT_COMPLETE,
        ArchiveRunState.SNAPSHOT_CLOSED,
    },
    ArchiveRunState.EXPORT_COMPLETE: {ArchiveRunState.SNAPSHOT_CLOSED},
    ArchiveRunState.SNAPSHOT_CLOSED: {
        ArchiveRunState.SNAPSHOT_OPEN,
        ArchiveRunState.VERIFICATION_COMPLETE,
    },
    ArchiveRunState.VERIFICATION_COMPLETE: {
        ArchiveRunState.RESTORE_VERIFIED,
        ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING,
    },
    ArchiveRunState.VERIFICATION_COMPLETE_RESTORE_PENDING: {
        ArchiveRunState.COMPLETED,
        ArchiveRunState.BLOCKED,
    },
    ArchiveRunState.RESTORE_VERIFIED: {ArchiveRunState.COMPLETED},
    ArchiveRunState.COMPLETED: set(),
    ArchiveRunState.FAILED: set(),
    ArchiveRunState.BLOCKED: set(),
}


class ArchiveStateStore:
    """Crash-safe append-only event store for one immutable archive ID."""

    def __init__(self, layout: ArchiveLayout):
        self.layout = layout
        self.events_root = layout.archive_path("state", "events")
        self.lock_path = layout.archive_path("state", ".event.lock")

    @contextmanager
    def _lock(self):
        self.events_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def events(self) -> tuple[SSArchiveEvent, ...]:
        if not self.events_root.exists():
            return ()
        events = tuple(
            SSArchiveEvent.model_validate_json(path.read_text(encoding="utf-8"))
            for path in sorted(self.events_root.glob("*.json"))
        )
        if [event.sequence for event in events] != list(range(1, len(events) + 1)):
            raise ValueError("archive event sequence is not contiguous")
        if any(event.archive_id != self.layout.archive_id for event in events):
            raise ValueError("archive event belongs to a different archive")
        if any(
            current.occurred_at < previous.occurred_at
            for previous, current in zip(events, events[1:], strict=False)
        ):
            raise ValueError("archive event timestamps are not monotonic")
        return events

    def current_state(self) -> ArchiveRunState | None:
        events = self.events()
        return events[-1].state if events else None

    def record(
        self,
        state: ArchiveRunState,
        *,
        evidence: dict[str, str | int | bool | None] | None = None,
        attempt_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> SSArchiveEvent:
        with self._lock():
            existing = self.events()
            previous = existing[-1].state if existing else None
            if previous is None and state != ArchiveRunState.PLANNED:
                raise ValueError("first archive state must be planned")
            if previous in {
                ArchiveRunState.COMPLETED,
                ArchiveRunState.FAILED,
                ArchiveRunState.BLOCKED,
            }:
                raise ValueError(f"terminal archive state cannot transition: {previous}")
            if previous is not None and state not in _TRANSITIONS[previous]:
                if state != ArchiveRunState.FAILED:
                    raise ValueError(f"invalid archive transition: {previous} -> {state}")
            event = SSArchiveEvent(
                sequence=len(existing) + 1,
                archive_id=self.layout.archive_id,
                state=state,
                occurred_at=occurred_at or datetime.now(UTC),
                attempt_id=attempt_id or f"attempt-{secrets.token_hex(8)}",
                evidence=evidence or {},
            )
            payload = (
                json.dumps(event.model_dump(mode="json"), sort_keys=True, indent=2) + "\n"
            ).encode()
            path = self.events_root / f"{event.sequence:06d}.json"
            atomic_write_immutable_bytes(
                path,
                payload,
                artifact_kind="archive state event",
                validate=lambda value: SSArchiveEvent.model_validate_json(
                    value.read_text(encoding="utf-8")
                ),
            )
            descriptor = os.open(self.events_root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return event
