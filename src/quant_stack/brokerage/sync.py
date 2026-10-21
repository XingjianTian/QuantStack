"""Offline-safe account synchronization and private SQLite persistence."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Never

from .contracts import (
    AccountData,
    AccountSnapshot,
    BrokerageModel,
    Fill,
    Order,
    Position,
    SyncAttempt,
    SyncState,
    SyncStatus,
    Transaction,
)

SYNC_FAILURE_GENERATION_NAMESPACE = "brokerage.sync"


class SnapshotStoreError(RuntimeError):
    """Raised for invalid private-runtime state or persistence failures."""


class SyncError(RuntimeError):
    """Raised only by the explicit ``sync_or_raise`` convenience method."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


def _json(value: Any) -> str:
    if isinstance(value, BrokerageModel):
        return value.canonical_json()
    # Generic investing state is stored as JSON.  Decimal values are accepted
    # here so callers do not need to round money before entering the shared
    # transaction; JSON reloads them as strings, which preserves the exact
    # representation for the investing layer to validate again.
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: str(item) if isinstance(item, Decimal) else _unsupported_json(item),
    )


def _unsupported_json(value: Any) -> Never:
    raise TypeError(f"value is not JSON serializable: {type(value).__name__}")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _fact_version(fact_type: str, payload: str) -> datetime | None:
    """Return the provider version timestamp for a mutable fact, if known."""

    field = {"position": "as_of", "order": "updated_at"}.get(fact_type)
    if field is None:
        return None
    try:
        decoded = json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    value = decoded.get(field)
    if value in (None, ""):
        return None
    try:
        if isinstance(value, str) and value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class SyncResult:
    attempt: SyncAttempt
    state: SyncState
    snapshot: AccountSnapshot | None = None
    published: bool = False
    stale: bool = False

    @property
    def proposal_blocked(self) -> bool:
        return (
            self.attempt.status is not SyncStatus.SUCCEEDED
            or not self.state.complete
            or bool(self.state.pending_fact_ids)
            or self.state.last_attempt_status is not SyncStatus.SUCCEEDED
            or self.snapshot is None
        )


class JsonTransaction:
    """Small namespaced JSON key/value view over a live SQLite transaction."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, namespace: str, key: str, default: Any = None) -> Any:
        row = self._connection.execute(
            "SELECT payload FROM kv WHERE namespace = ? AND key = ?",
            (namespace, key),
        ).fetchone()
        return default if row is None else json.loads(row[0])

    def set(self, namespace: str, key: str, value: Any) -> None:
        self._connection.execute(
            """
            INSERT INTO kv(namespace, key, payload) VALUES (?, ?, ?)
            ON CONFLICT(namespace, key) DO UPDATE SET payload = excluded.payload
            """,
            (namespace, key, _json(value)),
        )

    def delete(self, namespace: str, key: str) -> None:
        self._connection.execute(
            "DELETE FROM kv WHERE namespace = ? AND key = ?", (namespace, key)
        )

    def state(self, account_id: str) -> SyncState:
        """Read sync state through the current transaction connection."""

        row = self._connection.execute(
            "SELECT payload FROM states WHERE account_id = ?", (account_id,)
        ).fetchone()
        if row is None:
            return SyncState(account_id=account_id)
        return SyncState.model_validate_json(row["payload"])

    def latest(self, account_id: str) -> AccountSnapshot | None:
        """Read the effective snapshot without opening a second connection."""

        state = self.state(account_id)
        if state.last_success_snapshot_id is None:
            return None
        row = self._connection.execute(
            """
            SELECT payload FROM snapshots
            WHERE account_id = ? AND snapshot_id = ?
            """,
            (account_id, state.last_success_snapshot_id),
        ).fetchone()
        return None if row is None else AccountSnapshot.model_validate_json(row["payload"])

    def facts(
        self,
        account_id: str,
        *,
        fact_type: str | None = None,
    ) -> tuple[BrokerageModel, ...]:
        """Read normalized immutable/current facts through this transaction.

        This deliberately uses the transaction's existing SQLite connection;
        callers that are already inside ``store.transaction()`` must not open
        a second lock-taking store read.  The returned models are provider
        neutral and retain the exact decimal/timestamp representation stored
        by synchronization.
        """

        models: dict[str, type[BrokerageModel]] = {
            "position": Position,
            "order": Order,
            "transaction": Transaction,
            "fill": Fill,
        }
        if fact_type is not None and fact_type not in models:
            raise ValueError(f"unsupported fact type: {fact_type}")
        if fact_type is None:
            rows = self._connection.execute(
                """
                SELECT fact_type, payload FROM facts
                WHERE account_id = ?
                ORDER BY fact_type, fact_id
                """,
                (account_id,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT fact_type, payload FROM facts
                WHERE account_id = ? AND fact_type = ?
                ORDER BY fact_id
                """,
                (account_id, fact_type),
            ).fetchall()
        return tuple(models[row["fact_type"]].model_validate_json(row["payload"]) for row in rows)

    def read_facts(
        self,
        account_id: str,
        *,
        fact_type: str | None = None,
    ) -> tuple[BrokerageModel, ...]:
        """Alias for callers that prefer an explicit read verb."""

        return self.facts(account_id, fact_type=fact_type)

    def transactions(self, account_id: str) -> tuple[Transaction, ...]:
        return tuple(self.facts(account_id, fact_type="transaction"))  # type: ignore[return-value]

    def read_transactions(self, account_id: str) -> tuple[Transaction, ...]:
        return self.transactions(account_id)

    def fills(self, account_id: str) -> tuple[Fill, ...]:
        return tuple(self.facts(account_id, fact_type="fill"))  # type: ignore[return-value]

    def read_fills(self, account_id: str) -> tuple[Fill, ...]:
        return self.fills(account_id)


class PrivateSnapshotStore:
    """A single-writer, source-tree-external SQLite store for private snapshots.

    The store defaults to rollback journaling instead of WAL so no ``-wal`` or
    ``-shm`` sidecar can escape the explicitly private file permissions.  Every
    write takes a process lock and a SQLite ``BEGIN IMMEDIATE`` transaction.
    """

    def __init__(self, root: str | Path, *, source_root: str | Path | None = None) -> None:
        raw_root = Path(root).expanduser()
        if raw_root.is_symlink():
            raise SnapshotStoreError("private runtime root must not be a symlink")
        if not raw_root.is_absolute():
            raise SnapshotStoreError("private runtime root must be an absolute path")
        self.root = raw_root.resolve(strict=False)
        source = (
            Path(source_root).expanduser().resolve(strict=False)
            if source_root is not None
            else Path(__file__).resolve().parents[3]
        )
        if self.root == source or self.root.is_relative_to(source):
            raise SnapshotStoreError("private runtime must be outside the source tree")
        if self.root.exists():
            if not self.root.is_dir() or self.root.is_symlink():
                raise SnapshotStoreError("private runtime root must be a real directory")
            root_mode = stat.S_IMODE(self.root.stat().st_mode)
            if root_mode != 0o700:
                raise SnapshotStoreError(
                    f"existing private runtime directory must be mode 0700, got {root_mode:04o}"
                )
        else:
            self.root.mkdir(parents=True, exist_ok=False)
            os.chmod(self.root, 0o700)
        self.db_path = self.root / "brokerage.sqlite3"
        self.lock_path = self.root / ".brokerage.write.lock"
        self._ensure_file(self.db_path)
        self._ensure_file(self.lock_path)
        self._initialize()

    @staticmethod
    def _assert_private_file(path: Path) -> None:
        if path.is_symlink():
            raise SnapshotStoreError(f"private runtime file must not be a symlink: {path}")
        try:
            file_stat = path.stat()
        except FileNotFoundError:
            return
        if not stat.S_ISREG(file_stat.st_mode):
            raise SnapshotStoreError(f"private runtime path must be a regular file: {path}")
        mode = stat.S_IMODE(file_stat.st_mode)
        if mode != 0o600:
            raise SnapshotStoreError(
                f"existing private runtime file must be mode 0600, got {mode:04o}: {path}"
            )

    @classmethod
    def _ensure_file(cls, path: Path) -> None:
        cls._assert_private_file(path)
        if path.exists():
            return
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise SnapshotStoreError(f"could not create private runtime file: {path}") from exc
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self._assert_private_file(self.lock_path)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
        except OSError as exc:
            raise SnapshotStoreError(
                f"could not open private runtime lock: {self.lock_path}"
            ) from exc
        try:
            descriptor_stat = os.fstat(descriptor)
            if not stat.S_ISREG(descriptor_stat.st_mode) or stat.S_IMODE(
                descriptor_stat.st_mode
            ) != 0o600:
                raise SnapshotStoreError(
                    f"private runtime lock must be a regular mode 0600 file: {self.lock_path}"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        self._assert_private_file(self.db_path)
        connection = sqlite3.connect(self.db_path, isolation_level=None, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._lock(), self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    account_id TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    business_time TEXT,
                    received_at TEXT NOT NULL,
                    complete INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (account_id, snapshot_id)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    account_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    attempted_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (account_id, attempt_id)
                );
                CREATE TABLE IF NOT EXISTS states (
                    account_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facts (
                    account_id TEXT NOT NULL,
                    fact_type TEXT NOT NULL,
                    fact_id TEXT NOT NULL,
                    canonical_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (account_id, fact_type, fact_id)
                );
                CREATE TABLE IF NOT EXISTS fact_conflicts (
                    account_id TEXT NOT NULL,
                    fact_type TEXT NOT NULL,
                    fact_id TEXT NOT NULL,
                    prior_payload TEXT NOT NULL,
                    incoming_payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fact_history (
                    account_id TEXT NOT NULL,
                    fact_type TEXT NOT NULL,
                    fact_id TEXT NOT NULL,
                    prior_payload TEXT NOT NULL,
                    incoming_payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kv (
                    namespace TEXT NOT NULL,
                    key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (namespace, key)
                );
                """
            )

    @contextmanager
    def transaction(self) -> Iterator[JsonTransaction]:
        """Read and modify generic account/plan/ledger/proposal state atomically."""

        with self._lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield JsonTransaction(connection)
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    def get_json(self, namespace: str, key: str, default: Any = None) -> Any:
        with self._lock(), self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM kv WHERE namespace = ? AND key = ?",
                (namespace, key),
            ).fetchone()
            return default if row is None else json.loads(row[0])

    @staticmethod
    def _failure_generation_from_connection(
        connection: sqlite3.Connection, account_id: str
    ) -> int:
        row = connection.execute(
            "SELECT payload FROM kv WHERE namespace = ? AND key = ?",
            (SYNC_FAILURE_GENERATION_NAMESPACE, account_id),
        ).fetchone()
        if row is None:
            return 0
        try:
            generation = int(json.loads(row["payload"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SnapshotStoreError(
                f"invalid sync failure generation for account: {account_id}"
            ) from exc
        if generation < 0:
            raise SnapshotStoreError("sync failure generation must not be negative")
        return generation

    @staticmethod
    def _set_failure_generation(
        connection: sqlite3.Connection, account_id: str, generation: int
    ) -> None:
        connection.execute(
            """
            INSERT INTO kv(namespace, key, payload) VALUES (?, ?, ?)
            ON CONFLICT(namespace, key) DO UPDATE SET payload = excluded.payload
            """,
            (SYNC_FAILURE_GENERATION_NAMESPACE, account_id, _json(generation)),
        )

    def sync_failure_generation(self, account_id: str) -> int:
        """Return the durable failure generation for proposal invalidation."""

        with self._lock(), self._connect() as connection:
            return self._failure_generation_from_connection(connection, account_id)

    def read_facts(
        self,
        account_id: str,
        *,
        fact_type: str | None = None,
    ) -> tuple[BrokerageModel, ...]:
        """Read facts while taking one store lock for the whole operation."""

        with self._lock(), self._connect() as connection:
            return JsonTransaction(connection).facts(account_id, fact_type=fact_type)

    def transactions(self, account_id: str) -> tuple[Transaction, ...]:
        return tuple(self.read_facts(account_id, fact_type="transaction"))  # type: ignore[return-value]

    def read_transactions(self, account_id: str) -> tuple[Transaction, ...]:
        return self.transactions(account_id)

    def fills(self, account_id: str) -> tuple[Fill, ...]:
        return tuple(self.read_facts(account_id, fact_type="fill"))  # type: ignore[return-value]

    def read_fills(self, account_id: str) -> tuple[Fill, ...]:
        return self.fills(account_id)

    def put_json(self, namespace: str, key: str, value: Any) -> None:
        with self.transaction() as transaction:
            transaction.set(namespace, key, value)

    @staticmethod
    def _fact_keys(snapshot: AccountSnapshot) -> set[str]:
        """Return stable keys for immutable transaction and fill evidence."""

        return {
            *(f"transaction:{item.transaction_id}" for item in snapshot.transactions),
            *(f"fill:{item.execution_id}" for item in snapshot.fills),
        }

    @classmethod
    def _new_late_fact_ids(
        cls,
        snapshot: AccountSnapshot,
        current_snapshot: AccountSnapshot | None,
    ) -> tuple[str, ...]:
        """Find immutable facts outside the currently published observation.

        A result whose business time predates the current observation may be a
        late broker query window.  A new transaction/fill in that result can
        change the budget ledger even though the current snapshot remains the
        newer one, so it must force reconciliation instead of being silently
        retained only in the facts table.
        """

        if (
            current_snapshot is None
            or snapshot.business_time is None
            or current_snapshot.business_time is None
            or snapshot.business_time >= current_snapshot.business_time
        ):
            return ()
        current_keys = cls._fact_keys(current_snapshot)
        return tuple(sorted(cls._fact_keys(snapshot) - current_keys))

    @staticmethod
    def _fact_records(snapshot: AccountSnapshot) -> Iterator[tuple[str, str, str]]:
        for fact_type, values, key in (
            ("position", snapshot.positions, "position_id"),
            ("order", snapshot.orders, "order_id"),
            ("transaction", snapshot.transactions, "transaction_id"),
            ("fill", snapshot.fills, "execution_id"),
        ):
            for value in values:
                yield fact_type, str(getattr(value, key)), value.canonical_json()

    @staticmethod
    def _record_fact_history(
        connection: sqlite3.Connection,
        *,
        account_id: str,
        fact_type: str,
        fact_id: str,
        prior_payload: str,
        incoming_payload: str,
        observed_at: str,
    ) -> None:
        # A repeated overlapping window should retain one piece of evidence,
        # rather than growing an unbounded duplicate history on every retry.
        connection.execute(
            """
            INSERT INTO fact_history(
                account_id, fact_type, fact_id, prior_payload, incoming_payload, observed_at
            )
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_history
                WHERE account_id = ? AND fact_type = ? AND fact_id = ?
                  AND prior_payload = ? AND incoming_payload = ?
            )
            """,
            (
                account_id,
                fact_type,
                fact_id,
                prior_payload,
                incoming_payload,
                observed_at,
                account_id,
                fact_type,
                fact_id,
                prior_payload,
                incoming_payload,
            ),
        )

    @staticmethod
    def _record_fact_conflict(
        connection: sqlite3.Connection,
        *,
        account_id: str,
        fact_type: str,
        fact_id: str,
        prior_payload: str,
        incoming_payload: str,
        observed_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO fact_conflicts(
                account_id, fact_type, fact_id, prior_payload, incoming_payload, observed_at
            )
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_conflicts
                WHERE account_id = ? AND fact_type = ? AND fact_id = ?
                  AND prior_payload = ? AND incoming_payload = ?
            )
            """,
            (
                account_id,
                fact_type,
                fact_id,
                prior_payload,
                incoming_payload,
                observed_at,
                account_id,
                fact_type,
                fact_id,
                prior_payload,
                incoming_payload,
            ),
        )

    def _preserve_fact_conflicts(
        self,
        connection: sqlite3.Connection,
        snapshot: AccountSnapshot,
        *,
        apply_facts: bool,
    ) -> tuple[str, ...]:
        conflicts = set(snapshot.conflicts)
        observed = snapshot.received_at.isoformat()
        pending_inserts: list[tuple[str, str, str, str]] = []
        pending_updates: list[tuple[str, str, str, str, str]] = []
        batch_facts: dict[tuple[str, str], tuple[str, str]] = {}
        for fact_type, fact_id, payload in self._fact_records(snapshot):
            digest = _hash(payload)
            batch_key = (fact_type, fact_id)
            prior_batch = batch_facts.get(batch_key)
            if prior_batch is not None:
                if prior_batch[0] != digest:
                    conflict_id = f"{fact_type}:{fact_id}"
                    conflicts.add(conflict_id)
                    self._record_fact_conflict(
                        connection,
                        account_id=snapshot.account.account_id,
                        fact_type=fact_type,
                        fact_id=fact_id,
                        prior_payload=prior_batch[1],
                        incoming_payload=payload,
                        observed_at=observed,
                    )
                continue
            batch_facts[batch_key] = (digest, payload)
            row = connection.execute(
                """
                SELECT canonical_hash, payload FROM facts
                WHERE account_id = ? AND fact_type = ? AND fact_id = ?
                """,
                (snapshot.account.account_id, fact_type, fact_id),
            ).fetchone()
            if row is None:
                pending_inserts.append(
                    (fact_type, fact_id, digest, payload)
                )
            elif row["canonical_hash"] != digest and fact_type in {"position", "order"}:
                self._record_fact_history(
                    connection,
                    account_id=snapshot.account.account_id,
                    fact_type=fact_type,
                    fact_id=fact_id,
                    prior_payload=row["payload"],
                    incoming_payload=payload,
                    observed_at=observed,
                )
                prior_version = _fact_version(fact_type, row["payload"])
                incoming_version = _fact_version(fact_type, payload)
                if prior_version is None or incoming_version is None:
                    conflict_id = f"{fact_type}:{fact_id}:version_unknown"
                    conflicts.add(conflict_id)
                    self._record_fact_conflict(
                        connection,
                        account_id=snapshot.account.account_id,
                        fact_type=fact_type,
                        fact_id=fact_id,
                        prior_payload=row["payload"],
                        incoming_payload=payload,
                        observed_at=observed,
                    )
                elif incoming_version > prior_version:
                    pending_updates.append((fact_type, fact_id, digest, payload, row["payload"])
                    )
                elif incoming_version < prior_version:
                    conflict_id = f"{fact_type}:{fact_id}:stale_version"
                    conflicts.add(conflict_id)
                    self._record_fact_conflict(
                        connection,
                        account_id=snapshot.account.account_id,
                        fact_type=fact_type,
                        fact_id=fact_id,
                        prior_payload=row["payload"],
                        incoming_payload=payload,
                        observed_at=observed,
                    )
                else:
                    conflict_id = f"{fact_type}:{fact_id}:version_collision"
                    conflicts.add(conflict_id)
                    self._record_fact_conflict(
                        connection,
                        account_id=snapshot.account.account_id,
                        fact_type=fact_type,
                        fact_id=fact_id,
                        prior_payload=row["payload"],
                        incoming_payload=payload,
                        observed_at=observed,
                    )
            elif row["canonical_hash"] != digest:
                conflict_id = f"{fact_type}:{fact_id}"
                conflicts.add(conflict_id)
                self._record_fact_conflict(
                    connection,
                    account_id=snapshot.account.account_id,
                    fact_type=fact_type,
                    fact_id=fact_id,
                    prior_payload=row["payload"],
                    incoming_payload=payload,
                    observed_at=observed,
                )
        if apply_facts:
            for fact_type, fact_id, digest, payload in pending_inserts:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO facts(
                        account_id, fact_type, fact_id, canonical_hash, payload
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (snapshot.account.account_id, fact_type, fact_id, digest, payload),
                )
            # A candidate with any conflict is not current state.  New fact IDs
            # are retained as evidence, while mutable current facts are only
            # advanced when the whole candidate is conflict-free.
            if not conflicts:
                for fact_type, fact_id, digest, payload, _prior_payload in pending_updates:
                    connection.execute(
                        """
                        UPDATE facts SET canonical_hash = ?, payload = ?
                        WHERE account_id = ? AND fact_type = ? AND fact_id = ?
                        """,
                        (
                            digest,
                            payload,
                            snapshot.account.account_id,
                            fact_type,
                            fact_id,
                        ),
                    )
        return tuple(sorted(conflicts))

    def publish(
        self, snapshot: AccountSnapshot, attempt: SyncAttempt
    ) -> tuple[AccountSnapshot, SyncState, bool, bool]:
        """Publish one complete candidate without allowing an older one to win."""

        if attempt.status not in {SyncStatus.SUCCEEDED, SyncStatus.FAILED}:
            raise SnapshotStoreError("attempt status must be succeeded or failed")
        account_id = snapshot.account.account_id
        with self._lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = self._state_from_connection(connection, account_id)
                current_snapshot = self._snapshot_from_connection(
                    connection, account_id, current.last_success_snapshot_id
                )
                candidate_is_current_or_newer = snapshot.business_time is not None and (
                    current_snapshot is None
                    or current_snapshot.business_time is None
                    or snapshot.business_time >= current_snapshot.business_time
                )
                incoming_is_older = (
                    current.last_attempt_at is not None
                    and attempt.finished_at < current.last_attempt_at
                )
                incoming_success = attempt.status is SyncStatus.SUCCEEDED
                late_fact_ids = set(
                    self._new_late_fact_ids(snapshot, current_snapshot)
                )
                candidate_fact_ids = self._fact_keys(snapshot)
                pending_fact_ids = set(current.pending_fact_ids)
                unresolved_pending = pending_fact_ids - candidate_fact_ids
                conflicts = set(
                    self._preserve_fact_conflicts(
                        connection,
                        snapshot,
                        # Retain all facts from a successful provider response,
                        # including an incomplete/late candidate.  Current
                        # mutable facts are still advanced only when the helper
                        # sees no conflict, while immutable evidence remains
                        # available for a later complete rebuild.
                        apply_facts=incoming_success,
                    )
                )
                if late_fact_ids:
                    conflicts.add("coverage:late_window_fact_requires_rebuild")
                    conflicts.update(
                        f"coverage:late_window_fact:{fact_id}"
                        for fact_id in sorted(late_fact_ids)
                    )
                if unresolved_pending:
                    conflicts.add("coverage:pending_fact_requires_rebuild")
                    conflicts.update(
                        f"coverage:pending_fact_missing:{fact_id}"
                        for fact_id in sorted(unresolved_pending)
                    )
                normalized_conflicts = tuple(sorted(conflicts))
                if normalized_conflicts != snapshot.conflicts:
                    snapshot = snapshot.model_copy(
                        update={"conflicts": normalized_conflicts, "complete": False}
                    )
                if not snapshot.complete and incoming_success:
                    late_reconciliation = late_fact_ids or unresolved_pending
                    failure_sections = set(attempt.failure_sections)
                    failure_sections.update(
                        {"transactions", "fills"}
                        if late_reconciliation
                        else {"conflicts"}
                    )
                    error_code = (
                        "LATE_WINDOW_FACTS" if late_reconciliation else "INCOMPLETE_SNAPSHOT"
                    )
                    error_message = (
                        "late transaction/fill facts require a newer complete rebuild"
                        if late_reconciliation
                        else "snapshot contains incomplete or conflicting facts"
                    )
                    attempt = attempt.model_copy(
                        update={
                            "status": SyncStatus.FAILED,
                            "failure_sections": tuple(sorted(failure_sections)),
                            "error_code": error_code,
                            "error_message": error_message,
                        }
                    )
                failure_generation = self._failure_generation_from_connection(
                    connection, account_id
                )
                if attempt.status is SyncStatus.FAILED and not incoming_is_older:
                    failure_generation += 1
                    self._set_failure_generation(
                        connection, account_id, failure_generation
                    )
                payload = snapshot.canonical_json()
                connection.execute(
                    """
                    INSERT OR IGNORE INTO snapshots(
                        account_id, snapshot_id, business_time, received_at, complete, payload
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        snapshot.snapshot_id,
                        (
                            snapshot.business_time.isoformat()
                            if snapshot.business_time is not None
                            else None
                        ),
                        snapshot.received_at.isoformat(),
                        int(snapshot.complete),
                        payload,
                    ),
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO attempts(
                        account_id, attempt_id, attempted_at, status, payload
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        attempt.attempt_id,
                        attempt.finished_at.isoformat(),
                        attempt.status.value,
                        attempt.canonical_json(),
                    ),
                )
                should_publish = (
                    incoming_success
                    and attempt.status is SyncStatus.SUCCEEDED
                    and snapshot.complete
                    and not incoming_is_older
                    and not late_fact_ids
                    and not unresolved_pending
                    and (current_snapshot is None or candidate_is_current_or_newer)
                )
                if should_publish:
                    state = SyncState(
                        account_id=account_id,
                        last_success_snapshot_id=snapshot.snapshot_id,
                        last_success_at=attempt.finished_at,
                        last_attempt_id=attempt.attempt_id,
                        last_attempt_status=attempt.status,
                        last_attempt_at=attempt.finished_at,
                        cursor=snapshot.cursor,
                        pending_fact_ids=(),
                        complete=True,
                    )
                    published_snapshot = snapshot
                elif incoming_is_older:
                    # A late result may still have its facts retained as
                    # evidence, but it must not replace the newest attempt
                    # state or clear a newer failure.
                    if late_fact_ids:
                        pending_fact_ids.update(late_fact_ids)
                    if pending_fact_ids != set(current.pending_fact_ids):
                        state = current.model_copy(
                            update={
                                "pending_fact_ids": tuple(sorted(pending_fact_ids)),
                                "last_failure_sections": tuple(
                                    sorted(
                                        set(current.last_failure_sections)
                                        | {"transactions", "fills"}
                                    )
                                ),
                                "last_error_code": "LATE_WINDOW_FACTS",
                                "last_error_message": (
                                    "late transaction/fill facts require a newer complete rebuild"
                                ),
                                "complete": False,
                            }
                        )
                    else:
                        state = current
                    published_snapshot = current_snapshot
                else:
                    if late_fact_ids:
                        pending_fact_ids.update(late_fact_ids)
                    state = SyncState(
                        account_id=account_id,
                        last_success_snapshot_id=(
                            current_snapshot.snapshot_id if current_snapshot else None
                        ),
                        last_success_at=current.last_success_at,
                        last_attempt_id=attempt.attempt_id,
                        last_attempt_status=attempt.status,
                        last_attempt_at=attempt.finished_at,
                        last_failure_sections=(
                            attempt.failure_sections
                            if attempt.status is SyncStatus.FAILED
                            else current.last_failure_sections
                        ),
                        last_error_code=(
                            attempt.error_code
                            if attempt.status is SyncStatus.FAILED
                            else current.last_error_code
                        ),
                        last_error_message=(
                            attempt.error_message
                            if attempt.status is SyncStatus.FAILED
                            else current.last_error_message
                        ),
                        cursor=current.cursor,
                        pending_fact_ids=tuple(sorted(pending_fact_ids)),
                        complete=(
                            current.complete
                            if attempt.status is SyncStatus.SUCCEEDED
                            else False
                        ),
                    )
                    published_snapshot = current_snapshot
                connection.execute(
                    "INSERT OR REPLACE INTO states(account_id, payload) VALUES (?, ?)",
                    (account_id, state.canonical_json()),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return published_snapshot or snapshot, state, should_publish, not should_publish

    def record_failure(self, attempt: SyncAttempt) -> SyncState:
        if attempt.status is not SyncStatus.FAILED:
            raise SnapshotStoreError("record_failure requires a failed attempt")
        account_id = attempt.account_id
        with self._lock(), self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO attempts(
                        account_id, attempt_id, attempted_at, status, payload
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        attempt.attempt_id,
                        attempt.finished_at.isoformat(),
                        attempt.status.value,
                        attempt.canonical_json(),
                    ),
                )
                prior = self._state_from_connection(connection, account_id)
                if (
                    prior.last_attempt_at is not None
                    and attempt.finished_at < prior.last_attempt_at
                ):
                    state = prior
                else:
                    failure_generation = self._failure_generation_from_connection(
                        connection, account_id
                    ) + 1
                    self._set_failure_generation(
                        connection, account_id, failure_generation
                    )
                    state = SyncState(
                        account_id=account_id,
                        last_success_snapshot_id=prior.last_success_snapshot_id,
                        last_success_at=prior.last_success_at,
                        last_attempt_id=attempt.attempt_id,
                        last_attempt_status=SyncStatus.FAILED,
                        last_attempt_at=attempt.finished_at,
                        last_failure_sections=attempt.failure_sections,
                        last_error_code=attempt.error_code,
                        last_error_message=attempt.error_message,
                        cursor=prior.cursor,
                        pending_fact_ids=prior.pending_fact_ids,
                        complete=False,
                    )
                connection.execute(
                    "INSERT OR REPLACE INTO states(account_id, payload) VALUES (?, ?)",
                    (account_id, state.canonical_json()),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return state

    def _state_from_connection(self, connection: sqlite3.Connection, account_id: str) -> SyncState:
        row = connection.execute(
            "SELECT payload FROM states WHERE account_id = ?", (account_id,)
        ).fetchone()
        if row is None:
            return SyncState(account_id=account_id)
        return SyncState.model_validate_json(row["payload"])

    def state(self, account_id: str) -> SyncState:
        with self._lock(), self._connect() as connection:
            return self._state_from_connection(connection, account_id)

    def _snapshot_from_connection(
        self, connection: sqlite3.Connection, account_id: str, snapshot_id: str | None
    ) -> AccountSnapshot | None:
        if snapshot_id is None:
            return None
        row = connection.execute(
            "SELECT payload FROM snapshots WHERE account_id = ? AND snapshot_id = ?",
            (account_id, snapshot_id),
        ).fetchone()
        return None if row is None else AccountSnapshot.model_validate_json(row["payload"])

    def latest(self, account_id: str) -> AccountSnapshot | None:
        with self._lock(), self._connect() as connection:
            state = self._state_from_connection(connection, account_id)
            return self._snapshot_from_connection(
                connection, account_id, state.last_success_snapshot_id
            )

    def load(self, account_id: str, snapshot_id: str) -> AccountSnapshot | None:
        with self._lock(), self._connect() as connection:
            return self._snapshot_from_connection(connection, account_id, snapshot_id)


class SyncService:
    """Prepare-then-publish synchronization coordinator."""

    def __init__(
        self,
        adapter: Any,
        store: PrivateSnapshotStore,
        *,
        clock: Any = None,
        max_source_age: timedelta = timedelta(hours=24),
    ) -> None:
        self.adapter = adapter
        self.store = store
        self.clock = clock or (lambda: datetime.now(UTC))
        if max_source_age < timedelta(0):
            raise ValueError("max_source_age must not be negative")
        self.max_source_age = max_source_age

    def sync_account(
        self,
        account_id: str,
        account_hash: str,
        *,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        symbols: Sequence[str] | None = None,
    ) -> SyncResult:
        started_at = _utc(self.clock())
        attempt_id = f"attempt:{account_id}:{started_at.isoformat()}:{os.urandom(6).hex()}"
        try:
            data: AccountData = self.adapter.read_snapshot(
                account_hash,
                account_id=account_id,
                received_at=started_at,
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
            )
            stable_payload = data.model_dump(mode="json")
            # Reception time and query window are transport metadata, not facts.
            stable_payload.pop("received_at", None)
            coverage = stable_payload.get("coverage")
            if isinstance(coverage, dict):
                coverage.pop("start", None)
                coverage.pop("end", None)
            stable_facts = _json(stable_payload)
            batch_id = _hash(stable_facts)
            snapshot_id = _hash(
                _json(
                    {
                        "account_id": account_id,
                        "business_time": (
                            data.business_time.isoformat()
                            if data.business_time is not None
                            else None
                        ),
                        "facts": stable_facts,
                    }
                )
            )
            finished_at = _utc(self.clock())
            conflicts = set(data.conflicts)
            source_as_of = data.coverage.source_as_of or data.business_time
            if source_as_of is None:
                conflicts.add("coverage:source_as_of_unknown")
            else:
                source_age = finished_at - source_as_of
                if source_age < timedelta(0):
                    conflicts.add("coverage:source_as_of_in_future")
                elif source_age > self.max_source_age:
                    conflicts.add("coverage:source_as_of_too_old")
            snapshot = AccountSnapshot(
                snapshot_id=snapshot_id,
                account=data.account,
                positions=data.positions,
                orders=data.orders,
                transactions=data.transactions,
                fills=data.fills,
                quotes=data.quotes,
                business_time=data.business_time,
                received_at=data.received_at,
                sync_batch_id=batch_id,
                mapping_version=data.mapping_version,
                coverage=data.coverage,
                cursor=data.cursor,
                complete=(
                    data.coverage.complete
                    and data.business_time is not None
                    and not conflicts
                ),
                conflicts=tuple(sorted(conflicts)),
            )
            attempt = SyncAttempt(
                attempt_id=attempt_id,
                account_id=account_id,
                status=SyncStatus.SUCCEEDED,
                started_at=started_at,
                finished_at=finished_at,
                batch_id=batch_id,
            )
            if not snapshot.complete:
                attempt = attempt.model_copy(
                    update={
                        "status": SyncStatus.FAILED,
                        "failure_sections": ("coverage", "conflicts"),
                        "error_code": "INCOMPLETE_SNAPSHOT",
                        "error_message": "snapshot contains incomplete or conflicting facts",
                    }
                )
            published_snapshot, state, published, stale = self.store.publish(snapshot, attempt)
            if (
                state.last_attempt_status is SyncStatus.FAILED
                and attempt.status is SyncStatus.SUCCEEDED
            ):
                attempt = attempt.model_copy(
                    update={
                        "status": SyncStatus.FAILED,
                        "failure_sections": state.last_failure_sections,
                        "error_code": state.last_error_code,
                        "error_message": state.last_error_message,
                    }
                )
            return SyncResult(
                attempt=attempt,
                state=state,
                snapshot=published_snapshot,
                published=published,
                stale=stale,
            )
        except Exception as exc:
            finished_at = _utc(self.clock())
            attempt = SyncAttempt(
                attempt_id=attempt_id,
                account_id=account_id,
                status=SyncStatus.FAILED,
                started_at=started_at,
                finished_at=finished_at,
                failure_sections=("account", "positions", "orders", "transactions"),
                error_code=type(exc).__name__,
                error_message=str(exc)[:500],
            )
            state = self.store.record_failure(attempt)
            return SyncResult(attempt=attempt, state=state, snapshot=self.store.latest(account_id))

    def sync_or_raise(self, *args: Any, **kwargs: Any) -> SyncResult:
        result = self.sync_account(*args, **kwargs)
        if result.attempt.status is SyncStatus.FAILED:
            raise SyncError(result.attempt.error_message or "brokerage synchronization failed")
        return result


SnapshotStore = PrivateSnapshotStore

__all__ = [
    "JsonTransaction",
    "PrivateSnapshotStore",
    "SnapshotStore",
    "SnapshotStoreError",
    "SyncError",
    "SyncResult",
    "SyncService",
]
