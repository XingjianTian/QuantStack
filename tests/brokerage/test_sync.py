from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from quant_stack.brokerage.contracts import SyncAttempt, SyncStatus
from quant_stack.brokerage.schwab_adapter import SchwabAdapter
from quant_stack.brokerage.sync import PrivateSnapshotStore, SnapshotStoreError, SyncService

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "brokerage"
NOW = datetime(2026, 10, 16, tzinfo=UTC)


def _payload(name: str):
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _adapter(*, order_name: str = "normal_orders.json") -> SchwabAdapter:
    return SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload(order_name),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )


def test_sync_is_idempotent_and_survives_reopen(tmp_path: Path) -> None:
    runtime = tmp_path / "private-runtime"
    store = PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
    times = iter((NOW, NOW, NOW + timedelta(days=1), NOW + timedelta(days=1)))
    service = SyncService(_adapter(), store, clock=lambda: next(times))

    first = service.sync_account("fictional-account", "fictional-account-hash")
    second = service.sync_account("fictional-account", "fictional-account-hash")

    assert first.attempt.status is SyncStatus.SUCCEEDED
    assert second.attempt.status is SyncStatus.SUCCEEDED
    assert first.snapshot is not None and second.snapshot is not None
    assert first.snapshot.business_time == second.snapshot.business_time
    assert store.latest("fictional-account").snapshot_id == second.snapshot.snapshot_id

    reopened = PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
    restored = reopened.latest("fictional-account")
    assert restored is not None
    assert restored.snapshot_id == second.snapshot.snapshot_id
    assert oct(os.stat(runtime).st_mode & 0o777) == "0o700"
    assert oct(os.stat(reopened.db_path).st_mode & 0o777) == "0o600"
    assert oct(os.stat(reopened.lock_path).st_mode & 0o777) == "0o600"


def test_latest_failed_attempt_blocks_previous_success(tmp_path: Path) -> None:
    runtime = tmp_path / "private-runtime"
    store = PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
    adapter = _adapter()
    service = SyncService(adapter, store, clock=lambda: NOW)
    success = service.sync_account("fictional-account", "fictional-account-hash")
    assert success.proposal_blocked is False

    class BrokenAdapter:
        def read_snapshot(self, *args, **kwargs):
            raise RuntimeError("fixture section unavailable")

    failed = SyncService(BrokenAdapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert failed.attempt.status is SyncStatus.FAILED
    assert failed.snapshot is not None
    assert failed.snapshot.snapshot_id == success.snapshot.snapshot_id
    assert failed.proposal_blocked is True
    assert store.state("fictional-account").last_success_snapshot_id == success.snapshot.snapshot_id
    assert store.state("fictional-account").complete is False


def test_generic_transaction_rolls_back_and_accounts_are_isolated(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    store.put_json("plans", "fictional-account", {"budget": "100.00"})
    store.put_json("plans", "decimal", {"budget": Decimal("0.10")})
    assert store.get_json("plans", "fictional-account") == {"budget": "100.00"}
    assert store.get_json("plans", "decimal") == {"budget": "0.10"}
    try:
        with store.transaction() as transaction:
            transaction.set("plans", "fictional-account", {"budget": "999.00"})
            transaction.set("plans", "other-account", {"budget": "20.00"})
            raise RuntimeError("abort before commit")
    except RuntimeError:
        pass
    assert store.get_json("plans", "fictional-account") == {"budget": "100.00"}
    assert store.get_json("plans", "other-account") is None


def test_order_state_change_updates_history_without_blocking(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    changed_orders = _payload("normal_orders.json")
    changed_orders[0]["price"] = "36.00"
    changed_orders[0]["lastUpdatedTime"] = "2026-10-16T00:01:00+00:00"
    changed_adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": changed_orders,
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    changed = SyncService(changed_adapter, store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    assert changed.attempt.status is SyncStatus.SUCCEEDED
    assert changed.proposal_blocked is False


def test_conflicting_transaction_is_preserved_and_blocks_snapshot(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    changed_transactions = _payload("normal_transactions.json")
    changed_transactions[0]["netAmount"] = "-71.00"
    changed_adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": changed_transactions,
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    changed = SyncService(changed_adapter, store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    assert changed.attempt.status is SyncStatus.FAILED
    assert changed.proposal_blocked is True
    assert changed.state.last_error_code == "INCOMPLETE_SNAPSHOT"


def test_failed_same_snapshot_cannot_restore_completion(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    success = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert success.snapshot is not None
    incomplete = success.snapshot.model_copy(update={"complete": False})
    failed_attempt = SyncAttempt(
        attempt_id="attempt:failed-same-snapshot",
        account_id="fictional-account",
        status=SyncStatus.FAILED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        failure_sections=("transactions",),
        error_code="FIXTURE_FAILURE",
        error_message="fixture section unavailable",
    )

    restored, state, published, stale = store.publish(incomplete, failed_attempt)

    assert published is False
    assert stale is True
    assert restored.snapshot_id == success.snapshot.snapshot_id
    assert state.last_attempt_status is SyncStatus.FAILED
    assert state.complete is False
    latest = store.latest("fictional-account")
    assert latest is not None and latest.complete is True


def test_snapshot_publish_commit_failure_rolls_back_and_reopens_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    candidate = first.snapshot.model_copy(
        update={"snapshot_id": "faulted-snapshot", "sync_batch_id": "faulted-batch"}
    )
    attempt = SyncAttempt(
        attempt_id="attempt:faulted-publish",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        batch_id="faulted-batch",
    )

    real_connect = store._connect

    class CommitFailConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self._connection = connection

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def commit(self) -> None:
            raise RuntimeError("injected commit failure")

        def rollback(self) -> None:
            self._connection.rollback()

    @contextmanager
    def failing_connect():
        with real_connect() as connection:
            yield CommitFailConnection(connection)

    monkeypatch.setattr(store, "_connect", failing_connect)
    with pytest.raises(RuntimeError, match="injected commit failure"):
        store.publish(candidate, attempt)

    reopened = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    restored = reopened.latest("fictional-account")
    assert restored is not None
    assert restored.snapshot_id == first.snapshot.snapshot_id
    assert reopened.load("fictional-account", "faulted-snapshot") is None
    assert reopened.state("fictional-account").last_attempt_id == first.attempt.attempt_id


def test_older_success_cannot_clear_a_newer_failure(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    success = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert success.snapshot is not None
    newer_failure = SyncAttempt(
        attempt_id="attempt:newer-failure",
        account_id="fictional-account",
        status=SyncStatus.FAILED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        failure_sections=("orders",),
        error_code="FIXTURE_FAILURE",
        error_message="newer failure",
    )
    failure_state = store.record_failure(newer_failure)
    older_snapshot = success.snapshot.model_copy(
        update={
            "snapshot_id": "older-success",
            "sync_batch_id": "older-success-batch",
            "business_time": NOW - timedelta(days=1),
            "received_at": NOW - timedelta(days=1),
        }
    )
    older_success = SyncAttempt(
        attempt_id="attempt:older-success",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW - timedelta(minutes=2),
        finished_at=NOW - timedelta(minutes=1),
        batch_id="older-success-batch",
    )

    restored, state, published, stale = store.publish(older_snapshot, older_success)

    assert published is False
    assert stale is True
    assert restored.snapshot_id == success.snapshot.snapshot_id
    assert state == failure_state
    assert state.last_attempt_id == "attempt:newer-failure"
    assert state.last_attempt_status is SyncStatus.FAILED
    assert state.complete is False


def test_source_as_of_ttl_is_configurable_and_blocks_old_data(tmp_path: Path) -> None:
    old_coverage = _payload("normal_coverage.json")
    old_coverage["sourceAsOf"] = "2025-10-15T00:00:00+00:00"
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    service = SyncService(
        _adapter(),
        store,
        clock=lambda: NOW,
        max_source_age=timedelta(days=365),
    )
    # Recreate the adapter with explicit old provider coverage.
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": old_coverage,
        },
        clock=lambda: NOW,
    )
    service.adapter = adapter
    result = service.sync_account("fictional-account", "fictional-account-hash")

    assert result.attempt.status is SyncStatus.FAILED
    assert result.proposal_blocked is True
    assert result.snapshot is not None
    assert "coverage:source_as_of_too_old" in result.snapshot.conflicts


def test_default_source_as_of_ttl_rejects_data_older_than_24_hours(
    tmp_path: Path,
) -> None:
    old_coverage = _payload("normal_coverage.json")
    old_coverage["sourceAsOf"] = "2026-10-14T23:59:59+00:00"
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": old_coverage,
        },
        clock=lambda: NOW,
    )
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    try:
        result = SyncService(adapter, store, clock=lambda: NOW).sync_account(
            "fictional-account", "fictional-account-hash"
        )
    finally:
        adapter.close()

    assert result.attempt.status is SyncStatus.FAILED
    assert result.proposal_blocked is True
    assert result.snapshot is not None
    assert "coverage:source_as_of_too_old" in result.snapshot.conflicts


def test_newer_mutable_fact_wins_and_later_snapshot_cannot_regress_it(
    tmp_path: Path,
) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    original_order = first.snapshot.orders[0]
    newer_order = original_order.model_copy(
        update={
            "filled_quantity": Decimal("4"),
            "updated_at": NOW + timedelta(minutes=1),
        }
    )
    newer_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "newer-order-state",
            "sync_batch_id": "newer-order-batch",
            "business_time": NOW + timedelta(days=1),
            "orders": (newer_order, *first.snapshot.orders[1:]),
        }
    )
    newer_attempt = SyncAttempt(
        attempt_id="attempt:newer-order-state",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        batch_id="newer-order-batch",
    )
    published, state, did_publish, _ = store.publish(newer_snapshot, newer_attempt)
    assert did_publish is True
    assert published.orders[0].filled_quantity == Decimal("4")
    assert state.complete is True

    regressed_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "regressed-order-state",
            "sync_batch_id": "regressed-order-batch",
            "business_time": NOW + timedelta(days=2),
        }
    )
    regressed_attempt = SyncAttempt(
        attempt_id="attempt:regressed-order-state",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(days=2),
        finished_at=NOW + timedelta(days=2),
        batch_id="regressed-order-batch",
    )
    restored, state, did_publish, stale = store.publish(regressed_snapshot, regressed_attempt)

    assert did_publish is False
    assert stale is True
    assert restored.snapshot_id == "newer-order-state"
    assert state.complete is False
    assert state.last_error_code == "INCOMPLETE_SNAPSHOT"
    with sqlite3.connect(store.db_path) as connection:
        fact = connection.execute(
            """
            SELECT payload FROM facts
            WHERE account_id = ? AND fact_type = 'order' AND fact_id = ?
            """,
            ("fictional-account", original_order.order_id),
        ).fetchone()
        history = connection.execute(
            """
            SELECT incoming_payload FROM fact_history
            WHERE account_id = ? AND fact_type = 'order' AND fact_id = ?
            """,
            ("fictional-account", original_order.order_id),
        ).fetchall()
    assert fact is not None
    assert json.loads(fact[0])["filled_quantity"] == "4"
    assert any(json.loads(row[0])["filled_quantity"] == "2" for row in history)


def test_late_window_fact_is_retained_and_blocks_until_complete_rebuild(
    tmp_path: Path,
) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    source_transaction = first.snapshot.transactions[0]
    source_fill = first.snapshot.fills[0]
    late_time = NOW - timedelta(days=1)
    late_transaction_id = "SAMPLE-LATE-TRANSACTION"
    late_transaction = source_transaction.model_copy(
        update={
            "transaction_id": late_transaction_id,
            "transaction_time": late_time,
        }
    )
    late_fill = source_fill.model_copy(
        update={
            "execution_id": f"{late_transaction_id}:execution",
            "source_transaction_id": late_transaction_id,
            "executed_at": late_time,
        }
    )
    late_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "late-window-snapshot",
            "sync_batch_id": "late-window-batch",
            "business_time": late_time,
            "received_at": late_time,
            "transactions": (*first.snapshot.transactions, late_transaction),
            "fills": (*first.snapshot.fills, late_fill),
        }
    )
    late_attempt = SyncAttempt(
        attempt_id="attempt:late-window",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        batch_id="late-window-batch",
    )

    restored, state, published, stale = store.publish(late_snapshot, late_attempt)

    assert published is False
    assert stale is True
    assert restored.snapshot_id == first.snapshot.snapshot_id
    assert state.complete is False
    assert state.last_attempt_status is SyncStatus.FAILED
    assert state.last_error_code == "LATE_WINDOW_FACTS"
    assert state.pending_fact_ids == (
        f"fill:{late_transaction_id}:execution",
        f"transaction:{late_transaction_id}",
    )
    stored_late = store.load("fictional-account", "late-window-snapshot")
    assert stored_late is not None
    assert "coverage:late_window_fact_requires_rebuild" in stored_late.conflicts
    with sqlite3.connect(store.db_path) as connection:
        rows = connection.execute(
            """
            SELECT fact_type, fact_id FROM facts
            WHERE account_id = ? AND fact_id IN (?, ?)
            """,
            (
                "fictional-account",
                late_transaction_id,
                f"{late_transaction_id}:execution",
            ),
        ).fetchall()
    assert {(row[0], row[1]) for row in rows} == {
        ("transaction", late_transaction_id),
        ("fill", f"{late_transaction_id}:execution"),
    }
    assert {item.transaction_id for item in store.transactions("fictional-account")} >= {
        late_transaction_id
    }
    with store.transaction() as transaction:
        assert {item.transaction_id for item in transaction.transactions("fictional-account")} >= {
            late_transaction_id
        }

    rebuilt_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "rebuilt-after-late-window",
            "sync_batch_id": "rebuilt-after-late-window-batch",
            "business_time": NOW + timedelta(days=1),
            "transactions": (*first.snapshot.transactions, late_transaction),
            "fills": (*first.snapshot.fills, late_fill),
        }
    )
    rebuilt_attempt = SyncAttempt(
        attempt_id="attempt:rebuilt-after-late-window",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=2),
        finished_at=NOW + timedelta(minutes=2),
        batch_id="rebuilt-after-late-window-batch",
    )
    rebuilt, rebuilt_state, rebuilt_published, rebuilt_stale = store.publish(
        rebuilt_snapshot,
        rebuilt_attempt,
    )
    assert rebuilt_published is True
    assert rebuilt_stale is False
    assert rebuilt.snapshot_id == "rebuilt-after-late-window"
    assert rebuilt_state.complete is True
    assert rebuilt_state.pending_fact_ids == ()


def test_position_version_never_regresses_in_a_newer_snapshot(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    first = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert first.snapshot is not None
    original_position = first.snapshot.positions[0]
    newer_position = original_position.model_copy(
        update={
            "quantity": Decimal("9"),
            "as_of": NOW + timedelta(minutes=1),
        }
    )
    newer_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "newer-position-state",
            "sync_batch_id": "newer-position-batch",
            "business_time": NOW + timedelta(days=1),
            "positions": (newer_position, *first.snapshot.positions[1:]),
        }
    )
    newer_attempt = SyncAttempt(
        attempt_id="attempt:newer-position-state",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        batch_id="newer-position-batch",
    )
    published, state, did_publish, _ = store.publish(newer_snapshot, newer_attempt)
    assert did_publish is True
    assert published.positions[0].quantity == Decimal("9")
    assert state.complete is True

    regressed_snapshot = first.snapshot.model_copy(
        update={
            "snapshot_id": "regressed-position-state",
            "sync_batch_id": "regressed-position-batch",
            "business_time": NOW + timedelta(days=2),
        }
    )
    regressed_attempt = SyncAttempt(
        attempt_id="attempt:regressed-position-state",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(days=2),
        finished_at=NOW + timedelta(days=2),
        batch_id="regressed-position-batch",
    )
    restored, state, did_publish, stale = store.publish(regressed_snapshot, regressed_attempt)

    assert did_publish is False
    assert stale is True
    assert restored.snapshot_id == "newer-position-state"
    assert state.complete is False
    with sqlite3.connect(store.db_path) as connection:
        fact = connection.execute(
            """
            SELECT payload FROM facts
            WHERE account_id = ? AND fact_type = 'position' AND fact_id = ?
            """,
            ("fictional-account", original_position.position_id),
        ).fetchone()
    assert fact is not None
    assert json.loads(fact[0])["quantity"] == "9"


def test_older_snapshot_does_not_overwrite_current_mutable_fact(tmp_path: Path) -> None:
    store = PrivateSnapshotStore(tmp_path / "private-runtime", source_root=tmp_path / "source")
    success = SyncService(_adapter(), store, clock=lambda: NOW).sync_account(
        "fictional-account", "fictional-account-hash"
    )
    assert success.snapshot is not None
    original_order = success.snapshot.orders[0]
    older_order = original_order.model_copy(
        update={
            "filled_quantity": Decimal("1"),
            "updated_at": NOW - timedelta(days=1),
        }
    )
    older_snapshot = success.snapshot.model_copy(
        update={
            "snapshot_id": "older-snapshot",
            "sync_batch_id": "older-batch",
            "business_time": NOW - timedelta(days=1),
            "orders": (older_order, *success.snapshot.orders[1:]),
        }
    )
    older_attempt = SyncAttempt(
        attempt_id="attempt:older-snapshot",
        account_id="fictional-account",
        status=SyncStatus.SUCCEEDED,
        started_at=NOW + timedelta(minutes=1),
        finished_at=NOW + timedelta(minutes=1),
        batch_id="older-batch",
    )

    restored, state, published, stale = store.publish(older_snapshot, older_attempt)

    assert published is False
    assert stale is True
    assert restored.snapshot_id == success.snapshot.snapshot_id
    assert state.last_success_snapshot_id == success.snapshot.snapshot_id
    with sqlite3.connect(store.db_path) as connection:
        payload = connection.execute(
            """
            SELECT payload FROM facts
            WHERE account_id = ? AND fact_type = 'order' AND fact_id = ?
            """,
            ("fictional-account", original_order.order_id),
        ).fetchone()
    assert payload is not None
    assert json.loads(payload[0])["filled_quantity"] == "2"


def test_private_runtime_rejects_existing_unsafe_paths(tmp_path: Path) -> None:
    non_private = tmp_path / "non-private"
    non_private.mkdir()
    non_private.chmod(0o755)
    with pytest.raises(SnapshotStoreError):
        PrivateSnapshotStore(non_private, source_root=tmp_path / "source")

    linked_root = tmp_path / "linked-runtime"
    linked_root_target = tmp_path / "linked-runtime-target"
    linked_root_target.mkdir()
    linked_root.symlink_to(linked_root_target, target_is_directory=True)
    with pytest.raises(SnapshotStoreError):
        PrivateSnapshotStore(linked_root, source_root=tmp_path / "source")

    runtime = tmp_path / "private-runtime"
    PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
    external = tmp_path / "external.sqlite3"
    external.touch(mode=0o600)
    runtime.joinpath("brokerage.sqlite3").unlink()
    runtime.joinpath("brokerage.sqlite3").symlink_to(external)
    with pytest.raises(SnapshotStoreError):
        PrivateSnapshotStore(runtime, source_root=tmp_path / "source")

    runtime.joinpath("brokerage.sqlite3").unlink()
    PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
    runtime.joinpath(".brokerage.write.lock").unlink()
    runtime.joinpath(".brokerage.write.lock").symlink_to(external)
    with pytest.raises(SnapshotStoreError):
        PrivateSnapshotStore(runtime, source_root=tmp_path / "source")
