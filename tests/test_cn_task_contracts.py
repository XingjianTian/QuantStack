from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from quant_stack.data.providers.cn import (
    CNDailyIncrementPolicy,
    CNDataTask,
    CNDataTaskAttempt,
    CNTaskStatus,
    CNTaskType,
)


def _task(**updates: object) -> CNDataTask:
    values = {
        "task_type": CNTaskType.DAILY_INCREMENT,
        "provider": "akshare",
        "provider_endpoint": "stock_zh_a_hist",
        "provider_version": "1.18.81",
        "native_schema_version": "fixture-required-v1",
        "exchange": "XSHG",
        "instrument_scope": ("XSHG.600000", "XSHG.600519"),
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 28),
        "created_at": datetime(2026, 7, 29, tzinfo=UTC),
    }
    values.update(updates)
    return CNDataTask.model_validate(values)


def test_task_idempotency_key_is_deterministic_for_same_definition() -> None:
    first = _task()
    later_record = _task(
        created_at=datetime(2026, 7, 30, tzinfo=UTC),
        status=CNTaskStatus.RETRYABLE_FAILED,
        instrument_scope=("XSHG.600519", "XSHG.600000"),
    )

    assert first.idempotency_key == later_record.idempotency_key
    assert first.task_id == later_record.task_id
    assert first.model_dump()["idempotency_key"] == first.idempotency_key
    assert first.model_dump()["task_id"] == first.task_id


def test_retry_attempts_are_independent_and_checkpoint_is_serializable() -> None:
    task = _task()
    now = datetime(2026, 7, 29, tzinfo=UTC)
    first = CNDataTaskAttempt(
        attempt_id="attempt-1",
        task_id=task.task_id,
        attempt_count=1,
        status=CNTaskStatus.RETRYABLE_FAILED,
        checkpoint={"instrument": "XSHG.600000", "year": 2025},
        cursor="2025-12-31",
        records_received=242,
        raw_uri="quant-data://raw/CN/akshare/attempt-1.json",
        error_summary="fixture timeout",
        started_at=now,
        completed_at=now + timedelta(minutes=1),
        next_retry_at=now + timedelta(minutes=5),
    )
    second = CNDataTaskAttempt(
        attempt_id="attempt-2",
        task_id=task.task_id,
        attempt_count=2,
        status=CNTaskStatus.SUCCEEDED,
        checkpoint={"instrument": "XSHG.600519", "year": 2026},
        records_received=260,
        raw_uri="quant-data://raw/CN/akshare/attempt-2.json",
        output_manifest_uri="quant-data://catalog/manifests/cn-bars.json",
        started_at=now + timedelta(minutes=5),
        completed_at=now + timedelta(minutes=6),
    )

    assert first.attempt_id != second.attempt_id
    assert first.task_id == second.task_id
    assert first.model_dump_json()
    assert second.attempt_count == 2


def test_invalid_task_state_is_rejected() -> None:
    with pytest.raises(ValidationError):
        CNDataTaskAttempt(
            attempt_id="attempt-running",
            task_id=_task().task_id,
            attempt_count=1,
            status="not-a-state",
        )
    with pytest.raises(ValidationError, match="running attempts require started_at"):
        CNDataTaskAttempt(
            attempt_id="attempt-running",
            task_id=_task().task_id,
            attempt_count=1,
            status=CNTaskStatus.RUNNING,
        )


def test_daily_increment_window_is_configurable() -> None:
    assert CNDailyIncrementPolicy().lookback_trading_days == 20
    assert CNDailyIncrementPolicy(lookback_trading_days=10).lookback_trading_days == 10
