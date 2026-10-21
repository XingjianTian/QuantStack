from __future__ import annotations

import sys
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    source = path.read_text(encoding="utf-8")
    if source.count(old) != 1:
        raise SystemExit(f"runtime recovery anchor drifted in {path}")
    path.write_text(source.replace(old, new), encoding="utf-8")


def main(root: Path) -> None:
    replace_once(
        root / "app/services/runtime_preferences_service.py",
        '''    bootstrap_state = prefs.bootstrap_state
    readiness_active = bootstrap_state in {"running", "ready", "failed"}
    primary_result = readiness.market_results.get(prefs.primary_market)
    primary_market_ready = (
        bool(primary_result and primary_result.ready)
        if readiness_active
        else False
    )
    if primary_market_ready:
        bootstrap_state = "ready"
    elif empty_system and bootstrap_state == "ready":
        bootstrap_state = "not_started"
    elif bootstrap_state == "ready" and not primary_market_ready:
        bootstrap_state = "running"
''',
        '''    bootstrap_state = prefs.bootstrap_state
    readiness_active = bootstrap_state in {"running", "failed"}
    primary_result = readiness.market_results.get(prefs.primary_market)
    primary_market_ready = (
        bool(primary_result and primary_result.ready)
        if readiness_active
        else False
    )
    # A persisted ready state records completion of first-run initialization.
    # Ongoing scan/fundamental freshness is operational health, not a reason to
    # put an initialized installation back behind the bootstrap screen.  Only
    # a genuinely empty database can invalidate the completed state.
    if bootstrap_state == "ready" and empty_system:
        bootstrap_state = "not_started"
    elif bootstrap_state in {"running", "failed"} and primary_market_ready:
        bootstrap_state = "ready"
''',
    )
    activity_path = root / "app/services/market_activity_service.py"
    replace_once(
        activity_path,
        '''def _queued_bootstrap_market_payload(
''',
        '''def _clear_orphaned_runtime_activity(
    db: Session,
    market: str,
    record: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Persist an orphaned data-fetch activity as idle exactly once."""
    if record is None:
        return None
    stale_payload = _overlay_stale_runtime_activity(record)
    if stale_payload.get("status") != "stale":
        return record
    try:
        typed_record = RuntimeActivityRecord.from_payload(record)
    except ValueError:
        return record
    reason = f"No live data-fetch lock owns task {typed_record.task_id or 'unknown'}."
    logger.warning(
        "Finalizing orphaned runtime activity; market=%s task_id=%s",
        market,
        typed_record.task_id,
    )
    return _save_market_activity(
        db,
        market,
        _activity_payload(
            market=market,
            stage_key=typed_record.stage_key,
            lifecycle="idle",
            status="idle",
            task_id=typed_record.task_id,
            message=f"Idle; orphaned task was cleared: {reason}",
        ),
    )


def _queued_bootstrap_market_payload(
''',
    )
    replace_once(
        activity_path,
        '''    for market in enabled_markets:
        record = _load_market_activity(db, market)
        market_payloads.append(
''',
        '''    for market in enabled_markets:
        record = _clear_orphaned_runtime_activity(
            db, market, _load_market_activity(db, market)
        )
        market_payloads.append(
''',
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch_runtime_recovery.py APP_ROOT")
    main(Path(sys.argv[1]))
