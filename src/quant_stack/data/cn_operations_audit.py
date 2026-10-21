from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb


def _bars_profile(path: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        connection.execute("SET threads = 1")
        connection.execute("SET memory_limit = '768MB'")
        connection.execute("SET preserve_insertion_order = false")
        row = connection.execute(
            "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
            "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
            "list(DISTINCT lineage.adjustment ORDER BY lineage.adjustment) "
            "FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return {
        "rows": int(row[0]),
        "unique_keys": int(row[1]),
        "instruments": int(row[2]),
        "start_date": row[3].isoformat(),
        "end_date": row[4].isoformat(),
        "adjustments": list(row[5]),
    }


def _factor_profile(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    requests = payload.get("requests", [])
    statuses: dict[str, int] = {}
    for request in requests:
        status = str(request.get("status"))
        statuses[status] = statuses.get(status, 0) + 1
    instrument_ids = [str(request.get("instrument_id", "")) for request in requests]
    return {
        "complete": payload.get("complete") is True,
        "universe_count": int(payload.get("universe_count", 0)),
        "request_count": len(requests),
        "request_end": payload.get("request_end"),
        "request_policy": payload.get("request_policy"),
        "statuses": statuses,
        "instrument_ids": instrument_ids,
        "unique_instrument_ids": len(set(instrument_ids)),
    }


def _bars_key_coverage(raw_path: Path, qfq_path: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        connection.execute("SET threads = 1")
        connection.execute("SET memory_limit = '768MB'")
        connection.execute("SET preserve_insertion_order = false")
        raw_only = connection.execute(
            "SELECT count(*) FROM ("
            "SELECT instrument_id, trading_date FROM read_parquet(?) "
            "EXCEPT SELECT instrument_id, trading_date FROM read_parquet(?))",
            [str(raw_path), str(qfq_path)],
        ).fetchone()
        qfq_only = connection.execute(
            "SELECT count(*) FROM ("
            "SELECT instrument_id, trading_date FROM read_parquet(?) "
            "EXCEPT SELECT instrument_id, trading_date FROM read_parquet(?))",
            [str(qfq_path), str(raw_path)],
        ).fetchone()
        qfq_ids = {
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT instrument_id FROM read_parquet(?)", [str(qfq_path)]
            ).fetchall()
        }
    finally:
        connection.close()
    return {
        "raw_only_keys": int(raw_only[0]) if raw_only else 0,
        "qfq_only_keys": int(qfq_only[0]) if qfq_only else 0,
        "qfq_instrument_ids": qfq_ids,
    }


def audit_cn_operations(
    *,
    work_root: Path,
    expected_sessions: list[str],
    raw_path: Path | None,
    qfq_path: Path | None,
    factor_path: Path | None,
    runtime_evidence_path: Path,
    through_date: str,
    promotion_work_root: Path | None = None,
    artifact_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one fail-closed daily/weekly CN operations report."""

    alerts: list[str] = []
    daily: dict[str, Any] = {}
    promotions: dict[str, Any] = {}
    for session in expected_sessions:
        summary_path = work_root / f"daily-{session.replace('-', '')}" / "runner-summary.json"
        if not summary_path.is_file():
            alerts.append(f"missing_daily_summary:{session}")
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        daily[session] = summary
        if summary.get("schema_version") == "quantstack.cn-session-daily-run.v1":
            rows = int(summary.get("records_received", 0))
            instruments = int(summary.get("distinct_instruments", 0))
            omissions = int(summary.get("omissions", 0))
            checks = {
                "status": summary.get("status") == "completed",
                "session": summary.get("session_date") == session,
                "persistence": summary.get("persistence") == "single_session_parquet",
                "coverage": rows == instruments and instruments + omissions > 0,
                "provider_retries": summary.get("provider_retries") == 0,
                "isolated": summary.get("catalog_published") is False
                and summary.get("production_pointer_updated") is False,
            }
            for name, passed in checks.items():
                if not passed:
                    alerts.append(f"daily_{name}:{session}")
        else:
            verification_mode = summary.get("nas_verification_mode", "full_replay")
            first_imports = int(summary.get("nas_new_imports", 0)) + int(
                summary.get("nas_recovered_imports", 0)
            )
            expected_batches = summary.get("planned_batches")
            checks = {
                "status": summary.get("status") == "completed",
                "duplicate_keys": summary.get("duplicate_keys") == 0,
                "rows_reconciled": summary.get("records_received")
                == summary.get("nas_persisted_bar_rows"),
                "batches_reconciled": expected_batches
                == summary.get("succeeded_batches")
                == first_imports
                == summary.get("nas_verified_receipts"),
                "replay_policy": (
                    verification_mode == "full_replay"
                    and summary.get("nas_idempotent_replays") == expected_batches
                    and summary.get("nas_idempotent_replay_deferred") in (None, False)
                )
                or (
                    verification_mode == "readback_only"
                    and summary.get("nas_idempotent_replays") == 0
                    and summary.get("nas_idempotent_replay_deferred") is True
                ),
                "isolated": summary.get("canonical_eligible") is False
                and summary.get("catalog_published") is False
                and summary.get("production_pointer_updated") is False,
            }
            for name, passed in checks.items():
                if not passed:
                    alerts.append(f"daily_{name}:{session}")

        if promotion_work_root is not None:
            promotion_path = (
                promotion_work_root / f"qfq-{session.replace('-', '')}" / "summary.json"
            )
            if not promotion_path.is_file():
                alerts.append(f"missing_qfq_promotion_summary:{session}")
                continue
            promotion = json.loads(promotion_path.read_text(encoding="utf-8"))
            promotions[session] = promotion
            expected_version = f"daily-through-{session.replace('-', '')}.qfq-release-v1"
            raw_evidence = promotion.get("raw", {})
            qfq_evidence = promotion.get("qfq", {})
            factor_evidence = promotion.get("factors", {})
            plan_evidence = promotion.get("plan", {})
            import_evidence = promotion.get("nas_import", {})
            catalog_evidence = promotion.get("catalog", {})
            first_catalog = catalog_evidence.get("first", {})
            replay_catalog = catalog_evidence.get("replay", {})
            ss_evidence = promotion.get("stock_screener", {})
            expected_policy = {
                "concurrency": 1,
                "request_interval_ms": 50,
                "batch_size": 500,
                "batch_interval_seconds": 5,
            }
            promotion_checks = {
                "identity": promotion.get("status") == "completed"
                and promotion.get("session_date") == session
                and promotion.get("dataset_version") == expected_version,
                "raw": raw_evidence.get("rows") == raw_evidence.get("unique_keys")
                and raw_evidence.get("end_date") == session
                and raw_evidence.get("adjustments") == ["none"],
                "factor": factor_evidence.get("complete") is True
                and factor_evidence.get("request_end") == session
                and factor_evidence.get("request_count")
                == factor_evidence.get("universe_count")
                == factor_evidence.get("unique_instrument_ids")
                and factor_evidence.get("request_policy") == expected_policy
                and not (set(factor_evidence.get("statuses", {})) - {"ok", "ok_no_adjustments"}),
                "qfq": qfq_evidence.get("rows")
                == qfq_evidence.get("unique_keys")
                == raw_evidence.get("rows")
                and qfq_evidence.get("end_date") == session
                and qfq_evidence.get("adjustments") == ["qfq"],
                "plan": plan_evidence
                == {
                    "status": "ready",
                    "strategy": "full_rebuild_required",
                    "target_watermark": session,
                    "target_instrument_count": qfq_evidence.get("instruments"),
                    "raw_inputs_unchanged": True,
                },
                "nas_import": import_evidence.get("first_idempotent_replay") in {False, True}
                and import_evidence.get("replay_idempotent_replay") is True
                and import_evidence.get("import_resumed", False)
                is import_evidence.get("first_idempotent_replay")
                and import_evidence.get("catalog_published_during_import") is False,
                "catalog": first_catalog.get("status") == "published"
                and isinstance(first_catalog.get("idempotent_replay"), bool)
                and replay_catalog.get("status") == "published"
                and replay_catalog.get("idempotent_replay") is True
                and all(
                    item.get("dataset_version") == expected_version
                    and item.get("last_date") == session
                    and item.get("row_count") == qfq_evidence.get("rows")
                    and item.get("instrument_count") == qfq_evidence.get("instruments")
                    and item.get("catalog_replay_verified") is True
                    and item.get("ss_exact_version_pinned") is True
                    for item in (first_catalog, replay_catalog)
                ),
                "stock_screener": ss_evidence.get("status") == "passed"
                and ss_evidence.get("dataset_version") == expected_version
                and ss_evidence.get("watermark") == session
                and ss_evidence.get("read_mode") == "quantstack_primary"
                and ss_evidence.get("shadow_enabled") is False
                and ss_evidence.get("frontend_http_status") == 200,
            }
            for name, passed in promotion_checks.items():
                if not passed:
                    alerts.append(f"qfq_promotion_{name}:{session}")

    if artifact_evidence is None:
        if raw_path is None or qfq_path is None or factor_path is None:
            raise ValueError("local artifact audit requires raw, qfq and factor paths")
        raw = _bars_profile(raw_path)
        qfq = _bars_profile(qfq_path)
        factors = _factor_profile(factor_path)
        key_coverage = _bars_key_coverage(raw_path, qfq_path)
        factor_ids = factors.pop("instrument_ids")
        qfq_ids = key_coverage.pop("qfq_instrument_ids")
        factors["missing_qfq_instruments"] = len(qfq_ids - set(factor_ids))
        factors["extra_factor_instruments"] = len(set(factor_ids) - qfq_ids)
    else:
        raw = dict(artifact_evidence.get("raw", {}))
        qfq = dict(artifact_evidence.get("qfq", {}))
        factors = dict(artifact_evidence.get("factors", {}))
        exact = artifact_evidence.get("exact_key_coverage") is True
        key_coverage = {
            "raw_only_keys": 0 if exact else 1,
            "qfq_only_keys": 0 if exact else 1,
            "evidence_source": "nas_local_materialization",
        }
        factor_ids = []
        factors["missing_qfq_instruments"] = (
            0 if factors.get("universe_count") == qfq.get("instruments") else 1
        )
        factors["extra_factor_instruments"] = factors["missing_qfq_instruments"]
    if raw["rows"] != raw["unique_keys"]:
        alerts.append("raw_duplicate_keys")
    if qfq["rows"] != qfq["unique_keys"]:
        alerts.append("qfq_duplicate_keys")
    if key_coverage["raw_only_keys"] or key_coverage["qfq_only_keys"]:
        alerts.append("raw_qfq_key_coverage")
    if raw["end_date"] != through_date:
        alerts.append("raw_watermark")
    if qfq["end_date"] != through_date:
        alerts.append("qfq_watermark")
    if raw["adjustments"] != ["none"]:
        alerts.append("raw_adjustment")
    if qfq["adjustments"] != ["qfq"]:
        alerts.append("qfq_adjustment")
    if not factors["complete"] or factors["request_count"] != factors["universe_count"]:
        alerts.append("factor_coverage")
    if (
        (artifact_evidence is None and not all(factor_ids))
        or factors["unique_instrument_ids"] != factors["request_count"]
        or factors["missing_qfq_instruments"]
        or factors["extra_factor_instruments"]
    ):
        alerts.append("factor_instrument_coverage")
    if factors["request_end"] != through_date:
        alerts.append("factor_watermark")
    policy = factors["request_policy"] or {}
    if policy != {
        "concurrency": 1,
        "request_interval_ms": 50,
        "batch_size": 500,
        "batch_interval_seconds": 5,
    }:
        alerts.append("factor_request_policy")
    if set(factors["statuses"]) - {"ok", "ok_no_adjustments"}:
        alerts.append("factor_provider_errors")

    runtime = json.loads(runtime_evidence_path.read_text(encoding="utf-8"))
    if runtime.get("daily_timer") != {"enabled": True, "active": True}:
        alerts.append("daily_timer")
    if runtime.get("operations_timer") != {"enabled": True, "active": True}:
        alerts.append("operations_timer")
    if runtime.get("tailscale_required", True) and runtime.get("tailscale_service") != {
        "enabled": True,
        "active": True,
    }:
        alerts.append("tailscale_service")
    scheduler_backend = runtime.get("scheduler_backend", "systemd_user")
    if scheduler_backend == "systemd_user":
        if runtime.get("operator_linger") is not True:
            alerts.append("operator_linger")
    elif scheduler_backend == "user_cron":
        if runtime.get("scheduler_service") != {"enabled": True, "active": True}:
            alerts.append("scheduler_service")
        cron_entries = runtime.get("cron_entries") or {}
        if cron_entries.get("daily", {}).get("valid") is not True:
            alerts.append("daily_cron_entry")
        if cron_entries.get("audit", {}).get("valid") is not True:
            alerts.append("audit_cron_entry")
        if any(
            cron_entries.get(name, {}).get("single_flight") is not True
            for name in ("daily", "audit")
        ):
            alerts.append("cron_single_flight")
    else:
        alerts.append("scheduler_backend")
    if runtime.get("weekly_reconciliation", {}).get("scheduled") is not True:
        alerts.append("weekly_reconciliation")
    if runtime.get("provider_budget", {}).get("blocked") is not None:
        alerts.append("provider_budget_blocked")
    unhealthy = [
        name for name, status in runtime.get("nas_containers", {}).items() if status != "healthy"
    ]
    if unhealthy:
        alerts.append("nas_containers:" + ",".join(sorted(unhealthy)))
    if runtime.get("stale_tasks"):
        alerts.append("stale_tasks")
    nas_isolated = runtime.get("nas_isolated", {})
    for key in ("first_import", "idempotent_replay", "readback"):
        if nas_isolated.get(key) != "passed":
            alerts.append(f"nas_isolated_{key}")
    if nas_isolated.get("exact_key_coverage") is not True:
        alerts.append("nas_isolated_key_coverage")
    if float(nas_isolated.get("latest_anchor_max_close_drift", 1)) > 1e-9:
        alerts.append("nas_isolated_latest_anchor")
    if runtime.get("ss_kline", {}).get("last_date") != through_date:
        alerts.append("ss_kline_last_date")

    return {
        "schema_version": "1.0.0",
        "status": "passed" if not alerts else "failed",
        "through_date": through_date,
        "expected_sessions": expected_sessions,
        "actual_sessions": sorted(daily),
        "daily": daily,
        "promotions": promotions,
        "raw": raw,
        "qfq": qfq,
        "key_coverage": key_coverage,
        "factors": factors,
        "runtime": runtime,
        "alerts": alerts,
        "late_revision_policy": "new_immutable_version_only",
    }
