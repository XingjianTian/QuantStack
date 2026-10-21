from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from quant_stack.data.cn_qfq_incremental import build_factor_query_plan
from quant_stack.data.session_parquet import install_session_package
from scripts import run_r12_cn_qfq_candidate as r12_qfq
from scripts.run_r12_cn_qfq_candidate import (
    RAW_SESSIONS,
    _completed_qfq_summary,
    _factor_plan,
    _instrument_id_digest,
    _reconcile_raw_budget,
    _reserve_factor,
    _sha256,
    _validate_frozen_factor_universe,
)


def _raw_fixture(tmp_path: Path) -> tuple[dict[str, object], Path, Path, Path, dict[str, object]]:
    candidate = tmp_path / "raw-candidate"
    authority = tmp_path / "authority-sessions"
    authority.mkdir(parents=True)
    reservations: dict[str, object] = {}
    validation_sessions: dict[str, object] = {}
    for index, session in enumerate(RAW_SESSIONS):
        key = session.isoformat()
        package = install_session_package(
            records=[
                {
                    "instrument_id": "XSHG.600000",
                    "symbol": "600000",
                    "venue": "XSHG",
                    "trading_date": session,
                    "ts_event": f"{key}T07:00:00Z",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.5,
                    "close": 10.0,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "currency": "CNY",
                    "provider_preclose": 10.0,
                }
            ],
            output_root=candidate / "session-parquet",
            market="CN",
            session=session,
            dataset_id="cn_equity_daily",
            version=f"daily-{key}",
            adjustment="raw",
        )
        run = candidate / f"work/daily-{session:%Y%m%d}"
        run.mkdir(parents=True)
        instruments = run / "instruments.json"
        instruments.write_text(
            json.dumps({"instruments": [{"instrument_id": "XSHG.600000"}]}),
            encoding="utf-8",
        )
        evidence = candidate / f"evidence/cn-session-{key}.json"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "session": key,
                    "provider_retries": 0,
                    "budget": {
                        "day": "2026-09-30",
                        "used_before": index,
                        "actual_shared_ledger_delta": 1,
                    },
                    "object_partition": {
                        "raw_complete": True,
                        "omission_semantics": "none",
                        "union_equals_u": True,
                        "returned_omission_overlap": 0,
                        "u_count": 1,
                        "returned_count": 1,
                        "omission_count": 0,
                        "u_sha256": hashlib.sha256(b"XSHG.600000\n").hexdigest(),
                        "returned_sha256": hashlib.sha256(b"XSHG.600000\n").hexdigest(),
                        "omission_sha256": hashlib.sha256(b"\n").hexdigest(),
                    },
                }
            ),
            encoding="utf-8",
        )
        reservations[key] = {
            "status": "completed",
            "allocated_requests": 11_000,
            "actual_shared_ledger_delta": 1,
        }
        bars = package.path / "bars.parquet"
        manifest = package.path / "manifest.json"
        assert pq.ParquetFile(bars).metadata.num_rows == 1
        validation_sessions[key] = {
            "status": "pass",
            "checks": {"physical": True},
            "counts": {"U": 1, "R": 1, "O": 0, "rows": 1},
            "digests": {
                "U": hashlib.sha256(b"XSHG.600000\n").hexdigest(),
                "R": hashlib.sha256(b"XSHG.600000\n").hexdigest(),
                "O": hashlib.sha256(b"\n").hexdigest(),
                "bars_sha256": _sha256(bars),
                "manifest_sha256": _sha256(manifest),
                "instruments_sha256": _sha256(instruments),
            },
        }
    validation = candidate / "evidence/independent-validation.json"
    validation.write_text(
        json.dumps(
            {
                "schema_version": "quantstack.r12-cn-candidate-independent-validation.v1",
                "overall_status": "pass",
                "sessions": validation_sessions,
            }
        ),
        encoding="utf-8",
    )
    task = {
        "schema_version": "quantstack.r12-provider-budget.v1",
        "total_limit": 44_000,
        "allocated": 44_000,
        "reservations": reservations,
    }
    shared = {
        "schema_version": "1.0.0",
        "blocked": False,
        "days": {"2026-09-30": {"used": 4}},
    }
    return task, candidate, authority, validation, shared


def test_raw_settlement_requires_physical_proof_and_is_idempotent(tmp_path: Path) -> None:
    task, candidate, authority, validation, shared = _raw_fixture(tmp_path)
    settled = _reconcile_raw_budget(
        task,
        raw_candidate_root=candidate,
        authority_session_root=authority,
        validation_receipt_path=validation,
        shared_budget=shared,
    )
    assert settled["allocated"] == 4
    assert settled["raw_settlement"]["original_allocated"] == 44_000
    assert settled["raw_settlement"]["released_capacity"] == 43_996

    before = deepcopy(settled)
    again = _reconcile_raw_budget(
        settled,
        raw_candidate_root=candidate,
        authority_session_root=authority,
        validation_receipt_path=validation,
        shared_budget=shared,
    )
    assert again == before


def test_raw_settlement_rejects_unknown_or_drifted_proof(tmp_path: Path) -> None:
    task, candidate, authority, validation, shared = _raw_fixture(tmp_path)
    task["reservations"][RAW_SESSIONS[0].isoformat()]["status"] = "failed"
    with pytest.raises(RuntimeError, match="not safely complete"):
        _reconcile_raw_budget(
            task,
            raw_candidate_root=candidate,
            authority_session_root=authority,
            validation_receipt_path=validation,
            shared_budget=shared,
        )

    task, candidate, authority, validation, shared = _raw_fixture(tmp_path / "drift")
    _reconcile_raw_budget(
        task,
        raw_candidate_root=candidate,
        authority_session_root=authority,
        validation_receipt_path=validation,
        shared_budget=shared,
    )
    receipt = json.loads(validation.read_text(encoding="utf-8"))
    receipt["note"] = "changed"
    validation.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(RuntimeError, match="settlement proof drifted"):
        _reconcile_raw_budget(
            task,
            raw_candidate_root=candidate,
            authority_session_root=authority,
            validation_receipt_path=validation,
            shared_budget=shared,
        )


def test_factor_reservation_uses_composite_key_and_keeps_full_charge(
    tmp_path: Path,
) -> None:
    task, candidate, authority, validation, shared = _raw_fixture(tmp_path)
    _reconcile_raw_budget(
        task,
        raw_candidate_root=candidate,
        authority_session_root=authority,
        validation_receipt_path=validation,
        shared_budget=shared,
    )
    reservation = _reserve_factor(task, session=date(2026, 9, 22), query_count=1_370)
    assert reservation["allocated_requests"] == 1_371
    assert "2026-09-22" not in task["reservations"]
    assert task["factor_reservations"]["factor:2026-09-22"] == reservation
    assert task["allocated"] == 1_375
    with pytest.raises(RuntimeError, match="already reserved"):
        _reserve_factor(task, session=date(2026, 9, 22), query_count=1)


def test_factor_reservation_enforces_44k_boundary(tmp_path: Path) -> None:
    task, candidate, authority, validation, shared = _raw_fixture(tmp_path)
    _reconcile_raw_budget(
        task,
        raw_candidate_root=candidate,
        authority_session_root=authority,
        validation_receipt_path=validation,
        shared_budget=shared,
    )
    task["raw_settlement"]["raw_actual_total"] = 43_000
    task["allocated"] = 43_000
    with pytest.raises(RuntimeError, match="task budget exhausted"):
        _reserve_factor(task, session=date(2026, 9, 22), query_count=1_370)
    with pytest.raises(RuntimeError, match="frozen cap"):
        _reserve_factor(task, session=date(2026, 9, 22), query_count=1_371)


def test_factor_plan_rejects_overlay_missing_first_frozen_session(tmp_path: Path) -> None:
    session_root = tmp_path / "sessions/market=CN/session=2026-09-23"
    session_root.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="exact frozen five sessions"):
        _factor_plan(
            {"session_root": str(tmp_path / "sessions")},
            {"watermark": "2026-09-21"},
        )


def test_frozen_factor_universe_requires_exact_evolution_count_and_digest(
    monkeypatch,
) -> None:
    session = date(2026, 9, 22)
    universe = [
        {"instrument_id": "XSHG.600000", "provider_symbol": "sh.600000"},
        {"instrument_id": "XSHE.000001", "provider_symbol": "sz.000001"},
    ]
    monkeypatch.setitem(
        r12_qfq.EXPECTED_FACTOR_UNIVERSE,
        session,
        2,
    )
    monkeypatch.setitem(
        r12_qfq.EXPECTED_FACTOR_UNIVERSE_DIGEST,
        session,
        _instrument_id_digest([item["instrument_id"] for item in universe]),
    )
    _validate_frozen_factor_universe(session, universe)

    with pytest.raises(RuntimeError, match="count differs"):
        _validate_frozen_factor_universe(session, universe[:1])
    with pytest.raises(RuntimeError, match="digest differs"):
        _validate_frozen_factor_universe(
            session,
            [universe[0], {"instrument_id": "XSHE.000002", "provider_symbol": "sz.000002"}],
        )


def test_factor_plan_rejects_raw_id_outside_frozen_universe() -> None:
    with pytest.raises(ValueError, match="daily Raw contains instruments outside"):
        build_factor_query_plan(
            session=date(2026, 9, 22),
            session_ordinal=date(2026, 9, 22).toordinal(),
            universe=[
                {"instrument_id": "XSHG.600000", "provider_symbol": "sh.600000"}
            ],
            daily_native_records={
                "XSHG.600001": {
                    "instrument_id": "XSHG.600001",
                    "preclose": 10.0,
                }
            },
            previous_closes={},
        )


def test_completed_qfq_result_rejects_wrong_state_before_budget_completion(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "watermark": "2026-09-21",
                "dataset_path": str(tmp_path / "wrong-dataset"),
                "factor_ledger_path": str(tmp_path / "wrong-ledger"),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="did not advance"):
        _completed_qfq_summary(
            result={"status": "completed", "session": "2026-09-22"},
            state_path=state,
            config={
                "data_root": str(tmp_path / "data"),
                "work_root": str(tmp_path / "work"),
            },
            session=date(2026, 9, 22),
            query_count=1,
            reserved_requests=2,
            actual_shared_ledger_delta=2,
        )
