#!/usr/bin/env python3
"""Build the R8 local-only candidate, serving, rollback and Gate evidence packet."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_stack.data.daily.r8_artifacts import (
    DenyExternalIO,
    build_candidate_package,
    canonical_json_bytes,
    resolve_isolation_root,
    validate_isolated_path,
    verify_candidate_package,
    write_immutable_artifact,
)
from quant_stack.data.daily.r8_preflight import run_no_write_preflight
from quant_stack.data.daily.r8_serving import (
    CN_QFQ_CONSUMER,
    CN_RAW_CONSUMER,
    US_RAW_CONSUMER,
    US_TOTAL_RETURN_CONSUMER,
    CandidateRejectedError,
    ConsumerBlockedError,
    IsolatedCandidateRoute,
    PublicationInterrupted,
    verification_summary,
)
from quant_stack.data.daily.tolerance import (
    ProviderOutcome,
    assess_daily_tolerance,
)

SNAPSHOT = "us-equity-xnas-xnys-2026-08-27.v1"
SESSION = date(2026, 9, 18)


def _rows(assessment) -> list[dict[str, object]]:
    return [
        {
            "instrument_id": instrument_id,
            "market": assessment.market,
            "session": assessment.session.isoformat(),
            "close": index + 100.0,
        }
        for index, instrument_id in enumerate(assessment.returned_ids)
    ]


def _fixture_assessment(fixture_root: Path):
    payload = json.loads(
        (fixture_root / "usable_partial_98.json").read_text(encoding="utf-8")
    )
    returned = set(payload["returned_ids"])
    outcomes = {
        identity: "returned" if identity in returned else payload["unresolved_outcome"]
        for identity in payload["universe_ids"]
    }
    return assess_daily_tolerance(
        market="US",
        session=date.fromisoformat(payload["session"]),
        snapshot_id=payload["snapshot_id"],
        universe_ids=payload["universe_ids"],
        outcomes=outcomes,
    ), payload["universe_ids"]


def _complete_assessment(fixture_root: Path):
    payload = json.loads(
        (fixture_root / "complete_with_no_bar.json").read_text(encoding="utf-8")
    )
    return (
        assess_daily_tolerance(
            market="US",
            session=date.fromisoformat(payload["session"]),
            snapshot_id=payload["snapshot_id"],
            universe_ids=payload["universe_ids"],
            outcomes=payload["outcomes"],
            no_bar_evidence_ids=payload["no_bar_evidence_ids"],
        ),
        payload["universe_ids"],
    )


def _cn_assessment():
    universe = [f"XSHG.R8{index:04d}" for index in range(50)]
    outcomes = {
        identity: ProviderOutcome.RETURNED if index < 49 else ProviderOutcome.PROVIDER_ERROR
        for index, identity in enumerate(universe)
    }
    return (
        assess_daily_tolerance(
            market="CN",
            session=SESSION,
            snapshot_id="cn-a-share-r8-fixture-2026-09-18.v1",
            universe_ids=universe,
            outcomes=outcomes,
        ),
        universe,
    )


def _guardrail_assessment():
    universe = [f"XNAS.GUARD{index:03d}" for index in range(100)]
    outcomes = {
        identity: ProviderOutcome.RETURNED if index < 95 else ProviderOutcome.PROVIDER_ERROR
        for index, identity in enumerate(universe)
    }
    return (
        assess_daily_tolerance(
            market="US",
            session=SESSION,
            snapshot_id=SNAPSHOT,
            universe_ids=universe,
            outcomes=outcomes,
        ),
        universe,
    )


def _write_exact(path: Path, value: object, *, boundary: Path) -> None:
    write_immutable_artifact(path, canonical_json_bytes(value), boundary=boundary)


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def run_local_gate(*, isolation_root: Path, fixture_root: Path) -> dict[str, object]:
    root = resolve_isolation_root(isolation_root)
    preflight_path = root / "evidence" / "wp-r8-03" / "preflight.json"
    serving_path = root / "evidence" / "wp-r8-02" / "serving.json"
    packet_path = root / "gate-review-packet" / "r8-local-gate.json"
    # Validate the complete Gate output graph before any candidate or evidence content
    # is read or written. Later checks in the immutable writer protect each actual write.
    for output_path in (preflight_path, serving_path, packet_path):
        validate_isolated_path(
            output_path,
            boundary=root,
            kind="file",
            must_exist=False,
        )
    adapter = DenyExternalIO()
    us_partial_assessment, us_partial_universe = _fixture_assessment(fixture_root)
    us_complete_assessment, us_complete_universe = _complete_assessment(fixture_root)
    cn_partial_assessment, cn_partial_universe = _cn_assessment()
    guardrail_assessment, guardrail_universe = _guardrail_assessment()

    us_complete = build_candidate_package(
        isolation_root=root,
        candidate_id="us-raw-complete-r1",
        assessment=us_complete_assessment,
        universe_ids=us_complete_universe,
        rows=_rows(us_complete_assessment),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="tests/fixtures/daily_tolerance/complete_with_no_bar.json",
    )
    us_partial = build_candidate_package(
        isolation_root=root,
        candidate_id="us-raw-partial-r1",
        assessment=us_partial_assessment,
        universe_ids=us_partial_universe,
        rows=_rows(us_partial_assessment),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="tests/fixtures/daily_tolerance/usable_partial_98.json",
    )
    cn_partial = build_candidate_package(
        isolation_root=root,
        candidate_id="cn-raw-partial-r1",
        assessment=cn_partial_assessment,
        universe_ids=cn_partial_universe,
        rows=_rows(cn_partial_assessment),
        dataset="cn_equity_daily_raw",
        price_basis="raw",
        source_ref="synthetic:cn-r8-50-id-fixture",
    )
    guardrail = build_candidate_package(
        isolation_root=root,
        candidate_id="us-guardrail-failure-r1",
        assessment=guardrail_assessment,
        universe_ids=guardrail_universe,
        rows=_rows(guardrail_assessment),
        dataset="us_equity_daily_raw",
        price_basis="raw",
        source_ref="synthetic:us-r8-95-percent-guardrail",
    )

    packages = (us_complete, us_partial, cn_partial, guardrail)
    verifications = {
        package.manifest["candidate_id"]: verification_summary(
            verify_candidate_package(package.candidate_root, expected_isolation_root=root)
        )
        for package in packages
    }

    us_route = IsolatedCandidateRoute(root, route_name="r8-us-local-v1")
    us_route.enable()
    complete_state = us_route.publish(us_complete.candidate_root)
    complete_read = us_route.read(US_RAW_CONSUMER)
    partial_state = us_route.publish(us_partial.candidate_root)
    partial_read = us_route.read(US_RAW_CONSUMER)
    total_return_blocked = None
    try:
        us_route.read(US_TOTAL_RETURN_CONSUMER)
    except ConsumerBlockedError as error:
        total_return_blocked = str(error)
    rolled_back_state = us_route.rollback()
    interrupted = None
    try:
        us_route.publish(us_partial.candidate_root, interrupt_before_pointer=True)
    except PublicationInterrupted as error:
        interrupted = str(error)
    recovery = us_route.recover_interrupted_publication()
    us_current_after_recovery = us_route.current_state()
    us_route.disable()

    cn_route = IsolatedCandidateRoute(root, route_name="r8-cn-local-v1")
    cn_route.enable()
    cn_state = cn_route.publish(cn_partial.candidate_root)
    cn_raw_read = cn_route.read(CN_RAW_CONSUMER)
    cn_qfq_blocked = None
    try:
        cn_route.read(CN_QFQ_CONSUMER)
    except ConsumerBlockedError as error:
        cn_qfq_blocked = str(error)
    cn_route.disable()
    us_current_after_cn = us_route.current_state()

    guardrail_rejected = None
    guardrail_route = IsolatedCandidateRoute(root, route_name="r8-guardrail-check-v1")
    guardrail_route.enable()
    try:
        guardrail_route.publish(guardrail.candidate_root)
    except CandidateRejectedError as error:
        guardrail_rejected = str(error)
    guardrail_route.disable()

    required_checks = {
        "all_candidate_integrity_verifications_passed": all(
            bool(summary["passed"]) for summary in verifications.values()
        ),
        "route_catalogs_and_pointers_are_independent": (
            us_route.catalog_root != cn_route.catalog_root
            and us_route.pointer_path != cn_route.pointer_path
            and complete_state["route_name"] == us_route.route_name
            and cn_state["route_name"] == cn_route.route_name
        ),
        "us_complete_consumer_read": (
            complete_read.candidate_id == complete_state["candidate_id"]
            and complete_read.generation == complete_state["generation"]
            and complete_read.quality_status == "complete"
        ),
        "us_partial_consumer_read": (
            partial_read.candidate_id == partial_state["candidate_id"]
            and partial_read.generation == partial_state["generation"]
            and partial_read.quality_status in {"usable_partial", "usable_partial_exception"}
        ),
        "us_total_return_consumer_rejected": total_return_blocked is not None,
        "us_rollback_identity": (
            rolled_back_state["route_name"] == us_route.route_name
            and rolled_back_state["candidate_id"] == complete_state["candidate_id"]
            and us_current_after_recovery == rolled_back_state
        ),
        "us_interruption_and_recovery_preserved_pointer": (
            interrupted is not None
            and recovery == "staged_generation_not_committed_pointer_preserved"
            and us_current_after_recovery == rolled_back_state
        ),
        "cn_raw_consumer_read": (
            cn_raw_read.candidate_id == cn_state["candidate_id"]
            and cn_raw_read.generation == cn_state["generation"]
        ),
        "cn_qfq_consumer_rejected": cn_qfq_blocked is not None,
        "cn_publication_did_not_change_us_route": (
            us_current_after_cn == us_current_after_recovery
            and us_current_after_cn is not None
            and us_current_after_cn.get("route_name") == us_route.route_name
            and str(us_current_after_cn.get("candidate_id", "")).startswith("us-")
        ),
        "guardrail_candidate_rejected": guardrail_rejected is not None,
    }

    preflight = run_no_write_preflight(
        isolation_root=root,
        candidate_root=us_partial.candidate_root,
        route_enabled=False,
        side_effects={"external_adapter_attempts": len(adapter.attempts)},
        consumer_matrix=(
            {
                "consumer": "cn_raw",
                "market": "CN",
                "layer": "raw",
                "supports_partial": True,
                "observed": (
                    "readable"
                    if required_checks["cn_raw_consumer_read"]
                    else "unexpected_identity_mismatch"
                ),
            },
            {
                "consumer": "cn_qfq",
                "market": "CN",
                "layer": "qfq",
                "supports_partial": False,
                "observed": (
                    "blocked" if cn_qfq_blocked is not None else "unexpected_readable"
                ),
                "reason": cn_qfq_blocked,
            },
            {
                "consumer": "us_raw",
                "market": "US",
                "layer": "raw",
                "supports_partial": True,
                "observed": (
                    "readable"
                    if required_checks["us_partial_consumer_read"]
                    else "unexpected_identity_mismatch"
                ),
            },
            {
                "consumer": "us_total_return",
                "market": "US",
                "layer": "total_return",
                "supports_partial": False,
                "observed": (
                    "blocked"
                    if total_return_blocked is not None
                    else "unexpected_readable"
                ),
                "reason": total_return_blocked,
            },
        ),
        required_checks=required_checks,
    )

    serving_evidence = {
        "schema_version": "quantstack.daily-r8-serving-evidence.v1",
        "route_default_enabled": False,
        "us": {
            "complete_generation": complete_state["generation"],
            "complete_quality": complete_read.quality_status,
            "partial_generation": partial_state["generation"],
            "partial_quality": partial_read.quality_status,
            "total_return": (
                "blocked" if total_return_blocked is not None else "unexpected_readable"
            ),
            "rollback_candidate": rolled_back_state["candidate_id"],
            "interruption_message": interrupted,
            "interruption_recovery": recovery,
            "current_after_recovery": us_current_after_recovery,
            "current_after_cn_publication": us_current_after_cn,
        },
        "cn": {
            "raw_candidate": cn_raw_read.candidate_id,
            "raw_quality": cn_raw_read.quality_status,
            "qfq": "blocked" if cn_qfq_blocked is not None else "unexpected_readable",
        },
        "guardrail_candidate": {
            "quality": guardrail.manifest["quality_status"],
            "publish": "rejected" if guardrail_rejected is not None else "unexpected_published",
            "reason": guardrail_rejected,
        },
        "required_checks": required_checks,
    }
    _write_exact(preflight_path, preflight.to_dict(), boundary=root)
    _write_exact(serving_path, serving_evidence, boundary=root)
    packet = {
        "schema_version": "quantstack.daily-r8-gate-review-packet.v1",
        "plan_id": "PLAN-PHASE-2.30-CN-US-DAILY-CONTINUITY-R8-20260924",
        "endpoint": preflight.status,
        "isolation_root": str(root),
        "production_route_changed": False,
        "provider_requests": 0,
        "nas_writes": 0,
        "production_writes": 0,
        "packages": [
            {
                "candidate_id": package.manifest["candidate_id"],
                "manifest_path": _relative(root, package.manifest_path),
                "quality_status": package.manifest["quality_status"],
                "verification": verifications[package.manifest["candidate_id"]],
            }
            for package in packages
        ],
        "serving_evidence_path": _relative(root, serving_path),
        "preflight_evidence_path": _relative(root, preflight_path),
        "preflight": preflight.to_dict(),
        "human_gate_required_for_production": True,
    }
    _write_exact(packet_path, packet, boundary=root)
    return {
        "status": preflight.status,
        "isolation_root": str(root),
        "candidate_count": len(packages),
        "verification": verifications,
        "serving_evidence_path": str(serving_path),
        "preflight_evidence_path": str(preflight_path),
        "gate_review_packet_path": str(packet_path),
        "side_effects": preflight.side_effects,
        "rollback_candidate": rolled_back_state["candidate_id"],
        "recovery": recovery,
    }


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_daily_r8_local_gate")
    parser.add_argument("--isolation-root", required=True, type=Path)
    parser.add_argument(
        "--fixture-root",
        type=Path,
        default=Path(__file__).parents[1] / "tests" / "fixtures" / "daily_tolerance",
    )
    args = parser.parse_args()
    result = run_local_gate(isolation_root=args.isolation_root, fixture_root=args.fixture_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
