import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
pytestmark = pytest.mark.skipif(
    not (ROOT / "docs/evidence").is_dir(),
    reason="private historical operations evidence is excluded from the public snapshot",
)


def test_current_handoff_points_to_current_operations() -> None:
    handoff = (ROOT / "HANDOFF.md").read_text(encoding="utf-8")

    for required in (
        "Phase 2.18",
        "Phase 2.20",
        "CN_OPERATIONS_RUNBOOK.md",
        "US_EQUITY_FULL_MARKET_OPERATIONS.md",
        "CN_US_SESSION_PARQUET_OPERATIONS.md",
        "HANDOFF_HISTORY.md",
        "factor-delta-routine.json",
    ):
        assert required in handoff
    assert "当前活动里程碑：SS K线快速加载" not in handoff


def test_historical_handoff_and_roadmap_use_the_same_phase_status() -> None:
    historical_root = (ROOT / "docs" / "HANDOFF_HISTORY.md").read_text(encoding="utf-8")
    handoff = (ROOT / "docs" / "DATA_LAYER_HANDOFF.md").read_text(encoding="utf-8")
    roadmap = (ROOT / "docs" / "DATA_LAYER_ROADMAP.md").read_text(encoding="utf-8")

    assert "Phase 2.0：completed" in handoff
    assert "Phase 2.0（completed）" in roadmap
    assert "Phase 2.0（current）" not in roadmap
    phase_21 = "CN Ingest Runtime + Offline Replay + Hardware Placement Benchmark"
    assert f"Phase 2.1：completed — {phase_21}" in handoff
    assert f"Phase 2.1（completed）— {phase_21}" in roadmap
    phase_22 = "SS Legacy Archive Inventory + Frozen Export Foundation"
    assert f"Phase 2.2：completed — {phase_22}" in handoff
    assert f"Phase 2.2（completed）— {phase_22}" in roadmap
    assert "Phase 2.3：current" in handoff
    assert "Phase 2.3A：completed（PR #9 merged）" in handoff
    assert "Phase 2.3B：completed" in handoff
    assert "Phase 2.3C：completed" in handoff
    assert "Phase 2.3D：current" in handoff
    assert "Phase 2.3（current）— SS Consumer Migration" in roadmap
    assert "2.3C（current）" not in roadmap
    assert "2.3D（current）" in roadmap
    assert "Phase 2.4（planned）— Reviewed SS Cutover" in roadmap
    assert "feature/phase-2.3d-historical-research-completion" in historical_root
    assert "cn_equity_daily_backfill_pilot" in historical_root
    assert "165,480" in historical_root
    assert "903,363" in historical_root
    assert "2026-08-04.real-cn.8" in historical_root
    assert "2026-08-04.calendar-state.1" in historical_root


def test_phase_23c_medium_evidence_preserves_research_boundaries() -> None:
    path = (
        ROOT
        / "docs"
        / "evidence"
        / "phase-2.3c-medium-backfill-nas-research-2026-08-04.json"
    )
    evidence_text = path.read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)

    assert evidence["universe"]["instrument_count"] == 500
    assert evidence["clean_run"]["rows"] == 903363
    assert evidence["clean_run"]["duplicate_instrument_date_keys"] == 0
    assert evidence["clean_run"]["final_task_states"] == {
        "succeeded": 240,
        "failed": 0,
    }
    assert evidence["tdx_sample"]["unique_instruments"] == 12
    assert evidence["tdx_sample"]["status"] == "passed"
    assert evidence["transfer_and_mirror"][
        "idempotent_repeat_transferred_bytes"
    ] == 0
    assert evidence["nas_query_proof"]["mac_worker_required"] is False
    assert evidence["isolation"]["production_pointer_updated"] is False
    assert evidence["isolation"]["ss_candidate"] is False
    assert evidence["isolation"]["ss_database_accessed"] is False
    for forbidden in ("/users/", "/volumes/", "ssh-rsa", "password", "token"):
        assert forbidden not in evidence_text.lower()


def test_phase_21_docs_preserve_lightweight_single_writer_topology() -> None:
    runtime = (ROOT / "docs" / "CN_INGEST_RUNTIME.md").read_text(encoding="utf-8")
    placement = (ROOT / "docs" / "HARDWARE_PLACEMENT_BENCHMARK.md").read_text(encoding="utf-8")
    implementation = (ROOT / "docs" / "DATA_LAYER_IMPLEMENTATION.md").read_text(encoding="utf-8")
    combined = "\n".join((runtime, placement, implementation))

    for required in (
        "quant-stack-ingest",
        "CatalogPublisher",
        "single-writer",
        "PostgreSQL",
        "Redis",
        "Celery",
        "authoritative data root",
        "Mac mini",
    ):
        assert required in combined


def test_cn_docs_preserve_route_and_architecture_boundaries() -> None:
    documents = [
        ROOT / "docs" / "CN_PROVIDER_EVALUATION.md",
        ROOT / "docs" / "DATA_LAYER_HANDOFF.md",
        ROOT / "docs" / "DATA_LAYER_ROADMAP.md",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in documents)

    for required in (
        "BaoStock",
        "AKShare",
        "Tushare",
        "XSHG",
        "XSHE",
        "BJSE",
        "immutable provider-native Raw",
        "Parquet",
        "DuckDB",
        "Market Data API",
        "quarantined",
    ):
        assert required in combined
    assert "BaoStack" not in combined


def test_phase_23b_adr_and_sanitized_evidence_preserve_boundaries() -> None:
    adr = (ROOT / "docs" / "ADR_0006_ASHARE_LAKE_REFERENCE.md").read_text(encoding="utf-8")
    evidence_text = (
        ROOT
        / "docs"
        / "evidence"
        / "phase-2.3b-provider-reliability-2026-08-04.json"
    ).read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)

    assert "ba5681af5e8be5ae9965f08a27051de9b15d03bd" in adr
    assert "0.5.0" in adr
    assert "Apache-2.0" in adr
    for decision in ("adapt", "defer", "reject"):
        assert decision in adr
    assert evidence["scope"]["full_market"] is False
    assert evidence["scope"]["cutover_eligible"] is False
    assert evidence["scope"]["sample_only"] is True
    assert evidence["scope"]["production_ready"] is False
    assert "TDXReferenceLiveProvider" in adr
    assert evidence["reliability"]["local"]["akshare"]["success_rate"] == 1
    assert evidence["reliability"]["nas"]["akshare"]["success_rate"] == 0
    for environment in ("local", "nas"):
        assert (
            evidence["reliability"][environment]["quantstack_tdx_reference"][
                "success_rate"
            ]
            == 1
        )
        assert evidence["reliability"][environment]["asl_tdx_oracle"]["success_rate"] == 1
    decision = evidence["route_decision"]
    assert decision["required_comparison"] == "quantstack_tdx_reference"
    assert decision["automatic_switching"] is False
    assert decision["comparison_may_overwrite_canonical"] is False
    assert evidence["cross_source_alignment"]["status"] == "passed"
    assert (
        evidence["cross_source_alignment"][
            "conversion_error_shares_for_observed_raw_values"
        ]
        == 0
    )
    assert evidence["volume_contract"]["fractional_lot_example"]["canonical_shares"] == 12395
    assert evidence["volume_contract"]["premature_truncation_removed"] is True
    transport = evidence["transport_source_evidence"]
    assert transport["raw_discrepancy_bundle_receipt_identity_bound"] is True
    assert transport["transport_security"] == "unauthenticated_unencrypted_tcp"
    assert transport["server_authenticated"] is False
    assert transport["encrypted"] is False
    for environment in ("local", "nas"):
        publication = evidence["publication_validation"][environment]
        assert publication["discrepancy_status"] == "passed"
        assert publication["discrepancy_records"] == 0
        assert publication["publication_gate"] == "eligible"
        assert publication["receipt_replay"]["verified"] is True
        assert publication["metadata_comparison_complete"] is True
        assert publication["metadata_candidate_eligible"] is True
        assert publication["cutover_eligible"] is False
    for environment in ("local", "nas"):
        assert evidence["controlled_ss_shadow"][environment]["candidate_errors"] == 0
        assert evidence["controlled_ss_shadow"][environment]["etag_revalidation_status"] == 304
    assert evidence["controlled_ss_shadow"]["ss_database_accessed"] is False
    assert evidence["nas_resource_gate"]["temporary_networks_after"] == 0
    assert evidence["nas_resource_gate"]["oom_killed"] is False
    assert evidence["nas_resource_gate"]["recommended_defaults_not_sla"] is True
    for forbidden in ("/users/", "/volumes/", "postgresql://", "api-key", "token"):
        assert forbidden not in evidence_text.lower()


def test_phase_23b_network_placement_and_worker_evidence_fail_closed() -> None:
    adr = (ROOT / "docs" / "ADR_0007_PROVIDER_NETWORK_PLACEMENT.md").read_text(
        encoding="utf-8"
    )
    evidence_text = (
        ROOT
        / "docs"
        / "evidence"
        / "phase-2.3b-network-placement-mac-worker-2026-08-04.json"
    ).read_text(encoding="utf-8")
    evidence = json.loads(evidence_text)

    matrix = {
        item["environment_id"]: item
        for item in evidence["network_matrix"]["comparable_environments"]
    }
    assert matrix["development-native"]["successes"] == 3
    assert matrix["macmini-native"]["successes"] == 2
    assert matrix["macmini-container"]["successes"] == 2
    assert matrix["nas-docker-bridge"]["successes"] == 1
    assert matrix["nas-docker-host"]["successes"] == 1
    attribution = evidence["network_matrix"]["attribution"]
    assert attribution["failure_boundary"] == "http_response_after_verified_tls"
    assert attribution["failure_codes"] == [56]
    assert attribution["mac_container_boundary_supported"] is False
    assert attribution["nas_docker_bridge_boundary_supported"] is False
    assert evidence["mac_worker_poc"]["catalog_published_on_worker"] is False
    assert evidence["mac_worker_poc"]["direct_duckdb_write"] is False
    assert evidence["transfer"]["hmac_verified_at_import"] is True
    assert evidence["transfer"]["controlled_interrupted_partial_recovered"] is True
    assert evidence["nas_import_and_publication"]["second_import"][
        "publication_idempotent_replay"
    ] is True
    assert evidence["nas_import_and_publication"]["ss_database_accessed"] is False
    assert evidence["worker_offline_nas_chain"]["catalog_published"] is True
    assert evidence["worker_offline_nas_chain"]["automatic_fallback"] is False
    assert evidence["placement_decision"]["catalog_publisher"] == "NAS only"
    assert evidence["scope"]["production_ready"] is False
    assert evidence["scope"]["cutover_eligible"] is False
    assert "激活为默认路由" in adr
    for forbidden in ("/users/", "/volumes/", "postgresql://", "api-key", "token"):
        assert forbidden not in evidence_text.lower()


def test_ss_continuity_docs_preserve_consumer_and_archive_boundaries() -> None:
    continuity = (ROOT / "docs" / "SS_DATA_CONTINUITY_ROADMAP.md").read_text(encoding="utf-8")
    handoff = (ROOT / "docs" / "DATA_LAYER_HANDOFF.md").read_text(encoding="utf-8")
    roadmap = (ROOT / "docs" / "DATA_LAYER_ROADMAP.md").read_text(encoding="utf-8")
    combined = "\n".join((continuity, handoff, roadmap))

    for required in (
        "SS QuantStack Consumer Adapter",
        "Market Data API / Batch API",
        "stock-screener-legacy-price-export-v1",
        "source_provider: stock_screener_legacy",
        "source_endpoint: stock_prices_export_v1",
        "volume_unit: unknown_or_mixed",
        "lineage_quality: legacy",
        "canonical_eligible: false",
        "controlled offline shadow",
        "disposable cache",
        "只读 legacy archive",
    ):
        assert required in combined

    assert "SS 前端和业务代码不直接读取 QuantStack 的 Parquet" in continuity
    assert "QuantStack runtime 查询 SS 数据库" in continuity
