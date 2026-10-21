from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from quant_stack.data.batch import BatchDatasetNotFoundError, CanonicalBatchRepository
from quant_stack.data.models import DatasetManifest, DatasetQualityMetadata
from quant_stack.data.publication import PublicationState
from quant_stack.data.semantics import (
    CN_EQUITY_DAILY_SEMANTICS,
    DEFAULT_DATASET_SEMANTICS,
    CapabilityState,
    DatasetSemanticsRegistry,
    PITState,
    RouteStatus,
    RouteValidationStage,
)


def _manifest(**overrides: object) -> DatasetManifest:
    values: dict[str, object] = {
        "dataset_id": "cn_equity_daily",
        "version": "2026-08-03.1",
        "layer": "normalized",
        "kind": "market_bars",
        "market": "CN",
        "provider": "akshare",
        "provider_version": "1.18.81",
        "adapter_version": "real-cn-v2",
        "native_schema_version": "live-observed-2026-08-03-v1",
        "frequency": "1d",
        "schema_ref": "schemas/data/bar.schema.json",
        "storage_uri": (
            "quant-data://normalized/bars_1d/market=CN/"
            f"version={overrides.get('version', '2026-08-03.1')}/bars.parquet"
        ),
        "created_at": datetime(2026, 8, 3, tzinfo=UTC),
        "available_at_policy": "retrieval_time_after_provider_publication",
        "revision_policy": "immutable",
        "row_count": 3,
        "sha256": "a" * 64,
        "quality_status": "passed",
        "quality": DatasetQualityMetadata(status="passed"),
        "license_scope": "controlled-research-only",
    }
    values.update(overrides)
    return DatasetManifest(**values)


def test_cn_semantics_are_program_readable_and_fail_closed() -> None:
    semantics = DEFAULT_DATASET_SEMANTICS.require("cn_equity_daily")
    capability = semantics.research

    assert capability.unadjusted is CapabilityState.EXACT
    assert capability.adjustment is CapabilityState.UNAVAILABLE
    assert capability.universe is CapabilityState.PARTIAL
    assert capability.pit is PITState.UNSUPPORTED
    assert "adjusted_price_query" in capability.blocked_operations
    assert semantics.routing.automatic_switching is False
    assert semantics.routing.comparison_may_overwrite_canonical is False
    assert semantics.partition.keys == ("market", "version")
    routes = {route.route_id: route for route in semantics.routing.routes}
    assert routes["xshg-xshe-bootstrap"].status is RouteStatus.VALIDATED
    assert routes["cn-daily-and-repair"].status is RouteStatus.BLOCKED
    assert semantics.routing.cross_validation[0].status is RouteStatus.VALIDATED
    assert semantics.routing.cross_validation[0].comparison_provider == "tdx_reference"
    assert semantics.routing.cross_validation[1].status is RouteStatus.BLOCKED
    assert semantics.routing.cross_validation[1].comparison_provider == "akshare"
    assert (
        routes["xshg-xshe-bootstrap"].validation_stage
        is RouteValidationStage.BOUNDED_SAMPLE_VALIDATED
    )
    assert routes["xshg-xshe-bootstrap"].evidence_scope.sample_only is True
    assert routes["xshg-xshe-bootstrap"].evidence_scope.production_ready is False
    assert semantics.routing.cross_validation[0].evidence_scope.full_history_validated is False


def test_registry_validates_manifest_without_owning_dataset_version() -> None:
    first = _manifest(version="2026-08-03.1")
    second = _manifest(version="2026-08-03.2")

    CN_EQUITY_DAILY_SEMANTICS.validate_manifest(first)
    CN_EQUITY_DAILY_SEMANTICS.validate_manifest(second)
    with pytest.raises(ValueError, match="frequency"):
        CN_EQUITY_DAILY_SEMANTICS.validate_manifest(_manifest(frequency="1m"))
    with pytest.raises(ValueError, match="not consumer eligible"):
        CN_EQUITY_DAILY_SEMANTICS.validate_manifest(
            _manifest(
                quality_status="warning",
                quality=DatasetQualityMetadata(status="warning"),
            )
        )


def test_registered_consumer_eligibility_is_applied_during_snapshot_selection(
    tmp_path,
) -> None:
    warning = _manifest(
        quality_status="warning",
        quality=DatasetQualityMetadata(status="warning"),
    )
    with pytest.raises(BatchDatasetNotFoundError):
        CanonicalBatchRepository(tmp_path)._select_snapshot(  # noqa: SLF001
            [warning],
            kind=warning.kind,
            dataset_id=warning.dataset_id,
            dataset_version=warning.version,
            market=warning.market,
            frequency=warning.frequency,
        )


def test_batch_rejects_dataset_without_declared_batch_consumer(tmp_path) -> None:
    manifest = _manifest(dataset_id="undeclared_cn_daily")
    with pytest.raises(BatchDatasetNotFoundError):
        CanonicalBatchRepository(tmp_path)._select_snapshot(  # noqa: SLF001
            [manifest],
            kind=manifest.kind,
            dataset_id=manifest.dataset_id,
            dataset_version=manifest.version,
            market=manifest.market,
            frequency=manifest.frequency,
        )


def test_batch_metadata_serializes_registered_research_capability(tmp_path) -> None:
    publication_state = PublicationState(
        quality_passed=True,
        catalog_published=True,
        comparison_complete=True,
        shadow_eligible=True,
        candidate_eligible=True,
        cutover_eligible=False,
    )
    metadata = CanonicalBatchRepository(tmp_path).metadata_response(
        SimpleNamespace(
            snapshot=SimpleNamespace(
                manifest=_manifest(),
                fingerprint="a" * 64,
            ),
            snapshot_token="b" * 64,
            etag='"metadata"',
            publication_receipt=SimpleNamespace(
                publication_state=publication_state,
                publication_gate="eligible",
                comparison_status="passed",
                comparison_required=True,
                canonical_source_provider="akshare",
                canonical_source_endpoint="stock_zh_a_hist",
                selection_rule_id="cn-route-v1",
                publication_evidence_fingerprint="c" * 64,
            ),
        )
    )

    payload = metadata.model_dump(mode="json")
    assert payload["research_capability"]["adjustment"] == "unavailable"
    assert payload["research_capability"]["pit"] == "unsupported"
    assert (
        payload["routing_evidence"]["routes"][0]["validation_stage"] == "bounded_sample_validated"
    )
    assert payload["routing_evidence"]["routes"][0]["evidence_scope"]["sample_only"] is True
    assert payload["calendar_capability"]["status"] == "verified"
    assert payload["calendar_capability"]["exchanges"] == ["XSHG", "XSHE", "BJSE"]
    assert "evidence_refs" not in payload["calendar_capability"]
    assert payload["market_state_capability"]["status"] == "partial"
    assert payload["market_state_capability"]["finite_sample_only"] is True
    assert "evidence_refs" not in payload["market_state_capability"]
    assert payload["consumer_eligibility"]["route_class"] == "canonical"


def test_semantics_fingerprint_is_stable_and_changes_with_research_contract() -> None:
    original = CN_EQUITY_DAILY_SEMANTICS.fingerprint()
    restored = type(CN_EQUITY_DAILY_SEMANTICS).model_validate(
        CN_EQUITY_DAILY_SEMANTICS.model_dump(mode="python")
    )
    changed = restored.model_copy(
        update={
            "research": restored.research.model_copy(
                update={"blocked_operations": (*restored.research.blocked_operations, "new_gate")}
            )
        }
    )
    assert restored.fingerprint() == original
    assert changed.fingerprint() != original
    registry = DatasetSemanticsRegistry((changed,))
    assert registry.fingerprint(changed.dataset_id) == changed.fingerprint()
