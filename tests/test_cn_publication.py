from __future__ import annotations

import hashlib
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

import quant_stack.data.publication as publication_module
from quant_stack.data.catalog.duckdb_catalog import (
    DuckDBCatalog,
    manifest_catalog_values,
    manifest_sha256,
)
from quant_stack.data.ingestion import IngestionService
from quant_stack.data.models import DatasetManifest
from quant_stack.data.providers.cn import (
    CNDiscrepancyRecord,
    CNProviderDiscrepancyReport,
    ComparisonFailure,
    EvidenceMode,
    ProviderSourceIdentity,
    ProviderTransportIdentity,
)
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    BaoStockReplayProvider,
    normalize_akshare_replay_bars,
    normalize_baostock_replay_bars,
)
from quant_stack.data.publication import (
    CatalogPublisher,
    PublicationArtifact,
    PublicationBundle,
    PublicationReceipt,
    find_publication_receipt_for_manifest,
    load_publication_bundle,
    load_publication_decision,
    publication_state_for,
)
from quant_stack.data.storage import LocalFileSystemStorage, StagingFileSystemStorage
from quant_stack.data.tasks import Task, TaskRuntime


def _execute(
    tmp_path: Path,
    provider_name: str = "akshare",
    dataset_version: str = "2026-07-28.1",
):
    if provider_name == "akshare":
        provider = AKShareReplayProvider()
        normalizer = normalize_akshare_replay_bars
        symbols = ("XSHG.600000", "XSHE.000001", "BJSE.430047")
    else:
        provider = BaoStockReplayProvider()
        normalizer = normalize_baostock_replay_bars
        symbols = ("XSHG.600000", "XSHE.000001")
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(tmp_path),
        bar_normalizers={provider.name: normalizer},
    )
    task = Task(
        dataset_id="cn_equity_daily",
        dataset_version=dataset_version,
        provider=provider.name,
        provider_version=("1.18.81" if provider_name == "akshare" else "0.9.3"),
        endpoint=provider.endpoint,
        adapter_version="contract-only",
        native_schema_version="fixture-required-v1",
        market="CN",
        instrument_scope=symbols,
        start_date=date(2026, 7, 27),
        end_date=date(2026, 7, 28),
        created_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    return TaskRuntime().execute(task, service)


def test_worker_stages_bundle_and_publisher_is_only_catalog_writer(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    assert not (tmp_path / "catalog/quant.duckdb").exists()

    receipt = CatalogPublisher(tmp_path).publish(execution.bundle)
    replay = CatalogPublisher(tmp_path).publish(execution.bundle)

    assert receipt.bundle_id == execution.bundle.bundle_id
    assert replay.idempotent_replay is True
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM datasets").fetchone() == (3,)
    finally:
        connection.close()


def test_two_publication_requests_are_serialized(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    publisher = CatalogPublisher(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(publisher.publish, (execution.bundle, execution.bundle)))
    assert {receipt.bundle_id for receipt in receipts} == {execution.bundle.bundle_id}
    assert sum(receipt.idempotent_replay for receipt in receipts) == 1


def test_receipt_lookup_skips_unreadable_unrelated_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    receipt = CatalogPublisher(tmp_path).publish(execution.bundle)
    unreadable = tmp_path / "catalog/publication_receipts/unreadable-history.json"
    unreadable.write_text("{}\n", encoding="utf-8")
    original = publication_module.load_publication_receipt

    def load(path: Path) -> PublicationReceipt:
        if path == unreadable:
            raise PermissionError(path)
        return original(path)

    monkeypatch.setattr(publication_module, "load_publication_receipt", load)

    resolved = find_publication_receipt_for_manifest(
        tmp_path, execution.bundle.proposed_manifests[0]
    )

    assert resolved is not None
    assert resolved.receipt_id == receipt.receipt_id


def test_conflicting_dataset_version_is_rejected_without_partial_registration(
    tmp_path: Path,
) -> None:
    primary = _execute(tmp_path / "shared", "akshare")
    conflict = _execute(tmp_path / "shared", "baostock", "2026-07-28.2")
    assert primary.bundle is not None and conflict.bundle is not None
    conflict_bundle = conflict.bundle.model_copy(
        update={
            "dataset_version": "2026-07-28.1",
            "proposed_manifests": tuple(
                manifest.model_copy(update={"version": "2026-07-28.1"})
                for manifest in conflict.bundle.proposed_manifests
            ),
        }
    )
    publisher = CatalogPublisher(tmp_path / "shared")
    publisher.publish(primary.bundle)
    with pytest.raises(ValueError, match="dataset version must change|conflict"):
        publisher.publish(conflict_bundle)
    connection = duckdb.connect(str(tmp_path / "shared/catalog/quant.duckdb"), read_only=True)
    try:
        rows = connection.execute(
            "SELECT provider FROM datasets WHERE dataset_id = 'cn_equity_daily'"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [("akshare_replay",)]


def test_quarantined_bundle_cannot_publish(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    quarantined = execution.bundle.model_copy(
        update={"canonical_eligible": False, "quarantined": True}
    )
    with pytest.raises(ValueError, match="conflicts with publication gate"):
        CatalogPublisher(tmp_path).publish(quarantined)


def _bundle_with_report(tmp_path: Path, status: str, *, required: bool = False):
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    bundle = execution.bundle
    # The helper replaces the task-only decision with a comparison decision before
    # publication. Production code builds the final Bundle once and never rewrites it.
    (tmp_path / "catalog/publication_bundles" / f"{bundle.bundle_id}.json").unlink()
    raw = next(item for item in bundle.artifacts if item.kind == "raw")
    transport_identity = ProviderTransportIdentity(
        host_pool_contract_id="tdx-public-host-pool-v1:" + "a" * 16,
        host_pool_contract_version="tdx-public-host-pool-v1",
        host_pool_fingerprint="a" * 64,
        configured_candidates=1,
        max_host_attempts=1,
        attempted_source_fingerprints=("b" * 64,),
        successful_source_fingerprint=(None if status == "unavailable" else "b" * 64),
        failure_classes=(("ConnectionError",) if status == "unavailable" else ()),
        protocol_identity="tdx-standard-market-binary/category-9/daily-bars",
        client_package="tdxpy",
        client_version="0.2.7",
        transport_security="unauthenticated_unencrypted_tcp",
        server_authenticated=False,
        encrypted=False,
        source_terms_status="upstream-data-terms-unverified-controlled-research-only",
    )
    comparison_payload = (
        json.dumps(
            {
                "outcome": "controlled-test",
                "transport_audit": transport_identity.model_dump(mode="json"),
            }
        )
        + "\n"
    ).encode()
    storage = StagingFileSystemStorage(tmp_path)
    comparison_artifact = storage.write_raw_object(
        Path("raw/CN/tdx_reference/comparison.json"), comparison_payload
    )
    primary_identity = ProviderSourceIdentity(
        provider=bundle.provider,
        endpoint=bundle.endpoint,
        provider_version="1.18.81",
        adapter_version="contract-only",
        native_schema_version="fixture-required-v1",
        raw_uri=raw.uri,
        raw_sha256=raw.sha256,
    )
    comparison_identity = ProviderSourceIdentity(
        provider="tdx_reference",
        endpoint="standard_market_daily_bars",
        provider_version="0.2.7",
        adapter_version="real-cn-tdx-v1",
        native_schema_version="live-observed-2026-08-03-v1",
        raw_uri=comparison_artifact.uri,
        raw_sha256=comparison_artifact.sha256,
        transport_identity=transport_identity,
    )
    records = ()
    failure = None
    if status == "quarantined":
        records = (
            CNDiscrepancyRecord(
                dataset_partition="market=CN/version=2026-07-28.1",
                instrument_id="XSHG.600000",
                trade_date=date(2026, 7, 28),
                primary_provider=primary_identity.provider,
                comparison_provider=comparison_identity.provider,
                primary_raw_uri=primary_identity.raw_uri,
                comparison_raw_uri=comparison_identity.raw_uri,
                differing_fields=("close",),
                tolerance_policy="test-v1",
                created_at=datetime(2026, 8, 3, tzinfo=UTC),
            ),
        )
    elif status == "unavailable":
        failure = ComparisonFailure(error_class="ConnectionError", stage="provider_fetch")
    report = CNProviderDiscrepancyReport(
        report_id=f"publication-{status}",
        generated_at=datetime(2026, 8, 3, tzinfo=UTC),
        evidence_mode=EvidenceMode.LIVE,
        fixture_only=False,
        primary_raw_sha256=primary_identity.raw_sha256,
        comparison_raw_sha256=comparison_identity.raw_sha256,
        primary_identity=primary_identity,
        comparison_identity=comparison_identity,
        status=status,
        comparison_required=required,
        failure=failure,
        canonical_selection_reason="controlled publication test",
        records=records,
    )
    payload = (json.dumps(report.model_dump(mode="json"), indent=2) + "\n").encode()
    report_artifact = storage.write_quality_report(
        Path(f"quality/discrepancy-{status}.json"), payload
    )
    artifact = PublicationArtifact(
        uri=report_artifact.uri,
        kind="discrepancy",
        size_bytes=report_artifact.path.stat().st_size,
        sha256=report_artifact.sha256,
        dataset_id=bundle.dataset_id,
        dataset_version=bundle.dataset_version,
    )
    comparison_raw = PublicationArtifact(
        uri=comparison_artifact.uri,
        kind="comparison_raw",
        size_bytes=comparison_artifact.path.stat().st_size,
        sha256=comparison_artifact.sha256,
        dataset_id=bundle.dataset_id,
        dataset_version=bundle.dataset_version,
    )
    comparison_status = {
        "passed": "passed",
        "unavailable": "unavailable",
        "quarantined": "serious_discrepancy",
    }[status]
    gate = {
        "passed": "eligible",
        "unavailable": "blocked" if required else "incomplete",
        "quarantined": "quarantined",
    }[status]
    return type(bundle).model_validate(
        {
            **bundle.model_dump(mode="python", exclude_computed_fields=True),
            "artifacts": (*bundle.artifacts, comparison_raw, artifact),
            "discrepancy_report_uri": artifact.uri,
            "discrepancy_report_sha256": artifact.sha256,
            "comparison_status": comparison_status,
            "comparison_required": required,
            "publication_gate": gate,
            "canonical_eligible": gate in {"eligible", "incomplete"},
            "quarantined": gate == "quarantined",
                "publication_state": publication_state_for(
                    gate,
                    quality_passed=True,
                    comparison_status=comparison_status,
                    consumer_eligibility=bundle.consumer_eligibility,
                ),
            "primary_source_identity": primary_identity,
            "comparison_source_identity": comparison_identity,
            "canonical_selection_reason": "controlled publication test",
            "selection_rule_id": "controlled-test-v2",
        }
    )


def test_comparison_passed_publication_evidence_is_consistent(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "passed")
    receipt = CatalogPublisher(tmp_path).publish(bundle)
    assert receipt.dataset_version == bundle.dataset_version
    assert receipt.selection_rule_id == bundle.selection_rule_id
    assert receipt.primary_source_identity == bundle.primary_source_identity
    assert receipt.comparison_source_identity == bundle.comparison_source_identity


def test_verified_comparison_receipt_requires_primary_source_identity(tmp_path: Path) -> None:
    receipt = CatalogPublisher(tmp_path).publish(_bundle_with_report(tmp_path, "passed"))
    payload = receipt.model_dump(mode="python")
    payload["primary_source_identity"] = None

    with pytest.raises(ValueError, match="requires primary source identity"):
        PublicationReceipt.model_validate(payload)


def test_receipt_replays_bundle_decision_manifests_and_both_raw_after_staging_cleanup(
    tmp_path: Path,
) -> None:
    bundle = _bundle_with_report(tmp_path, "passed")
    publisher = CatalogPublisher(tmp_path)
    receipt = publisher.publish(bundle)

    assert receipt.bundle_uri is not None
    assert receipt.decision_uri is not None
    assert receipt.bundle_uri.startswith("quant-data://catalog/publication_bundles/")
    assert receipt.decision_uri.startswith("quant-data://catalog/publication_decisions/")
    persisted_bundle = load_publication_bundle(
        tmp_path / receipt.bundle_uri.removeprefix("quant-data://")
    )
    persisted_decision = load_publication_decision(
        tmp_path / receipt.decision_uri.removeprefix("quant-data://")
    )
    assert persisted_bundle.bundle_id == bundle.bundle_id
    assert persisted_decision.fingerprint == receipt.publication_evidence_fingerprint

    shutil.rmtree(tmp_path / "staging")
    verification = publisher.replay_receipt(receipt)
    assert verification.verified is True
    assert verification.bundle_verified is True
    assert verification.manifests_verified is True
    assert verification.pointer_verified is True
    assert verification.duckdb_verified is True
    assert verification.discrepancy_verified is True
    assert verification.primary_raw_verified is True
    assert verification.comparison_raw_verified is True


def test_receipt_replay_rejects_mutated_receipt_decision_binding(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "passed")
    publisher = CatalogPublisher(tmp_path)
    receipt = publisher.publish(bundle)
    forged = receipt.model_copy(update={"selection_rule_id": "forged-rule"})

    with pytest.raises(ValueError, match="decision content"):
        publisher.replay_receipt(forged)


def test_receipt_replay_rejects_comparison_raw_mutation(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "passed")
    publisher = CatalogPublisher(tmp_path)
    receipt = publisher.publish(bundle)
    comparison_raw = next(
        artifact for artifact in bundle.artifacts if artifact.kind == "comparison_raw"
    )
    (tmp_path / comparison_raw.uri.removeprefix("quant-data://")).write_bytes(b'{"mutated":true}\n')

    with pytest.raises(ValueError, match="artifact hash conflict"):
        publisher.replay_receipt(receipt)


def test_legacy_receipt_remains_explicitly_unknown(tmp_path: Path) -> None:
    receipt = PublicationReceipt(
        schema_version="1.1.0",
        bundle_id="legacy-bundle",
        published_at=datetime(2026, 8, 3, tzinfo=UTC),
        manifest_uris=("quant-data://catalog/manifests/legacy.json",),
    )

    assert receipt.evidence_status == "legacy_unknown"
    verification = CatalogPublisher(tmp_path).replay_receipt(receipt)
    assert verification.evidence_status == "legacy_unknown"
    assert verification.verified is False


def test_current_bundle_rejects_cross_version_artifact_binding(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    payload = bundle.model_dump(mode="python", exclude_computed_fields=True)
    payload["artifacts"][0]["dataset_version"] = "different-version"

    with pytest.raises(ValidationError, match="artifact dataset version conflict"):
        PublicationBundle.model_validate(payload)


def test_required_comparison_cannot_be_downgraded_to_not_required(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    payload = bundle.model_dump(mode="python", exclude_computed_fields=True)
    payload["comparison_required"] = True

    with pytest.raises(ValidationError, match="cannot be downgraded"):
        PublicationBundle.model_validate(payload)


def test_publication_rejects_discrepancy_gate_mismatch(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "passed")
    mismatched = type(bundle).model_validate(
        {
            **bundle.model_dump(mode="python", exclude_computed_fields=True),
            "comparison_status": "unavailable",
            "publication_gate": "incomplete",
            "publication_state": publication_state_for(
                "incomplete",
                quality_passed=True,
                comparison_status="unavailable",
            ),
        }
    )
    with pytest.raises(ValueError, match="conflicts with discrepancy report"):
        CatalogPublisher(tmp_path).publish(mismatched)


def test_optional_comparison_unavailable_publishes_as_incomplete(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "unavailable")
    assert bundle.publication_gate == "incomplete"
    assert bundle.quarantined is False
    CatalogPublisher(tmp_path).publish(bundle)


def test_required_comparison_unavailable_blocks_publication(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "unavailable", required=True)
    assert bundle.publication_gate == "blocked"
    with pytest.raises(ValueError, match="not eligible"):
        CatalogPublisher(tmp_path).publish(bundle)


def test_quality_failure_blocks_without_becoming_quarantine(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    failed_manifests = tuple(
        item.model_copy(
            update={
                "quality_status": "failed",
                "quality": item.quality.model_copy(update={"status": "failed"}),
            }
        )
        if item.dataset_id == "cn_equity_daily"
        else item
        for item in bundle.proposed_manifests
    )
    blocked = type(bundle).model_validate(
        {
            **bundle.model_dump(mode="python", exclude_computed_fields=True),
            "proposed_manifests": failed_manifests,
            "publication_gate": "blocked",
            "canonical_eligible": False,
            "publication_state": publication_state_for(
                "blocked",
                quality_passed=False,
                comparison_status="not_required",
            ),
        }
    )
    assert blocked.quarantined is False
    with pytest.raises(ValueError, match="not eligible|different persisted"):
        CatalogPublisher(tmp_path).publish(blocked)


def test_serious_discrepancy_quarantines_and_blocks_publication(tmp_path: Path) -> None:
    bundle = _bundle_with_report(tmp_path, "quarantined")
    assert bundle.publication_gate == "quarantined"
    with pytest.raises(ValueError, match="not eligible"):
        CatalogPublisher(tmp_path).publish(bundle)


def test_publication_requires_canonical_source_and_reason(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    payload = bundle.model_dump(mode="python", exclude_computed_fields=True)
    payload["canonical_selection_reason"] = ""
    with pytest.raises(ValidationError, match="canonical_selection_reason"):
        type(bundle).model_validate(payload)


def test_selection_rule_change_requires_new_dataset_version(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    publisher = CatalogPublisher(tmp_path)
    publisher.publish(bundle)
    changed = type(bundle).model_validate(
        {
            **bundle.model_dump(mode="python", exclude_computed_fields=True),
            "selection_rule_id": "changed-rule-v2",
        }
    )
    with pytest.raises(ValueError, match="dataset version must change"):
        publisher.publish(changed)


def test_manifest_set_switch_recovers_after_pointer_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    storage = LocalFileSystemStorage(tmp_path)
    assert storage.list_manifests() == []
    real_atomic_write = publication_module._atomic_write

    def fail_pointer(path: Path, payload: bytes) -> None:
        if path.name == "current-manifest-set.json":
            raise OSError("simulated pointer failure")
        real_atomic_write(path, payload)

    monkeypatch.setattr(publication_module, "_atomic_write", fail_pointer)
    with pytest.raises(OSError, match="pointer failure"):
        CatalogPublisher(tmp_path).publish(execution.bundle)
    assert storage.list_manifests() == []
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM datasets").fetchone() == (3,)
    finally:
        connection.close()

    monkeypatch.setattr(publication_module, "_atomic_write", real_atomic_write)
    receipt = CatalogPublisher(tmp_path).publish(execution.bundle)
    assert len(storage.list_manifests()) == 3
    assert len(receipt.manifest_uris) == 3


def test_manifest_fingerprint_rejects_metadata_conflict_after_pointer_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    bundle = execution.bundle
    real_atomic_write = publication_module._atomic_write

    def fail_pointer(path: Path, payload: bytes) -> None:
        if path.name == "current-manifest-set.json":
            raise OSError("simulated pointer failure")
        real_atomic_write(path, payload)

    monkeypatch.setattr(publication_module, "_atomic_write", fail_pointer)
    with pytest.raises(OSError, match="pointer failure"):
        CatalogPublisher(tmp_path).publish(bundle)
    assert not (tmp_path / "catalog/current-manifest-set.json").exists()

    target = bundle.proposed_manifests[-1]
    conflicting_manifest = target.model_copy(update={"license_scope": "conflicting-license"})
    conflict_path = (
        tmp_path
        / "staging/conflicting_proposed_manifests"
        / f"{target.dataset_id}@{target.version}.json"
    )
    conflict_path.parent.mkdir(parents=True, exist_ok=True)
    conflict_payload = (
        json.dumps(conflicting_manifest.model_dump(mode="json"), indent=2) + "\n"
    ).encode()
    conflict_path.write_bytes(conflict_payload)
    conflict_uri = f"quant-data://{conflict_path.relative_to(tmp_path).as_posix()}"
    artifacts: list[PublicationArtifact] = []
    for artifact in bundle.artifacts:
        if artifact.kind != "proposed_manifest":
            artifacts.append(artifact)
            continue
        path = tmp_path / artifact.uri.removeprefix("quant-data://")
        manifest = DatasetManifest.model_validate_json(path.read_text(encoding="utf-8"))
        if (manifest.dataset_id, manifest.version) != (target.dataset_id, target.version):
            artifacts.append(artifact)
            continue
        artifacts.append(
            PublicationArtifact(
                uri=conflict_uri,
                kind="proposed_manifest",
                size_bytes=len(conflict_payload),
                sha256=hashlib.sha256(conflict_payload).hexdigest(),
                dataset_id=bundle.dataset_id,
                dataset_version=bundle.dataset_version,
            )
        )
    conflicting_bundle = bundle.model_copy(
        update={
            "artifacts": tuple(artifacts),
            "proposed_manifests": tuple(
                conflicting_manifest if item == target else item
                for item in bundle.proposed_manifests
            ),
        }
    )

    monkeypatch.setattr(publication_module, "_atomic_write", real_atomic_write)
    with pytest.raises(ValueError, match="dataset version must change"):
        CatalogPublisher(tmp_path).publish(conflicting_bundle)

    CatalogPublisher(tmp_path).publish(bundle)
    pointer = json.loads(
        (tmp_path / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    manifests = [
        DatasetManifest.model_validate_json((tmp_path / relative).read_text(encoding="utf-8"))
        for relative in pointer["manifests"]
    ]
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    try:
        rows = connection.execute(
            "SELECT dataset_id, version, manifest_sha256 FROM datasets"
        ).fetchall()
    finally:
        connection.close()
    expected = {
        (manifest.dataset_id, manifest.version): manifest_sha256(manifest) for manifest in manifests
    }
    actual = {(dataset_id, version): fingerprint for dataset_id, version, fingerprint in rows}
    assert actual == expected


def test_legacy_catalog_fingerprint_requires_authoritative_manifest(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    manifest = execution.bundle.proposed_manifests[-1]
    catalog_path = tmp_path / "catalog/quant.duckdb"
    catalog = DuckDBCatalog(catalog_path)
    try:
        catalog.register(manifest)
        catalog.connection.execute(
            "UPDATE datasets SET manifest_sha256 = NULL WHERE dataset_id = ? AND version = ?",
            [manifest.dataset_id, manifest.version],
        )
    finally:
        catalog.close()

    with pytest.raises(ValueError, match="missing a manifest fingerprint"):
        CatalogPublisher(tmp_path).publish(execution.bundle)


def test_receipt_replay_backfills_from_authoritative_manifest(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    publisher = CatalogPublisher(tmp_path)
    publisher.publish(execution.bundle)
    manifest = execution.bundle.proposed_manifests[-1]
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"))
    try:
        connection.execute(
            "UPDATE datasets SET manifest_sha256 = NULL WHERE dataset_id = ? AND version = ?",
            [manifest.dataset_id, manifest.version],
        )
    finally:
        connection.close()

    replay = publisher.publish(execution.bundle)
    assert replay.idempotent_replay is True
    connection = duckdb.connect(str(tmp_path / "catalog/quant.duckdb"), read_only=True)
    try:
        fingerprint = connection.execute(
            "SELECT manifest_sha256 FROM datasets WHERE dataset_id = ? AND version = ?",
            [manifest.dataset_id, manifest.version],
        ).fetchone()[0]
    finally:
        connection.close()
    assert fingerprint == manifest_sha256(manifest)


def test_bundle_id_changes_when_attempt_changes(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    changed = bundle.model_copy(update={"attempt_id": "attempt-different"})
    assert changed.bundle_id != bundle.bundle_id


def test_bundle_id_changes_when_manifest_metadata_changes(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    changed_manifest = bundle.proposed_manifests[-1].model_copy(
        update={"license_scope": "changed-license"}
    )
    changed = bundle.model_copy(
        update={"proposed_manifests": (*bundle.proposed_manifests[:-1], changed_manifest)}
    )
    assert changed.bundle_id != bundle.bundle_id


def test_bundle_id_changes_when_producer_environment_changes(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    changed = bundle.model_copy(
        update={
            "producer_environment": bundle.producer_environment.model_copy(
                update={"environment_id": "different-environment"}
            )
        }
    )
    assert changed.bundle_id != bundle.bundle_id


def test_identical_bundle_content_has_stable_id(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    restored = type(bundle).model_validate_json(
        json.dumps(bundle.model_dump(mode="json", exclude_computed_fields=True))
    )
    assert restored.content_payload() == bundle.content_payload()
    assert restored.bundle_id == bundle.bundle_id


def test_bundle_rejects_duplicate_proposed_manifest_keys(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    payload = bundle.model_dump(mode="json", exclude_computed_fields=True)
    payload["proposed_manifests"] = [
        payload["proposed_manifests"][0],
        payload["proposed_manifests"][0],
        payload["proposed_manifests"][2],
    ]
    with pytest.raises(ValueError, match="keys must be unique"):
        type(bundle).model_validate(payload)


def test_bundle_requires_one_to_one_proposed_manifest_artifacts(tmp_path: Path) -> None:
    bundle = _execute(tmp_path).bundle
    assert bundle is not None
    payload = bundle.model_dump(mode="json", exclude_computed_fields=True)
    removed = False
    kept = []
    for artifact in payload["artifacts"]:
        if artifact["kind"] == "proposed_manifest" and not removed:
            removed = True
            continue
        kept.append(artifact)
    payload["artifacts"] = kept
    with pytest.raises(ValueError, match="one-to-one"):
        type(bundle).model_validate(payload)


def _corrupt_legacy_projection(catalog_path: Path, manifest: DatasetManifest) -> None:
    connection = duckdb.connect(str(catalog_path))
    try:
        connection.execute(
            """UPDATE datasets SET manifest_sha256 = NULL, provider = 'wrong-provider',
            quality_status = 'failed', quality_json = '{}', storage_uri = 'stale-uri',
            schema_ref = 'stale-schema', available_at = NULL,
            license_scope = 'stale-license', source_url = 'stale-source'
            WHERE dataset_id = ? AND version = ?""",
            [manifest.dataset_id, manifest.version],
        )
    finally:
        connection.close()


def _assert_catalog_projection_matches(catalog_path: Path, manifest: DatasetManifest) -> None:
    catalog = DuckDBCatalog(catalog_path)
    try:
        row = catalog.find(manifest.dataset_id, manifest.version)
    finally:
        catalog.close()
    assert row is not None
    expected = manifest_catalog_values(manifest)
    assert {key: row[key] for key in expected} == expected


def test_legacy_fingerprint_backfill_updates_all_catalog_projection_fields(
    tmp_path: Path,
) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    publisher = CatalogPublisher(tmp_path)
    publisher.publish(execution.bundle)
    manifest = execution.bundle.proposed_manifests[-1]
    catalog_path = tmp_path / "catalog/quant.duckdb"
    _corrupt_legacy_projection(catalog_path, manifest)

    assert publisher.publish(execution.bundle).idempotent_replay is True
    _assert_catalog_projection_matches(catalog_path, manifest)


def test_legacy_backfill_rejects_without_authoritative_manifest(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    manifest = execution.bundle.proposed_manifests[-1]
    catalog = DuckDBCatalog(tmp_path / "catalog/quant.duckdb")
    try:
        catalog.register(manifest)
        catalog.connection.execute(
            "UPDATE datasets SET manifest_sha256 = NULL WHERE dataset_id = ? AND version = ?",
            [manifest.dataset_id, manifest.version],
        )
        with pytest.raises(ValueError, match="no authoritative manifest"):
            catalog.register(manifest)
    finally:
        catalog.close()


def test_legacy_backfill_is_transactional(tmp_path: Path) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    publisher = CatalogPublisher(tmp_path)
    publisher.publish(execution.bundle)
    first, second = execution.bundle.proposed_manifests[:2]
    catalog_path = tmp_path / "catalog/quant.duckdb"
    _corrupt_legacy_projection(catalog_path, first)
    conflicting_second = second.model_copy(update={"license_scope": "conflict"})
    catalog = DuckDBCatalog(catalog_path)
    try:
        with pytest.raises(ValueError, match="Immutable manifest conflict"):
            catalog.register_many_atomic(
                [first, conflicting_second],
                authoritative_manifests={(first.dataset_id, first.version): first},
            )
        row = catalog.find(first.dataset_id, first.version)
    finally:
        catalog.close()
    assert row is not None
    assert row["manifest_sha256"] is None
    assert row["provider"] == "wrong-provider"


def test_pointer_manifest_and_duckdb_projection_match_after_backfill(
    tmp_path: Path,
) -> None:
    execution = _execute(tmp_path)
    assert execution.bundle is not None
    publisher = CatalogPublisher(tmp_path)
    publisher.publish(execution.bundle)
    target = execution.bundle.proposed_manifests[-1]
    catalog_path = tmp_path / "catalog/quant.duckdb"
    _corrupt_legacy_projection(catalog_path, target)
    publisher.publish(execution.bundle)

    pointer = json.loads(
        (tmp_path / "catalog/current-manifest-set.json").read_text(encoding="utf-8")
    )
    for relative in pointer["manifests"]:
        manifest = DatasetManifest.model_validate_json(
            (tmp_path / relative).read_text(encoding="utf-8")
        )
        _assert_catalog_projection_matches(catalog_path, manifest)
