from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from quant_stack.data.ingestion import IngestionService
from quant_stack.data.providers.cn.replay import (
    AKShareReplayProvider,
    normalize_akshare_replay_bars,
)
from quant_stack.data.providers.cn.replay.providers import AKSHARE_REPLAY_RUNTIME_CONTRACT
from quant_stack.data.publication import CatalogPublisher
from quant_stack.data.storage import StagingFileSystemStorage
from quant_stack.data.tasks import Task, TaskRuntime
from quant_stack.data.worker_bundle import (
    build_worker_envelope,
    create_worker_archive,
    import_worker_archive,
    publish_imported_worker_bundle,
)

_AUDIT = {
    "schema_version": "1.0.0",
    "endpoint_host": "push2his.eastmoney.com",
    "tls_verification": True,
    "candidate_source_fingerprints": ["a" * 64],
    "attempted_source_fingerprints": ["a" * 64],
    "successful_source_fingerprint": "a" * 64,
    "failure_classes": [],
    "failure_codes": [],
    "failure_stages": [],
    "completed_requests": 3,
    "request_attempted_source_fingerprints": [["a" * 64]] * 3,
    "request_successful_source_fingerprints": ["a" * 64] * 3,
    "request_failure_codes": [[]] * 3,
    "request_failure_stages": [[]] * 3,
    "all_requests_succeeded": True,
}


class _AuditedReplayProvider(AKShareReplayProvider):
    audit = _AUDIT

    def transport_audit(self) -> dict[str, object]:
        return dict(self.audit)


def _worker_execution(root: Path, *, audit: dict[str, object] | None = None):
    provider = _AuditedReplayProvider()
    if audit is not None:
        provider.audit = audit
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(root),
        bar_normalizers={provider.name: normalize_akshare_replay_bars},
    )
    task = Task(
        dataset_id="cn_equity_daily_akshare_reference",
        dataset_version="worker-test.1",
        provider=AKSHARE_REPLAY_RUNTIME_CONTRACT.provider,
        provider_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.provider_version,
        endpoint=AKSHARE_REPLAY_RUNTIME_CONTRACT.endpoint,
        adapter_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.adapter_version,
        native_schema_version=AKSHARE_REPLAY_RUNTIME_CONTRACT.native_schema_version,
        market="CN",
        instrument_scope=("XSHG.600000", "XSHE.000001", "BJSE.430047"),
        start_date=date(2026, 7, 27),
        end_date=date(2026, 7, 28),
        frequency="1d",
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    execution = TaskRuntime().execute(task, service)
    assert execution.bundle is not None
    bundle_path = root / "catalog/publication_bundles" / f"{execution.bundle.bundle_id}.json"
    raw_payload = json.loads(execution.result.raw_artifact.path.read_text(encoding="utf-8"))
    assert raw_payload["transport_audit"] == (audit or _AUDIT)
    return execution, bundle_path


def test_signed_worker_bundle_import_and_nas_only_publication_are_idempotent(
    tmp_path: Path,
) -> None:
    producer = tmp_path / "producer"
    execution, bundle_path = _worker_execution(producer)
    key = b"k" * 32
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="b" * 40,
        environment_id="controlled-worker",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=dict(_AUDIT),
    )
    archive_path = tmp_path / "worker.tar"
    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=archive_path,
        hmac_key=key,
    )
    replayed_archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=archive_path,
        hmac_key=key,
    )
    assert archive.created is True
    assert replayed_archive.created is False
    assert archive.sha256 == replayed_archive.sha256

    nas_root = tmp_path / "nas"
    imported = import_worker_archive(
        archive_path=archive_path,
        data_root=nas_root,
        hmac_key=key,
    )
    replayed_import = import_worker_archive(
        archive_path=archive_path,
        data_root=nas_root,
        hmac_key=key,
    )
    assert imported.idempotent_replay is False
    assert replayed_import.idempotent_replay is True
    assert not (nas_root / "catalog/quant.duckdb").exists()
    assert not (nas_root / "catalog/current-manifest-set.json").exists()

    with pytest.raises(PermissionError, match="NAS-persisted"):
        publish_imported_worker_bundle(
            data_root=nas_root,
            import_receipt_path=archive_path,
            allowed_source_commits={envelope.source_commit},
        )
    with pytest.raises(PermissionError, match="source commit"):
        publish_imported_worker_bundle(
            data_root=nas_root,
            import_receipt_path=imported.receipt_path,
            allowed_source_commits={"c" * 40},
        )
    assert not (nas_root / "catalog/quant.duckdb").exists()

    receipt = publish_imported_worker_bundle(
        data_root=nas_root,
        import_receipt_path=imported.receipt_path,
        allowed_source_commits={envelope.source_commit},
    )
    replay = publish_imported_worker_bundle(
        data_root=nas_root,
        import_receipt_path=imported.receipt_path,
        allowed_source_commits={envelope.source_commit},
    )
    assert receipt.bundle_id == execution.bundle.bundle_id
    assert replay.idempotent_replay is True
    assert receipt.worker_import_evidence is not None
    assert receipt.worker_import_evidence.source_commit == envelope.source_commit
    assert receipt.consumer_eligibility.route_class == "optional"
    assert receipt.publication_state is not None
    assert receipt.publication_state.shadow_eligible is True
    assert receipt.publication_state.candidate_eligible is False
    assert CatalogPublisher(nas_root).replay_receipt(receipt).worker_import_verified is True
    assert (nas_root / "catalog/quant.duckdb").is_file()
    assert (nas_root / "catalog/current-manifest-set.json").is_file()


def test_worker_archive_creation_streams_large_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    producer = tmp_path / "producer"
    _, bundle_path = _worker_execution(producer)
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="e" * 40,
        environment_id="controlled-worker",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=dict(_AUDIT),
    )
    bars = next(item for item in envelope.artifacts if item.kind == "bars")
    bars_path = (producer / bars.relative_path).resolve()
    original_read_bytes = Path.read_bytes

    def reject_bar_read_bytes(path: Path) -> bytes:
        if path.resolve() == bars_path:
            raise AssertionError("large worker artifacts must be streamed")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_bar_read_bytes)
    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=tmp_path / "streamed-worker.tar",
        hmac_key=b"s" * 32,
    )

    assert archive.created is True
    assert archive.size_bytes > bars.size_bytes


def test_worker_archive_preserves_failed_retry_before_successful_final_batch(
    tmp_path: Path,
) -> None:
    retry_audit = {
        **_AUDIT,
        "completed_requests": 4,
        "request_attempted_source_fingerprints": [
            ["b" * 64],
            *_AUDIT["request_attempted_source_fingerprints"],
        ],
        "request_successful_source_fingerprints": [
            None,
            *_AUDIT["request_successful_source_fingerprints"],
        ],
        "request_failure_codes": [[56], *_AUDIT["request_failure_codes"]],
        "request_failure_stages": [
            ["http_response"],
            *_AUDIT["request_failure_stages"],
        ],
        "all_requests_succeeded": False,
    }
    producer = tmp_path / "producer"
    _, bundle_path = _worker_execution(producer, audit=retry_audit)
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="a" * 40,
        environment_id="controlled-worker",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=retry_audit,
    )

    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=tmp_path / "retry-history.tar",
        hmac_key=b"h" * 32,
    )
    imported = import_worker_archive(
        archive_path=archive.path,
        data_root=tmp_path / "nas",
        hmac_key=b"h" * 32,
    )

    assert imported.envelope.provider_transport_audit["completed_requests"] == 4
    assert imported.envelope.provider_transport_audit["all_requests_succeeded"] is False


def test_worker_archive_rejects_wrong_authentication_before_import(tmp_path: Path) -> None:
    producer = tmp_path / "producer"
    _, bundle_path = _worker_execution(producer)
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="c" * 40,
        environment_id="controlled-worker",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=dict(_AUDIT),
    )
    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=tmp_path / "worker.tar",
        hmac_key=b"a" * 32,
    )
    nas_root = tmp_path / "nas"

    with pytest.raises(ValueError, match="HMAC authentication failed"):
        import_worker_archive(
            archive_path=archive.path,
            data_root=nas_root,
            hmac_key=b"b" * 32,
        )

    assert not nas_root.exists()


def test_interrupted_import_never_creates_catalog_state_and_can_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import quant_stack.data.worker_bundle as worker_module

    producer = tmp_path / "producer"
    _, bundle_path = _worker_execution(producer)
    key = b"r" * 32
    envelope = build_worker_envelope(
        data_root=producer,
        publication_bundle_path=bundle_path,
        source_commit="d" * 40,
        environment_id="controlled-worker",
        provider_contract=AKSHARE_REPLAY_RUNTIME_CONTRACT,
        provider_transport_audit=dict(_AUDIT),
    )
    archive = create_worker_archive(
        data_root=producer,
        envelope=envelope,
        output_path=tmp_path / "worker.tar",
        hmac_key=key,
    )
    real_write = worker_module.atomic_write_immutable_bytes
    writes = 0

    def interrupt(path, payload, *, artifact_kind, validate=None):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("controlled transfer interruption")
        return real_write(
            path,
            payload,
            artifact_kind=artifact_kind,
            validate=validate,
        )

    monkeypatch.setattr(worker_module, "atomic_write_immutable_bytes", interrupt)
    nas_root = tmp_path / "nas"
    with pytest.raises(OSError, match="controlled transfer interruption"):
        import_worker_archive(
            archive_path=archive.path,
            data_root=nas_root,
            hmac_key=key,
        )
    assert not (nas_root / "catalog/quant.duckdb").exists()
    assert not (nas_root / "catalog/current-manifest-set.json").exists()
    assert not list((nas_root / "staging/worker_import_receipts").glob("*.json"))
    assert not list(nas_root.glob(f".{envelope.bundle_id}.*.partial"))

    monkeypatch.setattr(worker_module, "atomic_write_immutable_bytes", real_write)
    resumed = import_worker_archive(
        archive_path=archive.path,
        data_root=nas_root,
        hmac_key=key,
    )
    receipt = publish_imported_worker_bundle(
        data_root=nas_root,
        import_receipt_path=resumed.receipt_path,
        allowed_source_commits={resumed.envelope.source_commit},
    )
    assert resumed.idempotent_replay is False
    assert receipt.idempotent_replay is False
    assert (nas_root / "catalog/quant.duckdb").is_file()
