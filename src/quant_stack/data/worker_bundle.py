from __future__ import annotations

import hashlib
import hmac
import io
import json
import platform
import re
import shutil
import tarfile
import tempfile
from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import Field, computed_field, model_validator

from .ingestion import ProviderRuntimeContract
from .models import DataModel, DatasetManifest
from .publication import (
    CatalogPublisher,
    PublicationBundle,
    PublicationReceipt,
    WorkerImportEvidenceRef,
    load_publication_bundle,
)
from .storage.atomic import (
    atomic_replace_bytes,
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)

_SHA256_PATTERN = r"^[a-f0-9]{64}$"
_COMMIT_PATTERN = r"^[a-f0-9]{40}$"
_IPV4_TEXT = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("worker artifact paths must be normalized relative paths")
    return path


def _resolve_relative(root: Path, value: str) -> Path:
    relative = _safe_relative_path(value)
    resolved_root = root.expanduser().resolve()
    resolved = (resolved_root / Path(*relative.parts)).resolve()
    if resolved_root not in resolved.parents:
        raise ValueError("worker artifact path escapes the configured root")
    return resolved


class WorkerEnvironment(DataModel):
    environment_id: str = Field(min_length=1)
    host_fingerprint: str = Field(pattern=_SHA256_PATTERN)
    system: str = Field(min_length=1)
    machine: str = Field(min_length=1)
    python_version: str = Field(min_length=1)
    containerized: bool

    @classmethod
    def current(cls, environment_id: str) -> WorkerEnvironment:
        cgroup = Path("/proc/1/cgroup")
        cgroup_text = (
            cgroup.read_text(encoding="utf-8", errors="replace").lower() if cgroup.is_file() else ""
        )
        return cls(
            environment_id=environment_id,
            host_fingerprint=_sha256((platform.node() or "unknown").encode()),
            system=platform.system(),
            machine=platform.machine(),
            python_version=platform.python_version(),
            containerized=Path("/.dockerenv").exists()
            or "docker" in cgroup_text
            or "container" in cgroup_text,
        )


class WorkerArtifact(DataModel):
    relative_path: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_relative_path(self) -> WorkerArtifact:
        _safe_relative_path(self.relative_path)
        return self


class WorkerBundleEnvelope(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    dataset_version: str = Field(min_length=1)
    source_commit: str = Field(pattern=_COMMIT_PATTERN)
    created_at: datetime
    worker_environment: WorkerEnvironment
    provider_contract: ProviderRuntimeContract
    provider_transport_audit: dict[str, object]
    provider_transport_audit_sha256: str = Field(pattern=_SHA256_PATTERN)
    publication_bundle_relative_path: str = Field(min_length=1)
    publication_bundle_sha256: str = Field(pattern=_SHA256_PATTERN)
    proposed_manifests: tuple[DatasetManifest, ...] = Field(min_length=1)
    artifacts: tuple[WorkerArtifact, ...] = Field(min_length=1)
    transfer_protocol: Literal["ssh-public-key+sha256+hmac-sha256"] = (
        "ssh-public-key+sha256+hmac-sha256"
    )
    worker_write_scope: Literal["local-immutable-staging-only"] = "local-immutable-staging-only"
    publication_authority: Literal["nas-catalog-publisher-only"] = "nas-catalog-publisher-only"
    webdav_active_write: Literal[False] = False
    direct_duckdb_write: Literal[False] = False

    @model_validator(mode="after")
    def validate_envelope(self) -> WorkerBundleEnvelope:
        if self.created_at.tzinfo is None:
            raise ValueError("worker bundle created_at must be timezone-aware")
        _safe_relative_path(self.publication_bundle_relative_path)
        if self.provider_transport_audit_sha256 != _sha256(
            _canonical_json(self.provider_transport_audit)
        ):
            raise ValueError("worker provider transport audit hash conflict")
        serialized_audit = _canonical_json(self.provider_transport_audit).decode()
        if _IPV4_TEXT.search(serialized_audit):
            raise ValueError("worker transport audit must not contain IP addresses")
        successful = self.provider_transport_audit.get("successful_source_fingerprint")
        if not isinstance(successful, str) or not re.fullmatch(_SHA256_PATTERN, successful):
            raise ValueError("worker bundle requires a successful source fingerprint")
        paths = [item.relative_path for item in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("worker bundle artifact paths must be unique")
        publication_artifact = next(
            (
                item
                for item in self.artifacts
                if item.relative_path == self.publication_bundle_relative_path
            ),
            None,
        )
        if publication_artifact is None or publication_artifact.kind != "publication_bundle":
            raise ValueError("worker bundle must inventory the publication bundle")
        if publication_artifact.sha256 != self.publication_bundle_sha256:
            raise ValueError("worker publication bundle hash conflicts with artifact inventory")
        return self

    def content_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude_computed_fields=True)

    @computed_field
    @property
    def bundle_id(self) -> str:
        return f"worker-{_sha256(_canonical_json(self.content_payload()))[:32]}"


@dataclass(frozen=True)
class WorkerArchive:
    envelope: WorkerBundleEnvelope
    path: Path
    sha256: str
    size_bytes: int
    created: bool


class WorkerImportEvidence(DataModel):
    """Immutable NAS receipt proving what authenticated worker archive was imported."""

    schema_version: Literal["1.1.0"] = "1.1.0"
    bundle_id: str = Field(pattern=r"^worker-[a-f0-9]{32}$")
    envelope: WorkerBundleEnvelope
    envelope_sha256: str = Field(pattern=_SHA256_PATTERN)
    archive_sha256: str = Field(pattern=_SHA256_PATTERN)
    publication_bundle_id: str = Field(pattern=r"^bundle-[a-f0-9]{32}$")
    publication_bundle_sha256: str = Field(pattern=_SHA256_PATTERN)
    source_commit: str = Field(pattern=_COMMIT_PATTERN)
    worker_environment_sha256: str = Field(pattern=_SHA256_PATTERN)
    provider_contract_sha256: str = Field(pattern=_SHA256_PATTERN)
    provider_transport_audit_sha256: str = Field(pattern=_SHA256_PATTERN)
    artifact_count: int = Field(ge=1)
    publication_authority: Literal["nas-catalog-publisher-only"]
    imported_at: datetime
    catalog_published: Literal[False] = False

    @model_validator(mode="after")
    def bind_authenticated_envelope(self) -> WorkerImportEvidence:
        if self.imported_at.tzinfo is None:
            raise ValueError("worker import time must be timezone-aware")
        expected = {
            "bundle_id": self.envelope.bundle_id,
            "envelope_sha256": _sha256(_canonical_json(self.envelope.content_payload())),
            "publication_bundle_sha256": self.envelope.publication_bundle_sha256,
            "source_commit": self.envelope.source_commit,
            "worker_environment_sha256": _sha256(
                _canonical_json(self.envelope.worker_environment.model_dump(mode="json"))
            ),
            "provider_contract_sha256": _sha256(
                _canonical_json(self.envelope.provider_contract.model_dump(mode="json"))
            ),
            "provider_transport_audit_sha256": (self.envelope.provider_transport_audit_sha256),
            "artifact_count": len(self.envelope.artifacts),
            "publication_authority": self.envelope.publication_authority,
        }
        if any(getattr(self, key) != value for key, value in expected.items()):
            raise ValueError("worker import evidence conflicts with authenticated envelope")
        return self

    def publication_ref(self, *, receipt_uri: str, receipt_sha256: str) -> WorkerImportEvidenceRef:
        return WorkerImportEvidenceRef(
            receipt_uri=receipt_uri,
            receipt_sha256=receipt_sha256,
            worker_bundle_id=self.bundle_id,
            envelope_sha256=self.envelope_sha256,
            archive_sha256=self.archive_sha256,
            source_commit=self.source_commit,
            worker_environment_sha256=self.worker_environment_sha256,
            provider_contract_sha256=self.provider_contract_sha256,
        )


@dataclass(frozen=True)
class WorkerImport:
    envelope: WorkerBundleEnvelope
    archive_sha256: str
    receipt_path: Path
    evidence: WorkerImportEvidence
    idempotent_replay: bool


def _artifact_for(root: Path, relative_path: str, kind: str) -> WorkerArtifact:
    path = _resolve_relative(root, relative_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    return WorkerArtifact(
        relative_path=relative_path,
        kind=kind,
        size_bytes=path.stat().st_size,
        sha256=_sha256_file(path),
    )


def build_worker_envelope(
    *,
    data_root: Path,
    publication_bundle_path: Path,
    source_commit: str,
    environment_id: str,
    provider_contract: ProviderRuntimeContract,
    provider_transport_audit: dict[str, object],
    created_at: datetime | None = None,
) -> WorkerBundleEnvelope:
    root = data_root.expanduser().resolve()
    bundle = load_publication_bundle(publication_bundle_path)
    publication_relative = publication_bundle_path.resolve().relative_to(root).as_posix()
    artifacts = [
        _artifact_for(root, item.uri.removeprefix("quant-data://"), item.kind)
        for item in bundle.artifacts
    ]
    artifacts.append(_artifact_for(root, publication_relative, "publication_bundle"))
    return WorkerBundleEnvelope(
        dataset_id=bundle.dataset_id,
        dataset_version=bundle.dataset_version,
        source_commit=source_commit,
        created_at=(created_at or datetime.now(UTC)).astimezone(UTC),
        worker_environment=WorkerEnvironment.current(environment_id),
        provider_contract=provider_contract,
        provider_transport_audit=provider_transport_audit,
        provider_transport_audit_sha256=_sha256(_canonical_json(provider_transport_audit)),
        publication_bundle_relative_path=publication_relative,
        publication_bundle_sha256=_sha256(publication_bundle_path.read_bytes()),
        proposed_manifests=bundle.proposed_manifests,
        artifacts=tuple(artifacts),
    )


def _tar_member(name: str, payload: bytes) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name=name)
    info.size = len(payload)
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o600
    return info, payload


def _tar_file_member(name: str, path: Path) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = path.stat().st_size
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o600
    return info


def _write_worker_archive(
    path: Path,
    *,
    root: Path,
    envelope: WorkerBundleEnvelope,
    envelope_payload: bytes,
    signature: bytes,
) -> None:
    with tarfile.open(path, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, payload in (("bundle.json", envelope_payload), ("bundle.hmac", signature)):
            info, value = _tar_member(name, payload)
            archive.addfile(info, io.BytesIO(value))
        for artifact in sorted(envelope.artifacts, key=lambda item: item.relative_path):
            source = _resolve_relative(root, artifact.relative_path)
            if (
                source.stat().st_size != artifact.size_bytes
                or _sha256_file(source) != artifact.sha256
            ):
                raise ValueError(
                    f"worker artifact changed before packaging: {artifact.relative_path}"
                )
            with source.open("rb") as stream:
                archive.addfile(
                    _tar_file_member(f"artifacts/{artifact.relative_path}", source),
                    stream,
                )


def _read_tar_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    stream = archive.extractfile(member)
    if stream is None:
        raise ValueError("worker archive member cannot be read")
    return stream.read()


def _verify_worker_archive_streaming(archive_path: Path, hmac_key: bytes) -> None:
    with tarfile.open(archive_path, mode="r:") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise ValueError("worker archive member names must be unique")
        for member in members:
            _safe_relative_path(member.name)
            if not member.isfile() or member.issym() or member.islnk():
                raise ValueError("worker archive accepts only regular files")
        keyed = {member.name: member for member in members}
        try:
            envelope_payload = _read_tar_member(archive, keyed["bundle.json"])
            signature = _read_tar_member(archive, keyed["bundle.hmac"]).decode().strip()
        except (KeyError, UnicodeDecodeError) as exc:
            raise ValueError("worker archive is missing authenticated metadata") from exc
        expected_signature = hmac.new(hmac_key, envelope_payload, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise ValueError("worker archive HMAC authentication failed")
        envelope = WorkerBundleEnvelope.model_validate_json(envelope_payload)
        expected_names = {
            "bundle.json",
            "bundle.hmac",
            *(f"artifacts/{item.relative_path}" for item in envelope.artifacts),
        }
        if set(names) != expected_names:
            raise ValueError("worker archive inventory conflicts with its envelope")
        publication_payload: bytes | None = None
        raw_payload: bytes | None = None
        for artifact in envelope.artifacts:
            member = keyed[f"artifacts/{artifact.relative_path}"]
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("worker archive member cannot be read")
            digest = hashlib.sha256()
            captured = io.BytesIO() if artifact.kind in {"publication_bundle", "raw"} else None
            size = 0
            for chunk in iter(lambda stream=stream: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
                if captured is not None:
                    captured.write(chunk)
            if size != artifact.size_bytes or digest.hexdigest() != artifact.sha256:
                raise ValueError(f"worker archive artifact hash conflict: {artifact.relative_path}")
            if captured is not None and artifact.kind == "publication_bundle":
                publication_payload = captured.getvalue()
            elif captured is not None and artifact.kind == "raw":
                raw_payload = captured.getvalue()
        if publication_payload is None or raw_payload is None:
            raise ValueError("worker archive is missing publication or Raw evidence")
        _validate_archive_semantics(envelope, publication_payload, raw_payload)


def create_worker_archive(
    *,
    data_root: Path,
    envelope: WorkerBundleEnvelope,
    output_path: Path,
    hmac_key: bytes,
) -> WorkerArchive:
    if len(hmac_key) < 32:
        raise ValueError("worker HMAC key must contain at least 32 bytes")
    root = data_root.expanduser().resolve()
    envelope_payload = _canonical_json(envelope.content_payload()) + b"\n"
    signature = hmac.new(hmac_key, envelope_payload, hashlib.sha256).hexdigest().encode() + b"\n"
    created = atomic_write_immutable_file(
        output_path,
        write=lambda temporary: _write_worker_archive(
            temporary,
            root=root,
            envelope=envelope,
            envelope_payload=envelope_payload,
            signature=signature,
        ),
        artifact_kind=f"worker archive {envelope.bundle_id}",
        validate=lambda temporary: _verify_worker_archive_streaming(temporary, hmac_key),
        equivalent=lambda current, candidate: _sha256_file(current) == _sha256_file(candidate),
    )
    return WorkerArchive(
        envelope=envelope,
        path=output_path,
        sha256=_sha256_file(output_path),
        size_bytes=output_path.stat().st_size,
        created=created,
    )


def _validate_archive_semantics(
    envelope: WorkerBundleEnvelope,
    publication_payload: bytes,
    raw_payload: bytes,
) -> None:
    publication_bundle = PublicationBundle.model_validate_json(publication_payload)
    if (
        publication_bundle.dataset_id,
        publication_bundle.dataset_version,
        publication_bundle.proposed_manifests,
    ) != (
        envelope.dataset_id,
        envelope.dataset_version,
        envelope.proposed_manifests,
    ):
        raise ValueError("worker envelope conflicts with its publication bundle")
    primary_identity = publication_bundle.primary_source_identity
    contract = envelope.provider_contract
    if primary_identity is None or (
        publication_bundle.provider,
        publication_bundle.endpoint,
        primary_identity.provider_version,
        primary_identity.adapter_version,
        primary_identity.native_schema_version,
    ) != (
        contract.provider,
        contract.endpoint,
        contract.provider_version,
        contract.adapter_version,
        contract.native_schema_version,
    ):
        raise ValueError("worker Provider contract conflicts with publication lineage")
    publication_paths = {
        item.uri.removeprefix("quant-data://") for item in publication_bundle.artifacts
    }
    envelope_paths = {
        item.relative_path for item in envelope.artifacts if item.kind != "publication_bundle"
    }
    if publication_paths != envelope_paths:
        raise ValueError("worker artifacts conflict with publication inventory")
    raw = json.loads(raw_payload)
    if raw.get("transport_audit") != envelope.provider_transport_audit:
        raise ValueError("worker Raw transport evidence conflicts with its envelope")
    requested_symbols = raw.get("job", {}).get("symbols", [])
    transport_audit = envelope.provider_transport_audit
    completed_requests = transport_audit.get("completed_requests")
    request_successes = transport_audit.get("request_successful_source_fingerprints", [])
    if (
        not isinstance(requested_symbols, list)
        or not isinstance(completed_requests, int)
        or completed_requests < len(requested_symbols)
        or len(request_successes) != completed_requests
        or any(value is None for value in request_successes[-len(requested_symbols) :])
    ):
        raise ValueError("worker transport audit does not prove the final request batch")


def _read_verified_archive(
    archive_path: Path,
    hmac_key: bytes,
) -> tuple[WorkerBundleEnvelope, dict[str, bytes]]:
    if len(hmac_key) < 32:
        raise ValueError("worker HMAC key must contain at least 32 bytes")
    members: dict[str, bytes] = {}
    with tarfile.open(archive_path, mode="r:") as archive:
        for member in archive.getmembers():
            _safe_relative_path(member.name)
            if not member.isfile() or member.issym() or member.islnk():
                raise ValueError("worker archive accepts only regular files")
            if member.name in members:
                raise ValueError("worker archive member names must be unique")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("worker archive member cannot be read")
            members[member.name] = stream.read()
    try:
        envelope_payload = members["bundle.json"]
        signature = members["bundle.hmac"].decode().strip()
    except (KeyError, UnicodeDecodeError) as exc:
        raise ValueError("worker archive is missing authenticated metadata") from exc
    expected_signature = hmac.new(hmac_key, envelope_payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("worker archive HMAC authentication failed")
    envelope = WorkerBundleEnvelope.model_validate_json(envelope_payload)
    expected_names = {
        "bundle.json",
        "bundle.hmac",
        *(f"artifacts/{item.relative_path}" for item in envelope.artifacts),
    }
    if set(members) != expected_names:
        raise ValueError("worker archive inventory conflicts with its envelope")
    artifact_payloads: dict[str, bytes] = {}
    for artifact in envelope.artifacts:
        payload = members[f"artifacts/{artifact.relative_path}"]
        if len(payload) != artifact.size_bytes or _sha256(payload) != artifact.sha256:
            raise ValueError(f"worker archive artifact hash conflict: {artifact.relative_path}")
        artifact_payloads[artifact.relative_path] = payload
    publication_payload = artifact_payloads[envelope.publication_bundle_relative_path]
    publication_bundle = PublicationBundle.model_validate_json(publication_payload)
    if (
        publication_bundle.dataset_id,
        publication_bundle.dataset_version,
        publication_bundle.proposed_manifests,
    ) != (
        envelope.dataset_id,
        envelope.dataset_version,
        envelope.proposed_manifests,
    ):
        raise ValueError("worker envelope conflicts with its publication bundle")
    primary_identity = publication_bundle.primary_source_identity
    contract = envelope.provider_contract
    if primary_identity is None or (
        publication_bundle.provider,
        publication_bundle.endpoint,
        primary_identity.provider_version,
        primary_identity.adapter_version,
        primary_identity.native_schema_version,
    ) != (
        contract.provider,
        contract.endpoint,
        contract.provider_version,
        contract.adapter_version,
        contract.native_schema_version,
    ):
        raise ValueError("worker Provider contract conflicts with publication lineage")
    publication_paths = {
        item.uri.removeprefix("quant-data://") for item in publication_bundle.artifacts
    }
    envelope_paths = {
        item.relative_path for item in envelope.artifacts if item.kind != "publication_bundle"
    }
    if publication_paths != envelope_paths:
        raise ValueError("worker artifacts conflict with publication inventory")
    raw_artifact = next(item for item in publication_bundle.artifacts if item.kind == "raw")
    raw_payload = json.loads(artifact_payloads[raw_artifact.uri.removeprefix("quant-data://")])
    if raw_payload.get("transport_audit") != envelope.provider_transport_audit:
        raise ValueError("worker Raw transport evidence conflicts with its envelope")
    requested_symbols = raw_payload.get("job", {}).get("symbols", [])
    transport_audit = envelope.provider_transport_audit
    completed_requests = transport_audit.get("completed_requests")
    request_successes = transport_audit.get("request_successful_source_fingerprints", [])
    if (
        not isinstance(requested_symbols, list)
        or not isinstance(completed_requests, int)
        or completed_requests < len(requested_symbols)
        or len(request_successes) != completed_requests
        or any(value is None for value in request_successes[-len(requested_symbols) :])
    ):
        raise ValueError("worker transport audit does not prove the final request batch")
    return envelope, artifact_payloads


def import_worker_archive(
    *,
    archive_path: Path,
    data_root: Path,
    hmac_key: bytes,
) -> WorkerImport:
    envelope, artifacts = _read_verified_archive(archive_path, hmac_key)
    root = data_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    archive_sha256 = _sha256(archive_path.read_bytes())
    publication_bundle = PublicationBundle.model_validate_json(
        artifacts[envelope.publication_bundle_relative_path]
    )
    receipt_path = root / "staging" / "worker_import_receipts" / f"{envelope.bundle_id}.json"
    if receipt_path.is_file():
        evidence = WorkerImportEvidence.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        )
        if evidence.archive_sha256 != archive_sha256 or evidence.envelope != envelope:
            raise ValueError("worker import receipt archive hash conflict")
        return WorkerImport(envelope, archive_sha256, receipt_path, evidence, True)

    partial_root = Path(
        tempfile.mkdtemp(
            prefix=f".{envelope.bundle_id}.",
            suffix=".partial",
            dir=root,
        )
    )
    try:
        for relative_path, payload in artifacts.items():
            temporary = _resolve_relative(partial_root, relative_path)
            temporary.parent.mkdir(parents=True, exist_ok=True)
            atomic_replace_bytes(temporary, payload)
        for artifact in envelope.artifacts:
            temporary = _resolve_relative(partial_root, artifact.relative_path)
            payload = temporary.read_bytes()
            if len(payload) != artifact.size_bytes or _sha256(payload) != artifact.sha256:
                raise ValueError("worker partial import readback failed")
        for artifact in envelope.artifacts:
            payload = _resolve_relative(partial_root, artifact.relative_path).read_bytes()
            atomic_write_immutable_bytes(
                _resolve_relative(root, artifact.relative_path),
                payload,
                artifact_kind=f"worker import {artifact.kind}",
            )
        evidence = WorkerImportEvidence(
            bundle_id=envelope.bundle_id,
            envelope=envelope,
            envelope_sha256=_sha256(_canonical_json(envelope.content_payload())),
            archive_sha256=archive_sha256,
            publication_bundle_id=publication_bundle.bundle_id,
            publication_bundle_sha256=envelope.publication_bundle_sha256,
            source_commit=envelope.source_commit,
            worker_environment_sha256=_sha256(
                _canonical_json(envelope.worker_environment.model_dump(mode="json"))
            ),
            provider_contract_sha256=_sha256(
                _canonical_json(envelope.provider_contract.model_dump(mode="json"))
            ),
            provider_transport_audit_sha256=(envelope.provider_transport_audit_sha256),
            artifact_count=len(envelope.artifacts),
            publication_authority=envelope.publication_authority,
            imported_at=datetime.now(UTC),
        )
        receipt_payload = (
            json.dumps(
                evidence.model_dump(mode="json", exclude_computed_fields=True),
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode()
        atomic_write_immutable_bytes(
            receipt_path,
            receipt_payload,
            artifact_kind=f"worker import receipt {envelope.bundle_id}",
            validate=lambda temporary: WorkerImportEvidence.model_validate_json(
                temporary.read_text(encoding="utf-8")
            ),
        )
    finally:
        shutil.rmtree(partial_root, ignore_errors=True)
    return WorkerImport(envelope, archive_sha256, receipt_path, evidence, False)


def publish_imported_worker_bundle(
    *,
    data_root: Path,
    import_receipt_path: Path,
    allowed_source_commits: Collection[str],
) -> PublicationReceipt:
    root = data_root.expanduser().resolve()
    receipt_path = import_receipt_path.expanduser().resolve()
    receipts_root = (root / "staging" / "worker_import_receipts").resolve()
    if receipt_path.parent != receipts_root:
        raise PermissionError("worker publication requires NAS-persisted import evidence")
    evidence = WorkerImportEvidence.model_validate_json(receipt_path.read_text(encoding="utf-8"))
    expected_receipt_path = (receipts_root / f"{evidence.bundle_id}.json").resolve()
    if receipt_path != expected_receipt_path:
        raise PermissionError("worker publication requires NAS-persisted import evidence")
    if not allowed_source_commits or evidence.source_commit not in set(allowed_source_commits):
        raise PermissionError("worker source commit is not allowed for NAS publication")
    envelope = evidence.envelope
    for artifact in envelope.artifacts:
        path = _resolve_relative(root, artifact.relative_path)
        if (
            not path.is_file()
            or path.stat().st_size != artifact.size_bytes
            or _sha256(path.read_bytes()) != artifact.sha256
        ):
            raise ValueError("persisted worker import artifact verification failed")
    bundle_path = _resolve_relative(root, envelope.publication_bundle_relative_path)
    bundle = load_publication_bundle(bundle_path)
    if (
        bundle.bundle_id != evidence.publication_bundle_id
        or _sha256(bundle_path.read_bytes()) != evidence.publication_bundle_sha256
    ):
        raise ValueError("persisted worker import evidence conflicts with publication bundle")
    receipt_uri = f"quant-data://{receipt_path.relative_to(root).as_posix()}"
    publication_ref = evidence.publication_ref(
        receipt_uri=receipt_uri,
        receipt_sha256=_sha256(receipt_path.read_bytes()),
    )
    return CatalogPublisher(root).publish(bundle, worker_import_evidence=publication_ref)
