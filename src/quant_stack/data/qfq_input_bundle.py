from __future__ import annotations

import hashlib
import hmac
import io
import json
import tarfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from .storage.atomic import atomic_write_immutable_file

_EXPECTED_REQUEST_POLICY = {
    "concurrency": 1,
    "request_interval_ms": 50,
    "batch_size": 500,
    "batch_interval_seconds": 5,
}
_MEMBERS = {
    "envelope.json",
    "envelope.hmac",
    "factor-acquisition.json",
    "daily-import-report.json",
}
_MAX_ARCHIVE_BYTES = 10 * 1024 * 1024


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_key(key: bytes) -> None:
    if len(key) < 32:
        raise ValueError("qfq factor input HMAC key must contain at least 32 bytes")


def _validate_factor(payload: dict[str, object], session: date) -> None:
    requests = payload.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("qfq factor input has no requests")
    if payload.get("complete") is not True or payload.get("universe_count") != len(requests):
        raise ValueError("qfq factor input is incomplete")
    if payload.get("request_end") != session.isoformat():
        raise ValueError("qfq factor input watermark does not match its session")
    if payload.get("request_policy") != _EXPECTED_REQUEST_POLICY:
        raise ValueError("qfq factor input does not use the serial request policy")
    instrument_ids = [str(item.get("instrument_id", "")) for item in requests]
    if (
        not all(instrument_ids)
        or len(instrument_ids) != len(set(instrument_ids))
        or any(
            item.get("status") not in {"ok", "ok_no_adjustments"}
            for item in requests
        )
    ):
        raise ValueError("qfq factor input contains invalid request evidence")


def _validate_daily_report(payload: dict[str, object], session: date) -> None:
    entries = payload.get("entries")
    if (
        payload.get("session_date") != session.isoformat()
        or not isinstance(entries, list)
        or not entries
        or payload.get("imported_archives") != len(entries)
        or payload.get("catalog_published") is not False
        or payload.get("production_pointer_updated") not in {None, False}
    ):
        raise ValueError("qfq factor input daily import report is not eligible")
    for entry in entries:
        receipt = str(entry.get("receipt_relative_path", ""))
        if (
            not receipt.startswith("staging/worker_import_receipts/worker-")
            or not receipt.endswith(".json")
            or ".." in Path(receipt).parts
        ):
            raise ValueError("daily import report contains an invalid receipt path")


class QfqFactorInputEnvelope(BaseModel):
    schema_version: Literal["quantstack.qfq-factor-input.v1"] = (
        "quantstack.qfq-factor-input.v1"
    )
    session_date: date
    dataset_version: str = Field(min_length=1)
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    created_at: datetime
    factor_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    daily_import_report_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_policy: dict[str, object]
    contains_raw_parquet: Literal[False] = False
    contains_qfq_parquet: Literal[False] = False
    materialization_authority: Literal["nas-local-only"] = "nas-local-only"

    @model_validator(mode="after")
    def validate_contract(self) -> QfqFactorInputEnvelope:
        if self.created_at.tzinfo is None:
            raise ValueError("qfq factor input created_at must be timezone-aware")
        if self.request_policy != _EXPECTED_REQUEST_POLICY:
            raise ValueError("qfq factor input envelope has the wrong request policy")
        return self


@dataclass(frozen=True)
class VerifiedQfqFactorInput:
    envelope: QfqFactorInputEnvelope
    factor: dict[str, object]
    daily_import_report: dict[str, object]
    archive_size_bytes: int


def _tar_bytes(name: str, payload: bytes) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o600
    return info, payload


def build_qfq_factor_input_archive(
    *,
    factor_path: Path,
    daily_import_report_path: Path,
    session: date,
    dataset_version: str,
    source_commit: str,
    hmac_key: bytes,
    output_path: Path,
    created_at: datetime | None = None,
) -> QfqFactorInputEnvelope:
    _require_key(hmac_key)
    factor = json.loads(factor_path.read_text(encoding="utf-8"))
    daily_report = json.loads(daily_import_report_path.read_text(encoding="utf-8"))
    _validate_factor(factor, session)
    _validate_daily_report(daily_report, session)
    factor_bytes = _canonical_json(factor)
    daily_bytes = _canonical_json(daily_report)
    envelope = QfqFactorInputEnvelope(
        session_date=session,
        dataset_version=dataset_version,
        source_commit=source_commit,
        created_at=created_at or datetime.now(UTC),
        factor_sha256=_sha256(factor_bytes),
        daily_import_report_sha256=_sha256(daily_bytes),
        request_policy=dict(_EXPECTED_REQUEST_POLICY),
    )
    envelope_bytes = _canonical_json(envelope.model_dump(mode="json"))
    signature = hmac.new(hmac_key, envelope_bytes, hashlib.sha256).hexdigest().encode()

    def write(candidate: Path) -> None:
        with tarfile.open(candidate, "w") as archive:
            for name, payload in (
                ("envelope.json", envelope_bytes),
                ("envelope.hmac", signature),
                ("factor-acquisition.json", factor_bytes),
                ("daily-import-report.json", daily_bytes),
            ):
                info, content = _tar_bytes(name, payload)
                archive.addfile(info, io.BytesIO(content))

    def validate(candidate: Path) -> None:
        verified = read_qfq_factor_input_archive(candidate, hmac_key=hmac_key)
        if verified.envelope != envelope:
            raise ValueError("qfq factor input archive changed during creation")

    atomic_write_immutable_file(
        output_path,
        write=write,
        validate=validate,
        equivalent=lambda current, candidate: current.read_bytes() == candidate.read_bytes(),
        artifact_kind="qfq factor input archive",
    )
    if output_path.stat().st_size > _MAX_ARCHIVE_BYTES:
        raise ValueError("qfq factor input archive exceeds the 10MB WAN limit")
    return envelope


def read_qfq_factor_input_archive(
    archive_path: Path, *, hmac_key: bytes
) -> VerifiedQfqFactorInput:
    _require_key(hmac_key)
    size = archive_path.stat().st_size
    if size > _MAX_ARCHIVE_BYTES:
        raise ValueError("qfq factor input archive exceeds the 10MB WAN limit")
    with tarfile.open(archive_path, "r") as archive:
        members = archive.getmembers()
        if {item.name for item in members} != _MEMBERS:
            raise ValueError("qfq factor input archive members are not exact")
        if any(not item.isfile() or item.size > _MAX_ARCHIVE_BYTES for item in members):
            raise ValueError("qfq factor input archive contains an invalid member")
        payloads = {
            item.name: archive.extractfile(item).read()  # type: ignore[union-attr]
            for item in members
        }
    envelope_bytes = payloads["envelope.json"]
    expected = hmac.new(hmac_key, envelope_bytes, hashlib.sha256).hexdigest().encode()
    if not hmac.compare_digest(payloads["envelope.hmac"], expected):
        raise ValueError("qfq factor input HMAC verification failed")
    envelope = QfqFactorInputEnvelope.model_validate_json(envelope_bytes)
    factor_bytes = payloads["factor-acquisition.json"]
    daily_bytes = payloads["daily-import-report.json"]
    if envelope.factor_sha256 != _sha256(factor_bytes):
        raise ValueError("qfq factor input payload does not match its envelope")
    if envelope.daily_import_report_sha256 != _sha256(daily_bytes):
        raise ValueError("qfq daily import report does not match its envelope")
    factor = json.loads(factor_bytes)
    daily_report = json.loads(daily_bytes)
    _validate_factor(factor, envelope.session_date)
    _validate_daily_report(daily_report, envelope.session_date)
    return VerifiedQfqFactorInput(
        envelope=envelope,
        factor=factor,
        daily_import_report=daily_report,
        archive_size_bytes=size,
    )
