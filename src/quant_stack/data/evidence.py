from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, time
from pathlib import Path

from .models import (
    CalendarDayEvidence,
    CalendarEvidence,
    MarketStateEvidence,
    MarketStateSample,
)


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_official_calendar_evidence(
    *,
    raw_path: str | Path,
    normalized_path: str | Path,
    observed_at: datetime,
    raw_uri: str,
    normalized_uri: str,
) -> CalendarEvidence:
    raw = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    normalized = json.loads(Path(normalized_path).read_text(encoding="utf-8"))
    source_refs = tuple(item["url"] for item in raw["sources"])
    days = tuple(
        CalendarDayEvidence(
            trading_date=item["trading_date"],
            state=item["state"],
            source_refs=source_refs,
        )
        for item in normalized["days"]
    )
    sessions = tuple(item.trading_date for item in days if item.state == "session")
    non_sessions = tuple(
        item.trading_date
        for item in days
        if item.state in {"weekend", "holiday", "temporary_closure"}
    )
    return CalendarEvidence(
        schema_version="1.1.0",
        source="; ".join(item["publisher"] for item in raw["sources"]),
        source_kind="exchange_announcement",
        version=normalized["version"],
        market=normalized["market"],
        timezone=normalized["timezone"],
        open_time=time.fromisoformat(normalized["open_time"]),
        close_time=time.fromisoformat(normalized["close_time"]),
        supported_start=normalized["supported_start"],
        supported_end=normalized["supported_end"],
        confirmed_sessions=sessions,
        confirmed_non_sessions=non_sessions,
        observed_at=observed_at,
        exchanges=tuple(normalized["exchanges"]),
        day_evidence=days,
        raw_evidence_uri=raw_uri,
        raw_evidence_sha256=file_sha256(raw_path),
        normalized_evidence_uri=normalized_uri,
        normalized_evidence_sha256=file_sha256(normalized_path),
    )


def load_market_state_sample(
    *,
    raw_path: str | Path,
    raw_uri: str,
) -> MarketStateSample:
    raw = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    captured_at = datetime.fromisoformat(raw["captured_at"].replace("Z", "+00:00"))
    return MarketStateSample(
        version=raw["capture_id"],
        as_of=captured_at.astimezone(UTC),
        evidence=tuple(MarketStateEvidence.model_validate(item) for item in raw["evidence"]),
        raw_evidence_uri=raw_uri,
        raw_evidence_sha256=file_sha256(raw_path),
    )
