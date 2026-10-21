from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from quant_stack.data.calendars import (
    ObservedSessionCalendar,
    PartialMarketSessionError,
    UnknownMarketSessionError,
)
from quant_stack.data.evidence import (
    load_market_state_sample,
    load_official_calendar_evidence,
)
from quant_stack.data.models import (
    CalendarEvidence,
    DatasetManifest,
    MarketStateEvidence,
)
from quant_stack.data.semantics import CN_EQUITY_DAILY_SEMANTICS

ROOT = Path(__file__).resolve().parents[1]
CALENDAR_ROOT = ROOT / "tests/fixtures/exchanges/cn/calendar"
MARKET_STATE_ROOT = ROOT / "tests/fixtures/exchanges/cn/market_states"


def _official_calendar(observed_at: datetime = datetime(2026, 8, 4, tzinfo=UTC)):
    return load_official_calendar_evidence(
        raw_path=CALENDAR_ROOT / "2026-official-window-raw-v1.json",
        normalized_path=CALENDAR_ROOT / "2026-official-window-normalized-v1.json",
        observed_at=observed_at,
        raw_uri="quant-data://reference/calendars/cn/2026-official-window-raw-v1.json",
        normalized_uri=(
            "quant-data://reference/calendars/cn/2026-official-window-normalized-v1.json"
        ),
    )


def test_official_calendar_classifies_every_supported_day_and_fails_closed() -> None:
    evidence = _official_calendar()
    calendar = ObservedSessionCalendar(evidence)

    assert calendar.verified_for_live_publication is True
    assert evidence.exchanges == ("XSHG", "XSHE", "BJSE")
    assert calendar.is_trading_day("CN", date(2026, 4, 30)) is True
    assert calendar.is_trading_day("CN", date(2026, 5, 1)) is False
    assert calendar.is_trading_day("CN", date(2026, 5, 2)) is False
    with pytest.raises(UnknownMarketSessionError):
        calendar.is_trading_day("CN", date(2026, 5, 8))


def test_provider_observation_is_not_an_official_live_calendar() -> None:
    evidence = CalendarEvidence(
        source="provider rows",
        source_kind="provider_observed",
        version="provider-only-v1",
        market="CN",
        timezone="Asia/Shanghai",
        open_time=time(9, 30),
        close_time=time(15),
        supported_start=date(2026, 7, 29),
        supported_end=date(2026, 7, 29),
        confirmed_sessions=(date(2026, 7, 29),),
        observed_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    assert ObservedSessionCalendar(evidence).verified_for_live_publication is False


def test_unclosed_official_session_is_rejected() -> None:
    calendar = ObservedSessionCalendar(
        _official_calendar(datetime(2026, 4, 30, 6, 0, tzinfo=UTC))
    )
    with pytest.raises(PartialMarketSessionError):
        calendar.session_close("CN", date(2026, 4, 30))


def test_market_state_sample_is_finite_effective_dated_and_honest() -> None:
    sample = load_market_state_sample(
        raw_path=(
            MARKET_STATE_ROOT / "2026-representative-sample-raw-v1.json"
        ),
        raw_uri=(
            "quant-data://reference/market_states/cn/"
            "2026-representative-sample-raw-v1.json"
        ),
    )
    by_id = {item.evidence_id: item for item in sample.evidence}

    assert sample.finite_sample is True
    assert by_id["xshg-688066-suspended-20260430"].effective_to == date(2026, 4, 30)
    assert by_id["xshg-688066-risk-warning-20260506"].status == "verified"
    assert by_id["xshe-000001-normal-20260729"].status == "partial"
    assert by_id["bjse-920047-normal-blocked"].status == "blocked"
    assert by_id["bjse-code-change-blocked"].effective_from is None


def test_verified_historical_state_requires_official_effective_date() -> None:
    with pytest.raises(ValidationError, match="effective_from"):
        MarketStateEvidence(
            evidence_id="invalid-current-snapshot",
            instrument_id="XSHE.000001",
            exchange="XSHE",
            state="normal",
            status="verified",
            observed_at=datetime(2026, 8, 4, tzinfo=UTC),
            source_kind="exchange_announcement",
            source_url="quant-data://provider/snapshot.json",
            source_title="current snapshot",
        )


def test_current_manifest_requires_bound_calendar_and_state_versions() -> None:
    calendar = _official_calendar()
    sample = load_market_state_sample(
        raw_path=MARKET_STATE_ROOT / "2026-representative-sample-raw-v1.json",
        raw_uri=(
            "quant-data://reference/market_states/cn/"
            "2026-representative-sample-raw-v1.json"
        ),
    )
    manifest = {
        "manifest_schema_version": "1.3.0",
        "dataset_id": "cn_equity_daily",
        "version": "2026-08-04.calendar-state.test",
        "layer": "normalized",
        "kind": "market_bars",
        "market": "CN",
        "provider": "baostock",
        "provider_version": "0.9.3",
        "adapter_version": "real-cn-v1",
        "native_schema_version": "live-observed-2026-08-03-v1",
        "frequency": "1d",
        "schema_ref": "schemas/data/bar.schema.json",
        "storage_uri": (
            "quant-data://normalized/bars_1d/market=CN/"
            "version=2026-08-04.calendar-state.test/bars.parquet"
        ),
        "created_at": "2026-08-04T00:00:00Z",
        "available_at_policy": "retrieval_time_after_provider_publication",
        "revision_policy": "immutable",
        "row_count": 8,
        "sha256": "a" * 64,
        "quality_status": "passed",
        "quality": {"status": "passed"},
        "license_scope": "controlled-research-only",
        "calendar_evidence": calendar,
        "market_state_sample": sample,
    }
    validated = DatasetManifest.model_validate(manifest)
    CN_EQUITY_DAILY_SEMANTICS.validate_manifest(validated)
    schema = json.loads(
        (ROOT / "schemas/data/dataset_manifest.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        validated.model_dump(mode="json")
    )
    with pytest.raises(ValueError, match="market-state"):
        CN_EQUITY_DAILY_SEMANTICS.validate_manifest(
            validated.model_copy(update={"market_state_sample": None})
        )
