import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from quant_stack.data.markets import CN_MARKETS, canonical_mic_for_alias
from quant_stack.data.providers.cn import (
    AKSHARE_STOCK_ZH_A_HIST_CANDIDATE,
    BAOSTOCK_DAILY_CANDIDATE,
    AmountUnit,
    CNEndpointMetadata,
    CNEndpointRequestLineage,
    CNInstrumentRecord,
    CNInstrumentSnapshot,
    CNInstrumentStatus,
    CNSecurityType,
    PriceAdjustment,
    VolumeUnit,
    ensure_unadjusted_raw_endpoint,
    normalize_volume_to_shares,
)


def _instrument(source_timestamp: datetime) -> CNInstrumentRecord:
    return CNInstrumentRecord(
        instrument_id="XSHG.600000",
        symbol="600000",
        canonical_venue="XSHG",
        provider_symbol="sh.600000",
        provider_exchange_code="sh",
        security_type=CNSecurityType.EQUITY,
        listed_from=date(1999, 11, 10),
        status=CNInstrumentStatus.LISTED,
        source_provider="fixture",
        source_endpoint="instrument-list",
        source_timestamp=source_timestamp,
    )


def test_cn_market_metadata_uses_canonical_iso_mics() -> None:
    assert set(CN_MARKETS) == {"XSHG", "XSHE", "BJSE"}
    assert {metadata.canonical_mic for metadata in CN_MARKETS.values()} == {
        "XSHG",
        "XSHE",
        "BJSE",
    }
    assert {metadata.currency for metadata in CN_MARKETS.values()} == {"CNY"}
    assert {metadata.timezone for metadata in CN_MARKETS.values()} == {"Asia/Shanghai"}


def test_provider_exchange_aliases_map_explicitly_for_three_exchanges() -> None:
    assert canonical_mic_for_alias("baostock", "sh") == "XSHG"
    assert canonical_mic_for_alias("baostock", "sz") == "XSHE"
    assert canonical_mic_for_alias("akshare", "BJ") == "BJSE"
    with pytest.raises(ValueError, match="Unknown CN exchange alias"):
        canonical_mic_for_alias("baostock", "bj")


def test_instrument_master_is_typed_and_versioned_without_overwrite() -> None:
    first_time = datetime(2026, 7, 28, tzinfo=UTC)
    second_time = datetime(2026, 7, 29, tzinfo=UTC)
    first = CNInstrumentSnapshot(
        snapshot_id="akshare-universe-2026-07-28",
        as_of=first_time,
        source_provider="akshare",
        source_endpoint="fixture-instrument-list",
        instruments=(_instrument(first_time),),
    )
    second = CNInstrumentSnapshot(
        snapshot_id="akshare-universe-2026-07-29",
        as_of=second_time,
        source_provider="akshare",
        source_endpoint="fixture-instrument-list",
        instruments=(_instrument(second_time),),
    )

    assert first.snapshot_id != second.snapshot_id
    assert first.instruments[0].security_type is CNSecurityType.EQUITY
    assert first.instruments[0].instrument_id == "XSHG.600000"
    assert second.instruments[0].source_timestamp > first.instruments[0].source_timestamp

    invalid_identity = _instrument(first_time).model_dump()
    invalid_identity["instrument_id"] = "SSE.600000"
    with pytest.raises(ValidationError, match="canonical MIC"):
        CNInstrumentRecord.model_validate(invalid_identity)


def test_endpoint_specific_volume_conversion_never_guesses_units() -> None:
    lot_lineage = normalize_volume_to_shares(
        1_234, AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata
    )
    share_lineage = normalize_volume_to_shares(123_400, BAOSTOCK_DAILY_CANDIDATE.metadata)

    assert lot_lineage.native_unit is VolumeUnit.LOT
    assert lot_lineage.canonical_value_shares == 123_400
    assert share_lineage.native_unit is VolumeUnit.SHARE
    assert share_lineage.canonical_value_shares == 123_400

    payload = {
        "provider": "missing-unit",
        "endpoint": "daily",
        "upstream_source": "fixture",
        "provider_version": "1",
        "adapter_version": "1",
        "native_schema_version": "1",
        "volume_multiplier_to_shares": 1,
        "amount_unit": AmountUnit.CNY,
        "price_adjustment": PriceAdjustment.UNADJUSTED,
        "license_scope": "fixture-only",
    }
    with pytest.raises(ValidationError, match="volume_unit"):
        CNEndpointMetadata.model_validate(payload)


def test_endpoint_unit_declarations_match_offline_contract_fixtures() -> None:
    fixture_root = Path(__file__).parent / "fixtures" / "providers" / "cn"
    fixtures = {
        "akshare": (
            "akshare_stock_zh_a_hist_contract_v1.json",
            AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata,
        ),
        "baostock": (
            "baostock_history_contract_v1.json",
            BAOSTOCK_DAILY_CANDIDATE.metadata,
        ),
    }

    for provider, (filename, metadata) in fixtures.items():
        fixture = json.loads((fixture_root / filename).read_text(encoding="utf-8"))
        assert fixture["fixture_kind"] == "documentation_contract_sample"
        assert fixture["provider"] == provider == metadata.provider
        assert fixture["endpoint"] == metadata.endpoint
        assert fixture["provider_version"] == metadata.provider_version
        assert fixture["volume_unit"] == metadata.volume_unit
        assert fixture["volume_multiplier_to_shares"] == metadata.volume_multiplier_to_shares
        assert fixture["price_adjustment"] == metadata.price_adjustment


def test_cn_raw_policy_rejects_adjusted_prices() -> None:
    ensure_unadjusted_raw_endpoint(BAOSTOCK_DAILY_CANDIDATE.metadata)
    adjusted = BAOSTOCK_DAILY_CANDIDATE.metadata.model_copy(
        update={"price_adjustment": PriceAdjustment.FORWARD}
    )
    with pytest.raises(ValueError, match="unadjusted"):
        ensure_unadjusted_raw_endpoint(adjusted)


def test_endpoint_request_lineage_requires_timestamp_parameters_and_raw_hash() -> None:
    lineage = CNEndpointRequestLineage(
        endpoint_metadata=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata,
        request_parameters={"symbol": "600000", "adjust": ""},
        retrieval_timestamp=datetime(2026, 7, 29, tzinfo=UTC),
        raw_response_hash="a" * 64,
    )
    assert lineage.request_parameters["adjust"] == ""
    assert lineage.raw_response_hash == "a" * 64

    with pytest.raises(ValidationError, match="raw_response_hash"):
        CNEndpointRequestLineage(
            endpoint_metadata=AKSHARE_STOCK_ZH_A_HIST_CANDIDATE.metadata,
            request_parameters={"symbol": "600000"},
            retrieval_timestamp=datetime(2026, 7, 29, tzinfo=UTC),
            raw_response_hash="not-a-sha256",
        )
