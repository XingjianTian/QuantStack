from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, JsonValue, model_validator

from ...markets import CNExchange
from ...models import DataModel


class CNSecurityType(StrEnum):
    EQUITY = "equity"
    ETF = "etf"
    INDEX = "index"
    FUND = "fund"
    OTHER = "other"


class VolumeUnit(StrEnum):
    SHARE = "share"
    LOT = "lot"


class AmountUnit(StrEnum):
    CNY = "CNY"


class PriceAdjustment(StrEnum):
    UNADJUSTED = "unadjusted"
    FORWARD = "forward_adjusted"
    BACKWARD = "backward_adjusted"


class CNEndpointMetadata(DataModel):
    """Pinned metadata for one provider endpoint, never for a provider as a whole."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    upstream_source: str = Field(min_length=1)
    provider_version: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    volume_unit: VolumeUnit
    volume_multiplier_to_shares: float = Field(gt=0)
    amount_unit: AmountUnit
    price_adjustment: PriceAdjustment
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    license_scope: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unit_multiplier(self) -> CNEndpointMetadata:
        if self.volume_unit is VolumeUnit.SHARE and self.volume_multiplier_to_shares != 1:
            raise ValueError("share endpoints must use volume_multiplier_to_shares=1")
        return self


class CNEndpointRequestLineage(DataModel):
    """Per-request evidence stored with immutable provider-native Raw."""

    endpoint_metadata: CNEndpointMetadata
    request_parameters: dict[str, JsonValue]
    retrieval_timestamp: datetime
    raw_response_hash: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def retrieval_timestamp_is_aware(self) -> CNEndpointRequestLineage:
        if self.retrieval_timestamp.tzinfo is None:
            raise ValueError("retrieval_timestamp must be timezone-aware")
        return self


class VolumeNormalizationLineage(DataModel):
    provider: str
    endpoint: str
    native_value: float = Field(ge=0)
    native_unit: VolumeUnit
    multiplier_to_shares: float = Field(gt=0)
    canonical_value_shares: float = Field(ge=0)


def normalize_volume_to_shares(
    native_value: float, endpoint: CNEndpointMetadata
) -> VolumeNormalizationLineage:
    """Apply only the multiplier declared by the exact endpoint metadata."""

    canonical_value = native_value * endpoint.volume_multiplier_to_shares
    return VolumeNormalizationLineage(
        provider=endpoint.provider,
        endpoint=endpoint.endpoint,
        native_value=native_value,
        native_unit=endpoint.volume_unit,
        multiplier_to_shares=endpoint.volume_multiplier_to_shares,
        canonical_value_shares=canonical_value,
    )


def ensure_unadjusted_raw_endpoint(endpoint: CNEndpointMetadata) -> None:
    """Reject adjusted price series at the immutable CN Raw-bar boundary."""

    if endpoint.price_adjustment is not PriceAdjustment.UNADJUSTED:
        raise ValueError("CN Raw bars must be unadjusted")


class CNEndpointCapability(DataModel):
    metadata: CNEndpointMetadata
    supported_exchanges: tuple[CNExchange, ...] = Field(min_length=1)
    supported_security_types: tuple[CNSecurityType, ...] = Field(min_length=1)
    instrument_discovery: bool = False
    historical_daily_bars: bool = False
    daily_increment: bool = False
    gap_repair: bool = False
    cross_validation: bool = False


class CNProviderCapabilities(DataModel):
    """Versioned declaration for a candidate CN adapter and its pinned endpoints."""

    schema_version: Literal["2.0.0"] = "2.0.0"
    provider: str = Field(min_length=1)
    endpoints: tuple[CNEndpointCapability, ...] = Field(min_length=1)
    fundamentals: bool = False

    @property
    def exchanges(self) -> frozenset[CNExchange]:
        return frozenset(
            exchange for endpoint in self.endpoints for exchange in endpoint.supported_exchanges
        )


BAOSTOCK_DAILY_CANDIDATE = CNEndpointCapability(
    metadata=CNEndpointMetadata(
        provider="baostock",
        endpoint="query_history_k_data_plus",
        upstream_source="BaoStock public data service",
        provider_version="0.9.3",
        adapter_version="contract-only",
        native_schema_version="fixture-required-v1",
        volume_unit=VolumeUnit.SHARE,
        volume_multiplier_to_shares=1,
        amount_unit=AmountUnit.CNY,
        price_adjustment=PriceAdjustment.UNADJUSTED,
        license_scope="unverified-personal-research",
    ),
    supported_exchanges=("XSHG", "XSHE"),
    supported_security_types=(CNSecurityType.EQUITY,),
    historical_daily_bars=True,
    cross_validation=True,
)

AKSHARE_STOCK_ZH_A_HIST_CANDIDATE = CNEndpointCapability(
    metadata=CNEndpointMetadata(
        provider="akshare",
        endpoint="stock_zh_a_hist",
        upstream_source="Eastmoney",
        provider_version="1.18.81",
        adapter_version="contract-only",
        native_schema_version="fixture-required-v1",
        volume_unit=VolumeUnit.LOT,
        volume_multiplier_to_shares=100,
        amount_unit=AmountUnit.CNY,
        price_adjustment=PriceAdjustment.UNADJUSTED,
        license_scope="upstream-license-unverified-personal-research",
    ),
    supported_exchanges=("XSHG", "XSHE", "BJSE"),
    supported_security_types=(CNSecurityType.EQUITY,),
    historical_daily_bars=True,
    daily_increment=True,
    gap_repair=True,
    cross_validation=True,
)

AKSHARE_STOCK_ZH_A_SPOT_EM_CANDIDATE = CNEndpointCapability(
    metadata=CNEndpointMetadata(
        provider="akshare",
        endpoint="stock_zh_a_spot_em",
        upstream_source="Eastmoney",
        provider_version="1.18.81",
        adapter_version="contract-only",
        native_schema_version="fixture-required-v1",
        volume_unit=VolumeUnit.LOT,
        volume_multiplier_to_shares=100,
        amount_unit=AmountUnit.CNY,
        price_adjustment=PriceAdjustment.UNADJUSTED,
        license_scope="upstream-license-unverified-personal-research",
    ),
    supported_exchanges=("XSHG", "XSHE", "BJSE"),
    supported_security_types=(CNSecurityType.EQUITY,),
    instrument_discovery=True,
)

BAOSTOCK_CANDIDATE_CAPABILITIES = CNProviderCapabilities(
    provider="baostock", endpoints=(BAOSTOCK_DAILY_CANDIDATE,)
)
AKSHARE_CANDIDATE_CAPABILITIES = CNProviderCapabilities(
    provider="akshare",
    endpoints=(
        AKSHARE_STOCK_ZH_A_SPOT_EM_CANDIDATE,
        AKSHARE_STOCK_ZH_A_HIST_CANDIDATE,
    ),
)
