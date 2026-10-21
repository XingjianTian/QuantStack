from __future__ import annotations

from typing import Literal

from pydantic import Field

from ..models import DataModel

CNExchange = Literal["XSHG", "XSHE", "BJSE"]


class CNMarketMetadata(DataModel):
    """Canonical exchange identity plus explicitly scoped provider aliases."""

    market_code: Literal["CN"] = "CN"
    canonical_mic: CNExchange
    exchange_name: str = Field(min_length=1)
    currency: Literal["CNY"] = "CNY"
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    provider_exchange_aliases: dict[str, tuple[str, ...]] = Field(default_factory=dict)


CN_MARKETS: dict[CNExchange, CNMarketMetadata] = {
    "XSHG": CNMarketMetadata(
        canonical_mic="XSHG",
        exchange_name="Shanghai Stock Exchange",
        provider_exchange_aliases={
            "akshare": ("SH", "sh"),
            "baostock": ("sh",),
        },
    ),
    "XSHE": CNMarketMetadata(
        canonical_mic="XSHE",
        exchange_name="Shenzhen Stock Exchange",
        provider_exchange_aliases={
            "akshare": ("SZ", "sz"),
            "baostock": ("sz",),
        },
    ),
    "BJSE": CNMarketMetadata(
        canonical_mic="BJSE",
        exchange_name="Beijing Stock Exchange",
        provider_exchange_aliases={"akshare": ("BJ", "bj")},
    ),
}


def canonical_mic_for_alias(provider: str, alias: str) -> CNExchange:
    """Resolve only declared aliases; adapters may not guess an exchange from a symbol."""

    provider_key = provider.casefold()
    alias_key = alias.casefold()
    for mic, metadata in CN_MARKETS.items():
        aliases = metadata.provider_exchange_aliases.get(provider_key, ())
        if any(candidate.casefold() == alias_key for candidate in aliases):
            return mic
    raise ValueError(f"Unknown CN exchange alias {alias!r} for provider {provider!r}")
