from __future__ import annotations

from pydantic import Field

from ..models import DataModel, Market


class ProviderRuntimeContract(DataModel):
    """Exact, read-only execution contract declared by a provider adapter."""

    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    provider_version: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    native_schema_version: str = Field(min_length=1)
    supported_markets: tuple[Market, ...] = Field(min_length=1)
    supported_frequencies: tuple[str, ...] = Field(min_length=1)


class ProviderContractMismatchError(ValueError):
    """Raised before provider I/O when a task pin differs from the adapter contract."""

