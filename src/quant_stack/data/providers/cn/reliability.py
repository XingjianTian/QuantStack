from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum

from ...quality.provider_schema import ProviderSchemaDriftError


class ProviderProbeFailure(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION_FAILURE = "connection_failure"
    EMPTY_RESPONSE = "empty_response"
    SCHEMA_ANOMALY = "schema_anomaly"
    PROVIDER_FAILURE = "provider_failure"


class EmptyProviderResponseError(RuntimeError):
    """Raised when a live route returns no rows for a known trading window."""


def require_non_empty(records: Sequence[object]) -> None:
    if not records:
        raise EmptyProviderResponseError("provider returned no rows for the controlled window")


def classify_probe_failure(exc: BaseException) -> ProviderProbeFailure:
    """Map provider-specific exceptions to a stable reliability outcome."""

    if isinstance(exc, TimeoutError):
        return ProviderProbeFailure.TIMEOUT
    if isinstance(exc, ConnectionError):
        return ProviderProbeFailure.CONNECTION_FAILURE
    if isinstance(exc, EmptyProviderResponseError):
        return ProviderProbeFailure.EMPTY_RESPONSE
    if isinstance(exc, ProviderSchemaDriftError):
        return ProviderProbeFailure.SCHEMA_ANOMALY
    return ProviderProbeFailure.PROVIDER_FAILURE
