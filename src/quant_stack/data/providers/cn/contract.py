from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Protocol, runtime_checkable

from ...interfaces import DataProvider
from .capabilities import CNProviderCapabilities


@runtime_checkable
class CNProvider(DataProvider, Protocol):
    """CN extension of the existing DataProvider contract used by IngestionService."""

    @property
    def cn_capabilities(self) -> CNProviderCapabilities: ...


@runtime_checkable
class CNFundamentalsProvider(Protocol):
    """Optional capability; daily-bar providers are not required to implement it."""

    def fetch_fundamentals(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
    ) -> Iterable[Mapping[str, object]]: ...
