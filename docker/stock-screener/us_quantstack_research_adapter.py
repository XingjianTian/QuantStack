"""US-only QuantStack API research adapter; never installed into the default SS path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd


class USResearchAdapterError(RuntimeError):
    pass


class USResearchMarketDataClient(Protocol):
    def fetch_instruments(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def fetch_bars(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def get_dataset_metadata(self, dataset_id: str, version: str = "latest") -> dict[str, Any]: ...


@dataclass(frozen=True)
class USResearchResult:
    frame: pd.DataFrame
    instrument_id: str
    dataset_id: str
    dataset_version: str
    snapshot_token: str
    adjustment_semantics: str


class USQuantStackResearchAdapter:
    """Resolve plain US symbols through Batch API and read one explicit shadow version."""

    def __init__(
        self,
        client: USResearchMarketDataClient,
        *,
        bars_dataset_id: str,
        dataset_version: str,
    ) -> None:
        if not bars_dataset_id or not dataset_version or dataset_version == "latest":
            raise ValueError("US research shadow requires an explicit dataset and version")
        self.client = client
        self.bars_dataset_id = bars_dataset_id
        self.dataset_version = dataset_version

    def _metadata(self) -> dict[str, Any]:
        metadata = self.client.get_dataset_metadata(
            self.bars_dataset_id, self.dataset_version
        )
        state = metadata.get("publication_state")
        if (
            metadata.get("dataset_id") != self.bars_dataset_id
            or metadata.get("dataset_version") != self.dataset_version
            or metadata.get("quality_status") != "passed"
            or not isinstance(state, dict)
            or state.get("shadow_eligible") is not True
            or state.get("candidate_eligible") is not False
            or state.get("cutover_eligible") is not False
        ):
            raise USResearchAdapterError("US dataset is not an isolated research shadow")
        return metadata

    def _instrument_id(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized or "." in normalized or len(normalized) > 16:
            raise USResearchAdapterError("US research symbol is invalid")
        payload = self.client.fetch_instruments(
            {
                "market": "US",
                "symbols": [normalized],
                "active": True,
                "page_size": 10,
            }
        )
        items = [
            item
            for item in payload.get("items", [])
            if str(item.get("symbol", "")).upper() == normalized
        ]
        if len(items) != 1:
            raise USResearchAdapterError("US symbol does not resolve to one instrument")
        instrument_id = str(items[0].get("instrument_id", ""))
        if not instrument_id.startswith(("XNAS.", "XNYS.")):
            raise USResearchAdapterError("US symbol resolved to an unsupported venue")
        return instrument_id

    def get_price_history(self, symbol: str, *, start: date, end: date) -> USResearchResult:
        metadata = self._metadata()
        instrument_id = self._instrument_id(symbol)
        payload = self.client.fetch_bars(
            {
                "instrument_ids": [instrument_id],
                "market": "US",
                "start": start.isoformat(),
                "end": end.isoformat(),
                "frequency": "1d",
                "dataset_id": self.bars_dataset_id,
                "dataset_version": self.dataset_version,
                "page_size": 100_000,
            }
        )
        identity = (
            payload.get("dataset_id"),
            payload.get("dataset_version"),
            payload.get("snapshot_token"),
        )
        expected = (
            self.bars_dataset_id,
            self.dataset_version,
            metadata.get("snapshot_token"),
        )
        if identity != expected or payload.get("next_cursor") is not None:
            raise USResearchAdapterError("US Batch snapshot changed or exceeded one bounded page")
        rows = [
            item for item in payload.get("items", []) if item.get("instrument_id") == instrument_id
        ]
        adjustments = {str(item.get("adjustment_semantics")) for item in rows}
        if len(adjustments) != 1 or not adjustments.issubset(
            {"split_adjusted", "total_return"}
        ):
            raise USResearchAdapterError("US shadow adjustment semantics are not explicit")
        records = []
        for item in rows:
            timestamp = pd.Timestamp(item["ts_event"])
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            records.append(
                {
                    "Date": timestamp.tz_convert("America/New_York").normalize(),
                    "Open": float(item["open"]),
                    "High": float(item["high"]),
                    "Low": float(item["low"]),
                    "Close": float(item["close"]),
                    "Volume": float(item["volume"]),
                }
            )
        frame = (
            pd.DataFrame.from_records(records).set_index("Date").sort_index()
            if records
            else pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        )
        adjustment = next(iter(adjustments))
        frame.attrs.update(
            {
                "adjustment": adjustment,
                "volume_unit": "shares",
                "timezone": "America/New_York",
                "research_only": True,
            }
        )
        return USResearchResult(
            frame=frame,
            instrument_id=instrument_id,
            dataset_id=self.bars_dataset_id,
            dataset_version=self.dataset_version,
            snapshot_token=str(metadata["snapshot_token"]),
            adjustment_semantics=adjustment,
        )


def compare_us_research_view(
    legacy: pd.DataFrame, candidate: USResearchResult
) -> dict[str, object]:
    """Compare K-line, SMA20, a simple scan predicate and latest watermark."""

    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(legacy.columns) or not required.issubset(candidate.frame.columns):
        raise USResearchAdapterError("US research comparison is missing K-line columns")
    shared = legacy.index.intersection(candidate.frame.index)
    legacy_close = legacy.loc[shared, "Close"].astype(float)
    candidate_close = candidate.frame.loc[shared, "Close"].astype(float)
    legacy_sma20 = legacy_close.rolling(20).mean()
    candidate_sma20 = candidate_close.rolling(20).mean()
    valid = legacy_sma20.notna() & candidate_sma20.notna()
    close_denominator = legacy_close.abs()
    close_relative = close_denominator.gt(0)
    kline_difference_bps = (
        float(
            (
                (legacy_close[close_relative] - candidate_close[close_relative]).abs()
                / close_denominator[close_relative]
                * 10_000
            ).max()
        )
        if close_relative.any()
        else None
    )
    indicator_difference = (
        float((legacy_sma20[valid] - candidate_sma20[valid]).abs().max())
        if valid.any()
        else None
    )
    sma_denominator = legacy_sma20.abs()
    sma_relative = valid & sma_denominator.gt(0)
    indicator_difference_bps = (
        float(
            (
                (legacy_sma20[sma_relative] - candidate_sma20[sma_relative]).abs()
                / sma_denominator[sma_relative]
                * 10_000
            ).max()
        )
        if sma_relative.any()
        else None
    )
    legacy_scan = bool(valid.any() and legacy_close[valid].iloc[-1] > legacy_sma20[valid].iloc[-1])
    candidate_scan = bool(
        valid.any() and candidate_close[valid].iloc[-1] > candidate_sma20[valid].iloc[-1]
    )
    return {
        "schema_version": "quantstack.us-ss-research-comparison.v1",
        "shared_rows": len(shared),
        "legacy_latest_date": None if legacy.empty else legacy.index.max().date().isoformat(),
        "candidate_latest_date": (
            None if candidate.frame.empty else candidate.frame.index.max().date().isoformat()
        ),
        "kline_close_max_absolute_difference": (
            None if not len(shared) else float((legacy_close - candidate_close).abs().max())
        ),
        "kline_close_max_difference_bps": kline_difference_bps,
        "sma20_max_absolute_difference": indicator_difference,
        "sma20_max_difference_bps": indicator_difference_bps,
        "legacy_close_above_sma20": legacy_scan,
        "candidate_close_above_sma20": candidate_scan,
        "scan_predicate_equal": legacy_scan == candidate_scan,
        "adjustment_semantics": candidate.adjustment_semantics,
        "research_only": True,
        "production_path_changed": False,
    }
