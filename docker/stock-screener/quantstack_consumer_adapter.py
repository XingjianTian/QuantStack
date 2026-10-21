"""Read-only QuantStack market-data overlay for the locked Stock Screener upstream."""

from __future__ import annotations

import json
import logging
import math
import os
import queue
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol

import pandas as pd

LOCKED_SS_UPSTREAM_COMMIT = "da4d01a83e420ac9c148ff4fe0260ee1e4228885"
ADAPTER_VERSION = "1.7.6"
SHADOW_CONTRACT_VERSION = "1.5.0"
DEFAULT_READ_MODE = "legacy"

logger = logging.getLogger(__name__)

_READ_STAGE_TIMINGS: ContextVar[dict[str, float] | None] = ContextVar(
    "quantstack_read_stage_timings",
    default=None,
)


@contextmanager
def capture_read_stage_timings():
    """Collect one foreground read's internal timings without shared mutable state."""

    timings: dict[str, float] = {}
    token = _READ_STAGE_TIMINGS.set(timings)
    try:
        yield timings
    finally:
        _READ_STAGE_TIMINGS.reset(token)


def _record_read_stage(name: str, started_at: float) -> None:
    timings = _READ_STAGE_TIMINGS.get()
    if timings is None:
        return
    timings[name] = timings.get(name, 0.0) + (time.perf_counter() - started_at) * 1000


def dataset_release_date(dataset_version: str) -> date | None:
    """Return the dated release component while allowing immutable suffixes."""

    match = re.search(r"(?:daily-through-|\.)(\d{8})(?:[.-].*)?$", dataset_version)
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d").date()


class AdapterError(RuntimeError):
    pass


class MappingError(AdapterError):
    pass


class IncompatibleUnitError(AdapterError):
    pass


class CandidateModeDisabledError(AdapterError):
    pass


class QuantStackMarketDataClient(Protocol):
    def fetch_instruments(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def fetch_bars(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def get_dataset_metadata(self, dataset_id: str, version: str = "latest") -> dict[str, Any]: ...


class HTTPQuantStackMarketDataClient:
    """Bounded HTTP client with header-only credentials and no ambient proxy."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_seconds: float = 5.0,
        max_attempts: int = 2,
    ) -> None:
        if not base_url or "?" in base_url:
            raise ValueError("QuantStack base URL must be configured without query parameters")
        if not api_key:
            raise ValueError("QuantStack service credential is required")
        if max_attempts < 1 or max_attempts > 3:
            raise ValueError("QuantStack adapter attempts must be between 1 and 3")
        if timeout_seconds <= 0:
            raise ValueError("QuantStack adapter timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    @classmethod
    def from_environment(
        cls,
        *,
        timeout_seconds: float | None = None,
        max_attempts: int | None = None,
    ) -> HTTPQuantStackMarketDataClient:
        return cls(
            os.environ.get("SS_QUANTSTACK_BASE_URL", ""),
            os.environ.get("SS_QUANTSTACK_API_KEY", ""),
            timeout_seconds=(
                float(os.environ.get("SS_QUANTSTACK_TIMEOUT_SECONDS", "5"))
                if timeout_seconds is None
                else timeout_seconds
            ),
            max_attempts=(
                int(os.environ.get("SS_QUANTSTACK_MAX_ATTEMPTS", "2"))
                if max_attempts is None
                else max_attempts
            ),
        )

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Quant-API-Key": self._api_key,
                "User-Agent": f"ss-quantstack-adapter/{ADAPTER_VERSION}",
            },
        )
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with self._opener.open(request, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read())
            except (OSError, ValueError, urllib.error.HTTPError) as exc:
                last_error = exc
                if attempt < self.max_attempts:
                    time.sleep(min(0.1 * attempt, 0.25))
        error = AdapterError(f"QuantStack request failed: {type(last_error).__name__}")
        error.source_exception_class = type(last_error).__name__
        raise error

    def fetch_instruments(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/market/batch/instruments", payload)

    def fetch_bars(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/market/batch/bars", payload)

    def get_dataset_metadata(self, dataset_id: str, version: str = "latest") -> dict[str, Any]:
        safe_id = urllib.parse.quote(dataset_id, safe="_-0123456789abcdefghijklmnopqrstuvwxyz")
        safe_version = urllib.parse.quote(version, safe="._-0123456789abcdefghijklmnopqrstuvwxyz")
        return self._request("GET", f"/market/datasets/{safe_id}/metadata?version={safe_version}")


_MIC_TO_SUFFIX = {"XSHG": "SS", "XSHE": "SZ", "BJSE": "BJ"}
_SUFFIX_TO_MIC = {value: key for key, value in _MIC_TO_SUFFIX.items()}


def quantstack_instrument_to_ss_symbol(instrument_id: str) -> str:
    try:
        mic, code = instrument_id.strip().upper().split(".", maxsplit=1)
    except ValueError as exc:
        raise MappingError("QuantStack instrument ID must include an explicit MIC") from exc
    suffix = _MIC_TO_SUFFIX.get(mic)
    if suffix is None or len(code) != 6 or not code.isdigit():
        raise MappingError("QuantStack instrument ID is not an explicitly supported CN equity")
    return f"{code}.{suffix}"


def ss_symbol_to_quantstack_instrument(symbol: str) -> str:
    try:
        code, suffix = symbol.strip().upper().split(".", maxsplit=1)
    except ValueError as exc:
        raise MappingError("SS symbol must include an explicit exchange suffix") from exc
    mic = _SUFFIX_TO_MIC.get(suffix)
    if mic is None or len(code) != 6 or not code.isdigit():
        raise MappingError("SS symbol is not an explicitly supported CN equity")
    return f"{mic}.{code}"


@dataclass(frozen=True)
class SSAdapterMetadata:
    dataset_id: str
    dataset_version: str
    manifest_fingerprint: str
    snapshot_token: str
    etag: str
    instrument_id: str
    frequency: str
    adjustment: Literal["none", "qfq", "split_adjusted", "total_return"] = "none"
    adj_close_is_compatibility_alias: Literal[True] = True
    amount_by_date: dict[str, float | None] = field(default_factory=dict)


@dataclass(frozen=True)
class SSAdapterResult:
    frame: pd.DataFrame
    metadata: SSAdapterMetadata


class SSQuantStackConsumerAdapter:
    def __init__(
        self,
        client: QuantStackMarketDataClient,
        *,
        bars_dataset_id: str,
        dataset_version: str = "latest",
        max_cache_entries: int = 2048,
        metadata_ttl_seconds: float = 30.0,
        consumer_access: Literal["shadow", "candidate"] = "shadow",
        us_bars_dataset_id: str | None = None,
        us_instruments_dataset_id: str | None = None,
        us_dataset_version: str = "latest",
        us_instrument_resolver: Callable[[tuple[str, ...]], Mapping[str, str]]
        | None = None,
    ) -> None:
        if max_cache_entries < 1:
            raise ValueError("adapter cache must contain at least one entry")
        if metadata_ttl_seconds < 0:
            raise ValueError("metadata cache TTL must not be negative")
        if consumer_access not in {"shadow", "candidate"}:
            raise ValueError("unsupported SS QuantStack consumer access")
        self.client = client
        self.bars_dataset_id = bars_dataset_id
        self.dataset_version = dataset_version
        self.max_cache_entries = max_cache_entries
        self.metadata_ttl_seconds = metadata_ttl_seconds
        self.consumer_access = consumer_access
        self.us_bars_dataset_id = us_bars_dataset_id or ""
        self.us_instruments_dataset_id = us_instruments_dataset_id or ""
        self.us_dataset_version = us_dataset_version
        self.us_instrument_resolver = us_instrument_resolver
        self._cache: OrderedDict[tuple[str, ...], SSAdapterResult] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._metadata_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
        self._metadata_lock = threading.Lock()
        self.candidate_failure_stage: str | None = None

    @classmethod
    def from_environment(
        cls,
        *,
        us_instrument_resolver: Callable[[tuple[str, ...]], Mapping[str, str]]
        | None = None,
        timeout_seconds: float | None = None,
        max_attempts: int | None = None,
    ) -> SSQuantStackConsumerAdapter:
        dataset_id = os.environ.get("SS_QUANTSTACK_BARS_DATASET_ID", "")
        if not dataset_id:
            raise ValueError("SS_QUANTSTACK_BARS_DATASET_ID is required")
        read_mode = os.environ.get("SS_QUANTSTACK_READ_MODE", DEFAULT_READ_MODE).strip().lower()
        return cls(
            HTTPQuantStackMarketDataClient.from_environment(
                timeout_seconds=timeout_seconds,
                max_attempts=max_attempts,
            ),
            bars_dataset_id=dataset_id,
            dataset_version=os.environ.get("SS_QUANTSTACK_DATASET_VERSION", "latest"),
            max_cache_entries=int(os.environ.get("SS_QUANTSTACK_CACHE_MAX_ENTRIES", "2048")),
            metadata_ttl_seconds=float(
                os.environ.get("SS_QUANTSTACK_METADATA_TTL_SECONDS", "30")
            ),
            consumer_access=(
                "candidate"
                if read_mode in {"quantstack_candidate", "quantstack_primary"}
                else "shadow"
            ),
            us_bars_dataset_id=os.environ.get("SS_QUANTSTACK_US_BARS_DATASET_ID") or None,
            us_instruments_dataset_id=(
                os.environ.get("SS_QUANTSTACK_US_INSTRUMENTS_DATASET_ID") or None
            ),
            us_dataset_version=os.environ.get(
                "SS_QUANTSTACK_US_DATASET_VERSION", "latest"
            ),
            us_instrument_resolver=us_instrument_resolver,
        )

    @staticmethod
    def map_instrument(value: str, *, target: Literal["ss", "quantstack"]) -> str:
        if target == "ss":
            return quantstack_instrument_to_ss_symbol(value)
        return ss_symbol_to_quantstack_instrument(value)

    def dataset_version_for_market(self, market: str) -> str:
        return self.us_dataset_version if market.upper() == "US" else self.dataset_version

    def resolved_dataset_version_for_market(self, market: str) -> str:
        """Resolve a moving alias such as ``latest`` to its published version."""

        is_us = market.upper() == "US"
        dataset_id = self.us_bars_dataset_id if is_us else self.bars_dataset_id
        requested_version = self.dataset_version_for_market(market)
        return str(self._metadata(dataset_id, requested_version)["dataset_version"])

    def _metadata(
        self,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> dict[str, Any]:
        resolved_dataset_id = dataset_id or self.bars_dataset_id
        resolved_version = dataset_version or self.dataset_version
        cache_key = (resolved_dataset_id, resolved_version)
        with self._metadata_lock:
            cached = self._metadata_cache.get(cache_key)
            if cached is not None and time.monotonic() < cached[0]:
                return dict(cached[1])

            metadata = self.client.get_dataset_metadata(
                resolved_dataset_id, resolved_version
            )
            self.candidate_failure_stage = "metadata_contract"
            if metadata.get("quality_status") != "passed":
                raise AdapterError("QuantStack candidate dataset is not eligible")
            required = (
                "dataset_id",
                "dataset_version",
                "manifest_fingerprint",
                "snapshot_token",
                "publication_gate",
                "publication_state",
                "comparison_status",
                "comparison_complete",
                "canonical_source_provider",
                "canonical_source_endpoint",
                "selection_rule_id",
                "publication_evidence_fingerprint",
            )
            if any(key not in metadata or metadata[key] is None for key in required):
                raise AdapterError("QuantStack dataset snapshot metadata is incomplete")
            if any(
                not metadata.get(key)
                for key in (
                    "dataset_id",
                    "dataset_version",
                    "manifest_fingerprint",
                    "snapshot_token",
                    "canonical_source_provider",
                    "canonical_source_endpoint",
                    "selection_rule_id",
                    "publication_evidence_fingerprint",
                )
            ):
                raise AdapterError("QuantStack dataset snapshot metadata is incomplete")
            state = metadata["publication_state"]
            if not isinstance(state, Mapping):
                raise AdapterError("QuantStack publication state is invalid")
            required_state = (
                "quality_passed",
                "catalog_published",
                "comparison_complete",
                "shadow_eligible",
                "candidate_eligible",
                "cutover_eligible",
            )
            if any(not isinstance(state.get(key), bool) for key in required_state):
                raise AdapterError("QuantStack publication state is incomplete")
            if not state["quality_passed"] or not state["catalog_published"]:
                raise AdapterError("QuantStack publication state is not consumable")
            if metadata["comparison_complete"] != state["comparison_complete"]:
                raise AdapterError("QuantStack comparison state is inconsistent")
            fingerprint = metadata["publication_evidence_fingerprint"]
            if (
                not isinstance(fingerprint, str)
                or len(fingerprint) != 64
                or any(character not in "0123456789abcdef" for character in fingerprint)
            ):
                raise AdapterError("QuantStack publication evidence fingerprint is invalid")
            if self.consumer_access == "shadow":
                if (
                    metadata["publication_gate"] not in {"eligible", "incomplete"}
                    or not state["shadow_eligible"]
                ):
                    raise AdapterError("QuantStack dataset is not shadow eligible")
            elif (
                metadata["publication_gate"] != "eligible"
                or not state["comparison_complete"]
                or not state["candidate_eligible"]
            ):
                raise AdapterError("QuantStack dataset is not candidate eligible")

            self._metadata_cache[cache_key] = (
                time.monotonic() + self.metadata_ttl_seconds,
                dict(metadata),
            )
            return dict(metadata)

    def _cache_get(self, key: tuple[str, ...]) -> SSAdapterResult | None:
        with self._cache_lock:
            result = self._cache.get(key)
            if result is not None:
                self._cache.move_to_end(key)
            return result

    def _cache_set(self, key: tuple[str, ...], result: SSAdapterResult) -> None:
        with self._cache_lock:
            self._cache[key] = result
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_cache_entries:
                self._cache.popitem(last=False)

    @staticmethod
    def _frame(
        rows: list[dict[str, Any]],
        *,
        timezone: str = "Asia/Shanghai",
        allowed_adjustments: frozenset[str] = frozenset({"none", "qfq"}),
    ) -> tuple[pd.DataFrame, dict[str, float | None]]:
        if any(row.get("volume_unit") != "shares" for row in rows):
            raise IncompatibleUnitError("candidate volume is not explicitly measured in shares")
        adjustments = {row.get("adjustment_semantics") for row in rows}
        if not adjustments.issubset(allowed_adjustments) or len(adjustments) > 1:
            raise AdapterError(
                "candidate adjustment semantics must be homogeneous none or qfq for CN, "
                "or split_adjusted or total_return for US"
            )
        adjustment = next(iter(adjustments), "none")
        records: list[dict[str, Any]] = []
        amounts: dict[str, float | None] = {}
        for row in rows:
            timestamp = pd.Timestamp(row["ts_event"])
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            trade_date = timestamp.tz_convert(timezone).normalize()
            date_key = trade_date.date().isoformat()
            records.append(
                {
                    "Date": trade_date,
                    "Open": float(row["open"]),
                    "High": float(row["high"]),
                    "Low": float(row["low"]),
                    "Close": float(row["close"]),
                    "Volume": int(row["volume"]),
                    "Adj Close": float(row["close"]),
                }
            )
            amounts[date_key] = row.get("amount")
        if not records:
            columns = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
            empty_index = pd.DatetimeIndex([], name="Date")
            return pd.DataFrame(columns=columns, index=empty_index), amounts
        frame = pd.DataFrame.from_records(records).set_index("Date").sort_index()
        frame.attrs.update(
            {
                "adjustment": adjustment,
                "adj_close_is_compatibility_alias": True,
                "volume_unit": "shares",
                "timezone": timezone,
            }
        )
        return frame, amounts

    def get_price_history(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> SSAdapterResult:
        normalized = symbol.strip().upper()
        return self.get_batch_price_history(
            [normalized], start=start, end=end, frequency=frequency
        )[normalized]

    def get_last_trade_dates(
        self,
        symbols: list[str] | tuple[str, ...],
        *,
        start: date,
        end: date,
    ) -> dict[str, date | None]:
        """Return exact-version qfq watermarks for a bounded symbol set."""

        results = self.get_batch_price_history(
            symbols,
            start=start,
            end=end,
            frequency="1d",
        )
        return {
            symbol: (
                None
                if result.frame.empty
                else result.frame.index.max().date()
            )
            for symbol, result in results.items()
        }

    def get_batch_price_history(
        self,
        symbols: list[str] | tuple[str, ...],
        *,
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> dict[str, SSAdapterResult]:
        adapter_started_at = time.perf_counter()
        self.candidate_failure_stage = "symbol_mapping"
        normalized_symbols = tuple(str(symbol).strip().upper() for symbol in symbols)
        is_cn = tuple("." in symbol for symbol in normalized_symbols)
        if not normalized_symbols or len(set(is_cn)) != 1:
            raise MappingError("one Batch read must contain symbols from exactly one market")
        instrument_started_at = time.perf_counter()
        if is_cn[0]:
            mapping = {
                symbol: ss_symbol_to_quantstack_instrument(symbol)
                for symbol in normalized_symbols
            }
            dataset_id = self.bars_dataset_id
            requested_version = self.dataset_version
            timezone = "Asia/Shanghai"
            allowed_adjustments = frozenset({"none", "qfq"})
        else:
            if not self.us_bars_dataset_id:
                raise MappingError("US QuantStack dataset is not configured")
            resolved = {}
            if self.us_instrument_resolver is not None:
                resolved = {
                    str(symbol).upper(): str(instrument_id).upper()
                    for symbol, instrument_id in self.us_instrument_resolver(
                        normalized_symbols
                    ).items()
                    if str(instrument_id).upper().startswith(("XNAS.", "XNYS."))
                }
            missing_symbols = [
                symbol for symbol in normalized_symbols if symbol not in resolved
            ]
            if missing_symbols and not self.us_instruments_dataset_id:
                raise MappingError("US QuantStack instrument dataset is not configured")
            instrument_payload = (
                self.client.fetch_instruments(
                    {
                        "market": "US",
                        "symbols": missing_symbols,
                        "active": True,
                        "dataset_id": self.us_instruments_dataset_id,
                        "dataset_version": self.us_dataset_version,
                        "page_size": max(10, len(missing_symbols) * 2),
                    }
                )
                if missing_symbols
                else {"items": []}
            )
            resolved.update(
                {
                    str(item.get("symbol", "")).upper(): str(
                        item.get("instrument_id", "")
                    )
                    for item in instrument_payload.get("items", [])
                }
            )
            mapping = {symbol: resolved.get(symbol, "") for symbol in normalized_symbols}
            if any(
                not instrument_id.startswith(("XNAS.", "XNYS."))
                for instrument_id in mapping.values()
            ):
                raise MappingError("US symbol did not resolve to one supported equity")
            dataset_id = self.us_bars_dataset_id
            requested_version = self.us_dataset_version
            timezone = "America/New_York"
            allowed_adjustments = frozenset({"split_adjusted", "total_return"})
        _record_read_stage("instrument_resolution", instrument_started_at)
        exact_candidate_read = bool(
            self.consumer_access == "candidate"
            and requested_version != "latest"
        )
        metadata_started_at = time.perf_counter()
        if exact_candidate_read:
            dataset_version = requested_version
            manifest_fingerprint = ""
            snapshot_token = ""
            snapshot_identity: tuple[str, ...] | None = None
        else:
            self.candidate_failure_stage = "metadata_fetch"
            metadata = self._metadata(dataset_id, requested_version)
            dataset_id = str(metadata["dataset_id"])
            dataset_version = str(metadata["dataset_version"])
            manifest_fingerprint = str(metadata["manifest_fingerprint"])
            snapshot_token = str(metadata["snapshot_token"])
            snapshot_identity = (
                dataset_id,
                dataset_version,
                manifest_fingerprint,
                snapshot_token,
            )
        _record_read_stage("metadata", metadata_started_at)
        cache_lookup_started_at = time.perf_counter()
        cached: dict[str, SSAdapterResult] = {}
        missing: list[str] = []
        keys: dict[str, tuple[str, ...]] = {}
        for symbol, instrument_id in mapping.items():
            key = (
                dataset_id,
                dataset_version,
                snapshot_token or "exact-immutable-version",
                instrument_id,
                start.isoformat(),
                end.isoformat(),
                frequency,
            )
            keys[symbol] = key
            result = self._cache_get(key)
            if result is None:
                missing.append(symbol)
            else:
                cached[symbol] = result
        _record_read_stage("adapter_cache_lookup", cache_lookup_started_at)
        if not missing:
            self.candidate_failure_stage = None
            _record_read_stage("adapter_total", adapter_started_at)
            return cached

        request_payload = {
            "instrument_ids": [mapping[symbol] for symbol in missing],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "frequency": frequency,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "page_size": min(100_000, max(1, len(missing) * 4_000)),
        }
        rows_by_instrument: dict[str, list[dict[str, Any]]] = {
            mapping[symbol]: [] for symbol in missing
        }
        response_etag = ""
        cursor: str | None = None
        while True:
            page_payload = dict(request_payload)
            if cursor is not None:
                page_payload["cursor"] = cursor
            self.candidate_failure_stage = "bars_fetch"
            bars_started_at = time.perf_counter()
            payload = self.client.fetch_bars(page_payload)
            _record_read_stage("bars_http_and_json", bars_started_at)
            self.candidate_failure_stage = "bars_contract"
            if payload.get("quality_status") not in {"passed", "warning"}:
                raise AdapterError("QuantStack candidate batch response is not eligible")
            page_identity = tuple(
                str(payload.get(key, ""))
                for key in (
                    "dataset_id",
                    "dataset_version",
                    "manifest_fingerprint",
                    "snapshot_token",
                )
            )
            if snapshot_identity is None:
                if (
                    page_identity[0] != dataset_id
                    or page_identity[1] != dataset_version
                    or not all(page_identity)
                ):
                    raise AdapterError("QuantStack exact candidate identity is invalid")
                snapshot_identity = page_identity
                manifest_fingerprint = page_identity[2]
                snapshot_token = page_identity[3]
            elif page_identity != snapshot_identity:
                raise AdapterError("QuantStack candidate changed during pagination")
            response_etag = str(payload.get("etag", ""))
            for row in payload.get("items", []):
                instrument_id = str(row.get("instrument_id"))
                if instrument_id in rows_by_instrument:
                    rows_by_instrument[instrument_id].append(row)
            cursor = payload.get("next_cursor")
            if not cursor:
                break

        results = dict(cached)
        frame_started_at = time.perf_counter()
        for symbol in missing:
            instrument_id = mapping[symbol]
            self.candidate_failure_stage = "row_contract"
            frame, amounts = self._frame(
                rows_by_instrument[instrument_id],
                timezone=timezone,
                allowed_adjustments=allowed_adjustments,
            )
            result = SSAdapterResult(
                frame=frame,
                metadata=SSAdapterMetadata(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    manifest_fingerprint=manifest_fingerprint,
                    snapshot_token=snapshot_token,
                    etag=response_etag,
                    instrument_id=instrument_id,
                    frequency=frequency,
                    adjustment=frame.attrs.get("adjustment", "none"),
                    amount_by_date=amounts,
                ),
            )
            self._cache_set(keys[symbol], result)
            results[symbol] = result
        _record_read_stage("dataframe_conversion", frame_started_at)
        self.candidate_failure_stage = None
        _record_read_stage("adapter_total", adapter_started_at)
        return results


class ShadowComparisonStatus(StrEnum):
    PASSED = "passed"
    WARNING = "warning"
    INCOMPATIBLE_UNIT = "incompatible_unit"
    MISSING_CANDIDATE = "missing_candidate"
    MISSING_LEGACY = "missing_legacy"
    MAPPING_FAILED = "mapping_failed"
    CANDIDATE_ERROR = "candidate_error"
    LEGACY_ERROR = "legacy_error"
    BLOCKED_BY_ADJUSTMENT = "blocked_by_adjustment"


@dataclass(frozen=True)
class SSShadowComparisonReport:
    report_id: str
    created_at: str
    completed_at: str
    adapter_version: str
    shadow_contract_version: str
    read_mode: str
    read_path: str
    ss_symbol: str
    quantstack_instrument_id: str | None
    start_date: str
    end_date: str
    frequency: str
    period: str
    dataset_id: str | None
    dataset_version: str | None
    manifest_fingerprint: str | None
    snapshot_token: str | None
    status: ShadowComparisonStatus
    latency_ms: float
    error_class: str | None
    legacy_rows: int
    candidate_rows: int
    missing_legacy_dates: tuple[str, ...]
    missing_candidate_dates: tuple[str, ...]
    ohlc_mismatches: int
    volume_compatibility: str
    amount_coverage: int
    amount_mismatches: int
    adjustment_semantics: str
    legacy_adjustment_semantics: str
    adjustment_compatible: bool
    production_price_basis_required: str
    shared_dates: int
    price_ratio_median: float | None
    price_ratio_regime_changes: int
    close_return_overlap: int
    close_return_drift_p95: float | None
    close_return_drift_max: float | None
    normalized_ohlc_mismatches: int
    latest_complete_date: str | None
    ohlc_shape_compatible: bool
    consumer_contract_compatible: bool
    cutover_compatible: bool

    def sanitized(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def compare_shadow_frames(
    *,
    symbol: str,
    legacy: pd.DataFrame | None,
    candidate: SSAdapterResult | None,
    latency_ms: float,
    start: date,
    end: date,
    frequency: str = "1d",
    period: str = "2y",
    read_path: str = "get_historical_data",
    created_at: str | None = None,
    read_mode: str = "shadow",
    error: Exception | None = None,
    error_source: Literal["candidate", "legacy"] = "candidate",
    dataset_id_hint: str | None = None,
    dataset_version_hint: str | None = None,
    absolute_tolerance: float = 0.01,
    relative_tolerance: float = 0.001,
) -> SSShadowComparisonReport:
    candidate_metadata = candidate.metadata if candidate is not None else None
    dataset_id = candidate_metadata.dataset_id if candidate_metadata else dataset_id_hint
    dataset_version = (
        candidate_metadata.dataset_version if candidate_metadata else dataset_version_hint
    )
    manifest_fingerprint = candidate_metadata.manifest_fingerprint if candidate_metadata else None
    snapshot_token = candidate_metadata.snapshot_token if candidate_metadata else None
    common = {
        "report_id": f"ssshadow-{uuid.uuid4().hex}",
        "created_at": created_at or _iso_now(),
        "completed_at": _iso_now(),
        "adapter_version": ADAPTER_VERSION,
        "shadow_contract_version": SHADOW_CONTRACT_VERSION,
        "read_mode": read_mode,
        "read_path": read_path,
        "ss_symbol": symbol,
        "quantstack_instrument_id": (
            candidate_metadata.instrument_id if candidate_metadata else None
        ),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "frequency": frequency,
        "period": period,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "manifest_fingerprint": manifest_fingerprint,
        "snapshot_token": snapshot_token,
        "latency_ms": round(latency_ms, 3),
    }
    legacy_rows = 0 if legacy is None else len(legacy)
    candidate_rows = 0 if candidate is None else len(candidate.frame)
    if error is not None:
        if isinstance(error, MappingError):
            status = ShadowComparisonStatus.MAPPING_FAILED
        elif error_source == "legacy":
            status = ShadowComparisonStatus.LEGACY_ERROR
        else:
            status = ShadowComparisonStatus.CANDIDATE_ERROR
        return SSShadowComparisonReport(
            **common,
            status=status,
            error_class=type(error).__name__,
            legacy_rows=legacy_rows,
            candidate_rows=candidate_rows,
            missing_legacy_dates=(),
            missing_candidate_dates=(),
            ohlc_mismatches=0,
            volume_compatibility="not_compared",
            amount_coverage=0,
            amount_mismatches=0,
            adjustment_semantics="not_compared",
            legacy_adjustment_semantics="not_compared",
            adjustment_compatible=False,
            production_price_basis_required="qfq",
            shared_dates=0,
            price_ratio_median=None,
            price_ratio_regime_changes=0,
            close_return_overlap=0,
            close_return_drift_p95=None,
            close_return_drift_max=None,
            normalized_ohlc_mismatches=0,
            latest_complete_date=None,
            ohlc_shape_compatible=False,
            consumer_contract_compatible=False,
            cutover_compatible=False,
        )

    if legacy is None or legacy.empty:
        status = ShadowComparisonStatus.MISSING_LEGACY
    elif candidate is None or candidate.frame.empty:
        status = ShadowComparisonStatus.MISSING_CANDIDATE
    else:
        status = ShadowComparisonStatus.PASSED
    legacy_by_date = None
    if legacy is not None:
        legacy_by_date = legacy.copy(deep=False)
        legacy_by_date.index = pd.Index(
            [pd.Timestamp(value).date() for value in legacy.index], name="Date"
        )
    candidate_by_date = None
    if candidate is not None:
        candidate_by_date = candidate.frame.copy(deep=False)
        candidate_by_date.index = pd.Index(
            [pd.Timestamp(value).date() for value in candidate.frame.index], name="Date"
        )
    legacy_dates = (
        set() if legacy_by_date is None else {str(value) for value in legacy_by_date.index}
    )
    candidate_dates = (
        set() if candidate_by_date is None else {str(value) for value in candidate_by_date.index}
    )
    missing_legacy = tuple(sorted(candidate_dates - legacy_dates))
    missing_candidate = tuple(sorted(legacy_dates - candidate_dates))
    ohlc_mismatches = 0
    normalized_ohlc_mismatches = 0
    amount_mismatches = 0
    shared = pd.Index([])
    price_ratio_median: float | None = None
    price_ratio_regime_changes = 0
    close_return_overlap = 0
    close_return_drift_p95: float | None = None
    close_return_drift_max: float | None = None
    ohlc_columns = {"Open", "High", "Low", "Close"}
    if (
        legacy_by_date is not None
        and candidate_by_date is not None
        and ohlc_columns.issubset(legacy_by_date.columns)
        and ohlc_columns.issubset(candidate_by_date.columns)
    ):
        shared = legacy_by_date.index.intersection(candidate_by_date.index)
        for column in ("Open", "High", "Low", "Close"):
            left = legacy_by_date.loc[shared, column].astype(float)
            right = candidate_by_date.loc[shared, column].astype(float)
            tolerance = absolute_tolerance + relative_tolerance * left.abs()
            ohlc_mismatches += int(((left - right).abs() > tolerance).sum())
        legacy_close = legacy_by_date.loc[shared, "Close"].astype(float)
        candidate_close = candidate_by_date.loc[shared, "Close"].astype(float)
        valid_close = (
            legacy_close.map(math.isfinite)
            & candidate_close.map(math.isfinite)
            & (legacy_close > 0)
            & (candidate_close > 0)
        )
        close_ratio = legacy_close[valid_close] / candidate_close[valid_close]
        close_ratio = close_ratio[close_ratio.map(math.isfinite)]
        if not close_ratio.empty:
            price_ratio_median = round(float(close_ratio.median()), 12)
            ratio_change = close_ratio.pct_change(fill_method=None).abs().dropna()
            ratio_change = ratio_change[ratio_change.map(math.isfinite)]
            price_ratio_regime_changes = int((ratio_change > 0.02).sum())
        valid_return = valid_close & valid_close.shift(1, fill_value=False)
        legacy_returns = legacy_close / legacy_close.shift(1) - 1
        candidate_returns = candidate_close / candidate_close.shift(1) - 1
        return_drift = (legacy_returns[valid_return] - candidate_returns[valid_return]).abs()
        return_drift = return_drift[return_drift.map(math.isfinite)]
        close_return_overlap = len(return_drift)
        if not return_drift.empty:
            close_return_drift_p95 = round(float(return_drift.quantile(0.95)), 12)
            close_return_drift_max = round(float(return_drift.max()), 12)
        for column in ("Open", "High", "Low"):
            legacy_values = legacy_by_date.loc[shared, column].astype(float)
            candidate_values = candidate_by_date.loc[shared, column].astype(float)
            valid_shape = (
                valid_close
                & legacy_values.map(math.isfinite)
                & candidate_values.map(math.isfinite)
            )
            legacy_shape = legacy_values[valid_shape] / legacy_close[valid_shape]
            candidate_shape = candidate_values[valid_shape] / candidate_close[valid_shape]
            normalized_ohlc_mismatches += int(
                (
                    (legacy_shape - candidate_shape).abs() > relative_tolerance
                ).sum()
            )
            normalized_ohlc_mismatches += int((~valid_shape).sum())
        if "Amount" in legacy_by_date.columns:
            for value in shared:
                candidate_amount = candidate.metadata.amount_by_date.get(str(value))
                legacy_amount = legacy_by_date.loc[value, "Amount"]
                if candidate_amount is None or pd.isna(legacy_amount):
                    continue
                tolerance = absolute_tolerance + relative_tolerance * abs(float(legacy_amount))
                amount_mismatches += int(
                    abs(float(legacy_amount) - float(candidate_amount)) > tolerance
                )

    if status == ShadowComparisonStatus.PASSED and (ohlc_mismatches or amount_mismatches):
        status = ShadowComparisonStatus.WARNING
    volume_compatibility = "incompatible_legacy_unknown_or_mixed"
    if (
        legacy_by_date is not None
        and candidate_by_date is not None
        and legacy is not None
        and legacy.attrs.get("volume_unit") == "shares"
        and "Volume" in legacy_by_date.columns
        and "Volume" in candidate_by_date.columns
    ):
        shared = legacy_by_date.index.intersection(candidate_by_date.index)
        volume_mismatches = int(
            (
                legacy_by_date.loc[shared, "Volume"].astype(float)
                != candidate_by_date.loc[shared, "Volume"].astype(float)
            ).sum()
        )
        volume_compatibility = "shares_compatible" if volume_mismatches == 0 else "shares_mismatch"
        if volume_mismatches and status == ShadowComparisonStatus.PASSED:
            status = ShadowComparisonStatus.WARNING
    if status in {ShadowComparisonStatus.PASSED, ShadowComparisonStatus.WARNING} and (
        volume_compatibility != "shares_compatible"
    ):
        status = ShadowComparisonStatus.INCOMPATIBLE_UNIT

    adjustment = candidate_metadata.adjustment if candidate_metadata else "not_available"
    legacy_adjustment = (
        str(legacy.attrs.get("adjustment", "unknown"))
        if legacy is not None
        else "not_available"
    )
    adjustment_compatible = bool(
        candidate_metadata is not None
        and legacy is not None
        and adjustment in {"none", "qfq"}
        and adjustment == legacy_adjustment
    )
    adjustment_blocked = bool(
        candidate is not None
        and legacy is not None
        and not legacy.empty
        and not candidate.frame.empty
        and not adjustment_compatible
    )
    if adjustment_blocked and status in {
        ShadowComparisonStatus.PASSED,
        ShadowComparisonStatus.WARNING,
        ShadowComparisonStatus.INCOMPATIBLE_UNIT,
    }:
        status = ShadowComparisonStatus.BLOCKED_BY_ADJUSTMENT
    ohlc_shape_compatible = bool(
        candidate is not None
        and legacy is not None
        and not legacy.empty
        and not candidate.frame.empty
        and not missing_candidate
        and ohlc_mismatches == 0
    )
    consumer_contract_compatible = bool(
        ohlc_shape_compatible and volume_compatibility == "shares_compatible"
    )
    latest = max(legacy_dates & candidate_dates) if legacy_dates & candidate_dates else None
    return SSShadowComparisonReport(
        **common,
        status=status,
        error_class=None,
        legacy_rows=legacy_rows,
        candidate_rows=candidate_rows,
        missing_legacy_dates=missing_legacy,
        missing_candidate_dates=missing_candidate,
        ohlc_mismatches=ohlc_mismatches,
        volume_compatibility=volume_compatibility,
        amount_coverage=(
            0
            if candidate is None
            else sum(value is not None for value in candidate.metadata.amount_by_date.values())
        ),
        amount_mismatches=amount_mismatches,
        adjustment_semantics=adjustment,
        legacy_adjustment_semantics=legacy_adjustment,
        adjustment_compatible=adjustment_compatible,
        production_price_basis_required="qfq",
        shared_dates=len(shared),
        price_ratio_median=price_ratio_median,
        price_ratio_regime_changes=price_ratio_regime_changes,
        close_return_overlap=close_return_overlap,
        close_return_drift_p95=close_return_drift_p95,
        close_return_drift_max=close_return_drift_max,
        normalized_ohlc_mismatches=normalized_ohlc_mismatches,
        latest_complete_date=latest,
        ohlc_shape_compatible=ohlc_shape_compatible,
        consumer_contract_compatible=consumer_contract_compatible,
        # Phase 2.3A is offline shadow-only; adjustment is also explicitly unverified.
        cutover_compatible=False,
    )


def write_immutable_shadow_report(
    report_root: str | os.PathLike[str], report: SSShadowComparisonReport
) -> Path:
    """Create one fsync'd report without ever replacing an existing report."""

    if not report.report_id.startswith("ssshadow-") or "/" in report.report_id:
        raise ValueError("invalid shadow report id")
    day = report.completed_at[:10]
    root = Path(report_root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    day_root = root / day
    day_root.mkdir(mode=0o700, exist_ok=True)
    os.chmod(day_root, 0o700)
    final_path = day_root / f"{report.report_id}.json"
    temporary_path = day_root / f".{report.report_id}.{uuid.uuid4().hex}.tmp"
    payload = (
        json.dumps(
            report.sanitized(), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode()
    descriptor = os.open(temporary_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary_path, final_path)
        directory_descriptor = os.open(day_root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
    os.chmod(final_path, 0o600)
    return final_path


@dataclass(frozen=True)
class SSReadModeConfig:
    mode: Literal["legacy", "shadow", "quantstack_candidate", "quantstack_primary"] = "legacy"
    shadow_enabled: bool = False
    sample_rate: float = 0.0
    allowlist: frozenset[str] = frozenset()
    max_window_days: int = 730
    runtime_environment: str = "production"
    allow_candidate_mode: bool = False
    primary_enabled: bool = False
    report_root: str | None = None
    shadow_queue_size: int = 128
    max_symbols_per_task: int = 100
    max_legacy_rows_per_task: int = 100_000
    shutdown_timeout_seconds: float = 1.0

    @classmethod
    def from_environment(cls) -> SSReadModeConfig:
        mode = os.environ.get("SS_QUANTSTACK_READ_MODE", DEFAULT_READ_MODE).strip().lower()
        if mode not in {"legacy", "shadow", "quantstack_candidate", "quantstack_primary"}:
            raise ValueError("unsupported SS QuantStack read mode")
        allowlist = frozenset(
            value.strip().upper()
            for value in os.environ.get("SS_QUANTSTACK_SHADOW_ALLOWLIST", "").split(",")
            if value.strip()
        )
        return cls(
            mode=mode,
            shadow_enabled=os.environ.get("SS_QUANTSTACK_SHADOW_ENABLED", "false").lower()
            == "true",
            sample_rate=float(os.environ.get("SS_QUANTSTACK_SHADOW_SAMPLE_RATE", "0")),
            allowlist=allowlist,
            max_window_days=int(os.environ.get("SS_QUANTSTACK_SHADOW_MAX_WINDOW_DAYS", "730")),
            runtime_environment=os.environ.get("SS_RUNTIME_ENV", "production").lower(),
            allow_candidate_mode=os.environ.get("SS_QUANTSTACK_ALLOW_CANDIDATE", "false").lower()
            == "true",
            primary_enabled=os.environ.get("SS_QUANTSTACK_PRIMARY_ENABLED", "false").lower()
            == "true",
            report_root=os.environ.get("SS_QUANTSTACK_SHADOW_REPORT_ROOT") or None,
            shadow_queue_size=int(os.environ.get("SS_QUANTSTACK_SHADOW_QUEUE_SIZE", "128")),
            max_symbols_per_task=int(
                os.environ.get("SS_QUANTSTACK_SHADOW_MAX_SYMBOLS_PER_TASK", "100")
            ),
            max_legacy_rows_per_task=int(
                os.environ.get("SS_QUANTSTACK_SHADOW_MAX_LEGACY_ROWS_PER_TASK", "100000")
            ),
            shutdown_timeout_seconds=float(
                os.environ.get("SS_QUANTSTACK_SHUTDOWN_TIMEOUT_SECONDS", "1")
            ),
        )

    def candidate_mode_allowed(self) -> bool:
        return self.allow_candidate_mode and self.runtime_environment in {
            "test",
            "testing",
            "staging",
            "development",
        }

    def direct_mode_allowed(self) -> bool:
        if self.mode == "quantstack_primary":
            return self.primary_enabled
        return self.candidate_mode_allowed()

    def shadow_selected(self, symbol: str) -> bool:
        if self.mode != "shadow" or not self.shadow_enabled:
            return False
        normalized = symbol.strip().upper()
        try:
            ss_symbol_to_quantstack_instrument(normalized)
        except MappingError:
            return False
        if self.allowlist and normalized not in self.allowlist:
            return False
        return random.random() < min(1.0, max(0.0, self.sample_rate))


@dataclass(frozen=True)
class _ShadowTask:
    legacy_results: dict[str, pd.DataFrame | None]
    symbols: tuple[str, ...]
    start: date
    end: date
    frequency: str
    period: str
    created_at: str
    read_path: str = "unknown"


@dataclass(frozen=True)
class SSShadowWorkerHealth:
    thread_alive: bool
    queue_size: int
    queue_capacity: int
    submitted: int
    completed: int
    dropped: int
    candidate_failures: int
    comparison_failures: int
    report_failures: int
    worker_failures: int
    last_failure_class: str | None
    last_candidate_failure_stage: str | None
    last_candidate_failure_class: str | None


def _safe_frame_snapshot(value: object) -> pd.DataFrame | None:
    if not isinstance(value, pd.DataFrame):
        return None
    try:
        snapshot = value.copy(deep=True)
        adjustment = os.environ.get("SS_LEGACY_ADJUSTMENT_SEMANTICS", "").lower()
        if adjustment in {"none", "qfq"}:
            snapshot.attrs["adjustment"] = adjustment
        volume_unit = os.environ.get("SS_LEGACY_VOLUME_UNIT", "").lower()
        if volume_unit == "shares":
            snapshot.attrs["volume_unit"] = "shares"
        return snapshot
    except Exception as exc:
        logger.warning(
            "QuantStack shadow legacy snapshot failed error_class=%s",
            type(exc).__name__,
        )
        return None


class SSShadowWorker:
    """One bounded daemon worker; candidate I/O never runs on a shadow request."""

    def __init__(
        self,
        *,
        adapter_factory,
        report_root: str | None,
        queue_size: int,
        max_symbols_per_task: int = 100,
        max_legacy_rows_per_task: int = 100_000,
        shutdown_timeout_seconds: float,
    ) -> None:
        if queue_size < 1:
            raise ValueError("shadow queue must contain at least one task")
        if max_symbols_per_task < 1 or max_legacy_rows_per_task < 1:
            raise ValueError("shadow task limits must be positive")
        self._adapter_factory = adapter_factory
        self._adapter = None
        self._report_root = report_root
        self._queue: queue.Queue[_ShadowTask] = queue.Queue(maxsize=queue_size)
        self._stop = threading.Event()
        self._max_symbols_per_task = max_symbols_per_task
        self._max_legacy_rows_per_task = max_legacy_rows_per_task
        self._shutdown_timeout_seconds = max(0.01, shutdown_timeout_seconds)
        self._health_lock = threading.Lock()
        self._submitted = 0
        self._completed = 0
        self._dropped = 0
        self._candidate_failures = 0
        self._comparison_failures = 0
        self._report_failures = 0
        self._worker_failures = 0
        self._last_failure_class: str | None = None
        self._last_candidate_failure_stage: str | None = None
        self._last_candidate_failure_class: str | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="quantstack-shadow-worker-0",
            daemon=True,
        )
        self._thread.start()

    def _increment(self, field_name: str, amount: int = 1) -> None:
        with self._health_lock:
            setattr(self, field_name, getattr(self, field_name) + amount)

    def _record_failure(self, field_name: str, exc: Exception) -> None:
        with self._health_lock:
            setattr(self, field_name, getattr(self, field_name) + 1)
            self._last_failure_class = type(exc).__name__

    def _record_candidate_failure(self, exc: Exception, stage: str) -> None:
        failure_class = getattr(exc, "source_exception_class", type(exc).__name__)
        with self._health_lock:
            self._candidate_failures += 1
            self._last_failure_class = failure_class
            self._last_candidate_failure_stage = stage
            self._last_candidate_failure_class = failure_class

    def _drop(self, exc: Exception | None = None) -> bool:
        with self._health_lock:
            self._dropped += 1
            if exc is not None:
                self._last_failure_class = type(exc).__name__
        return False

    def health_snapshot(self) -> SSShadowWorkerHealth:
        with self._health_lock:
            return SSShadowWorkerHealth(
                thread_alive=self._thread.is_alive(),
                queue_size=self._queue.qsize(),
                queue_capacity=self._queue.maxsize,
                submitted=self._submitted,
                completed=self._completed,
                dropped=self._dropped,
                candidate_failures=self._candidate_failures,
                comparison_failures=self._comparison_failures,
                report_failures=self._report_failures,
                worker_failures=self._worker_failures,
                last_failure_class=self._last_failure_class,
                last_candidate_failure_stage=self._last_candidate_failure_stage,
                last_candidate_failure_class=self._last_candidate_failure_class,
            )

    def submit(self, task: _ShadowTask) -> bool:
        if self._stop.is_set() or not self._thread.is_alive():
            logger.warning("QuantStack shadow worker unavailable; observation skipped")
            return self._drop()
        legacy_rows = sum(
            len(frame) for frame in task.legacy_results.values() if isinstance(frame, pd.DataFrame)
        )
        if (
            len(task.symbols) > self._max_symbols_per_task
            or legacy_rows > self._max_legacy_rows_per_task
        ):
            logger.warning("QuantStack shadow task limit exceeded; observation skipped")
            return self._drop()
        try:
            self._queue.put_nowait(task)
            self._increment("_submitted")
            return True
        except queue.Full:
            logger.warning("QuantStack shadow queue full; candidate comparison skipped")
            return self._drop()

    def submit_observation(
        self,
        *,
        legacy_results: Mapping[str, object],
        symbols: tuple[str, ...],
        start: date,
        end: date,
        frequency: str,
        period: str,
        created_at: str,
        read_path: str = "unknown",
    ) -> bool:
        try:
            if self._stop.is_set() or not self._thread.is_alive():
                return self._drop()
            if len(symbols) > self._max_symbols_per_task:
                return self._drop()
            legacy_rows = sum(
                len(value)
                for symbol in symbols
                if isinstance((value := legacy_results.get(symbol)), pd.DataFrame)
            )
            if legacy_rows > self._max_legacy_rows_per_task or self._queue.full():
                return self._drop()
            snapshots = {
                symbol: _safe_frame_snapshot(legacy_results.get(symbol)) for symbol in symbols
            }
            return self.submit(
                _ShadowTask(
                    legacy_results=snapshots,
                    symbols=symbols,
                    start=start,
                    end=end,
                    frequency=frequency,
                    period=period,
                    created_at=created_at,
                    read_path=read_path,
                )
            )
        except Exception as exc:
            logger.warning(
                "QuantStack shadow instrumentation failed error_class=%s",
                type(exc).__name__,
            )
            return self._drop(exc)

    def _get_adapter(self):
        if self._adapter is None:
            self._adapter = self._adapter_factory()
        return self._adapter

    def _write_report(self, report: SSShadowComparisonReport) -> bool:
        if self._report_root is None:
            return True
        try:
            write_immutable_shadow_report(self._report_root, report)
            return True
        except Exception as exc:
            self._record_failure("_report_failures", exc)
            logger.warning(
                "QuantStack shadow report write failed error_class=%s",
                type(exc).__name__,
            )
            return False

    def _compare_and_report(
        self,
        task: _ShadowTask,
        symbol: str,
        candidate: SSAdapterResult | None,
        latency_ms: float,
        *,
        error: Exception | None = None,
    ) -> None:
        try:
            report = compare_shadow_frames(
                symbol=symbol,
                legacy=task.legacy_results.get(symbol),
                candidate=candidate,
                latency_ms=latency_ms,
                start=task.start,
                end=task.end,
                frequency=task.frequency,
                period=task.period,
                read_path=task.read_path,
                created_at=task.created_at,
                error=error,
                dataset_id_hint=(
                    os.environ.get("SS_QUANTSTACK_BARS_DATASET_ID") if error else None
                ),
                dataset_version_hint=(
                    os.environ.get("SS_QUANTSTACK_DATASET_VERSION", "latest") if error else None
                ),
            )
        except Exception as exc:
            self._record_failure("_comparison_failures", exc)
            logger.warning(
                "QuantStack shadow comparison failed error_class=%s",
                type(exc).__name__,
            )
            return
        self._write_report(report)

    def _process(self, task: _ShadowTask) -> None:
        started = time.perf_counter()
        try:
            adapter = self._get_adapter()
        except Exception as exc:
            self._record_candidate_failure(exc, "adapter_construction")
            logger.warning(
                "QuantStack shadow candidate failed stage=adapter_construction error_class=%s",
                type(exc).__name__,
            )
            latency_ms = (time.perf_counter() - started) * 1000
            for symbol in task.symbols:
                self._compare_and_report(task, symbol, None, latency_ms, error=exc)
            return
        try:
            candidates = adapter.get_batch_price_history(
                list(task.symbols),
                start=task.start,
                end=task.end,
                frequency=task.frequency,
            )
        except Exception as exc:
            stage = getattr(adapter, "candidate_failure_stage", None) or "unknown"
            self._record_candidate_failure(exc, stage)
            logger.warning(
                "QuantStack shadow candidate failed stage=%s error_class=%s",
                stage,
                type(exc).__name__,
            )
            latency_ms = (time.perf_counter() - started) * 1000
            for symbol in task.symbols:
                self._compare_and_report(task, symbol, None, latency_ms, error=exc)
            return
        latency_ms = (time.perf_counter() - started) * 1000
        for symbol in task.symbols:
            self._compare_and_report(
                task,
                symbol,
                candidates.get(symbol),
                latency_ms,
            )

    def _run(self) -> None:
        while not self._stop.is_set() or self._queue.unfinished_tasks:
            try:
                task = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self._process(task)
            except Exception as exc:
                self._record_failure("_worker_failures", exc)
                logger.warning(
                    "QuantStack shadow worker task failed error_class=%s",
                    type(exc).__name__,
                )
            finally:
                self._increment("_completed")
                self._queue.task_done()

    def wait_for_idle(self, timeout_seconds: float = 2.0) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while self._queue.unfinished_tasks:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.01)
        return True

    def shutdown(self) -> bool:
        self._stop.set()
        self._thread.join(timeout=self._shutdown_timeout_seconds)
        return not self._thread.is_alive()


def _period_start(period: str, *, end: date) -> date:
    fixed_days = {
        "1mo": 31,
        "3mo": 92,
        "6mo": 183,
        "1y": 365,
        "2y": 730,
        "5y": 1825,
        "10y": 3650,
        "max": 3650,
    }
    if period.endswith("d") and period[:-1].isdigit():
        # Stock Screener uses values such as ``230d`` to request at least 230
        # trading observations for moving averages. QuantStack accepts calendar
        # bounds, so include weekends, holidays, and a small market-closure
        # margin rather than treating this as 230 calendar days.
        requested_rows = int(period[:-1])
        days = max(requested_rows, math.ceil(requested_rows * 1.6))
    else:
        days = fixed_days.get(period, 730)
    return end - timedelta(days=days)


def _shadow_window(period: str, max_window_days: int) -> tuple[date, date]:
    end = datetime.now(UTC).date()
    start = max(_period_start(period, end=end), end - timedelta(days=max_window_days))
    return start, end


@dataclass
class _DirectYFinanceCallState:
    price_cache_satisfied: bool = False
    direct_observation_accepted: bool = False


_PRICE_CACHE_PROVIDER_CONTEXT: ContextVar[bool] = ContextVar(
    "quantstack_price_cache_provider_context",
    default=False,
)
_DIRECT_YFINANCE_CALL_STATE: ContextVar[_DirectYFinanceCallState | None] = ContextVar(
    "quantstack_direct_yfinance_call_state",
    default=None,
)


def _has_usable_frame(value: object) -> bool:
    return isinstance(value, pd.DataFrame) and not value.empty


def _call_price_cache_legacy(original_method, instance, *args, **kwargs):
    token = _PRICE_CACHE_PROVIDER_CONTEXT.set(True)
    try:
        return original_method(instance, *args, **kwargs)
    finally:
        _PRICE_CACHE_PROVIDER_CONTEXT.reset(token)


def _mark_direct_call_price_cache_result(value: object) -> None:
    state = _DIRECT_YFINANCE_CALL_STATE.get()
    if state is not None and _has_usable_frame(value):
        state.price_cache_satisfied = True


def install_price_cache_read_modes(
    price_cache_class,
    *,
    adapter_factory=SSQuantStackConsumerAdapter.from_environment,
    config: SSReadModeConfig | None = None,
) -> None:
    """Install a reversible overlay while preserving default legacy behavior."""

    if getattr(price_cache_class, "_quantstack_adapter_installed", False):
        return
    resolved = config or SSReadModeConfig.from_environment()
    single_read_methods = (
        "get_historical_data",
        "get_cached_only",
        "get_cached_only_fresh",
    )
    bulk_read_methods = (
        "get_many",
        "get_many_cached_only",
        "get_many_cached_only_fresh",
    )
    originals = {
        method_name: getattr(price_cache_class, method_name)
        for method_name in (*single_read_methods, *bulk_read_methods)
        if hasattr(price_cache_class, method_name)
    }
    if "get_historical_data" not in originals:
        raise TypeError("PriceCacheService must expose get_historical_data")
    candidate_adapter = None
    candidate_lock = threading.Lock()

    def get_candidate_adapter():
        nonlocal candidate_adapter
        with candidate_lock:
            if candidate_adapter is None:
                candidate_adapter = adapter_factory()
            return candidate_adapter

    worker = None
    if resolved.mode == "shadow" and resolved.shadow_enabled:
        worker = SSShadowWorker(
            adapter_factory=adapter_factory,
            report_root=resolved.report_root,
            queue_size=resolved.shadow_queue_size,
            max_symbols_per_task=resolved.max_symbols_per_task,
            max_legacy_rows_per_task=resolved.max_legacy_rows_per_task,
            shutdown_timeout_seconds=resolved.shutdown_timeout_seconds,
        )

    def _require_direct_mode() -> None:
        if not resolved.direct_mode_allowed():
            raise CandidateModeDisabledError(
                "direct QuantStack mode is not enabled for this runtime"
            )

    def get_quantstack_range(self, symbol, start, end):
        _require_direct_mode()
        candidate = (
            get_candidate_adapter()
            .get_price_history(symbol, start=start, end=end, frequency="1d")
            .frame
        )
        _mark_direct_call_price_cache_result(candidate)
        return candidate

    def _wrap_single(method_name, original_method):
        def wrapped_single(self, symbol, period="2y", *args, **kwargs):
            if resolved.mode == "legacy":
                return _call_price_cache_legacy(
                    original_method,
                    self,
                    symbol,
                    period,
                    *args,
                    **kwargs,
                )
            if resolved.mode in {"quantstack_candidate", "quantstack_primary"}:
                _require_direct_mode()
                start, end = _shadow_window(period, resolved.max_window_days)
                candidate = (
                    get_candidate_adapter()
                    .get_price_history(symbol, start=start, end=end, frequency="1d")
                    .frame
                )
                _mark_direct_call_price_cache_result(candidate)
                return candidate
            legacy = _call_price_cache_legacy(
                original_method,
                self,
                symbol,
                period,
                *args,
                **kwargs,
            )
            if method_name == "get_historical_data":
                _mark_direct_call_price_cache_result(legacy)
            if worker is not None and resolved.shadow_selected(symbol):
                start, end = _shadow_window(period, resolved.max_window_days)
                worker.submit_observation(
                    legacy_results={symbol: legacy},
                    symbols=(symbol,),
                    start=start,
                    end=end,
                    frequency="1d",
                    period=period,
                    created_at=_iso_now(),
                    read_path=method_name,
                )
            return legacy

        wrapped_single.__name__ = original_method.__name__
        wrapped_single.__doc__ = original_method.__doc__
        return wrapped_single

    def _wrap_bulk(method_name, original_method):
        def wrapped_bulk(self, symbols, period="2y", *args, **kwargs):
            if resolved.mode == "legacy":
                return _call_price_cache_legacy(
                    original_method,
                    self,
                    symbols,
                    period,
                    *args,
                    **kwargs,
                )
            if resolved.mode in {"quantstack_candidate", "quantstack_primary"}:
                _require_direct_mode()
                start, end = _shadow_window(period, resolved.max_window_days)
                candidates = get_candidate_adapter().get_batch_price_history(
                    list(symbols), start=start, end=end, frequency="1d"
                )
                return {symbol: candidates[symbol].frame for symbol in symbols}
            legacy_results = _call_price_cache_legacy(
                original_method,
                self,
                symbols,
                period,
                *args,
                **kwargs,
            )
            selected = [symbol for symbol in symbols if resolved.shadow_selected(symbol)]
            if selected and worker is not None:
                start, end = _shadow_window(period, resolved.max_window_days)
                safe_results = legacy_results if isinstance(legacy_results, Mapping) else {}
                worker.submit_observation(
                    legacy_results=safe_results,
                    symbols=tuple(selected),
                    start=start,
                    end=end,
                    frequency="1d",
                    period=period,
                    created_at=_iso_now(),
                    read_path=method_name,
                )
            return legacy_results

        wrapped_bulk.__name__ = original_method.__name__
        wrapped_bulk.__doc__ = original_method.__doc__
        return wrapped_bulk

    for method_name in single_read_methods:
        original_method = originals.get(method_name)
        if original_method is None:
            continue
        setattr(price_cache_class, f"_quantstack_legacy_{method_name}", original_method)
        setattr(price_cache_class, method_name, _wrap_single(method_name, original_method))

    for method_name in bulk_read_methods:
        original_method = originals.get(method_name)
        if original_method is None:
            continue
        setattr(price_cache_class, f"_quantstack_legacy_{method_name}", original_method)
        setattr(price_cache_class, method_name, _wrap_bulk(method_name, original_method))

    price_cache_class.get_quantstack_range = get_quantstack_range

    price_cache_class._quantstack_wrapped_read_methods = tuple(originals)
    price_cache_class._quantstack_shadow_worker = worker
    price_cache_class._quantstack_read_mode_config = resolved
    price_cache_class._quantstack_adapter_factory = adapter_factory
    price_cache_class._quantstack_adapter_installed = True


def install_yfinance_read_modes(
    yfinance_class,
    price_cache_class,
    *,
    config: SSReadModeConfig | None = None,
) -> None:
    """Observe direct CN daily reads without duplicating PriceCache observations."""

    if getattr(yfinance_class, "_quantstack_adapter_installed", False):
        return
    if not getattr(price_cache_class, "_quantstack_adapter_installed", False):
        raise TypeError("PriceCacheService QuantStack overlay must be installed first")
    if not hasattr(yfinance_class, "get_historical_data"):
        raise TypeError("YFinanceService must expose get_historical_data")

    resolved = config or getattr(price_cache_class, "_quantstack_read_mode_config", None)
    if resolved is None:
        raise TypeError("PriceCacheService QuantStack read-mode config is unavailable")
    price_cache_config = getattr(price_cache_class, "_quantstack_read_mode_config", None)
    if price_cache_config is not resolved and price_cache_config != resolved:
        raise ValueError("PriceCacheService and YFinanceService must share one read-mode config")
    worker = getattr(price_cache_class, "_quantstack_shadow_worker", None)
    adapter_factory = getattr(
        price_cache_class,
        "_quantstack_adapter_factory",
        SSQuantStackConsumerAdapter.from_environment,
    )
    candidate_adapter = None
    candidate_lock = threading.Lock()
    original_method = yfinance_class.get_historical_data

    def get_candidate_adapter():
        nonlocal candidate_adapter
        with candidate_lock:
            if candidate_adapter is None:
                candidate_adapter = adapter_factory()
            return candidate_adapter

    def direct_candidate(symbol: str, period: str, interval: str):
        if interval != "1d":
            return None
        normalized = symbol.strip().upper()
        if not normalized:
            return None
        # CN symbols retain the explicit exchange-suffix contract. Plain US
        # symbols are resolved against the published instruments_us snapshot
        # by the adapter, so they must not fall through to live yfinance.
        if "." in normalized:
            try:
                ss_symbol_to_quantstack_instrument(normalized)
            except MappingError:
                return None
        if not resolved.direct_mode_allowed():
            raise CandidateModeDisabledError(
                "direct QuantStack mode is not enabled for this runtime"
            )
        start, end = _shadow_window(period, resolved.max_window_days)
        return (
            get_candidate_adapter()
            .get_price_history(normalized, start=start, end=end, frequency="1d")
            .frame
        )

    def wrapped_historical_data(self, symbol, period="1y", *args, **kwargs):
        if resolved.mode == "legacy" or _PRICE_CACHE_PROVIDER_CONTEXT.get():
            return original_method(self, symbol, period, *args, **kwargs)

        interval = kwargs.get("interval", args[0] if args else "1d")
        if resolved.mode in {"quantstack_candidate", "quantstack_primary"}:
            candidate = direct_candidate(symbol, period, interval)
            if candidate is not None:
                return candidate
            return original_method(self, symbol, period, *args, **kwargs)

        existing_state = _DIRECT_YFINANCE_CALL_STATE.get()
        owns_state = existing_state is None
        state = existing_state or _DirectYFinanceCallState()
        token = _DIRECT_YFINANCE_CALL_STATE.set(state) if owns_state else None
        try:
            legacy = original_method(self, symbol, period, *args, **kwargs)
            if (
                not state.price_cache_satisfied
                and not state.direct_observation_accepted
                and interval == "1d"
                and worker is not None
                and resolved.shadow_selected(symbol)
            ):
                start, end = _shadow_window(period, resolved.max_window_days)
                state.direct_observation_accepted = worker.submit_observation(
                    legacy_results={symbol: legacy},
                    symbols=(symbol,),
                    start=start,
                    end=end,
                    frequency="1d",
                    period=period,
                    created_at=_iso_now(),
                    read_path="yfinance_direct",
                )
            return legacy
        finally:
            if token is not None:
                _DIRECT_YFINANCE_CALL_STATE.reset(token)

    wrapped_historical_data.__name__ = original_method.__name__
    wrapped_historical_data.__doc__ = original_method.__doc__
    yfinance_class._quantstack_legacy_get_historical_data = original_method
    yfinance_class.get_historical_data = wrapped_historical_data
    yfinance_class._quantstack_shadow_worker = worker
    yfinance_class._quantstack_read_mode_config = resolved
    yfinance_class._quantstack_adapter_installed = True
