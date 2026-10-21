from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import math
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import IO, Protocol

from .pacing import (
    require_live_yfinance_interval,
    resolve_yfinance_interval,
    yfinance_runtime_root,
)


class YFinanceTransport(Protocol):
    """Provider-native transport boundary, replaceable by an offline replay fixture."""

    def fetch_daily_bars(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]: ...

    def audit_snapshot(self) -> Mapping[str, object]: ...

    def fetch_daily_bars_with_actions(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]: ...


_SOURCE_FINGERPRINT = hashlib.sha256(b"yfinance-download-v1\nfinance.yahoo.com\n").hexdigest()


class YFinanceEmptyResponseError(ConnectionError):
    """Yahoo returned no usable rows for a bounded request."""


class YFinanceRateLimitError(ConnectionError):
    """Yahoo rejected the anonymous request with a rate-limit response."""


class YFinanceCircuitOpenError(ConnectionError):
    """A prior Yahoo rate limit is still inside the local cooldown window."""


class _RateLimitSignal(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.detected = False

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if "YFRateLimitError" in message or "Too Many Requests" in message:
            self.detected = True


@dataclass(frozen=True)
class YFinanceTransportAudit:
    schema_version: str
    endpoint_host: str
    protocol_identity: str
    completed_requests: int
    successful_source_fingerprint: str | None
    request_attempted_source_fingerprints: tuple[tuple[str, ...], ...]
    request_successful_source_fingerprints: tuple[str | None, ...]
    request_failure_codes: tuple[tuple[int | None, ...], ...]
    request_failure_stages: tuple[tuple[str, ...], ...]
    request_outcomes: tuple[str, ...]
    all_requests_succeeded: bool

    def sanitized(self) -> dict[str, object]:
        return asdict(self)


class _AuditedYFinanceTransport:
    def __init__(self) -> None:
        self._request_successes: list[str | None] = []
        self._request_failure_stages: list[tuple[str, ...]] = []
        self._request_outcomes: list[str] = []

    def _reset_audit(self) -> None:
        self._request_successes = []
        self._request_failure_stages = []
        self._request_outcomes = []

    def _record_success(self) -> None:
        self._request_successes.append(_SOURCE_FINGERPRINT)
        self._request_failure_stages.append(())
        self._request_outcomes.append("returned")

    def _record_no_bar(self) -> None:
        self._request_successes.append(_SOURCE_FINGERPRINT)
        self._request_failure_stages.append(())
        self._request_outcomes.append("no_bar")

    def _record_failure(self) -> None:
        self._request_successes.append(None)
        self._request_failure_stages.append(("provider_request",))
        self._request_outcomes.append("provider_error")

    def audit_snapshot(self) -> Mapping[str, object]:
        completed = len(self._request_successes)
        return YFinanceTransportAudit(
            schema_version="1.0.0",
            endpoint_host="finance.yahoo.com",
            protocol_identity="https/yfinance-download",
            completed_requests=completed,
            successful_source_fingerprint=(
                _SOURCE_FINGERPRINT if any(self._request_successes) else None
            ),
            request_attempted_source_fingerprints=tuple(
                (_SOURCE_FINGERPRINT,) for _ in range(completed)
            ),
            request_successful_source_fingerprints=tuple(self._request_successes),
            request_failure_codes=tuple(() for _ in range(completed)),
            request_failure_stages=tuple(self._request_failure_stages),
            request_outcomes=tuple(self._request_outcomes),
            all_requests_succeeded=completed > 0
            and all(value != "provider_error" for value in self._request_outcomes),
        ).sanitized()


class LiveYFinanceTransport(_AuditedYFinanceTransport):
    def __init__(
        self,
        *,
        runtime_root: str | Path | None = None,
        proxy_url: str | None = None,
        minimum_interval_seconds: float | None = None,
        rate_limit_cooldown_seconds: float = 24 * 60 * 60,
        include_actions: bool = False,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__()
        if minimum_interval_seconds is not None and minimum_interval_seconds < 0:
            raise ValueError("minimum_interval_seconds must be non-negative")
        if rate_limit_cooldown_seconds < 0:
            raise ValueError("rate_limit_cooldown_seconds must be non-negative")
        self.runtime_root = yfinance_runtime_root(runtime_root, proxy_url=proxy_url)
        self.proxy_url = proxy_url
        self.minimum_interval_seconds = resolve_yfinance_interval(
            self.runtime_root, minimum_interval_seconds
        )
        require_live_yfinance_interval(self.minimum_interval_seconds)
        self.rate_limit_cooldown_seconds = rate_limit_cooldown_seconds
        self.include_actions = include_actions
        self._sleeper = sleeper
        self._clock = clock

    @contextmanager
    def _request_slot(self) -> Iterator[None]:
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.runtime_root / "request.lock"
        pace_path = self.runtime_root / "last_request_at"
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            self._acquire_lock(lock_file)
            request_started = False
            try:
                self._require_closed_circuit()
                if pace_path.exists():
                    last_request_at = float(pace_path.read_text(encoding="utf-8").strip())
                    remaining = self.minimum_interval_seconds - (self._clock() - last_request_at)
                    if remaining > 0:
                        self._sleeper(remaining)
                request_started = True
                yield
            finally:
                if request_started:
                    pace_path.write_text(f"{self._clock():.6f}\n", encoding="utf-8")
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _acquire_lock(lock_file: IO[str]) -> None:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

    @property
    def _circuit_path(self) -> Path:
        return self.runtime_root / "rate_limit_blocked_until"

    def _require_closed_circuit(self) -> None:
        if not self._circuit_path.exists():
            return
        blocked_until = float(self._circuit_path.read_text(encoding="utf-8").strip())
        if self._clock() < blocked_until:
            raise YFinanceCircuitOpenError(
                "Yahoo request blocked by the persisted rate-limit cooldown"
            )
        self._circuit_path.unlink()

    def _open_rate_limit_circuit(self) -> None:
        blocked_until = self._clock() + self.rate_limit_cooldown_seconds
        self._circuit_path.write_text(f"{blocked_until:.6f}\n", encoding="utf-8")

    def fetch_daily_bars(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        return self._fetch_daily(symbols, start, end, include_actions=self.include_actions)

    def fetch_daily_bars_with_actions(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        return self._fetch_daily(symbols, start, end, include_actions=True)

    def fetch_daily_bars_batch(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        *,
        include_actions: bool,
        threads: bool | int,
    ) -> list[Mapping[str, object]]:
        """Fetch one bounded multi-ticker batch without application retries."""

        requested = [symbol.upper() for symbol in symbols]
        if not requested:
            return []
        if threads is not False and threads < 1:
            raise ValueError("threads must be positive")
        return self._download_daily(
            requested,
            start,
            end,
            include_actions=include_actions,
            threads=False if threads is False else min(threads, len(requested)),
            allow_partial=True,
        )

    def _fetch_daily(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        *,
        include_actions: bool,
    ) -> list[Mapping[str, object]]:
        requested = [symbol.upper() for symbol in symbols]
        records: list[Mapping[str, object]] = []
        for symbol in requested:
            records.extend(
                self._download_daily(
                    [symbol],
                    start,
                    end,
                    include_actions=include_actions,
                    threads=False,
                    allow_partial=False,
                )
            )
        return records

    def _download_daily(
        self,
        requested: list[str],
        start: date,
        end: date,
        *,
        include_actions: bool,
        threads: bool | int,
        allow_partial: bool,
    ) -> list[Mapping[str, object]]:
        try:
            import pandas as pd
            import yfinance as yf
        except ImportError as exc:
            raise RuntimeError("Install QuantStack with the data extra to use yfinance") from exc
        installed_version = version("yfinance")
        if installed_version != "1.5.2":
            raise RuntimeError(
                f"yfinance runtime version mismatch: expected 1.5.2, got {installed_version}"
            )

        if hasattr(yf, "config"):
            yf.config.network.proxy = self.proxy_url
            yf.config.network.retries = 0
        elif self.proxy_url:
            raise RuntimeError("installed yfinance does not support explicit proxy configuration")
        yf.set_tz_cache_location(str(self.runtime_root / "cache-v1"))
        self._reset_audit()
        records: list[Mapping[str, object]] = []
        rate_limit_signal = _RateLimitSignal()
        yfinance_logger = logging.getLogger("yfinance")
        yfinance_logger.addHandler(rate_limit_signal)
        try:
            with self._request_slot():
                frame = yf.download(
                    tickers=requested,
                    start=start.isoformat(),
                    end=(end + timedelta(days=1)).isoformat(),
                    interval="1d",
                    actions=include_actions,
                    auto_adjust=False,
                    repair=False,
                    keepna=False,
                    progress=False,
                    threads=threads,
                    group_by="ticker",
                    multi_level_index=True,
                    timeout=20,
                )
                if rate_limit_signal.detected:
                    self._open_rate_limit_circuit()
                    raise YFinanceRateLimitError(
                        f"Yahoo rate-limited bounded batch of {len(requested)} symbols"
                    )
        except Exception:
            for _ in requested:
                self._record_failure()
            raise
        finally:
            yfinance_logger.removeHandler(rate_limit_signal)
        if frame is None or frame.empty:
            for _ in requested:
                self._record_failure()
            raise YFinanceEmptyResponseError(
                f"Yahoo returned no daily rows for bounded request {requested[0]}"
            )
        for symbol in requested:
            records_before = len(records)
            if isinstance(frame.columns, pd.MultiIndex):
                if symbol not in frame.columns.get_level_values(0):
                    symbol_frame = frame.iloc[0:0]
                else:
                    symbol_frame = frame[symbol]
            else:
                symbol_frame = frame
            for timestamp, row in symbol_frame.iterrows():
                record: dict[str, object] = {
                    "ticker": symbol,
                    "Date": timestamp.isoformat(),
                }
                for field in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
                    value = row.get(field)
                    if value is None or (isinstance(value, float) and math.isnan(value)):
                        continue
                    record[field] = float(value)
                if include_actions:
                    for field in ("Dividends", "Stock Splits"):
                        value = row.get(field, 0.0)
                        record[field] = (
                            0.0
                            if value is None or (isinstance(value, float) and math.isnan(value))
                            else float(value)
                        )
                if all(field in record for field in ("Open", "High", "Low", "Close", "Volume")):
                    records.append(record)
            if len(records) == records_before:
                if not allow_partial:
                    self._record_failure()
                    raise YFinanceEmptyResponseError(
                        f"Yahoo returned no usable daily rows for bounded request {symbol}"
                    )
                self._record_no_bar()
                continue
            self._record_success()
        return records


class BufferedYFinanceTransport(_AuditedYFinanceTransport):
    """Expose provider-fetched rows to one-symbol task pipelines without network I/O."""

    def __init__(self, records: Sequence[Mapping[str, object]]) -> None:
        super().__init__()
        self.records = [dict(item) for item in records]

    def _fetch(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        requested = {symbol.upper() for symbol in symbols}
        self._reset_audit()
        result = [
            item
            for item in self.records
            if str(item.get("ticker", "")).upper() in requested
            and start <= date.fromisoformat(str(item["Date"])[:10]) <= end
        ]
        if not result:
            self._record_failure()
            raise YFinanceEmptyResponseError(
                f"Yahoo batch returned no usable daily rows for {sorted(requested)[0]}"
            )
        self._record_success()
        return result

    def fetch_daily_bars(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        return self._fetch(symbols, start, end)

    def fetch_daily_bars_with_actions(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        return self._fetch(symbols, start, end)


class ReplayYFinanceTransport(_AuditedYFinanceTransport):
    def __init__(self, fixture_path: str | Path, *, include_actions: bool = False) -> None:
        super().__init__()
        self.fixture_path = Path(fixture_path)
        payload = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        if payload.get("provider") != "yfinance":
            raise ValueError("Replay fixture is not a yfinance response")
        self.records = payload.get("records", [])
        self.include_actions = include_actions

    def fetch_daily_bars(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        requested = [symbol.upper() for symbol in symbols]
        self._reset_audit()
        result: list[Mapping[str, object]] = []
        for symbol in requested:
            for record in self.records:
                trading_date = date.fromisoformat(str(record["Date"])[:10])
                if str(record["ticker"]).upper() == symbol and start <= trading_date <= end:
                    item = dict(record)
                    if self.include_actions:
                        item["Dividends"] = float(item.get("Dividends", 0.0))
                        item["Stock Splits"] = float(item.get("Stock Splits", 0.0))
                    result.append(item)
            self._record_success()
        return result

    def fetch_daily_bars_with_actions(
        self, symbols: Sequence[str], start: date, end: date
    ) -> list[Mapping[str, object]]:
        return [
            {
                **record,
                "Dividends": float(record.get("Dividends", 0.0)),
                "Stock Splits": float(record.get("Stock Splits", 0.0)),
            }
            for record in self.fetch_daily_bars(symbols, start, end)
        ]
