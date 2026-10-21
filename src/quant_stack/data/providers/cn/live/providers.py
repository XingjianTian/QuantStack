from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from importlib import import_module
from importlib.metadata import version
from ipaddress import IPv4Address, ip_address
from threading import RLock
from typing import Any

from ....ingestion.contracts import ProviderRuntimeContract
from ....interfaces import MarketCalendarProvider
from ....models import Instrument, Market
from ....quality import ProviderNativeSchema, ProviderSchemaValidator
from .akshare_transport import (
    AKShareEastmoneyTransport,
    install_akshare_history_transport,
)
from .baostock_control import (
    BAOSTOCK_BLACKLIST_ERROR_CODE,
    BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS,
    BAOSTOCK_MAX_BATCH_SIZE,
    BaoStockAccessRejected,
    BaoStockRequestBudget,
)

_BAOSTOCK_ADAPTER_VERSION = "real-cn-v1"
_AKSHARE_ADAPTER_VERSION = "real-cn-v2"
_TDX_REFERENCE_ADAPTER_VERSION = "real-cn-tdx-v1"
_BAOSTOCK_NATIVE_SCHEMA_VERSION = "live-observed-2026-08-03-v1"
_AKSHARE_NATIVE_SCHEMA_VERSION = "live-observed-2026-08-03-v1"
_TDX_REFERENCE_NATIVE_SCHEMA_VERSION = "live-observed-2026-08-03-v1"
_TDX_HOST_POOL_CONTRACT_VERSION = "tdx-public-host-pool-v1"
_TDX_PROTOCOL_IDENTITY = "tdx-standard-market-binary/category-9/daily-bars"
_TDX_TRANSPORT_SECURITY = "unauthenticated_unencrypted_tcp"
_TDX_SOURCE_TERMS_STATUS = "upstream-data-terms-unverified-controlled-research-only"
_TDX_REFERENCE_HOSTS = (
    ("120.76.1.198", 7709),
    ("1.202.143.37", 7709),
    ("111.203.134.118", 7709),
    ("117.133.128.226", 7709),
)
_TDX_PAGE_SIZE = 800
_TDX_MAX_PAGES = 12
_BAOSTOCK_SOURCE_FINGERPRINT = hashlib.sha256(
    b"baostock-source-v1\nwww.baostock.com\nquery_history_k_data_plus\n"
).hexdigest()

BAOSTOCK_LIVE_DAILY_SCHEMA = ProviderNativeSchema(
    required_fields={
        "date": str,
        "code": str,
        "open": str,
        "high": str,
        "low": str,
        "close": str,
        "volume": str,
        "amount": str,
        "adjustflag": str,
        "_retrieval_timestamp": str,
    }
)

AKSHARE_LIVE_DAILY_SCHEMA = ProviderNativeSchema(
    required_fields={
        "日期": str,
        "股票代码": str,
        "开盘": (int, float),
        "最高": (int, float),
        "最低": (int, float),
        "收盘": (int, float),
        "成交量": (int, float),
        "成交额": (int, float),
        "复权": str,
        "_retrieval_timestamp": str,
    }
)

TDX_REFERENCE_LIVE_DAILY_SCHEMA = ProviderNativeSchema(
    required_fields={
        "datetime": str,
        "code": str,
        "market": int,
        "open": (int, float),
        "high": (int, float),
        "low": (int, float),
        "close": (int, float),
        "vol": (int, float),
        "amount": (int, float),
        "adjustment": str,
        "_retrieval_timestamp": str,
    }
)

BAOSTOCK_LIVE_RUNTIME_CONTRACT = ProviderRuntimeContract(
    provider="baostock",
    endpoint="query_history_k_data_plus",
    provider_version="0.9.3",
    adapter_version=_BAOSTOCK_ADAPTER_VERSION,
    native_schema_version=_BAOSTOCK_NATIVE_SCHEMA_VERSION,
    supported_markets=("CN",),
    supported_frequencies=("1d",),
)

AKSHARE_LIVE_RUNTIME_CONTRACT = ProviderRuntimeContract(
    provider="akshare",
    endpoint="stock_zh_a_hist",
    provider_version="1.18.81",
    adapter_version=_AKSHARE_ADAPTER_VERSION,
    native_schema_version=_AKSHARE_NATIVE_SCHEMA_VERSION,
    supported_markets=("CN",),
    supported_frequencies=("1d",),
)

TDX_REFERENCE_LIVE_RUNTIME_CONTRACT = ProviderRuntimeContract(
    provider="tdx_reference",
    endpoint="standard_market_daily_bars",
    provider_version="0.2.7",
    adapter_version=_TDX_REFERENCE_ADAPTER_VERSION,
    native_schema_version=_TDX_REFERENCE_NATIVE_SCHEMA_VERSION,
    supported_markets=("CN",),
    supported_frequencies=("1d",),
)


@dataclass(frozen=True)
class TDXReferenceTransportAudit:
    schema_version: str
    host_pool_contract_id: str
    host_pool_contract_version: str
    host_pool_fingerprint: str
    configured_candidates: int
    max_host_attempts: int
    attempted_source_fingerprints: tuple[str, ...]
    successful_source_fingerprint: str | None
    failure_classes: tuple[str, ...]
    close_failure_classes: tuple[str, ...]
    protocol_identity: str
    client_package: str
    client_version: str
    transport_security: str
    server_authenticated: bool
    encrypted: bool
    source_terms_status: str

    def sanitized(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BaoStockTransportAudit:
    """Sanitized per-request evidence without endpoint addresses or query payloads."""

    schema_version: str
    endpoint_host: str
    protocol_identity: str
    tls_verification: bool
    server_authenticated: bool
    encrypted: bool
    successful_source_fingerprint: str | None
    attempted_source_fingerprints: tuple[str, ...]
    failure_classes: tuple[str, ...]
    completed_requests: int
    request_attempted_source_fingerprints: tuple[tuple[str, ...], ...]
    request_successful_source_fingerprints: tuple[str | None, ...]
    request_failure_classes: tuple[tuple[str, ...], ...]
    all_requests_succeeded: bool

    def sanitized(self) -> dict[str, object]:
        return asdict(self)


def _tdx_source_fingerprint(host: str, port: int) -> str:
    return hashlib.sha256(f"tdx-source-v1\n{host}\n{port}\n".encode()).hexdigest()


def _tdx_host_pool_fingerprint(hosts: Sequence[tuple[str, int]]) -> str:
    payload = "\n".join(f"{host}:{port}" for host, port in hosts)
    return hashlib.sha256(f"{_TDX_HOST_POOL_CONTRACT_VERSION}\n{payload}\n".encode()).hexdigest()


def _validate_tdx_hosts(hosts: Sequence[tuple[str, int]]) -> tuple[tuple[str, int], ...]:
    validated: list[tuple[str, int]] = []
    for host, port in hosts:
        try:
            address = ip_address(host)
        except ValueError as exc:
            raise ValueError("TDX hosts must be literal public IPv4 addresses") from exc
        if not isinstance(address, IPv4Address) or any(
            (
                not address.is_global,
                address.is_private,
                address.is_loopback,
                address.is_link_local,
                address.is_multicast,
                address.is_reserved,
                address.is_unspecified,
            )
        ):
            raise ValueError("TDX hosts must reject private or non-public addresses")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("TDX host ports must be integers from 1 to 65535")
        validated.append((str(address), port))
    if len(validated) != len(set(validated)):
        raise ValueError("TDX host pool must not contain duplicate endpoints")
    return tuple(validated)


def _tdx_lots_to_canonical_shares(value: object) -> int:
    try:
        lots = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("TDX volume must be a finite numeric lot quantity") from exc
    shares = lots * Decimal(100)
    if not shares.is_finite() or shares < 0:
        raise ValueError("TDX volume must be finite and non-negative")
    integral = shares.to_integral_value()
    if shares != integral:
        raise ValueError("TDX lot quantity resolves to a fractional canonical share")
    return int(integral)


def _require_runtime_version(distribution: str, expected: str) -> None:
    installed = version(distribution)
    if installed != expected:
        raise RuntimeError(f"{distribution} version mismatch: expected {expected}, got {installed}")


class _TimeoutSocketModule:
    """Delegate a socket module while applying a timeout to created sockets.

    BaoStock's client creates sockets through ``baostock.util.socketutil``.
    Replacing that module reference for the duration of one provider call keeps
    the timeout local to BaoStock and avoids changing the process-global
    ``socket.setdefaulttimeout`` setting.
    """

    def __init__(self, module: object, timeout: float) -> None:
        self._module = module
        self._timeout = timeout

    def socket(self, *args: object, **kwargs: object) -> object:
        created = self._module.socket(*args, **kwargs)  # type: ignore[attr-defined]
        created.settimeout(self._timeout)  # type: ignore[attr-defined]
        return created

    def __getattr__(self, name: str) -> object:
        return getattr(self._module, name)


_BAOSTOCK_SOCKET_PATCH_LOCK = RLock()


@contextmanager
def _scoped_baostock_socket_timeout(client: object, timeout: float):
    """Scope BaoStock socket creation to a per-call timeout.

    The BaoStock package exposes the stdlib socket module as an attribute of
    ``baostock.util.socketutil``.  A narrow, locked substitution is required
    because the package does not expose a socket-timeout argument itself.  The
    original module is restored even when login/query/logout raises.
    """

    util = getattr(client, "util", None)
    socketutil = getattr(util, "socketutil", None)
    if socketutil is None:
        yield
        return
    with _BAOSTOCK_SOCKET_PATCH_LOCK:
        original = socketutil.socket
        socketutil.socket = _TimeoutSocketModule(original, timeout)
        try:
            yield
        finally:
            socketutil.socket = original


class _ConfiguredInstrumentProvider:
    market: Market = "CN"

    def __init__(self, instruments: Sequence[Instrument]) -> None:
        if not instruments:
            raise ValueError("controlled live providers require an explicit instrument sample")
        self._instruments = tuple(instruments)
        self._aliases: dict[str, Instrument] = {}
        for instrument in self._instruments:
            if instrument.market != "CN" or instrument.provider_symbol is None:
                raise ValueError("live sample instruments require CN provider symbols")
            for alias in (
                instrument.instrument_id,
                instrument.symbol,
                instrument.provider_symbol,
            ):
                if alias in self._aliases and self._aliases[alias] != instrument:
                    raise ValueError(f"ambiguous configured instrument alias: {alias}")
                self._aliases[alias] = instrument

    def list_instruments(
        self, market: Market, symbols: Sequence[str] | None = None
    ) -> Iterable[Instrument]:
        if market != "CN":
            raise ValueError(f"{self.name} supports only CN")
        selected = symbols or tuple(item.instrument_id for item in self._instruments)
        emitted: set[str] = set()
        for symbol in selected:
            instrument = self._aliases.get(symbol)
            if instrument is None:
                raise ValueError(f"symbol is outside the controlled live sample: {symbol}")
            if instrument.instrument_id not in emitted:
                emitted.add(instrument.instrument_id)
                yield instrument


class BaoStockLiveProvider(_ConfiguredInstrumentProvider):
    name = "baostock"
    source_url = "https://www.baostock.com/baostock/index.php/Python_API文档"
    license_scope = "upstream-terms-unverified-controlled-research-only"
    available_at_policy = "retrieval_time_after_provider_publication"
    endpoint = "query_history_k_data_plus"
    runtime_contract = BAOSTOCK_LIVE_RUNTIME_CONTRACT
    bar_schema = BAOSTOCK_LIVE_DAILY_SCHEMA
    requires_verified_calendar = True
    socket_timeout_seconds = 30.0

    def __init__(
        self,
        instruments: Sequence[Instrument],
        *,
        request_budget: BaoStockRequestBudget | None = None,
        allow_expired_session_reauth_replay: bool = True,
    ) -> None:
        super().__init__(instruments)
        # A default ledger keeps direct callers safe.  Campaigns should pass a
        # data-root-local budget so all workers share the same persistent quota.
        self._request_budget = request_budget or BaoStockRequestBudget.from_default()
        self._allow_expired_session_reauth_replay = allow_expired_session_reauth_replay
        self._request_successes: list[str | None] = []
        self._request_failures: list[tuple[str, ...]] = []

    def transport_audit(self) -> dict[str, object]:
        attempted = tuple(_BAOSTOCK_SOURCE_FINGERPRINT for _ in self._request_successes)
        failures = tuple(
            failure for request_failures in self._request_failures for failure in request_failures
        )
        successful = next(
            (item for item in reversed(self._request_successes) if item is not None),
            None,
        )
        return BaoStockTransportAudit(
            schema_version="1.0.0",
            endpoint_host="www.baostock.com",
            protocol_identity="baostock-python/query_history_k_data_plus/daily",
            tls_verification=False,
            server_authenticated=False,
            encrypted=False,
            successful_source_fingerprint=successful,
            attempted_source_fingerprints=attempted,
            failure_classes=failures,
            completed_requests=len(self._request_successes),
            request_attempted_source_fingerprints=tuple((item,) for item in attempted),
            request_successful_source_fingerprints=tuple(self._request_successes),
            request_failure_classes=tuple(self._request_failures),
            all_requests_succeeded=bool(self._request_successes)
            and all(item is not None for item in self._request_successes),
        ).sanitized()

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        if frequency != "1d":
            raise ValueError("BaoStock live adapter supports only daily bars")
        _require_runtime_version("baostock", self.runtime_contract.provider_version)
        client = import_module("baostock")
        selected = tuple(self.list_instruments(market, symbols))
        if not selected:
            raise ValueError("BaoStock live adapter requires a non-empty request batch")
        if len(selected) > BAOSTOCK_MAX_BATCH_SIZE:
            raise ValueError("BaoStock live adapter batches must contain at most 500 instruments")

        def login_once() -> None:
            self._request_budget.reserve(1)
            login = client.login()
            login_code = str(getattr(login, "error_code", ""))
            login_message = str(getattr(login, "error_msg", login_code))
            if login_code == "0":
                return
            if login_code == BAOSTOCK_BLACKLIST_ERROR_CODE or "黑名单" in login_message:
                self._request_budget.mark_blocked(
                    error_code=login_code,
                    message=login_message,
                )
                raise BaoStockAccessRejected(login_code, login_message)
            raise ConnectionError(f"BaoStock login failed: {login_code} {login_message}")

        def query_once(instrument: Instrument):
            self._request_budget.reserve(1)
            try:
                return client.query_history_k_data_plus(
                    instrument.provider_symbol,
                    "date,code,open,high,low,close,preclose,volume,amount,adjustflag",
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    frequency="d",
                    adjustflag="3",
                )
            finally:
                # BaoStock is strictly single-flight, but serial calls can still
                # arrive too quickly.  Pace every attempted history request,
                # including API errors and exceptions, so failure paths cannot
                # accidentally turn into a tight retry loop.
                time.sleep(BAOSTOCK_HISTORY_REQUEST_INTERVAL_SECONDS)

        logged_in = False
        final_logout_allowed = True
        with self._request_budget.session_lock():
            self._request_budget.wait_for_batch_slot()
            # Preflight the complete batch before opening a session.  This
            # avoids starting a batch that can only fail half-way through the
            # daily quota and leaves the next wave untouched.
            # The normal production path reserves one bounded re-authentication
            # and query replay for an upstream-expired session (10001001).  A
            # recovery caller can disable that sequence and reserve exactly one
            # initial login plus one query per selected instrument.
            recovery_capacity = 2 if self._allow_expired_session_reauth_replay else 0
            self._request_budget.ensure_capacity(1 + len(selected) + recovery_capacity)
            self._request_budget.begin_batch(len(selected))
            try:
                with _scoped_baostock_socket_timeout(client, self.socket_timeout_seconds):
                    try:
                        login_once()
                        logged_in = True
                        reauthentication_available = (
                            self._allow_expired_session_reauth_replay
                        )
                        for instrument in selected:
                            try:
                                result = query_once(instrument)
                                result_code = str(getattr(result, "error_code", ""))
                                result_message = str(
                                    getattr(result, "error_msg", result_code)
                                )
                                if (
                                    result_code == "10001001"
                                    and not self._allow_expired_session_reauth_replay
                                ):
                                    # R12 recovery is fail-closed: after the first
                                    # expired-session response it makes no logout,
                                    # re-login, or query-replay call.
                                    final_logout_allowed = False
                                    raise ConnectionError(
                                        "BaoStock daily query failed: "
                                        f"{result_code} {result_message}"
                                    )
                                if result_code == "10001001" and reauthentication_available:
                                    client.logout()
                                    logged_in = False
                                    time.sleep(5.0)
                                    login_once()
                                    logged_in = True
                                    reauthentication_available = False
                                    result = query_once(instrument)
                                    result_code = str(getattr(result, "error_code", ""))
                                    result_message = str(
                                        getattr(result, "error_msg", result_code)
                                    )
                                if result_code != "0":
                                    raise ConnectionError(
                                        "BaoStock daily query failed: "
                                        f"{result_code} {result_message}"
                                    )
                                fields = tuple(result.fields)
                                while result.error_code == "0" and result.next():
                                    retrieved = datetime.now(UTC).isoformat()
                                    yield {
                                        **dict(zip(fields, result.get_row_data(), strict=True)),
                                        "_retrieval_timestamp": retrieved,
                                    }
                            except Exception as exc:
                                self._request_successes.append(None)
                                self._request_failures.append((type(exc).__name__,))
                                raise
                            else:
                                self._request_successes.append(_BAOSTOCK_SOURCE_FINGERPRINT)
                                self._request_failures.append(())
                    finally:
                        if logged_in and final_logout_allowed:
                            client.logout()
            finally:
                self._request_budget.complete_batch()


class AKShareLiveProvider(_ConfiguredInstrumentProvider):
    name = "akshare"
    source_url = "https://akshare.akfamily.xyz/data/stock/stock.html"
    license_scope = "upstream-terms-unverified-controlled-research-only"
    available_at_policy = "retrieval_time_after_provider_publication"
    endpoint = "stock_zh_a_hist"
    runtime_contract = AKSHARE_LIVE_RUNTIME_CONTRACT
    bar_schema = AKSHARE_LIVE_DAILY_SCHEMA
    requires_verified_calendar = True

    def __init__(
        self,
        instruments: Sequence[Instrument],
        *,
        request_transport: AKShareEastmoneyTransport | None = None,
    ) -> None:
        super().__init__(instruments)
        self._request_transport = request_transport or AKShareEastmoneyTransport()

    def transport_audit(self) -> dict[str, object]:
        return self._request_transport.audit_snapshot().sanitized()

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        if frequency != "1d":
            raise ValueError("AKShare live adapter supports only daily bars")
        _require_runtime_version("akshare", self.runtime_contract.provider_version)
        client = import_module("akshare")
        stock_hist_module = import_module("akshare.stock_feature.stock_hist_em")
        for instrument in self.list_instruments(market, symbols):
            with install_akshare_history_transport(stock_hist_module, self._request_transport):
                frame = client.stock_zh_a_hist(
                    symbol=instrument.provider_symbol,
                    period="daily",
                    start_date=start.strftime("%Y%m%d"),
                    end_date=end.strftime("%Y%m%d"),
                    adjust="",
                    timeout=15,
                )
            retrieved = datetime.now(UTC).isoformat()
            for native in frame.to_dict(orient="records"):
                yield {
                    "日期": str(native["日期"])[:10],
                    "股票代码": instrument.symbol,
                    "开盘": float(native["开盘"]),
                    "最高": float(native["最高"]),
                    "最低": float(native["最低"]),
                    "收盘": float(native["收盘"]),
                    "成交量": float(native["成交量"]),
                    "成交额": float(native["成交额"]),
                    "复权": "none",
                    "_retrieval_timestamp": retrieved,
                }


class TDXReferenceLiveProvider(_ConfiguredInstrumentProvider):
    """Bounded XSHG/XSHE comparison route over the TDX standard-market protocol."""

    name = "tdx_reference"
    source_url = "tdx://standard-market/category-9/daily-bars"
    license_scope = "MIT-client-upstream-data-terms-unverified-controlled-research-only"
    available_at_policy = "retrieval_time_after_provider_publication"
    endpoint = "standard_market_daily_bars"
    runtime_contract = TDX_REFERENCE_LIVE_RUNTIME_CONTRACT
    bar_schema = TDX_REFERENCE_LIVE_DAILY_SCHEMA
    requires_verified_calendar = True

    def __init__(
        self,
        instruments: Sequence[Instrument],
        *,
        verified_sessions: Sequence[date],
        hosts: Sequence[tuple[str, int]] = _TDX_REFERENCE_HOSTS,
        max_host_attempts: int = 4,
        timeout_seconds: int = 5,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        super().__init__(instruments)
        if any(item.venue not in {"XSHG", "XSHE"} for item in instruments):
            raise ValueError("TDX reference supports only XSHG and XSHE")
        if not verified_sessions:
            raise ValueError("TDX reference requires explicit verified trading sessions")
        if len(verified_sessions) != len(set(verified_sessions)):
            raise ValueError("TDX verified sessions must be unique")
        if not hosts:
            raise ValueError("TDX reference requires an explicit bounded host pool")
        if not 1 <= max_host_attempts <= min(4, len(hosts)):
            raise ValueError("TDX reference host attempts must be within the bounded host pool")
        if not 1 <= timeout_seconds <= 10:
            raise ValueError("TDX reference timeout must be between 1 and 10 seconds")
        self._verified_sessions = tuple(sorted(verified_sessions))
        self._hosts = _validate_tdx_hosts(hosts)
        self._host_pool_fingerprint = _tdx_host_pool_fingerprint(self._hosts)
        self._host_pool_contract_id = (
            f"{_TDX_HOST_POOL_CONTRACT_VERSION}:{self._host_pool_fingerprint[:16]}"
        )
        self._max_host_attempts = max_host_attempts
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory
        self._attempted_source_fingerprints: list[str] = []
        self._successful_source_fingerprint: str | None = None
        self._failure_classes: list[str] = []
        self._close_failure_classes: list[str] = []

    def transport_audit(self) -> dict[str, object]:
        return TDXReferenceTransportAudit(
            schema_version="1.0.0",
            host_pool_contract_id=self._host_pool_contract_id,
            host_pool_contract_version=_TDX_HOST_POOL_CONTRACT_VERSION,
            host_pool_fingerprint=self._host_pool_fingerprint,
            configured_candidates=len(self._hosts),
            max_host_attempts=self._max_host_attempts,
            attempted_source_fingerprints=tuple(self._attempted_source_fingerprints),
            successful_source_fingerprint=self._successful_source_fingerprint,
            failure_classes=tuple(self._failure_classes),
            close_failure_classes=tuple(self._close_failure_classes),
            protocol_identity=_TDX_PROTOCOL_IDENTITY,
            client_package="tdxpy",
            client_version=self.runtime_contract.provider_version,
            transport_security=_TDX_TRANSPORT_SECURITY,
            server_authenticated=False,
            encrypted=False,
            source_terms_status=_TDX_SOURCE_TERMS_STATUS,
        ).sanitized()

    def _new_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        module = import_module("tdxpy.hq")
        return module.TdxHq_API(
            heartbeat=False,
            auto_retry=False,
            raise_exception=True,
        )

    @staticmethod
    def _market_id(instrument: Instrument) -> int:
        if instrument.venue == "XSHG":
            return 1
        if instrument.venue == "XSHE":
            return 0
        raise ValueError("TDX reference supports only XSHG and XSHE")

    def _fetch_instrument(
        self,
        client: Any,
        instrument: Instrument,
        start: date,
        end: date,
    ) -> list[Mapping[str, object]]:
        records: list[Mapping[str, object]] = []
        reached_start = False
        for page_number in range(_TDX_MAX_PAGES):
            native_rows = client.get_security_bars(
                9,
                self._market_id(instrument),
                instrument.provider_symbol,
                page_number * _TDX_PAGE_SIZE,
                _TDX_PAGE_SIZE,
            )
            if native_rows is None:
                raise RuntimeError("TDX daily bar request returned no protocol response")
            if not native_rows:
                reached_start = True
                break
            retrieved = datetime.now(UTC).isoformat()
            for native in native_rows:
                observed = date.fromisoformat(str(native["datetime"])[:10])
                if observed < start:
                    reached_start = True
                if start <= observed <= end:
                    records.append(
                        {
                            **dict(native),
                            "code": instrument.provider_symbol,
                            "market": self._market_id(instrument),
                            "adjustment": "none",
                            "_retrieval_timestamp": retrieved,
                        }
                    )
            if len(native_rows) < _TDX_PAGE_SIZE:
                reached_start = True
            if reached_start:
                break
        if not reached_start:
            raise RuntimeError("TDX bounded pagination did not reach the requested start date")
        return records

    def _validate_candidate_records(
        self,
        records: Sequence[Mapping[str, object]],
        selected: Sequence[Instrument],
        start: date,
        end: date,
    ) -> None:
        expected_sessions = tuple(
            session for session in self._verified_sessions if start <= session <= end
        )
        if not expected_sessions:
            raise ValueError("requested TDX window has no verified trading sessions")
        ProviderSchemaValidator().validate(
            provider=self.name,
            records=records,
            schema=self.bar_schema,
        )
        expected_symbols = {str(item.provider_symbol) for item in selected}
        expected_markets = {str(item.provider_symbol): self._market_id(item) for item in selected}
        expected_keys = {
            (symbol, session.isoformat())
            for symbol in expected_symbols
            for session in expected_sessions
        }
        observed_keys: set[tuple[str, str]] = set()
        for record in records:
            symbol = str(record["code"])
            observed_date = str(record["datetime"])[:10]
            key = (symbol, observed_date)
            if symbol not in expected_symbols:
                raise ValueError("TDX candidate returned an unexpected instrument")
            if int(record["market"]) != expected_markets[symbol]:
                raise ValueError("TDX candidate market identity drift")
            if key in observed_keys:
                raise ValueError("TDX candidate returned duplicate instrument/date keys")
            observed_keys.add(key)
            _tdx_lots_to_canonical_shares(record["vol"])
            try:
                amount = Decimal(str(record["amount"]))
            except InvalidOperation as exc:
                raise ValueError("TDX amount must be numeric") from exc
            if not amount.is_finite() or amount < 0:
                raise ValueError("TDX amount must be finite and non-negative")
        if observed_keys != expected_keys:
            raise ValueError("TDX candidate did not cover every requested instrument/session")

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        if frequency != "1d":
            raise ValueError("TDX reference adapter supports only daily bars")
        if start > end:
            raise ValueError("TDX reference start date must not exceed end date")
        _require_runtime_version("tdxpy", self.runtime_contract.provider_version)
        selected = tuple(self.list_instruments(market, symbols))
        self._attempted_source_fingerprints = []
        self._successful_source_fingerprint = None
        self._failure_classes = []
        self._close_failure_classes = []
        last_error: Exception | None = None
        for host, port in self._hosts[: self._max_host_attempts]:
            client = self._new_client()
            source_fingerprint = _tdx_source_fingerprint(host, port)
            self._attempted_source_fingerprints.append(source_fingerprint)
            records: list[Mapping[str, object]] | None = None
            request_error: Exception | None = None
            try:
                connected = client.connect(host, int(port), time_out=self._timeout_seconds)
                if connected is False:
                    raise ConnectionError("TDX client declined the candidate connection")
                records = [
                    record
                    for instrument in selected
                    for record in self._fetch_instrument(client, instrument, start, end)
                ]
                if not records:
                    raise ValueError("TDX candidate returned an empty controlled response")
                self._validate_candidate_records(records, selected, start, end)
            except Exception as exc:  # noqa: BLE001 - rotate through the bounded pool
                request_error = exc
            finally:
                try:
                    client.close()
                except Exception as exc:  # noqa: BLE001 - audit close without masking request
                    self._close_failure_classes.append(type(exc).__name__)
                    if request_error is None:
                        request_error = exc
            if request_error is not None:
                last_error = request_error
                self._failure_classes.append(type(request_error).__name__)
                continue
            assert records is not None
            self._successful_source_fingerprint = source_fingerprint
            yield from sorted(records, key=lambda row: (str(row["code"]), row["datetime"]))
            return
        raise ConnectionError("TDX reference failed across the bounded host pool") from last_error


def _instrument_for_record(instruments: Mapping[str, Instrument], symbol: str) -> Instrument | None:
    return instruments.get(symbol) or next(
        (item for item in instruments.values() if item.symbol == symbol), None
    )


def _lineage(
    *,
    instrument: Instrument,
    provider_symbol: str,
    runtime_contract: ProviderRuntimeContract,
    original_volume_unit: str,
    volume_multiplier: float,
    trading_date: date,
    retrieval_timestamp: str,
) -> dict[str, Any]:
    return {
        "provider": runtime_contract.provider,
        "endpoint": runtime_contract.endpoint,
        "provider_version": runtime_contract.provider_version,
        "adapter_version": runtime_contract.adapter_version,
        "native_schema_version": runtime_contract.native_schema_version,
        "provider_symbol": provider_symbol,
        "provider_exchange_alias": instrument.provider_exchange_alias,
        "canonical_mic": instrument.venue,
        "source_timestamp": datetime.fromisoformat(f"{trading_date.isoformat()}T15:00:00+08:00"),
        "retrieval_timestamp": datetime.fromisoformat(retrieval_timestamp),
        "original_volume_unit": original_volume_unit,
        "applied_volume_multiplier": volume_multiplier,
        "adjustment": "none",
    }


def _session_close(calendar: MarketCalendarProvider, trading_date: date) -> datetime:
    close = calendar.session_close("CN", trading_date)
    if close is None:
        raise ValueError("live bar date is not a confirmed complete trading session")
    return close


def normalize_baostock_live_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        if record.get("adjustflag") != "3":
            raise ValueError("BaoStock live Raw must be unadjusted")
        symbol = str(record["code"]).split(".", maxsplit=1)[-1]
        instrument = _instrument_for_record(instruments, symbol)
        trading_date = date.fromisoformat(str(record["date"]))
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": _session_close(calendar, trading_date) if instrument else None,
                "open": float(str(record["open"])),
                "high": float(str(record["high"])),
                "low": float(str(record["low"])),
                "close": float(str(record["close"])),
                "volume": float(str(record["volume"])),
                "amount": float(str(record["amount"])),
                "currency": "CNY" if instrument else None,
                "lineage": _lineage(
                    instrument=instrument,
                    provider_symbol=str(record["code"]),
                    runtime_contract=BAOSTOCK_LIVE_RUNTIME_CONTRACT,
                    original_volume_unit="share",
                    volume_multiplier=1,
                    trading_date=trading_date,
                    retrieval_timestamp=str(record["_retrieval_timestamp"]),
                )
                if instrument
                else {},
            }
        )
    return bars


def normalize_akshare_live_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        if record.get("复权") != "none":
            raise ValueError("AKShare live Raw must be unadjusted")
        symbol = str(record["股票代码"])
        instrument = _instrument_for_record(instruments, symbol)
        trading_date = date.fromisoformat(str(record["日期"]))
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": _session_close(calendar, trading_date) if instrument else None,
                "open": record["开盘"],
                "high": record["最高"],
                "low": record["最低"],
                "close": record["收盘"],
                "volume": float(record["成交量"]) * 100,
                "amount": float(record["成交额"]),
                "currency": "CNY" if instrument else None,
                "lineage": _lineage(
                    instrument=instrument,
                    provider_symbol=symbol,
                    runtime_contract=AKSHARE_LIVE_RUNTIME_CONTRACT,
                    original_volume_unit="lot",
                    volume_multiplier=100,
                    trading_date=trading_date,
                    retrieval_timestamp=str(record["_retrieval_timestamp"]),
                )
                if instrument
                else {},
            }
        )
    return bars


def normalize_tdx_reference_live_bars(
    records: Iterable[Mapping[str, object]],
    instruments: Mapping[str, Instrument],
    calendar: MarketCalendarProvider,
) -> list[Mapping[str, object]]:
    bars: list[Mapping[str, object]] = []
    for record in records:
        if record.get("adjustment") != "none":
            raise ValueError("TDX reference Raw must be unadjusted")
        symbol = str(record["code"])
        instrument = _instrument_for_record(instruments, symbol)
        trading_date = date.fromisoformat(str(record["datetime"])[:10])
        bars.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id if instrument else None,
                "ts_event": _session_close(calendar, trading_date) if instrument else None,
                "open": float(record["open"]),
                "high": float(record["high"]),
                "low": float(record["low"]),
                "close": float(record["close"]),
                "volume": _tdx_lots_to_canonical_shares(record["vol"]),
                "amount": float(record["amount"]),
                "currency": "CNY" if instrument else None,
                "lineage": _lineage(
                    instrument=instrument,
                    provider_symbol=symbol,
                    runtime_contract=TDX_REFERENCE_LIVE_RUNTIME_CONTRACT,
                    original_volume_unit="lot",
                    volume_multiplier=100,
                    trading_date=trading_date,
                    retrieval_timestamp=str(record["_retrieval_timestamp"]),
                )
                if instrument
                else {},
            }
        )
    return bars
