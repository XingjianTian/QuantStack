from __future__ import annotations

import hashlib
import ipaddress
import socket
import ssl
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, version
from threading import RLock
from typing import Any

_PUSH2HIS_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_PUSH2HIS_HOST = "push2his.eastmoney.com"
_PUSH2HIS_TRAFFIC_MANAGER_HOST = "push2hisipv6.trafficmanager.cn"
_PUSH2_UT = "fa5fd1943c7b386f172d6893dbfba10b"
_CHROME_IMPERSONATE = "chrome131"
_CURL_CFFI_VERSION = "0.15.0"
_CHROME_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://quote.eastmoney.com/",
}
_DOH_URLS = (
    "https://dns.alidns.com/resolve",
    "https://doh.pub/dns-query",
    "https://dns.google/resolve",
    "https://cloudflare-dns.com/dns-query",
)
# Last-resort edges observed from multiple public resolvers. Dynamic discovery is
# preferred because Eastmoney rotates its Traffic Manager answers.
_FALLBACK_EDGE_IPS = (
    "117.184.38.143",
    "103.220.167.80",
    "117.184.40.129",
    "14.103.188.89",
    "101.42.128.173",
)
_AKSHARE_PATCH_LOCK = RLock()


@dataclass(frozen=True)
class AKShareTransportAudit:
    schema_version: str
    endpoint_host: str
    protocol_identity: str
    tls_verification: bool
    server_authenticated: bool
    encrypted: bool
    resolver_sources: tuple[str, ...]
    public_candidate_count: int
    fallback_candidate_count: int
    max_edge_attempts: int
    attempted_edges: int
    successful_source: str | None
    candidate_source_fingerprints: tuple[str, ...]
    candidate_sources: tuple[str, ...]
    attempted_source_fingerprints: tuple[str, ...]
    attempted_sources: tuple[str, ...]
    successful_source_fingerprint: str | None
    failure_classes: tuple[str, ...]
    failure_codes: tuple[int | None, ...]
    failure_stages: tuple[str, ...]
    completed_requests: int
    request_attempted_source_fingerprints: tuple[tuple[str, ...], ...]
    request_successful_source_fingerprints: tuple[str | None, ...]
    request_failure_codes: tuple[tuple[int | None, ...], ...]
    request_failure_stages: tuple[tuple[str, ...], ...]
    all_requests_succeeded: bool

    def sanitized(self) -> dict[str, object]:
        """Return auditable transport facts without IPs, query data or errors."""

        return asdict(self)


@dataclass(frozen=True)
class AKShareEdgeProbe:
    order: int
    edge_fingerprint: str
    resolver_source: str
    tcp_connected: bool
    tcp_latency_ms: float | None
    tls_handshake_succeeded: bool
    tls_latency_ms: float | None
    tls_verification: bool
    sni_hostname: str
    failure_stage: str | None
    failure_class: str | None

    def sanitized(self) -> dict[str, object]:
        """Return stage evidence without an edge IP or exception body."""

        return asdict(self)


def _edge_fingerprint(ip: str) -> str:
    return hashlib.sha256(f"akshare-eastmoney-edge-v1\n{ip}\n".encode()).hexdigest()


def _curl_failure_code(exc: BaseException) -> int | None:
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        code = getattr(current, "code", None)
        if isinstance(code, int):
            return code
        current = current.__cause__ or current.__context__
    return None


def _curl_failure_stage(code: int | None) -> str:
    if code in {5, 6}:
        return "dns_resolution"
    if code == 7:
        return "tcp_connect"
    if code in {35, 51, 58, 60, 77, 80, 82, 83, 90, 91}:
        return "tls_sni"
    if code in {28}:
        return "timeout"
    if code in {52, 55, 56}:
        return "http_response"
    return "request_transport"


def _curl_runtime() -> tuple[Any, Any]:
    try:
        installed = version("curl-cffi")
    except PackageNotFoundError as exc:
        raise RuntimeError("AKShare live transport requires the cn-live extra") from exc
    if installed != _CURL_CFFI_VERSION:
        raise RuntimeError(
            f"curl-cffi version mismatch: expected {_CURL_CFFI_VERSION}, got {installed}"
        )
    try:
        from curl_cffi import CurlOpt
        from curl_cffi import requests as curl_requests
    except ImportError as exc:  # pragma: no cover - guarded by the cn-live extra
        raise RuntimeError("AKShare live transport requires the cn-live extra") from exc
    return CurlOpt, curl_requests


def _public_ipv4(value: object) -> str | None:
    try:
        address = ipaddress.ip_address(str(value).strip())
    except ValueError:
        return None
    return str(address) if address.version == 4 and address.is_global else None


class AKShareEastmoneyTransport:
    """Requests-compatible transport for AKShare's Eastmoney history endpoint.

    Eastmoney may close ordinary Python TLS sessions after the handshake. The
    transport keeps AKShare's public parser but gives its single history request
    a browser TLS fingerprint and pins a working CDN edge discovered via DoH.
    """

    def __init__(
        self,
        *,
        min_interval_seconds: float = 0.5,
        max_edge_attempts: int = 8,
        max_resolvers: int = 4,
        resolver_timeout_seconds: float = 5.0,
    ) -> None:
        if not 0 <= min_interval_seconds <= 60:
            raise ValueError("AKShare pacing interval must be between 0 and 60 seconds")
        if not 1 <= max_edge_attempts <= 8:
            raise ValueError("AKShare edge attempts must be between 1 and 8")
        if not 0 <= max_resolvers <= len(_DOH_URLS):
            raise ValueError("AKShare resolver count is outside the allowlist")
        if not 1 <= resolver_timeout_seconds <= 5:
            raise ValueError("AKShare resolver timeout must be between 1 and 5 seconds")
        self._min_interval_seconds = min_interval_seconds
        self._max_edge_attempts = max_edge_attempts
        self._max_resolvers = max_resolvers
        self._resolver_timeout_seconds = resolver_timeout_seconds
        self._last_request = 0.0
        self._last_good_ip: str | None = None
        self._discovered_ips: tuple[str, ...] | None = None
        self._candidate_sources: dict[str, str] = {}
        self._resolver_sources: tuple[str, ...] = ()
        self._last_attempted_edges = 0
        self._last_successful_source: str | None = None
        self._last_attempted_source_fingerprints: list[str] = []
        self._last_attempted_sources: list[str] = []
        self._last_successful_source_fingerprint: str | None = None
        self._last_failure_classes: list[str] = []
        self._last_failure_codes: list[int | None] = []
        self._last_failure_stages: list[str] = []
        self._request_attempted_source_fingerprints: list[tuple[str, ...]] = []
        self._request_successful_source_fingerprints: list[str | None] = []
        self._request_failure_codes: list[tuple[int | None, ...]] = []
        self._request_failure_stages: list[tuple[str, ...]] = []
        self._throttle_lock = RLock()

    def _throttle(self) -> None:
        with self._throttle_lock:
            remaining = self._min_interval_seconds - (time.monotonic() - self._last_request)
            if remaining > 0:
                time.sleep(remaining)
            self._last_request = time.monotonic()

    def _discover_ips(self) -> tuple[str, ...]:
        if self._discovered_ips is not None:
            return self._discovered_ips
        _, curl_requests = _curl_runtime()
        found: list[str] = []

        resolver_sources: list[str] = []

        def add(value: object, source: str) -> None:
            ip = _public_ipv4(value)
            if ip is not None and ip not in found:
                found.append(ip)
                self._candidate_sources[ip] = source
                if source not in resolver_sources:
                    resolver_sources.append(source)

        for name in (_PUSH2HIS_HOST, _PUSH2HIS_TRAFFIC_MANAGER_HOST):
            try:
                for item in socket.getaddrinfo(name, 443, type=socket.SOCK_STREAM):
                    add(item[4][0], "system_dns")
            except OSError:
                pass
        for index, url in enumerate(_DOH_URLS[: self._max_resolvers], start=1):
            try:
                response = curl_requests.get(
                    url,
                    params={"name": _PUSH2HIS_TRAFFIC_MANAGER_HOST, "type": "A"},
                    headers={"Accept": "application/dns-json"},
                    timeout=self._resolver_timeout_seconds,
                    impersonate=_CHROME_IMPERSONATE,
                    verify=True,
                    allow_redirects=False,
                )
                if response.status_code != 200:
                    continue
                for answer in response.json().get("Answer") or ():
                    if int(answer.get("type") or 0) == 1:
                        add(answer.get("data"), f"doh_{index}")
            except Exception:  # noqa: BLE001 - each resolver is best-effort
                continue
        for ip in _FALLBACK_EDGE_IPS:
            add(ip, "pinned_fallback")
        self._discovered_ips = tuple(found)
        self._resolver_sources = tuple(resolver_sources)
        return self._discovered_ips

    def _candidate_ips(self) -> Iterator[str]:
        emitted: set[str] = set()
        if self._last_good_ip is not None:
            emitted.add(self._last_good_ip)
            yield self._last_good_ip
        for ip in self._discover_ips():
            if ip not in emitted:
                emitted.add(ip)
                yield ip

    def audit_snapshot(self) -> AKShareTransportAudit:
        sources = tuple(sorted(set(self._resolver_sources)))
        candidates = tuple(self._discovered_ips or ())
        return AKShareTransportAudit(
            schema_version="1.0.0",
            endpoint_host=_PUSH2HIS_HOST,
            protocol_identity="https/eastmoney-stock-kline",
            tls_verification=True,
            server_authenticated=True,
            encrypted=True,
            resolver_sources=sources,
            public_candidate_count=len(candidates),
            fallback_candidate_count=sum(
                source == "pinned_fallback" for source in self._candidate_sources.values()
            ),
            max_edge_attempts=self._max_edge_attempts,
            attempted_edges=self._last_attempted_edges,
            successful_source=self._last_successful_source,
            candidate_source_fingerprints=tuple(_edge_fingerprint(ip) for ip in candidates),
            candidate_sources=tuple(
                self._candidate_sources.get(ip, "unknown") for ip in candidates
            ),
            attempted_source_fingerprints=tuple(
                self._last_attempted_source_fingerprints
            ),
            attempted_sources=tuple(self._last_attempted_sources),
            successful_source_fingerprint=self._last_successful_source_fingerprint,
            failure_classes=tuple(self._last_failure_classes),
            failure_codes=tuple(self._last_failure_codes),
            failure_stages=tuple(self._last_failure_stages),
            completed_requests=len(self._request_attempted_source_fingerprints),
            request_attempted_source_fingerprints=tuple(
                self._request_attempted_source_fingerprints
            ),
            request_successful_source_fingerprints=tuple(
                self._request_successful_source_fingerprints
            ),
            request_failure_codes=tuple(self._request_failure_codes),
            request_failure_stages=tuple(self._request_failure_stages),
            all_requests_succeeded=bool(self._request_attempted_source_fingerprints)
            and all(
                value is not None for value in self._request_successful_source_fingerprints
            ),
        )

    def _record_completed_request(self) -> None:
        self._request_attempted_source_fingerprints.append(
            tuple(self._last_attempted_source_fingerprints)
        )
        self._request_successful_source_fingerprints.append(
            self._last_successful_source_fingerprint
        )
        self._request_failure_codes.append(tuple(self._last_failure_codes))
        self._request_failure_stages.append(tuple(self._last_failure_stages))

    def probe_edges(self, *, timeout_seconds: float = 5.0) -> tuple[AKShareEdgeProbe, ...]:
        """Probe bounded TCP and verified TLS/SNI stages without issuing HTTP requests."""

        if not 1 <= timeout_seconds <= 15:
            raise ValueError("AKShare diagnostic timeout must be between 1 and 15 seconds")
        probes: list[AKShareEdgeProbe] = []
        for order, ip in enumerate(self._candidate_ips(), start=1):
            if order > self._max_edge_attempts:
                break
            source = self._candidate_sources.get(ip, "unknown")
            started = time.perf_counter()
            connected: socket.socket | None = None
            tcp_latency: float | None = None
            tls_latency: float | None = None
            failure_stage: str | None = None
            failure_class: str | None = None
            tls_succeeded = False
            try:
                connected = socket.create_connection((ip, 443), timeout=timeout_seconds)
                tcp_latency = round((time.perf_counter() - started) * 1000, 1)
                tls_started = time.perf_counter()
                context = ssl.create_default_context()
                with context.wrap_socket(
                    connected,
                    server_hostname=_PUSH2HIS_HOST,
                ) as secured:
                    connected = None
                    secured.settimeout(timeout_seconds)
                    tls_succeeded = True
                tls_latency = round((time.perf_counter() - tls_started) * 1000, 1)
            except Exception as exc:  # noqa: BLE001 - only the class enters evidence
                failure_stage = "tls_sni" if tcp_latency is not None else "tcp_connect"
                failure_class = type(exc).__name__
            finally:
                if connected is not None:
                    connected.close()
            probes.append(
                AKShareEdgeProbe(
                    order=order,
                    edge_fingerprint=_edge_fingerprint(ip),
                    resolver_source=source,
                    tcp_connected=tcp_latency is not None,
                    tcp_latency_ms=tcp_latency,
                    tls_handshake_succeeded=tls_succeeded,
                    tls_latency_ms=tls_latency,
                    tls_verification=True,
                    sni_hostname=_PUSH2HIS_HOST,
                    failure_stage=failure_stage,
                    failure_class=failure_class,
                )
            )
        return tuple(probes)

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, object] | None = None,
        timeout: float | None = None,
        **_: object,
    ) -> Any:
        if url != _PUSH2HIS_URL:
            raise RuntimeError("AKShare live transport only permits stock_zh_a_hist")
        curl_opt, curl_requests = _curl_runtime()
        query = dict(params or {})
        query["ut"] = _PUSH2_UT
        request_timeout = min(max(float(timeout if timeout is not None else 15), 1.0), 15.0)
        last_error: Exception | None = None
        self._last_attempted_edges = 0
        self._last_successful_source = None
        self._last_attempted_source_fingerprints = []
        self._last_attempted_sources = []
        self._last_successful_source_fingerprint = None
        self._last_failure_classes = []
        self._last_failure_codes = []
        self._last_failure_stages = []
        for ip in self._candidate_ips():
            if self._last_attempted_edges >= self._max_edge_attempts:
                break
            self._throttle()
            self._last_attempted_edges += 1
            source = self._candidate_sources.get(ip, "unknown")
            fingerprint = _edge_fingerprint(ip)
            self._last_attempted_source_fingerprints.append(fingerprint)
            self._last_attempted_sources.append(source)
            try:
                response = curl_requests.get(
                    url,
                    params=query,
                    headers=_CHROME_HEADERS,
                    timeout=request_timeout,
                    impersonate=_CHROME_IMPERSONATE,
                    verify=True,
                    allow_redirects=False,
                    curl_options={curl_opt.RESOLVE: [f"{_PUSH2HIS_HOST}:443:{ip}"]},
                )
                if response.status_code == 200:
                    self._last_good_ip = ip
                    self._last_successful_source = source
                    self._last_successful_source_fingerprint = fingerprint
                    self._record_completed_request()
                    return response
                last_error = RuntimeError("Eastmoney returned a non-success status")
                self._last_failure_classes.append("HTTPStatusError")
                self._last_failure_codes.append(None)
                self._last_failure_stages.append("http_status")
            except Exception as exc:  # noqa: BLE001 - advance to the next CDN edge
                last_error = exc
                self._last_failure_classes.append(type(exc).__name__)
                failure_code = _curl_failure_code(exc)
                self._last_failure_codes.append(failure_code)
                self._last_failure_stages.append(_curl_failure_stage(failure_code))
        self._record_completed_request()
        raise ConnectionError("Eastmoney history endpoint rejected all CDN edges") from last_error


@contextmanager
def install_akshare_history_transport(
    stock_hist_module: Any,
    transport: AKShareEastmoneyTransport,
) -> Iterator[None]:
    """Scope the transport injection to AKShare's history module and restore it."""

    with _AKSHARE_PATCH_LOCK:
        original = stock_hist_module.requests
        stock_hist_module.requests = transport
        try:
            yield
        finally:
            stock_hist_module.requests = original
