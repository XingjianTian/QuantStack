from types import SimpleNamespace

import pytest

from quant_stack.data.providers.cn.live.akshare_transport import (
    _PUSH2HIS_URL,
    AKShareEastmoneyTransport,
    _curl_failure_stage,
    _edge_fingerprint,
    _public_ipv4,
)


def test_public_ipv4_rejects_fake_dns_and_private_addresses() -> None:
    assert _public_ipv4("198.18.0.238") is None
    assert _public_ipv4("127.0.0.1") is None
    assert _public_ipv4("117.184.38.143") == "117.184.38.143"


def test_transport_uses_chrome_tls_and_fails_over_cdn_edges(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    curl_opt = SimpleNamespace(RESOLVE="resolve")
    response = SimpleNamespace(status_code=200)

    class _CurlRequests:
        @staticmethod
        def get(url: str, **kwargs: object) -> object:
            calls.append({"url": url, **kwargs})
            resolve = kwargs["curl_options"]["resolve"]  # type: ignore[index]
            if resolve == ["push2his.eastmoney.com:443:117.184.38.143"]:
                raise ConnectionError("closed")
            return response

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.akshare_transport._curl_runtime",
        lambda: (curl_opt, _CurlRequests),
    )
    transport = AKShareEastmoneyTransport(min_interval_seconds=0)
    monkeypatch.setattr(
        transport,
        "_candidate_ips",
        lambda: iter(("117.184.38.143", "103.220.167.80")),
    )

    result = transport.get(
        _PUSH2HIS_URL,
        params={"secid": "1.600000", "ut": "stale-token"},
        timeout=12,
    )

    assert result is response
    assert len(calls) == 2
    assert calls[1]["impersonate"] == "chrome131"
    assert calls[1]["params"]["ut"] == "fa5fd1943c7b386f172d6893dbfba10b"  # type: ignore[index]
    assert calls[1]["headers"]["Referer"] == "https://quote.eastmoney.com/"  # type: ignore[index]
    assert calls[1]["verify"] is True
    assert calls[1]["allow_redirects"] is False
    assert transport._last_good_ip == "103.220.167.80"
    audit = transport.audit_snapshot()
    assert audit.attempted_edges == 2
    assert audit.attempted_source_fingerprints == (
        _edge_fingerprint("117.184.38.143"),
        _edge_fingerprint("103.220.167.80"),
    )
    assert audit.successful_source_fingerprint == _edge_fingerprint("103.220.167.80")
    assert audit.tls_verification is True
    assert audit.server_authenticated is True
    assert audit.encrypted is True
    assert audit.completed_requests == 1
    assert audit.request_attempted_source_fingerprints == (
        (
            _edge_fingerprint("117.184.38.143"),
            _edge_fingerprint("103.220.167.80"),
        ),
    )
    assert audit.request_successful_source_fingerprints == (
        _edge_fingerprint("103.220.167.80"),
    )
    assert audit.all_requests_succeeded is True


def test_transport_rejects_any_endpoint_outside_akshare_history() -> None:
    transport = AKShareEastmoneyTransport(min_interval_seconds=0)
    with pytest.raises(RuntimeError, match="only permits"):
        transport.get("https://example.com/")


def test_transport_bounds_edge_attempts_and_sanitizes_audit(monkeypatch) -> None:
    calls: list[str] = []
    curl_opt = SimpleNamespace(RESOLVE="resolve")

    class _CurlRequests:
        @staticmethod
        def get(url: str, **kwargs: object) -> object:
            calls.append(url)
            raise ConnectionError("sensitive upstream detail")

    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.akshare_transport._curl_runtime",
        lambda: (curl_opt, _CurlRequests),
    )
    transport = AKShareEastmoneyTransport(
        min_interval_seconds=0,
        max_edge_attempts=2,
    )
    monkeypatch.setattr(
        transport,
        "_candidate_ips",
        lambda: iter(("117.184.38.143", "103.220.167.80", "117.184.40.129")),
    )

    with pytest.raises(ConnectionError, match="rejected all CDN edges"):
        transport.get(_PUSH2HIS_URL, timeout=99)

    audit = transport.audit_snapshot().sanitized()
    assert len(calls) == 2
    assert audit["max_edge_attempts"] == 2
    assert audit["attempted_edges"] == 2
    assert len(audit["attempted_source_fingerprints"]) == 2
    assert all(len(value) == 64 for value in audit["attempted_source_fingerprints"])
    assert audit["failure_codes"] == (None, None)
    assert audit["failure_stages"] == ("request_transport", "request_transport")
    assert audit["completed_requests"] == 1
    assert audit["all_requests_succeeded"] is False
    assert "sensitive" not in str(audit)


def test_edge_probe_sanitizes_endpoint_and_distinguishes_tcp_from_tls(monkeypatch) -> None:
    class _Socket:
        def close(self) -> None:
            pass

    class _Context:
        def wrap_socket(self, connected, *, server_hostname: str):
            assert server_hostname == "push2his.eastmoney.com"
            raise TimeoutError("sensitive tls body")

    transport = AKShareEastmoneyTransport(min_interval_seconds=0, max_edge_attempts=1)
    transport._candidate_sources["117.184.38.143"] = "doh_1"
    monkeypatch.setattr(
        transport,
        "_candidate_ips",
        lambda: iter(("117.184.38.143",)),
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.akshare_transport.socket.create_connection",
        lambda *args, **kwargs: _Socket(),
    )
    monkeypatch.setattr(
        "quant_stack.data.providers.cn.live.akshare_transport.ssl.create_default_context",
        lambda: _Context(),
    )

    probe = transport.probe_edges(timeout_seconds=1)[0].sanitized()

    assert probe["edge_fingerprint"] == _edge_fingerprint("117.184.38.143")
    assert probe["resolver_source"] == "doh_1"
    assert probe["tcp_connected"] is True
    assert probe["tls_handshake_succeeded"] is False
    assert probe["failure_stage"] == "tls_sni"
    assert probe["failure_class"] == "TimeoutError"
    assert "117.184.38.143" not in str(probe)
    assert "sensitive" not in str(probe)


@pytest.mark.parametrize(
    ("code", "stage"),
    [
        (6, "dns_resolution"),
        (7, "tcp_connect"),
        (35, "tls_sni"),
        (28, "timeout"),
        (52, "http_response"),
        (56, "http_response"),
        (None, "request_transport"),
    ],
)
def test_curl_failure_codes_are_mapped_without_error_bodies(code, stage) -> None:
    assert _curl_failure_stage(code) == stage


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_interval_seconds": -1},
        {"max_edge_attempts": 0},
        {"max_resolvers": 99},
        {"resolver_timeout_seconds": 6},
    ],
)
def test_transport_rejects_unbounded_configuration(kwargs) -> None:
    with pytest.raises(ValueError):
        AKShareEastmoneyTransport(**kwargs)
