from datetime import date
from types import SimpleNamespace

from scripts import diagnose_akshare_network as diagnostics


def test_environment_snapshot_records_proxy_names_not_values(monkeypatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "https://secret.example:8443/token")

    snapshot = diagnostics.environment_snapshot("controlled", "native")

    assert "HTTPS_PROXY" in snapshot["proxy_variables_present"]
    assert "secret.example" not in str(snapshot)
    assert len(snapshot["host_fingerprint"]) == 64
    assert "hostname" not in snapshot


def test_network_report_uses_stable_contract_and_sanitized_failures(monkeypatch) -> None:
    class _Audit:
        def sanitized(self):
            return {
                "candidate_source_fingerprints": ("a" * 64,),
                "attempted_source_fingerprints": ("a" * 64,),
                "successful_source_fingerprint": None,
                "failure_classes": ("ConnectionError",),
            }

    class _Probe:
        def sanitized(self):
            return {
                "order": 1,
                "edge_fingerprint": "a" * 64,
                "resolver_source": "doh_1",
                "tcp_connected": False,
                "tls_handshake_succeeded": False,
                "failure_stage": "tcp_connect",
                "failure_class": "TimeoutError",
            }

    class _Transport:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def probe_edges(self, *, timeout_seconds: float):
            assert timeout_seconds == 5
            return (_Probe(),)

        def audit_snapshot(self):
            return _Audit()

    class _Provider:
        bar_schema = SimpleNamespace(required_fields={})

        def __init__(self, instruments, *, request_transport):
            self.transport = request_transport

        def fetch_bars(self, *args, **kwargs):
            raise ConnectionError("private upstream address and body")

    monkeypatch.setattr(diagnostics, "AKShareEastmoneyTransport", _Transport)
    monkeypatch.setattr(diagnostics, "AKShareLiveProvider", _Provider)
    args = SimpleNamespace(
        environment_id="nas-bridge",
        network_mode="docker_bridge",
        commit_sha="b" * 40,
        start=date(2026, 7, 29),
        end=date(2026, 7, 31),
        rounds=2,
        timeout_seconds=5,
        resolver_timeout_seconds=5,
        max_resolvers=2,
        max_edge_attempts=3,
        min_interval_seconds=0.5,
    )

    report = diagnostics.run(args)

    assert report["execution_contract"]["quantstack_commit"] == "b" * 40
    assert len(report["observations"]) == 2
    assert {item["failure_stage"] for item in report["observations"]} == {
        "provider_fetch"
    }
    assert {item["failure_class"] for item in report["observations"]} == {
        "ConnectionError"
    }
    assert "private upstream" not in str(report)
    assert report["safety"]["private_addresses_recorded"] is False
