from copy import deepcopy

import pytest

from scripts.classify_akshare_network import classify


def _report(environment_id: str, *, mode: str, successes: int, mtu: int = 1500):
    observations = []
    for index in range(3):
        succeeded = index < successes
        observations.append(
            {
                "status": "success" if succeeded else "connection_failure",
                "edge_probes": [
                    {
                        "edge_fingerprint": "a" * 64,
                        "tcp_connected": True,
                        "tls_handshake_succeeded": True,
                        "tls_verification": True,
                    }
                ],
                "transport_audit": {
                    "attempted_source_fingerprints": ["a" * 64],
                    "failure_codes": [] if succeeded else [56],
                    "failure_stages": [] if succeeded else ["http_response"],
                },
            }
        )
    return {
        "environment": {
            "environment_id": environment_id,
            "network_mode": mode,
            "system": "Darwin" if "mac" in environment_id else "Linux",
            "machine": "arm64" if "mac" in environment_id else "x86_64",
            "container_detected": "container" in environment_id or "docker" in environment_id,
            "default_route_mtu": mtu,
            "proxy_variables_present": [],
        },
        "execution_contract": {"commit": "b" * 40, "rounds": 3},
        "observations": observations,
    }


def _matrix():
    return [
        _report("development-native", mode="native", successes=3),
        _report("macmini-native", mode="native", successes=2),
        _report("macmini-container", mode="docker_bridge", successes=2, mtu=65535),
        _report("nas-docker-bridge", mode="docker_bridge", successes=1),
        _report("nas-docker-host", mode="docker_host", successes=1),
    ]


def test_classifier_attributes_failures_after_verified_tls() -> None:
    result = classify(_matrix())

    assert result["attribution"]["failure_boundary"] == "http_response_after_verified_tls"
    assert result["attribution"]["response_reset_only"] is True
    assert result["attribution"]["mac_container_boundary_supported"] is False
    assert result["attribution"]["nas_docker_bridge_boundary_supported"] is False
    assert result["attribution"]["shared_edges_with_mixed_http_outcomes"] == ["a" * 64]
    assert result["placement_decision"]["silent_fallback"] is False


def test_classifier_rejects_contract_drift() -> None:
    reports = _matrix()
    reports[-1] = deepcopy(reports[-1])
    reports[-1]["execution_contract"]["commit"] = "c" * 40

    with pytest.raises(ValueError, match="execution contracts differ"):
        classify(reports)
