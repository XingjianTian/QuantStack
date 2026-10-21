from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from quant_stack.data.storage.atomic import atomic_replace_bytes

_EXPECTED_ENVIRONMENTS = {
    "development-native",
    "macmini-native",
    "macmini-container",
    "nas-docker-bridge",
    "nas-docker-host",
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()


def classify(reports: list[dict[str, object]]) -> dict[str, object]:
    by_environment = {
        str(report["environment"]["environment_id"]): report  # type: ignore[index]
        for report in reports
    }
    if set(by_environment) != _EXPECTED_ENVIRONMENTS:
        raise ValueError(
            "network matrix must contain every required comparable environment exactly once"
        )
    contracts = [report["execution_contract"] for report in reports]
    if any(contract != contracts[0] for contract in contracts[1:]):
        raise ValueError("network matrix execution contracts differ")

    matrix: list[dict[str, object]] = []
    edge_outcomes: dict[str, set[str]] = defaultdict(set)
    failure_codes: set[int] = set()
    failure_stages: set[str] = set()
    every_tcp = True
    every_tls = True
    proxy_difference = False
    for environment_id in sorted(by_environment):
        report = by_environment[environment_id]
        environment = report["environment"]  # type: ignore[assignment]
        observations = report["observations"]  # type: ignore[assignment]
        successes = sum(item["status"] == "success" for item in observations)
        proxy_difference = proxy_difference or bool(environment["proxy_variables_present"])
        for observation in observations:
            outcome = "success" if observation["status"] == "success" else "failure"
            audit = observation["transport_audit"]
            for fingerprint in audit["attempted_source_fingerprints"]:
                edge_outcomes[str(fingerprint)].add(outcome)
            failure_codes.update(code for code in audit["failure_codes"] if code is not None)
            failure_stages.update(str(stage) for stage in audit["failure_stages"])
            for probe in observation["edge_probes"]:
                every_tcp = every_tcp and bool(probe["tcp_connected"])
                every_tls = every_tls and bool(probe["tls_handshake_succeeded"])
        matrix.append(
            {
                "environment_id": environment_id,
                "network_mode": environment["network_mode"],
                "system": environment["system"],
                "machine": environment["machine"],
                "container_detected": environment["container_detected"],
                "default_route_mtu": environment["default_route_mtu"],
                "successes": successes,
                "rounds": len(observations),
                "proxy_variables_present": environment["proxy_variables_present"],
                "tcp_preflight_all_succeeded": all(
                    probe["tcp_connected"]
                    for item in observations
                    for probe in item["edge_probes"]
                ),
                "verified_tls_sni_preflight_all_succeeded": all(
                    probe["tls_handshake_succeeded"] and probe["tls_verification"]
                    for item in observations
                    for probe in item["edge_probes"]
                ),
            }
        )

    success_counts = {item["environment_id"]: item["successes"] for item in matrix}
    shared_mixed_edges = sorted(
        fingerprint
        for fingerprint, outcomes in edge_outcomes.items()
        if outcomes == {"failure", "success"}
    )
    mac_pair_equal = success_counts["macmini-native"] == success_counts["macmini-container"]
    nas_pair_equal = success_counts["nas-docker-bridge"] == success_counts["nas-docker-host"]
    response_reset_only = failure_codes == {56} and failure_stages == {"http_response"}
    return {
        "schema_version": "1.0.0",
        "execution_contract_sha256": hashlib.sha256(_canonical(contracts[0])).hexdigest(),
        "execution_contract": contracts[0],
        "matrix": matrix,
        "unavailable_environments": [
            {
                "environment_id": "nas-native",
                "reason": "same Python 3.12.12 runtime unavailable on NAS host",
                "comparable": False,
            }
        ],
        "attribution": {
            "failure_boundary": "http_response_after_verified_tls",
            "failure_codes": sorted(failure_codes),
            "failure_stages": sorted(failure_stages),
            "all_candidate_tcp_preflights_succeeded": every_tcp,
            "all_candidate_verified_tls_sni_preflights_succeeded": every_tls,
            "proxy_difference_observed": proxy_difference,
            "mac_container_boundary_supported": not mac_pair_equal,
            "nas_docker_bridge_boundary_supported": not nas_pair_equal,
            "dns_or_single_edge_determinism_supported": not bool(shared_mixed_edges),
            "shared_edges_with_mixed_http_outcomes": shared_mixed_edges,
            "mtu_hypothesis_supported": not mac_pair_equal,
            "response_reset_only": response_reset_only,
            "classification": [
                "endpoint_transient_instability",
                "source_egress_or_cdn_path_affinity",
            ],
            "confidence": "bounded_matrix_evidence_not_long_term_sla",
        },
        "placement_decision": {
            "authoritative_data_root": "NAS",
            "catalog_publisher": "NAS only",
            "daily_cn_primary": "NAS BaoStock with required TDX comparison",
            "akshare_role": "optional reference and BJSE candidate",
            "mac_worker": "validated one-shot POC; not activated as default AKShare route",
            "silent_fallback": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify a controlled AKShare network matrix")
    parser.add_argument("--report", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = classify([json.loads(path.read_text(encoding="utf-8")) for path in args.report])
    atomic_replace_bytes(
        args.output,
        (json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(),
    )
    print(
        json.dumps(
            {
                "classification": result["attribution"]["classification"],
                "failure_boundary": result["attribution"]["failure_boundary"],
                "mac_worker": result["placement_decision"]["mac_worker"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
