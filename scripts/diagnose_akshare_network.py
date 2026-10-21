from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from time import perf_counter

from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import AKShareLiveProvider
from quant_stack.data.providers.cn.live.akshare_transport import AKShareEastmoneyTransport
from quant_stack.data.providers.cn.reliability import classify_probe_failure, require_non_empty
from quant_stack.data.quality import ProviderSchemaValidator
from quant_stack.data.storage.atomic import atomic_replace_bytes

_PROXY_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)


def _instrument(
    instrument_id: str,
    symbol: str,
    venue: str,
    provider_symbol: str,
    alias: str,
) -> Instrument:
    return Instrument(
        instrument_id=instrument_id,
        symbol=symbol,
        market="CN",
        venue=venue,
        provider_symbol=provider_symbol,
        provider_exchange_alias=alias,
        currency="CNY",
        timezone="Asia/Shanghai",
    )


INSTRUMENTS = (
    _instrument("XSHG.600000", "600000", "XSHG", "600000", "SH"),
    _instrument("XSHE.000001", "000001", "XSHE", "000001", "SZ"),
    _instrument("BJSE.920047", "920047", "BJSE", "920047", "BJ"),
)


def _package_version(distribution: str) -> str | None:
    try:
        return version(distribution)
    except PackageNotFoundError:
        return None


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _content_hash(records: Sequence[Mapping[str, object]]) -> str:
    sanitized = [
        {key: value for key, value in record.items() if key != "_retrieval_timestamp"}
        for record in records
    ]
    payload = json.dumps(
        sanitized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _linux_default_route() -> tuple[bool, bool, int | None]:
    ipv4 = False
    ipv6 = False
    mtu: int | None = None
    route = Path("/proc/net/route")
    if route.is_file():
        for line in route.read_text(encoding="utf-8", errors="replace").splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 4 and fields[1] == "00000000" and int(fields[3], 16) & 0x1:
                ipv4 = True
                mtu_path = Path("/sys/class/net") / fields[0] / "mtu"
                try:
                    mtu = int(mtu_path.read_text(encoding="utf-8").strip())
                except (OSError, ValueError):
                    pass
                break
    ipv6_route = Path("/proc/net/ipv6_route")
    if ipv6_route.is_file():
        ipv6 = any(
            line.startswith("0" * 32 + " 00")
            for line in ipv6_route.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        )
    return ipv4, ipv6, mtu


def _darwin_default_route() -> tuple[bool, bool, int | None]:
    def route(family: str) -> tuple[bool, str | None]:
        try:
            output = subprocess.run(
                ["route", "-n", "get", family, "default"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return False, None
        interface = None
        for line in output.splitlines():
            if line.strip().startswith("interface:"):
                interface = line.split(":", 1)[1].strip()
                break
        return True, interface

    ipv4, interface = route("-inet")
    ipv6, _ = route("-inet6")
    mtu = None
    if interface is not None and interface.replace("_", "").isalnum():
        try:
            output = subprocess.run(
                ["ifconfig", interface],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
            fields = output.replace("\n", " ").split()
            mtu_index = fields.index("mtu")
            mtu = int(fields[mtu_index + 1])
        except (
            OSError,
            ValueError,
            IndexError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
        ):
            pass
    return ipv4, ipv6, mtu


def environment_snapshot(environment_id: str, network_mode: str) -> dict[str, object]:
    if platform.system() == "Linux":
        ipv4, ipv6, mtu = _linux_default_route()
    elif platform.system() == "Darwin":
        ipv4, ipv6, mtu = _darwin_default_route()
    else:
        ipv4, ipv6, mtu = False, False, None
    cgroup = Path("/proc/1/cgroup")
    cgroup_text = (
        cgroup.read_text(encoding="utf-8", errors="replace").lower()
        if cgroup.is_file()
        else ""
    )
    return {
        "environment_id": environment_id,
        "host_fingerprint": _fingerprint(platform.node() or "unknown"),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "network_mode": network_mode,
        "container_detected": Path("/.dockerenv").exists()
        or "container" in cgroup_text
        or "docker" in cgroup_text,
        "proxy_variables_present": sorted(
            name for name in _PROXY_VARIABLES if os.environ.get(name)
        ),
        "ipv4_default_route": ipv4,
        "ipv6_default_route": ipv6,
        "socket_ipv6_available": socket.has_ipv6,
        "default_route_mtu": mtu,
    }


def run(args: argparse.Namespace) -> dict[str, object]:
    observations: list[dict[str, object]] = []
    for round_number in range(1, args.rounds + 1):
        transport = AKShareEastmoneyTransport(
            min_interval_seconds=args.min_interval_seconds,
            max_edge_attempts=args.max_edge_attempts,
            max_resolvers=args.max_resolvers,
            resolver_timeout_seconds=args.resolver_timeout_seconds,
        )
        started = perf_counter()
        stage = "edge_preflight"
        edge_probes: list[dict[str, object]] = []
        try:
            edge_probes = [
                probe.sanitized()
                for probe in transport.probe_edges(timeout_seconds=args.timeout_seconds)
            ]
            stage = "provider_fetch"
            provider = AKShareLiveProvider(INSTRUMENTS, request_transport=transport)
            records = list(
                provider.fetch_bars(
                    "CN",
                    [instrument.instrument_id for instrument in INSTRUMENTS],
                    args.start,
                    args.end,
                )
            )
            stage = "empty_response_gate"
            require_non_empty(records)
            stage = "schema_validation"
            ProviderSchemaValidator().validate(
                provider="akshare",
                records=records,
                schema=provider.bar_schema,
            )
            outcome = {
                "status": "success",
                "failure_stage": None,
                "failure_class": None,
                "rows": len(records),
                "content_sha256": _content_hash(records),
            }
        except Exception as exc:  # noqa: BLE001 - report only the sanitized class
            outcome = {
                "status": classify_probe_failure(exc).value,
                "failure_stage": stage,
                "failure_class": type(exc).__name__,
                "rows": 0,
                "content_sha256": None,
            }
        observations.append(
            {
                "round": round_number,
                "latency_ms": round((perf_counter() - started) * 1000, 1),
                "edge_probes": edge_probes,
                "transport_audit": transport.audit_snapshot().sanitized(),
                **outcome,
            }
        )
    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": environment_snapshot(args.environment_id, args.network_mode),
        "execution_contract": {
            "quantstack_commit": args.commit_sha,
            "python_version": platform.python_version(),
            "provider": "akshare",
            "provider_version": _package_version("akshare"),
            "adapter_version": "real-cn-v2",
            "curl_cffi_version": _package_version("curl-cffi"),
            "endpoint": "stock_zh_a_hist",
            "symbols": [item.instrument_id for item in INSTRUMENTS],
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "rounds": args.rounds,
            "timeout_seconds": args.timeout_seconds,
            "resolver_timeout_seconds": args.resolver_timeout_seconds,
            "max_resolvers": args.max_resolvers,
            "max_edge_attempts": args.max_edge_attempts,
            "tls_verification": True,
            "sni_hostname": "push2his.eastmoney.com",
        },
        "observations": observations,
        "safety": {
            "private_addresses_recorded": False,
            "proxy_values_recorded": False,
            "exception_bodies_recorded": False,
            "tls_verification_disabled": False,
            "publication_attempted": False,
            "ss_database_accessed": False,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sanitized AKShare network placement probe")
    parser.add_argument("--environment-id", required=True)
    parser.add_argument(
        "--network-mode",
        choices=("native", "docker_bridge", "docker_host"),
        required=True,
    )
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 7, 29))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 31))
    parser.add_argument("--rounds", type=int, default=2, choices=range(2, 6))
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--resolver-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--max-resolvers", type=int, default=2, choices=range(0, 5))
    parser.add_argument("--max-edge-attempts", type=int, default=3, choices=range(1, 9))
    parser.add_argument("--min-interval-seconds", type=float, default=0.5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args)
    payload = (json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    atomic_replace_bytes(args.output, payload)
    print(
        json.dumps(
            {
                "environment_id": args.environment_id,
                "output": str(args.output),
                "successes": sum(
                    item["status"] == "success" for item in report["observations"]
                ),
                "rounds": args.rounds,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
