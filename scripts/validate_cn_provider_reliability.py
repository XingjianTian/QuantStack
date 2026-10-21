from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import statistics
import subprocess
import sys
import types
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from quant_stack.data.models import Instrument
from quant_stack.data.providers.cn.live import (
    AKShareLiveProvider,
    BaoStockLiveProvider,
    TDXReferenceLiveProvider,
)
from quant_stack.data.providers.cn.live.akshare_transport import AKShareEastmoneyTransport
from quant_stack.data.providers.cn.reliability import (
    EmptyProviderResponseError,
    classify_probe_failure,
    require_non_empty,
)
from quant_stack.data.quality import ProviderNativeSchema, ProviderSchemaValidator

PINNED_ASL_COMMIT = "ba5681af5e8be5ae9965f08a27051de9b15d03bd"
PINNED_ASL_SOURCE_SHA256 = {
    "quotes.py": "c806977c73c9e0dd0f5b42cb7db6a751e7edeb28b2794e565ce750b5a226b9bd",
    "hosts.py": "a4029488f3b7b33c3c2db2f538b8d0e870efc922647d8cfdad02234a3c3bdd6a",
}
_TDX_HOST_LIMIT = 4


def _instrument(
    instrument_id: str, symbol: str, venue: str, provider_symbol: str, alias: str
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


BAOSTOCK_INSTRUMENTS = (
    _instrument("XSHG.600000", "600000", "XSHG", "sh.600000", "sh"),
    _instrument("XSHE.000001", "000001", "XSHE", "sz.000001", "sz"),
)
AKSHARE_INSTRUMENTS = (
    _instrument("XSHG.600000", "600000", "XSHG", "600000", "SH"),
    _instrument("XSHE.000001", "000001", "XSHE", "000001", "SZ"),
    _instrument("BJSE.920047", "920047", "BJSE", "920047", "BJ"),
)
TDX_INSTRUMENTS = (
    _instrument("XSHG.600000", "600000", "XSHG", "600000", "SH"),
    _instrument("XSHE.000001", "000001", "XSHE", "000001", "SZ"),
)


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        default=str,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _hash(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _content_records(records: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [
        {key: value for key, value in record.items() if key != "_retrieval_timestamp"}
        for record in records
    ]


def _schema_signature(records: Sequence[Mapping[str, object]]) -> dict[str, list[str]]:
    fields: dict[str, set[str]] = {}
    for record in records:
        for key, value in record.items():
            fields.setdefault(key, set()).add(type(value).__name__)
    return {key: sorted(values) for key, values in sorted(fields.items())}


def _observed_dates(records: Sequence[Mapping[str, object]]) -> list[str]:
    values: set[str] = set()
    for record in records:
        value = record.get("date", record.get("日期", record.get("datetime")))
        if value is not None:
            values.add(str(value)[:10])
    return sorted(values)


def _probe(
    *,
    provider: str,
    run_number: int,
    fetch: Callable[[], Sequence[Mapping[str, object]]],
    schema: ProviderNativeSchema | None,
    transport_audit: Callable[[], Mapping[str, object]] | None = None,
) -> dict[str, object]:
    started = perf_counter()
    stage = "provider_fetch"
    try:
        records = list(fetch())
        stage = "empty_response_gate"
        require_non_empty(records)
        if schema is not None:
            stage = "schema_validation"
            ProviderSchemaValidator().validate(provider=provider, records=records, schema=schema)
        return {
            "run": run_number,
            "status": "success",
            "failure_stage": None,
            "error_class": None,
            "latency_ms": round((perf_counter() - started) * 1000, 1),
            "rows": len(records),
            "observed_dates": _observed_dates(records),
            "raw_sha256": _hash(records),
            "content_sha256": _hash(_content_records(records)),
            "schema": _schema_signature(records),
            "transport_audit": dict(transport_audit()) if transport_audit else None,
        }
    except Exception as exc:  # noqa: BLE001 - the report records a bounded failure class
        return {
            "run": run_number,
            "status": classify_probe_failure(exc).value,
            "failure_stage": stage,
            "error_class": type(exc).__name__,
            "latency_ms": round((perf_counter() - started) * 1000, 1),
            "rows": 0,
            "observed_dates": [],
            "raw_sha256": None,
            "content_sha256": None,
            "schema": {},
            "transport_audit": dict(transport_audit()) if transport_audit else None,
        }


def _summarize(observations: Sequence[Mapping[str, object]]) -> dict[str, object]:
    successful = [item for item in observations if item["status"] == "success"]
    content_hashes = sorted({str(item["content_sha256"]) for item in successful})
    latencies = [float(item["latency_ms"]) for item in observations]
    counts = Counter(str(item["status"]) for item in observations)
    return {
        "runs": len(observations),
        "successful_runs": len(successful),
        "success_rate": round(len(successful) / len(observations), 4),
        "outcome_counts": dict(sorted(counts.items())),
        "latency_ms": {
            "min": min(latencies),
            "median": round(statistics.median(latencies), 1),
            "max": max(latencies),
        },
        "successful_content_hash_count": len(content_hashes),
        "content_consistent_across_successes": len(content_hashes) <= 1,
        "observations": list(observations),
    }


def _verify_asl_checkout(asl_root: Path) -> dict[str, object]:
    protocol = asl_root / "src" / "ashare_lake" / "adapters" / "tdx_protocol"
    source_hashes = {
        name: hashlib.sha256((protocol / name).read_bytes()).hexdigest()
        for name in PINNED_ASL_SOURCE_SHA256
    }
    if source_hashes != PINNED_ASL_SOURCE_SHA256:
        raise RuntimeError("ASL reference source does not match the pinned file hashes")

    verification_mode = "pinned_source_sha256"
    commit: str | None = None
    try:
        commit = subprocess.run(
            ["git", "-C", str(asl_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    if commit is not None and commit != PINNED_ASL_COMMIT:
        raise RuntimeError("ASL reference checkout does not match the pinned commit")
    if commit == PINNED_ASL_COMMIT:
        verification_mode = "git_commit_and_source_sha256"
    return {
        "pinned_commit": PINNED_ASL_COMMIT,
        "observed_commit": commit,
        "source_sha256": source_hashes,
        "verification_mode": verification_mode,
        "verified": True,
        "runtime_dependency": False,
    }


def _load_asl_tdx(asl_root: Path) -> tuple[type[Any], Sequence[tuple[str, int]]]:
    source = asl_root / "src"
    protocol = source / "ashare_lake" / "adapters" / "tdx_protocol"
    if not (protocol / "quotes.py").is_file():
        raise RuntimeError("pinned ASL checkout does not contain the TDX adapter")
    packages = (
        ("ashare_lake", source / "ashare_lake"),
        ("ashare_lake.adapters", source / "ashare_lake" / "adapters"),
        ("ashare_lake.adapters.tdx_protocol", protocol),
    )
    for name, path in packages:
        module = types.ModuleType(name)
        module.__path__ = [str(path)]  # type: ignore[attr-defined]
        sys.modules[name] = module

    def load(name: str, path: Path) -> Any:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load pinned ASL module: {name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    quotes = load("ashare_lake.adapters.tdx_protocol.quotes", protocol / "quotes.py")
    hosts = load("ashare_lake.adapters.tdx_protocol.hosts", protocol / "hosts.py")
    return quotes.Quotes, hosts.VERIFIED_HOSTS[:_TDX_HOST_LIMIT]


def _tdx_fetch(
    quotes_type: type[Any], hosts: Sequence[tuple[str, int]], start: date, end: date
) -> Sequence[Mapping[str, object]]:
    last_error: Exception | None = None
    for host in hosts:
        client = None
        try:
            client = quotes_type.factory(server=host, timeout=5, heartbeat=False)
            rows: list[dict[str, object]] = []
            for code, market, instrument_id in (
                ("600000", 1, "XSHG.600000"),
                ("000001", 0, "XSHE.000001"),
            ):
                for native in client.bars(code, market=market, start=0, offset=800):
                    observed = str(native.get("datetime", ""))[:10]
                    if start.isoformat() <= observed <= end.isoformat():
                        rows.append({**native, "instrument_id": instrument_id})
            return rows
        except Exception as exc:  # noqa: BLE001 - rotate within the bounded host pool
            last_error = exc
        finally:
            if client is not None:
                client.close()
    raise ConnectionError("all bounded TDX candidates failed") from last_error


def _controlled_failure_matrix() -> dict[str, object]:
    cases: tuple[tuple[str, BaseException], ...] = (
        ("timeout", TimeoutError("controlled timeout")),
        ("connection_failure", ConnectionError("controlled connection failure")),
        ("empty_response", EmptyProviderResponseError("controlled empty response")),
    )
    matrix = {
        expected: {
            "observed": classify_probe_failure(exc).value,
            "blocks_required_comparison": True,
            "network_call": False,
        }
        for expected, exc in cases
    }
    invalid_schema = ProviderNativeSchema(required_fields={"volume": (int, float)})
    try:
        ProviderSchemaValidator().validate(
            provider="controlled", records=[{"volume": "bad"}], schema=invalid_schema
        )
    except Exception as exc:  # expected schema drift
        matrix["schema_anomaly"] = {
            "observed": classify_probe_failure(exc).value,
            "blocks_required_comparison": True,
            "network_call": False,
        }
    return matrix


def run(args: argparse.Namespace) -> dict[str, object]:
    asl_verification = _verify_asl_checkout(args.asl_root)
    quotes_type, tdx_hosts = _load_asl_tdx(args.asl_root)
    by_provider: dict[str, list[dict[str, object]]] = {
        "baostock": [],
        "akshare": [],
        "quantstack_tdx_reference": [],
        "asl_tdx_oracle": [],
    }
    verified_sessions = tuple(args.verified_sessions)
    for run_number in range(1, args.rounds + 1):
        baostock = BaoStockLiveProvider(BAOSTOCK_INSTRUMENTS)
        by_provider["baostock"].append(
            _probe(
                provider="baostock",
                run_number=run_number,
                fetch=lambda provider=baostock: list(
                    provider.fetch_bars(
                        "CN",
                        [item.instrument_id for item in BAOSTOCK_INSTRUMENTS],
                        args.start,
                        args.end,
                    )
                ),
                schema=baostock.bar_schema,
            )
        )

        transport = AKShareEastmoneyTransport(
            max_edge_attempts=args.akshare_edge_attempts,
            max_resolvers=args.akshare_resolvers,
        )
        akshare = AKShareLiveProvider(AKSHARE_INSTRUMENTS, request_transport=transport)
        by_provider["akshare"].append(
            _probe(
                provider="akshare",
                run_number=run_number,
                fetch=lambda provider=akshare: list(
                    provider.fetch_bars(
                        "CN",
                        [item.instrument_id for item in AKSHARE_INSTRUMENTS],
                        args.start,
                        args.end,
                    )
                ),
                schema=akshare.bar_schema,
                transport_audit=akshare.transport_audit,
            )
        )

        quantstack_tdx = TDXReferenceLiveProvider(
            TDX_INSTRUMENTS,
            verified_sessions=verified_sessions,
        )
        by_provider["quantstack_tdx_reference"].append(
            _probe(
                provider="tdx_reference",
                run_number=run_number,
                fetch=lambda provider=quantstack_tdx: list(
                    provider.fetch_bars(
                        "CN",
                        [item.instrument_id for item in TDX_INSTRUMENTS],
                        args.start,
                        args.end,
                    )
                ),
                schema=quantstack_tdx.bar_schema,
                transport_audit=quantstack_tdx.transport_audit,
            )
        )

        by_provider["asl_tdx_oracle"].append(
            _probe(
                provider="asl_tdx_oracle",
                run_number=run_number,
                fetch=lambda: _tdx_fetch(quotes_type, tdx_hosts, args.start, args.end),
                schema=None,
            )
        )

    return {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": args.environment,
        "scope": {
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "rounds": args.rounds,
            "sample_only": True,
            "full_market": False,
            "production_ready": False,
            "publication_attempted": False,
            "ss_database_accessed": False,
        },
        "providers": {
            "baostock": {
                "role_under_test": "primary",
                "coverage": ["XSHG", "XSHE"],
                "endpoint": "query_history_k_data_plus",
                "provider_version": "0.9.3",
                "adapter_version": "real-cn-v1",
                "native_volume_unit": "share",
                "volume_multiplier_to_shares": 1,
                "adjustment": "none",
                "date_semantics": "exchange_trade_date_at_Asia/Shanghai_session_close",
                **_summarize(by_provider["baostock"]),
            },
            "akshare": {
                "role_under_test": "optional_reference_and_bjse_candidate",
                "coverage": ["XSHG", "XSHE", "BJSE"],
                "endpoint": "stock_zh_a_hist",
                "provider_version": "1.18.81",
                "adapter_version": "real-cn-v2",
                "native_volume_unit": "lot",
                "volume_multiplier_to_shares": 100,
                "adjustment": "none",
                "date_semantics": "exchange_trade_date_at_Asia/Shanghai_session_close",
                **_summarize(by_provider["akshare"]),
            },
            "quantstack_tdx_reference": {
                "role_under_test": "required_comparison",
                "coverage": ["XSHG", "XSHE"],
                "explicitly_unsupported": ["BJSE"],
                "endpoint": "standard_market_daily_bars",
                "client_package": "tdxpy",
                "client_version": "0.2.7",
                "adapter_version": "real-cn-tdx-v1",
                "native_volume_unit": "lot",
                "volume_multiplier_to_shares": 100,
                "canonical_volume_type": "int64_shares",
                "volume_conversion": "Decimal(str(raw_vol))*100_then_exact_integral_check",
                "adjustment": "none",
                "date_semantics": "exchange_trade_date_at_Asia/Shanghai_session_close",
                **_summarize(by_provider["quantstack_tdx_reference"]),
            },
            "asl_tdx_oracle": {
                "role_under_test": "auxiliary_oracle_not_quantstack_adapter",
                "coverage": ["XSHG", "XSHE"],
                "explicitly_unsupported": ["BJSE"],
                "endpoint": "ASL TDX adapter auxiliary oracle",
                "reference_verification": asl_verification,
                "native_volume_unit": "lot",
                "volume_multiplier_to_shares": 100,
                "adjustment": "none",
                **_summarize(by_provider["asl_tdx_oracle"]),
            },
        },
        "controlled_failure_matrix": _controlled_failure_matrix(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded CN provider reliability validation")
    parser.add_argument("--environment", choices=("local", "nas"), required=True)
    parser.add_argument("--asl-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 7, 29))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 31))
    parser.add_argument("--rounds", type=int, default=3, choices=range(2, 6))
    parser.add_argument("--akshare-edge-attempts", type=int, default=3, choices=range(1, 9))
    parser.add_argument("--akshare-resolvers", type=int, default=2, choices=range(0, 5))
    parser.add_argument(
        "--verified-session",
        dest="verified_sessions",
        action="append",
        type=date.fromisoformat,
        default=None,
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.verified_sessions is None:
        args.verified_sessions = [date(2026, 7, 29), date(2026, 7, 30), date(2026, 7, 31)]
    report = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "environment": args.environment}))


if __name__ == "__main__":
    main()
