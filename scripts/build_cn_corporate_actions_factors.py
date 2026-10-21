#!/usr/bin/env python3
"""Collect/replay BaoStock corporate-action and adjustment-factor evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from quant_stack.data.providers.cn.live.providers import _scoped_baostock_socket_timeout

GENERATOR_VERSION = "cn-corporate-actions-factors-v1"
PROVIDER = "baostock"
PROVIDER_VERSION = "0.9.3"
FACTOR_ENDPOINT = "query_adjust_factor"
ACTION_ENDPOINT = "query_dividend_data"
FACTOR_RAW_URI = "quant-research://cn_equity_research/adjustment_factors/raw/"
ACTION_RAW_URI = "quant-research://cn_equity_research/corporate_actions/raw/"
REQUEST_TIMEOUT_SECONDS = 5.0
RELOGIN_INTERVAL_REQUESTS = 100


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(payload))


def _parse_date(value: object) -> str | None:
    if value in (None, "", "null"):
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return None


def _load_universe(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    instruments = payload.get("instruments")
    if not isinstance(instruments, list) or len(instruments) != 500:
        raise ValueError("corporate-action build requires the 500-instrument medium universe")
    return instruments


def _result_rows(result: object) -> tuple[list[str], list[dict[str, str]]]:
    error_code = str(getattr(result, "error_code", ""))
    if error_code != "0":
        raise ConnectionError(str(getattr(result, "error_msg", error_code)))
    fields = [str(field) for field in getattr(result, "fields", ())]
    rows: list[dict[str, str]] = []
    while result.next():  # type: ignore[attr-defined]
        values = [str(value) for value in result.get_row_data()]  # type: ignore[attr-defined]
        if len(values) != len(fields):
            raise ValueError("BaoStock corporate-action row does not match native schema")
        rows.append(dict(zip(fields, values, strict=True)))
    return fields, rows


def _first_date(native: dict[str, str], fields: tuple[str, ...]) -> str | None:
    for field in fields:
        parsed = _parse_date(native.get(field))
        if parsed is not None:
            return parsed
    return None


def _query_one(
    client: object,
    endpoint: str,
    provider_symbol: str,
    *,
    start_date: str,
    end_date: str,
    year: int | None = None,
) -> dict[str, Any]:
    try:
        if endpoint == FACTOR_ENDPOINT:
            result = client.query_adjust_factor(  # type: ignore[attr-defined]
                provider_symbol, start_date=start_date, end_date=end_date
            )
        else:
            result = client.query_dividend_data(  # type: ignore[attr-defined]
                provider_symbol, year=year, yearType="operate"
            )
        fields, rows = _result_rows(result)
        return {"status": "ok", "fields": fields, "rows": rows}
    except Exception as exc:  # noqa: BLE001 - per-request evidence is retained
        return {
            "status": "error",
            "fields": [],
            "rows": [],
            "error_class": type(exc).__name__,
            "error_message": str(exc)[:300],
        }


def _retry_after_login_loss(
    client: object,
    response: dict[str, Any],
    endpoint: str,
    provider_symbol: str,
    *,
    start_date: str,
    end_date: str,
    year: int | None = None,
) -> dict[str, Any]:
    if response["status"] == "ok":
        return response
    message = str(response.get("error_message", ""))
    if not any(marker in message.lower() for marker in ("未登录", "not login", "login")):
        return response
    login = client.login()  # type: ignore[attr-defined]
    if str(login.error_code) != "0":
        return response
    return _query_one(
        client,
        endpoint,
        provider_symbol,
        start_date=start_date,
        end_date=end_date,
        year=year,
    )


def _collect_live(
    universe: list[dict[str, Any]],
    *,
    start_date: str,
    end_date: str,
    action_years: list[int],
    max_action_requests: int | None,
    observed_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if version(PROVIDER) != PROVIDER_VERSION:
        raise RuntimeError(f"{PROVIDER} version mismatch")
    import importlib

    client = importlib.import_module(PROVIDER)
    with _scoped_baostock_socket_timeout(client, REQUEST_TIMEOUT_SECONDS):
        login = client.login()
        if str(login.error_code) != "0":
            raise ConnectionError("BaoStock corporate-action login failed")
        try:
            factors: list[dict[str, Any]] = []
            for instrument in universe:
                if factors and len(factors) % RELOGIN_INTERVAL_REQUESTS == 0:
                    client.logout()
                    login = client.login()
                    if str(login.error_code) != "0":
                        raise ConnectionError("BaoStock factor refresh login failed")
                response = _query_one(
                    client,
                    FACTOR_ENDPOINT,
                    instrument["provider_symbol"],
                    start_date=start_date,
                    end_date=end_date,
                )
                response = _retry_after_login_loss(
                    client,
                    response,
                    FACTOR_ENDPOINT,
                    instrument["provider_symbol"],
                    start_date=start_date,
                    end_date=end_date,
                )
                factors.append(
                    {
                        "instrument_id": instrument["instrument_id"],
                        "provider_symbol": instrument["provider_symbol"],
                        "request_start": start_date,
                        "request_end": end_date,
                        **response,
                    }
                )
            actions: list[dict[str, Any]] = []
            request_limit = max_action_requests
            for instrument in universe:
                for action_year in action_years:
                    if request_limit is not None and len(actions) >= request_limit:
                        break
                    if actions and len(actions) % RELOGIN_INTERVAL_REQUESTS == 0:
                        client.logout()
                        login = client.login()
                        if str(login.error_code) != "0":
                            raise ConnectionError("BaoStock action refresh login failed")
                    response = _query_one(
                        client,
                        ACTION_ENDPOINT,
                        instrument["provider_symbol"],
                        start_date=start_date,
                        end_date=end_date,
                        year=action_year,
                    )
                    response = _retry_after_login_loss(
                        client,
                        response,
                        ACTION_ENDPOINT,
                        instrument["provider_symbol"],
                        start_date=start_date,
                        end_date=end_date,
                        year=action_year,
                    )
                    actions.append(
                        {
                            "instrument_id": instrument["instrument_id"],
                            "provider_symbol": instrument["provider_symbol"],
                            "year": action_year,
                            **response,
                        }
                    )
                if request_limit is not None and len(actions) >= request_limit:
                    break
        finally:
            client.logout()
    metadata = {
        "schema_version": "1.0.0",
        "provider": PROVIDER,
        "provider_version": PROVIDER_VERSION,
        "retrieved_at": observed_at.astimezone(UTC).isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "factor_endpoint": FACTOR_ENDPOINT,
        "action_endpoint": ACTION_ENDPOINT,
        "action_years": action_years,
        "action_request_limit": max_action_requests,
        "factor_request_count": len(factors),
        "action_request_count": len(actions),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
    }
    return {**metadata, "requests": factors}, {**metadata, "requests": actions}


def _collect_actions_live(
    universe: list[dict[str, Any]],
    *,
    start_date: str,
    end_date: str,
    action_years: list[int],
    max_action_requests: int | None,
    observed_at: datetime,
) -> dict[str, Any]:
    if version(PROVIDER) != PROVIDER_VERSION:
        raise RuntimeError(f"{PROVIDER} version mismatch")
    import importlib

    client = importlib.import_module(PROVIDER)
    actions: list[dict[str, Any]] = []
    with _scoped_baostock_socket_timeout(client, REQUEST_TIMEOUT_SECONDS):
        login = client.login()
        if str(login.error_code) != "0":
            raise ConnectionError("BaoStock corporate-action login failed")
        try:
            for instrument in universe:
                for action_year in action_years:
                    if max_action_requests is not None and len(actions) >= max_action_requests:
                        break
                    if actions and len(actions) % RELOGIN_INTERVAL_REQUESTS == 0:
                        client.logout()
                        login = client.login()
                        if str(login.error_code) != "0":
                            raise ConnectionError("BaoStock action refresh login failed")
                    response = _query_one(
                        client,
                        ACTION_ENDPOINT,
                        instrument["provider_symbol"],
                        start_date=start_date,
                        end_date=end_date,
                        year=action_year,
                    )
                    response = _retry_after_login_loss(
                        client,
                        response,
                        ACTION_ENDPOINT,
                        instrument["provider_symbol"],
                        start_date=start_date,
                        end_date=end_date,
                        year=action_year,
                    )
                    actions.append(
                        {
                            "instrument_id": instrument["instrument_id"],
                            "provider_symbol": instrument["provider_symbol"],
                            "year": action_year,
                            **response,
                        }
                    )
                if max_action_requests is not None and len(actions) >= max_action_requests:
                    break
        finally:
            client.logout()
    return {
        "schema_version": "1.0.0",
        "provider": PROVIDER,
        "provider_version": PROVIDER_VERSION,
        "retrieved_at": observed_at.astimezone(UTC).isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "factor_endpoint": FACTOR_ENDPOINT,
        "action_endpoint": ACTION_ENDPOINT,
        "action_years": action_years,
        "action_request_limit": max_action_requests,
        "factor_request_count": 0,
        "action_request_count": len(actions),
        "requests": actions,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
    }


def _normalize(
    universe: list[dict[str, Any]],
    factor_payload: dict[str, Any],
    action_payload: dict[str, Any],
    *,
    factor_raw_uri: str,
    factor_raw_sha256: str,
    action_raw_uri: str,
    action_raw_sha256: str,
    observed_at: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_code = {item["provider_symbol"].lower(): item for item in universe}
    factor_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    factor_dates = ("dividOperateDate", "operateDate", "exDivDate", "date")
    action_dates = ("dividOperateDate", "dividPayableDate", "dividStockMarketDate", "date")
    factor_seen: dict[tuple[str, str], dict[str, Any]] = {}
    for request in factor_payload["requests"]:
        instrument = by_code.get(str(request["provider_symbol"]).lower())
        if instrument is None or request["status"] != "ok":
            continue
        for native in request["rows"]:
            effective_date = _first_date(native, factor_dates)
            if effective_date is None:
                quarantine.append(
                    {
                        "dataset": "adjustment_factors",
                        "request": request,
                        "native": native,
                        "reason": "missing_effective_date",
                    }
                )
                continue
            normalized = {
                "factor_version": GENERATOR_VERSION,
                "instrument_id": instrument["instrument_id"],
                "provider_symbol": instrument["provider_symbol"],
                "effective_date": effective_date,
                "adjust_factor": native.get("adjustFactor") or None,
                "forward_adjust_factor": native.get("foreAdjustFactor") or None,
                "backward_adjust_factor": native.get("backAdjustFactor") or None,
                "factor_direction": "provider_native_direction_unverified",
                "provider": PROVIDER,
                "endpoint": FACTOR_ENDPOINT,
                "provider_version": PROVIDER_VERSION,
                "raw_uri": factor_raw_uri,
                "raw_sha256": factor_raw_sha256,
                "observed_at": observed_at.astimezone(UTC).isoformat(),
                "native_row": native,
            }
            key = (instrument["instrument_id"], effective_date)
            previous = factor_seen.get(key)
            if previous is not None and previous != normalized:
                quarantine.append(
                    {
                        "dataset": "adjustment_factors",
                        "reason": "conflicting_effective_date",
                        "previous": previous,
                        "current": normalized,
                    }
                )
                continue
            factor_seen[key] = normalized
    factor_rows.extend(factor_seen.values())
    for request in action_payload["requests"]:
        instrument = by_code.get(str(request["provider_symbol"]).lower())
        if instrument is None or request["status"] != "ok":
            continue
        for native in request["rows"]:
            effective_date = _first_date(native, action_dates)
            if effective_date is None:
                quarantine.append(
                    {
                        "dataset": "corporate_actions",
                        "request": request,
                        "native": native,
                        "reason": "missing_effective_date",
                    }
                )
                continue
            action_rows.append(
                {
                    "action_version": GENERATOR_VERSION,
                    "instrument_id": instrument["instrument_id"],
                    "provider_symbol": instrument["provider_symbol"],
                    "effective_date": effective_date,
                    "action_kind": "provider_native_dividend_or_split",
                    "provider": PROVIDER,
                    "endpoint": ACTION_ENDPOINT,
                    "provider_version": PROVIDER_VERSION,
                    "raw_uri": action_raw_uri,
                    "raw_sha256": action_raw_sha256,
                    "observed_at": observed_at.astimezone(UTC).isoformat(),
                    "native_row": native,
                }
            )
    return factor_rows, action_rows, quarantine


def build_actions_factors(
    *,
    universe_path: Path,
    output_root: Path,
    factor_raw_path: Path | None,
    action_raw_path: Path | None,
    start_date: str,
    end_date: str,
    action_years: list[int],
    max_action_requests: int | None,
    observed_at: datetime,
) -> dict[str, Any]:
    import shutil

    universe = _load_universe(universe_path)
    if factor_raw_path is None and action_raw_path is None:
        factor_payload, action_payload = _collect_live(
            universe,
            start_date=start_date,
            end_date=end_date,
            action_years=action_years,
            max_action_requests=max_action_requests,
            observed_at=observed_at,
        )
        factor_raw_path = output_root / "raw/baostock-adjustment-factors.json"
        action_raw_path = output_root / "raw/baostock-corporate-actions.json"
        _write_json(factor_raw_path, factor_payload)
        _write_json(action_raw_path, action_payload)
    else:
        factor_payload = None
        action_payload = None
        if factor_raw_path is not None:
            factor_raw_path = factor_raw_path.expanduser().resolve()
            factor_payload = json.loads(factor_raw_path.read_text(encoding="utf-8"))
        if action_raw_path is not None:
            action_raw_path = action_raw_path.expanduser().resolve()
            action_payload = json.loads(action_raw_path.read_text(encoding="utf-8"))
        if factor_payload is None:
            factor_payload, _ = _collect_live(
                universe,
                start_date=start_date,
                end_date=end_date,
                action_years=action_years,
                max_action_requests=0,
                observed_at=observed_at,
            )
        if action_payload is None:
            action_payload = _collect_actions_live(
                universe,
                start_date=start_date,
                end_date=end_date,
                action_years=action_years,
                max_action_requests=max_action_requests,
                observed_at=observed_at,
            )
        for payload in (factor_payload, action_payload):
            if (
                payload.get("provider") != PROVIDER
                or payload.get("provider_version") != PROVIDER_VERSION
            ):
                raise ValueError("corporate-action raw artifact does not match provider contract")
        if factor_raw_path is None:
            factor_raw_path = output_root / "raw/baostock-adjustment-factors.json"
            _write_json(factor_raw_path, factor_payload)
        if action_raw_path is None:
            action_raw_path = output_root / "raw/baostock-corporate-actions.json"
            _write_json(action_raw_path, action_payload)
        for source, name in (
            (factor_raw_path, "baostock-adjustment-factors.json"),
            (action_raw_path, "baostock-corporate-actions.json"),
        ):
            target = output_root / "raw" / name
            if source != target:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
        factor_raw_path = output_root / "raw/baostock-adjustment-factors.json"
        action_raw_path = output_root / "raw/baostock-corporate-actions.json"
    factor_raw_sha256 = _sha256(factor_raw_path)
    action_raw_sha256 = _sha256(action_raw_path)
    factor_raw_uri = FACTOR_RAW_URI + factor_raw_path.name
    action_raw_uri = ACTION_RAW_URI + action_raw_path.name
    factor_rows, action_rows, quarantine = _normalize(
        universe,
        factor_payload,
        action_payload,
        factor_raw_uri=factor_raw_uri,
        factor_raw_sha256=factor_raw_sha256,
        action_raw_uri=action_raw_uri,
        action_raw_sha256=action_raw_sha256,
        observed_at=observed_at,
    )
    factor_path = output_root / "normalized/adjustment-factors.json"
    action_path = output_root / "normalized/corporate-actions.json"
    evidence_path = output_root / "evidence/corporate-actions-factors-evidence.json"
    quarantine_path = output_root / "quarantine/corporate-actions-factors-conflicts.json"
    _write_json(factor_path, {"schema_version": "1.0.0", "rows": factor_rows})
    _write_json(action_path, {"schema_version": "1.0.0", "rows": action_rows})
    factor_errors = [item for item in factor_payload["requests"] if item["status"] != "ok"]
    action_errors = [item for item in action_payload["requests"] if item["status"] != "ok"]
    expected_action_requests = len(universe) * len(action_years)
    blockers = [
        "BaoStock factor direction and qfq/hfq formula were not independently validated",
        "pit-safe corporate-action use requires first-party effective-date and conflict resolution",
    ]
    if len(action_payload["requests"]) < expected_action_requests:
        blockers.append(
            "corporate-action request plan was bounded before all symbol/year requests completed"
        )
    if factor_errors or action_errors:
        blockers.append(
            "one or more provider requests returned errors; missing evidence is not filled"
        )
    _write_json(
        evidence_path,
        {
            "schema_version": "1.0.0",
            "dataset_id": "cn_corporate_actions_and_adjustment_factors",
            "generator_version": GENERATOR_VERSION,
            "provider": PROVIDER,
            "provider_version": PROVIDER_VERSION,
            "factor_endpoint": FACTOR_ENDPOINT,
            "action_endpoint": ACTION_ENDPOINT,
            "factor_native_schema_versions": sorted(
                {
                    field
                    for request in factor_payload["requests"]
                    for field in request.get("fields", [])
                }
            ),
            "action_native_schema_versions": sorted(
                {
                    field
                    for request in action_payload["requests"]
                    for field in request.get("fields", [])
                }
            ),
            "factor_semantics": {
                "direction": "unverified_provider_native",
                "effective_date": "dividOperateDate_preferred_when_present",
                "availability": {"start": start_date, "end": end_date},
                "data_rights": "upstream-terms-unverified-controlled-research-only",
            },
            "factor_rows": len(factor_rows),
            "corporate_action_rows": len(action_rows),
            "factor_request_count": len(factor_payload["requests"]),
            "action_request_count": len(action_payload["requests"]),
            "factor_error_count": len(factor_errors),
            "action_error_count": len(action_errors),
            "quarantine_count": len(quarantine),
            "pit_safe": False,
            "qfq_view": "blocked",
            "hfq_view": "blocked",
            "bars_unadjusted_unchanged": True,
            "blockers": blockers,
            "observed_at": observed_at.astimezone(UTC).isoformat(),
        },
    )
    _write_json(quarantine_path, {"schema_version": "1.0.0", "rows": quarantine})
    files = [
        factor_raw_path,
        action_raw_path,
        factor_path,
        action_path,
        evidence_path,
        quarantine_path,
    ]
    parquet_paths: list[Path] = []
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        for name, rows in (("adjustment-factors", factor_rows), ("corporate-actions", action_rows)):
            parquet = output_root / f"normalized/{name}.parquet"
            parquet.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), parquet, compression="zstd")
            parquet_paths.append(parquet)
    except ImportError:
        pass
    files.extend(parquet_paths)
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "cn_corporate_actions_and_adjustment_factors",
        "generator_version": GENERATOR_VERSION,
        "provider": PROVIDER,
        "provider_version": PROVIDER_VERSION,
        "universe_count": len(universe),
        "factor_rows": len(factor_rows),
        "corporate_action_rows": len(action_rows),
        "quarantine_count": len(quarantine),
        "pit_safe": False,
        "qfq_view": "blocked",
        "hfq_view": "blocked",
        "bars_unadjusted_unchanged": True,
        "immutable": True,
        "offline_rebuild_command": (
            "python scripts/build_cn_corporate_actions_factors.py build "
            "--universe config/cn_backfill_medium_universe_v1.json "
            "--factor-raw <factor-raw-artifact> --action-raw <action-raw-artifact> "
            "--output <output-root>"
        ),
        "files": [
            {
                "path": path.relative_to(output_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in files
        ],
        "created_at": observed_at.astimezone(UTC).isoformat(),
    }
    _write_json(output_root / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--universe", required=True, type=Path)
    build.add_argument("--output", required=True, type=Path)
    build.add_argument("--factor-raw", type=Path)
    build.add_argument("--action-raw", type=Path)
    build.add_argument("--start-date", default="2015-01-01")
    build.add_argument("--end-date", default="2026-08-04")
    build.add_argument("--action-start-year", type=int, default=2015)
    build.add_argument("--action-end-year", type=int, default=2026)
    build.add_argument("--max-action-requests", type=int)
    build.add_argument("--observed-at", type=datetime.fromisoformat)
    return parser


def main() -> None:
    args = _parser().parse_args()
    years = list(range(args.action_start_year, args.action_end_year + 1))
    result = build_actions_factors(
        universe_path=args.universe.expanduser().resolve(),
        output_root=args.output.expanduser().resolve(),
        factor_raw_path=args.factor_raw,
        action_raw_path=args.action_raw,
        start_date=args.start_date,
        end_date=args.end_date,
        action_years=years,
        max_action_requests=args.max_action_requests,
        observed_at=(args.observed_at or datetime.now(UTC)),
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
