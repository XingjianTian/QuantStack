from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from .providers.yfinance import (
    LiveYFinanceTransport,
    ReplayYFinanceTransport,
    USInstrumentSnapshot,
    YFinanceTransport,
    load_us_instrument_snapshot,
    require_live_yfinance_interval,
    resolve_yfinance_interval,
    yfinance_runtime_root,
)
from .publication import PublicationBundle
from .storage.atomic import atomic_replace_bytes
from .us_adjustment import compare_us_adjustment_shadow, derive_us_adjustment_shadow


def _write_gzip_json(path: Path, payload: object) -> None:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    atomic_replace_bytes(path, gzip.compress(body.encode(), mtime=0))


def _read_gzip_json(path: Path) -> dict[str, object]:
    return json.loads(gzip.decompress(path.read_bytes()))


def run_us_corporate_action_campaign(
    args: object,
    *,
    transport_factory: Callable[[str], YFinanceTransport] | None = None,
) -> dict[str, object]:
    proxy_url = getattr(args, "yfinance_proxy_url", None)
    fixture = getattr(args, "fixture", None)
    runtime_root = yfinance_runtime_root(
        getattr(args, "yfinance_runtime_root", None), proxy_url=proxy_url
    )
    interval = resolve_yfinance_interval(runtime_root, args.request_interval_seconds)
    if fixture is None and transport_factory is None and not proxy_url:
        raise ValueError("live corporate-action campaign requires --yfinance-proxy-url")
    if fixture is None and transport_factory is None:
        require_live_yfinance_interval(interval)

    snapshot = load_us_instrument_snapshot(args.snapshot)
    state_root = Path(args.state_root)
    state_root.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    requests = 0
    resumed = 0
    for instrument in snapshot.instruments:
        symbol = instrument.provider_symbol
        state_path = state_root / f"{symbol}.json.gz"
        if state_path.is_file():
            entry = _read_gzip_json(state_path)
            expected_identity = (
                symbol,
                instrument.instrument_id,
                args.start.isoformat(),
                args.end.isoformat(),
            )
            observed_identity = (
                entry.get("symbol"),
                entry.get("instrument_id"),
                entry.get("requested_start"),
                entry.get("requested_end"),
            )
            if observed_identity != expected_identity:
                raise ValueError(f"corporate-action resume state conflicts for {symbol}")
            resumed += 1
        else:
            transport = (
                transport_factory(symbol)
                if transport_factory
                else ReplayYFinanceTransport(fixture)
                if fixture
                else LiveYFinanceTransport(
                    runtime_root=getattr(args, "yfinance_runtime_root", None),
                    proxy_url=proxy_url,
                    minimum_interval_seconds=interval,
                )
            )
            records = transport.fetch_daily_bars_with_actions(
                [symbol], args.start, args.end
            )
            if not records or any(
                "Dividends" not in row or "Stock Splits" not in row for row in records
            ):
                raise RuntimeError(f"corporate-action evidence is incomplete for {symbol}")
            observed_dates = sorted({str(row["Date"])[:10] for row in records})
            actions = [
                {
                    "Date": str(row["Date"])[:10],
                    "Dividends": float(row["Dividends"]),
                    "Stock Splits": float(row["Stock Splits"]),
                }
                for row in records
                if float(row["Dividends"]) != 0 or float(row["Stock Splits"]) not in (0, 1)
            ]
            audit = dict(transport.audit_snapshot())
            if (
                audit.get("completed_requests") != 1
                or audit.get("all_requests_succeeded") is not True
            ):
                raise RuntimeError(f"corporate-action transport audit failed for {symbol}")
            entry = {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id,
                "requested_start": args.start.isoformat(),
                "requested_end": args.end.isoformat(),
                "observed_dates": observed_dates,
                "actions": actions,
                "transport_audit": audit,
            }
            _write_gzip_json(state_path, entry)
            requests += 1
        entries.append(entry)

    package = {
        "schema_version": "quantstack.us-corporate-actions-evidence.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_id": snapshot.snapshot_id,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "instrument_count": len(entries),
        "strictly_serial": True,
        "request_interval_seconds": interval,
        "provider_retries": 0,
        "network_route": "fixture" if fixture else "explicit_proxy",
        "entries": entries,
    }
    package_path = Path(args.package)
    _write_gzip_json(package_path, package)
    report = {
        "schema_version": "quantstack.us-corporate-actions-campaign.v1",
        "status": "passed",
        "instrument_count": len(entries),
        "actual_requests": requests,
        "resumed_instruments": resumed,
        "observed_rows": sum(len(item["observed_dates"]) for item in entries),
        "corporate_action_rows": sum(len(item["actions"]) for item in entries),
        "strictly_serial": True,
        "request_interval_seconds": interval,
        "provider_retries": 0,
        "aggregate_packages": 1,
        "aggregate_package_bytes": package_path.stat().st_size,
    }
    report_path = Path(args.report)
    atomic_replace_bytes(
        report_path,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def load_us_raw_series(
    data_root: Path,
    *,
    dataset_version_prefix: str,
) -> dict[str, list[dict[str, object]]]:
    return dict(
        iter_us_raw_series(data_root, dataset_version_prefix=dataset_version_prefix)
    )


def iter_us_raw_series(
    data_root: Path,
    *,
    dataset_version_prefix: str,
):
    seen: set[str] = set()
    matched = False
    bundle_root = data_root / "catalog/publication_bundles"
    for path in sorted(bundle_root.glob("*.json")):
        bundle = PublicationBundle.model_validate_json(path.read_text(encoding="utf-8"))
        if (
            bundle.dataset_id != "yfinance_us_equity_daily"
            or not bundle.dataset_version.startswith(f"{dataset_version_prefix}.")
        ):
            continue
        raw = next(item for item in bundle.artifacts if item.kind == "raw")
        payload = json.loads(
            (data_root / raw.uri.removeprefix("quant-data://")).read_text(encoding="utf-8")
        )
        records = [dict(item) for item in payload["records"]]
        symbols = {str(item["ticker"]).upper() for item in records}
        if len(symbols) != 1:
            raise ValueError("US Core raw bundle must contain one symbol")
        symbol = symbols.pop()
        if symbol in seen:
            raise ValueError(f"duplicate US Core raw series for {symbol}")
        seen.add(symbol)
        matched = True
        yield symbol, records
    if not matched:
        raise ValueError("no US Core raw series matched the requested version")


def build_us_corporate_action_evidence_from_raw(
    *,
    data_root: Path,
    dataset_version_prefix: str,
    snapshot: USInstrumentSnapshot,
    start,
    end,
    package_path: Path,
    report_path: Path,
) -> dict[str, object]:
    instruments = {item.provider_symbol: item for item in snapshot.instruments}
    entries: list[dict[str, object]] = []
    for symbol, records in iter_us_raw_series(
        data_root, dataset_version_prefix=dataset_version_prefix
    ):
        instrument = instruments.get(symbol)
        if instrument is None:
            raise ValueError(f"raw action evidence symbol is outside snapshot: {symbol}")
        if any("Dividends" not in row or "Stock Splits" not in row for row in records):
            raise ValueError(f"raw corporate-action evidence is incomplete for {symbol}")
        observed_dates = sorted({str(row["Date"])[:10] for row in records})
        entries.append(
            {
                "symbol": symbol,
                "instrument_id": instrument.instrument_id,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "observed_dates": observed_dates,
                "actions": [
                    {
                        "Date": str(row["Date"])[:10],
                        "Dividends": float(row["Dividends"]),
                        "Stock Splits": float(row["Stock Splits"]),
                    }
                    for row in records
                    if float(row["Dividends"]) != 0
                    or float(row["Stock Splits"]) not in (0, 1)
                ],
            }
        )
    package = {
        "schema_version": "quantstack.us-corporate-actions-evidence.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_id": snapshot.snapshot_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "instrument_count": len(entries),
        "strictly_serial": True,
        "request_interval_seconds": 0,
        "provider_retries": 0,
        "network_route": "existing_authenticated_raw",
        "entries": entries,
    }
    _write_gzip_json(package_path, package)
    report = {
        "schema_version": "quantstack.us-corporate-actions-from-raw.v1",
        "status": "passed",
        "instrument_count": len(entries),
        "provider_requested": False,
        "actual_requests": 0,
        "observed_rows": sum(len(item["observed_dates"]) for item in entries),
        "corporate_action_rows": sum(len(item["actions"]) for item in entries),
        "aggregate_packages": 1,
        "aggregate_package_bytes": package_path.stat().st_size,
    }
    atomic_replace_bytes(
        report_path,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def materialize_us_adjustment_from_action_evidence(
    *,
    data_root: Path,
    dataset_version_prefix: str,
    action_package: Path,
    output_root: Path,
) -> dict[str, object]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Install QuantStack with the data extra") from exc

    package = _read_gzip_json(action_package)
    if package.get("schema_version") != "quantstack.us-corporate-actions-evidence.v1":
        raise ValueError("unsupported US corporate-action evidence package")
    evidence = {str(item["symbol"]): item for item in package["entries"]}
    if (
        package.get("strictly_serial") is not True
        or package.get("provider_retries") != 0
        or len(evidence) != package.get("instrument_count")
    ):
        raise ValueError("US corporate-action evidence package is incomplete")
    output_root.mkdir(parents=True, exist_ok=True)
    writers: dict[str, object] = {}
    comparisons: dict[str, dict[str, object]] = {}
    quarantines: list[dict[str, object]] = []
    rows_written = 0
    source_rows = 0
    try:
        raw_instruments = 0
        for symbol, records in iter_us_raw_series(
            data_root, dataset_version_prefix=dataset_version_prefix
        ):
            raw_instruments += 1
            source_rows += len(records)
            item = evidence.get(symbol)
            if item is None:
                raise ValueError(f"missing corporate-action evidence for {symbol}")
            observed = set(item["observed_dates"])
            raw_dates = {str(row["Date"])[:10] for row in records}
            if not raw_dates.issubset(observed):
                raise ValueError(f"corporate-action date coverage is incomplete for {symbol}")
            actions = {str(row["Date"])[:10]: row for row in item["actions"]}
            for row in records:
                action = actions.get(str(row["Date"])[:10], {})
                row["Dividends"] = float(action.get("Dividends", 0.0))
                row["Stock Splits"] = float(action.get("Stock Splits", 0.0))
            instrument_id = str(item["instrument_id"])
            try:
                shadow = derive_us_adjustment_shadow(records)
            except ValueError as exc:
                quarantines.append(
                    {
                        "instrument_id": instrument_id,
                        "symbol": symbol,
                        "reason_code": "invalid_action_semantics",
                        "detail": str(exc),
                    }
                )
                continue
            comparison = compare_us_adjustment_shadow(shadow)
            comparisons[instrument_id] = comparison
            if comparison["provider_alignment_passed"] is not True:
                quarantines.append(
                    {
                        "instrument_id": instrument_id,
                        "symbol": symbol,
                        "reason_code": "provider_alignment_failed",
                        "maximum_difference_bps": comparison[
                            "maximum_action_vs_provider_difference_bps"
                        ],
                    }
                )
                continue
            venue, canonical_symbol = instrument_id.split(".", 1)
            batches = {
                "split-adjusted.parquet": "split_adjusted",
                "total-return-action-derived.parquet": "total_return_action_derived",
                "total-return-provider-shadow.parquet": "total_return_provider_shadow",
            }
            for filename, layer in batches.items():
                adjustment = "split_adjusted" if layer == "split_adjusted" else "total_return"
                rows = [
                    {
                        "instrument_id": instrument_id,
                        "symbol": canonical_symbol,
                        "venue": venue,
                        "trading_date": row["trading_date"],
                        "ts_event": datetime.combine(
                            row["trading_date"],
                            time(16),
                            ZoneInfo("America/New_York"),
                        ).astimezone(UTC),
                        "amount": None,
                        "currency": "USD",
                        **dict(row[layer]),
                        "lineage": {"adjustment": adjustment},
                    }
                    for row in shadow
                ]
                table = pa.Table.from_pylist(rows)
                writer = writers.get(filename)
                if writer is None:
                    writer = pq.ParquetWriter(output_root / filename, table.schema)
                    writers[filename] = writer
                writer.write_table(table)
            rows_written += len(shadow)
    finally:
        for writer in writers.values():
            writer.close()

    aligned = sum(bool(item["provider_alignment_passed"]) for item in comparisons.values())
    misaligned = len(comparisons) - aligned
    report = {
        "schema_version": "quantstack.us-adjustment-shadow-materialization.v2",
        "status": "passed" if rows_written and aligned else "blocked",
        "source_instruments": raw_instruments,
        "source_rows": source_rows,
        "instruments": aligned,
        "rows": rows_written,
        "action_evidence_complete": True,
        "provider_aligned_instruments": aligned,
        "provider_misaligned_instruments": misaligned,
        "adjustment_quarantined_instruments": len(quarantines),
        "quarantines": quarantines,
        "source_instruments_without_usable_raw": len(evidence) - raw_instruments,
        "raw_or_normalized_overwritten": False,
        "catalog_published": False,
        "usable_for_indicators_or_scans": False,
        "comparisons": comparisons,
    }
    atomic_replace_bytes(
        output_root / "adjustment-shadow-report.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report
