from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from copy import deepcopy
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from .models import DatasetKind, DatasetLayer, DatasetManifest, DatasetQualityMetadata
from .publication import (
    CatalogPublisher,
    ConsumerEligibilityPolicy,
    ProducerEnvironment,
    PublicationArtifact,
    PublicationBundle,
    PublicationReceipt,
    load_publication_bundle,
    publication_state_for,
)
from .storage.atomic import (
    atomic_replace_bytes,
    atomic_write_immutable_bytes,
    atomic_write_immutable_file,
)


def _number(row: Mapping[str, object], field: str, *, default: float = 0.0) -> float:
    value = row.get(field, default)
    if not isinstance(value, (int, float)):
        raise ValueError(f"US adjustment field {field} must be numeric")
    return float(value)


def derive_us_adjustment_shadow(
    records: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Build explicit Yahoo split-basis and total-return shadows without mutating Raw."""

    source = [deepcopy(dict(item)) for item in records]
    source.sort(key=lambda item: str(item["Date"])[:10])
    dates = [date.fromisoformat(str(item["Date"])[:10]) for item in source]
    if not source or len(dates) != len(set(dates)):
        raise ValueError("US adjustment input must contain unique daily rows")
    for row in source:
        for field in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
            _number(row, field)
        split = _number(row, "Stock Splits")
        dividend = _number(row, "Dividends")
        if split < 0 or dividend < 0:
            raise ValueError("US corporate actions must be non-negative")
        if split not in (0.0, 1.0) and split <= 0:
            raise ValueError("US split ratio must be positive")

    action_total_factors = [1.0] * len(source)
    future_total = 1.0
    for index in range(len(source) - 1, -1, -1):
        action_total_factors[index] = future_total
        dividend = _number(source[index], "Dividends")
        if dividend:
            if index == 0:
                # The ex-date event only adjusts rows before this date.  At the
                # left edge of a bounded window there are no such rows to
                # materialize, so retain the event as evidence without
                # inventing an unavailable preceding close.
                continue
            preceding_close = _number(source[index - 1], "Close")
            if not 0 <= dividend < preceding_close:
                raise ValueError("cash dividend is inconsistent with the preceding close")
            future_total *= (preceding_close - dividend) / preceding_close

    result: list[dict[str, object]] = []
    for index, row in enumerate(source):
        close = _number(row, "Close")
        adjusted_close = _number(row, "Adj Close")
        if close <= 0 or adjusted_close <= 0:
            raise ValueError("US adjustment prices must be positive")
        provider_factor = adjusted_close / close
        split_prices = {
            field.lower().replace(" ", "_"): _number(row, field)
            for field in ("Open", "High", "Low", "Close")
        }
        provider_prices = {
            field.lower().replace(" ", "_"): _number(row, field) * provider_factor
            for field in ("Open", "High", "Low", "Close")
        }
        action_prices = {
            field.lower().replace(" ", "_"): _number(row, field) * action_total_factors[index]
            for field in ("Open", "High", "Low", "Close")
        }
        result.append(
            {
                "trading_date": dates[index],
                "raw": row,
                "split_adjusted": {
                    **split_prices,
                    "volume": _number(row, "Volume"),
                    "price_factor": 1.0,
                    "volume_factor": 1.0,
                    "semantics": "provider_ohlcv_already_split_adjusted",
                },
                "total_return_action_derived": {
                    **action_prices,
                    "volume": _number(row, "Volume"),
                    "price_factor": action_total_factors[index],
                    "semantics": "provider_split_basis_plus_cash_dividend_total_return",
                },
                "total_return_provider_shadow": {
                    **provider_prices,
                    "volume": _number(row, "Volume"),
                    "price_factor": provider_factor,
                    "semantics": "provider_adj_close_ratio_shadow",
                },
                "corporate_actions": {
                    "cash_dividend": _number(row, "Dividends"),
                    "split_ratio_new_shares_per_old_share": _number(row, "Stock Splits"),
                },
            }
        )
    return result


def compare_us_adjustment_shadow(rows: Iterable[Mapping[str, object]]) -> dict[str, object]:
    materialized = list(rows)
    if not materialized:
        raise ValueError("US adjustment comparison requires rows")
    action_close = [
        float(dict(item["total_return_action_derived"])["close"]) for item in materialized
    ]
    provider_close = [
        float(dict(item["total_return_provider_shadow"])["close"]) for item in materialized
    ]
    action_scale = provider_close[-1] / action_close[-1]
    differences_bps = [
        abs((action * action_scale / provider) - 1.0) * 10_000
        for action, provider in zip(action_close, provider_close, strict=True)
    ]
    action_rows = [
        item
        for item in materialized
        if dict(item["corporate_actions"])["cash_dividend"]
        or dict(item["corporate_actions"])["split_ratio_new_shares_per_old_share"] not in (0.0, 1.0)
    ]
    maximum = max(differences_bps)
    return {
        "schema_version": "quantstack.us-adjustment-shadow-comparison.v1",
        "rows": len(materialized),
        "corporate_action_rows": len(action_rows),
        "maximum_action_vs_provider_difference_bps": maximum,
        "mean_action_vs_provider_difference_bps": sum(differences_bps) / len(differences_bps),
        "provider_alignment_passed": maximum <= 1.0,
        "usable_for_indicators_or_scans": False,
    }


def materialize_us_adjustment_shadow(
    series: Mapping[str, Iterable[Mapping[str, object]]], output_root: Path
) -> dict[str, object]:
    """Persist three isolated Parquet shadows and a fail-closed semantic report."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Install QuantStack with the data extra") from exc

    output_root.mkdir(parents=True, exist_ok=True)
    split_rows: list[dict[str, object]] = []
    action_rows: list[dict[str, object]] = []
    provider_rows: list[dict[str, object]] = []
    comparisons: dict[str, dict[str, object]] = {}
    action_evidence_complete = True
    for instrument_id, raw_records in sorted(series.items()):
        if "." not in instrument_id:
            raise ValueError("US adjustment series keys must be venue-qualified")
        venue, symbol = instrument_id.split(".", 1)
        if venue not in {"XNAS", "XNYS"} or not symbol:
            raise ValueError("US adjustment series key has an unsupported venue")
        materialized_raw = [dict(item) for item in raw_records]
        action_evidence_complete = action_evidence_complete and all(
            "Dividends" in item and "Stock Splits" in item for item in materialized_raw
        )
        shadow = derive_us_adjustment_shadow(materialized_raw)
        comparisons[instrument_id] = compare_us_adjustment_shadow(shadow)
        for item in shadow:
            common = {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "venue": venue,
                "trading_date": item["trading_date"],
                "ts_event": datetime.combine(
                    item["trading_date"], time(16), ZoneInfo("America/New_York")
                ).astimezone(UTC),
                "amount": None,
                "currency": "USD",
            }
            split_rows.append(
                {
                    **common,
                    **dict(item["split_adjusted"]),
                    "lineage": {"adjustment": "split_adjusted"},
                }
            )
            action_rows.append(
                {
                    **common,
                    **dict(item["total_return_action_derived"]),
                    "lineage": {"adjustment": "total_return"},
                }
            )
            provider_rows.append(
                {
                    **common,
                    **dict(item["total_return_provider_shadow"]),
                    "lineage": {"adjustment": "total_return"},
                }
            )
    if not split_rows:
        raise ValueError("US adjustment materialization requires at least one series")

    def write_parquet(name: str, rows: list[dict[str, object]]) -> None:
        destination = output_root / name
        temporary = output_root / f".{name}.tmp"
        pq.write_table(pa.Table.from_pylist(rows), temporary)
        temporary.replace(destination)

    write_parquet("split-adjusted.parquet", split_rows)
    write_parquet("total-return-action-derived.parquet", action_rows)
    write_parquet("total-return-provider-shadow.parquet", provider_rows)
    aligned = sum(bool(item["provider_alignment_passed"]) for item in comparisons.values())
    report = {
        "schema_version": "quantstack.us-adjustment-shadow-materialization.v1",
        "status": (
            "passed" if action_evidence_complete and aligned == len(comparisons) else "blocked"
        ),
        "instruments": len(comparisons),
        "rows": len(split_rows),
        "action_evidence_complete": action_evidence_complete,
        "provider_aligned_instruments": aligned,
        "provider_misaligned_instruments": len(comparisons) - aligned,
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


def publish_us_adjustment_shadow(
    *,
    shadow_root: Path,
    data_root: Path,
    dataset_id: str,
    dataset_version: str,
    environment_id: str = "us-adjustment-shadow-publisher",
) -> tuple[PublicationReceipt, dict[str, object]]:
    """Publish one passed derivative as an explicit, research-only Batch version."""

    import errno
    import hashlib
    import os
    import shutil

    import duckdb

    from .us_total_return_dataset import load_us_total_return_dataset, us_total_return_relation_sql

    derivatives = {
        "us_equity_split_adjusted_shadow": (
            "split-adjusted.parquet",
            "split_adjusted",
        ),
        "us_equity_total_return_shadow": (
            "total-return-action-derived.parquet",
            "total_return",
        ),
        "us_equity_split_adjusted": (
            "split-adjusted.parquet",
            "split_adjusted",
        ),
        "us_equity_total_return": (
            "total-return-action-derived.parquet",
            "total_return",
        ),
    }
    if dataset_id not in derivatives:
        raise ValueError("unsupported US adjustment shadow dataset")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", dataset_version) is None:
        raise ValueError("US shadow dataset version contains unsafe path characters")
    shadow_root = shadow_root.expanduser().resolve()
    data_root = data_root.expanduser().resolve()
    report_path = shadow_root / "adjustment-shadow-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("status") != "passed" or report.get("action_evidence_complete") is not True:
        raise ValueError("US adjustment shadow must pass with complete action evidence")
    source_name, adjustment = derivatives[dataset_id]
    source_path = shadow_root / source_name
    if not source_path.is_file():
        raise ValueError("US adjustment derivative Parquet is missing")

    def digest(path: Path) -> str:
        value = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                value.update(chunk)
        return value.hexdigest()

    target_root = data_root / "normalized/bars_1d/market=US" / f"version={dataset_version}"
    evidence_root = target_root / "evidence"
    bars_path = target_root / "bars.parquet"
    quality_path = evidence_root / "quality.json"
    source_report_path = evidence_root / "adjustment-shadow-report.json"
    instruments_path = evidence_root / "instruments.json"
    instrument_parquet_path = (
        data_root
        / "normalized/instruments/market=US"
        / f"version={dataset_version}"
        / "instruments.parquet"
    )
    proposed_path = (
        data_root / "staging/proposed_manifests" / (f"{dataset_id}@{dataset_version}.json")
    )
    instrument_proposed_path = (
        data_root / "staging/proposed_manifests" / f"instruments_us@{dataset_version}.json"
    )
    proof: dict[str, object] = {}

    def copy_bars(temporary: Path) -> None:
        try:
            temporary.unlink()
            os.link(source_path, temporary)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            shutil.copyfile(source_path, temporary)

    def validate_bars(candidate: Path) -> None:
        dataset = load_us_total_return_dataset(candidate)
        if (
            dataset.row_count is not None
            and dataset.instrument_count is not None
            and dataset.watermark is not None
        ):
            proof.update(
                row_count=dataset.row_count,
                instrument_count=dataset.instrument_count,
                first_date=str(report.get("first_date", report.get("base_first_date", "unknown"))),
                last_date=dataset.watermark,
            )
            return
        relation = us_total_return_relation_sql(candidate)
        connection = duckdb.connect()
        try:
            row = connection.execute(
                "SELECT count(*), count(DISTINCT (instrument_id, trading_date)), "
                "count(DISTINCT instrument_id), min(trading_date), max(trading_date), "
                "count(DISTINCT lineage.adjustment), min(lineage.adjustment), "
                "count(*) FILTER (WHERE NOT (high >= greatest(open, close) "
                "AND low <= least(open, close) AND low <= high)) "
                f"FROM {relation}",
            ).fetchone()
        finally:
            connection.close()
        if row is None or row[0] != row[1] or row[2] < 1:
            raise ValueError("US adjustment derivative coverage is invalid")
        if row[5:] != (1, adjustment, 0):
            raise ValueError("US adjustment derivative semantics or OHLC shape is invalid")
        proof.update(
            row_count=int(row[0]),
            instrument_count=int(row[2]),
            first_date=row[3].isoformat(),
            last_date=row[4].isoformat(),
        )

    created = atomic_write_immutable_file(
        bars_path,
        write=copy_bars,
        validate=validate_bars,
        equivalent=lambda current, candidate: digest(current) == digest(candidate),
        artifact_kind="US adjustment research shadow Parquet",
    )
    if not proof:
        validate_bars(bars_path)
    atomic_write_immutable_bytes(
        source_report_path,
        report_path.read_bytes(),
        artifact_kind="US adjustment research source report",
    )
    dataset = load_us_total_return_dataset(bars_path)
    relation = us_total_return_relation_sql(bars_path)
    connection = duckdb.connect()
    try:
        if dataset.latest is not None:
            instruments = [
                {
                    "instrument_id": row[0],
                    "symbol": str(row[0]).split(".", maxsplit=1)[1],
                    "venue": str(row[0]).split(".", maxsplit=1)[0],
                }
                for row in connection.execute(
                    "SELECT instrument_id FROM read_parquet(?) ORDER BY instrument_id",
                    [str(dataset.latest)],
                ).fetchall()
            ]
        else:
            instruments = [
                {"instrument_id": row[0], "symbol": row[1], "venue": row[2]}
                for row in connection.execute(
                    f"SELECT DISTINCT instrument_id, symbol, venue FROM {relation} "
                    "ORDER BY instrument_id",
                ).fetchall()
            ]
    finally:
        connection.close()
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Install QuantStack with the data extra") from exc

    instrument_rows = [
        {
            **item,
            "currency": "USD",
            "timezone": "America/New_York",
            "active": True,
        }
        for item in instruments
    ]

    def write_instruments(temporary: Path) -> None:
        pq.write_table(pa.Table.from_pylist(instrument_rows), temporary)

    atomic_write_immutable_file(
        instrument_parquet_path,
        write=write_instruments,
        validate=lambda candidate: pq.read_metadata(candidate),
        equivalent=lambda current, candidate: digest(current) == digest(candidate),
        artifact_kind="US adjustment research instruments Parquet",
    )
    candidate_release = not dataset_id.endswith("_shadow")
    release_scope = (
        "validated SS candidate; immutable rollback remains available"
        if candidate_release
        else "research shadow only; production routing remains unchanged"
    )
    quality = {
        "schema_version": "1.0.0",
        "status": "passed",
        "checks": [
            "corporate_action_evidence_complete",
            "instrument_date_unique",
            "adjustment_semantics_explicit",
            "ohlc_shape_valid",
        ],
        "warnings": [release_scope],
    }
    for path, payload in (
        (instruments_path, {"schema_version": "1.0.0", "rows": instruments}),
        (quality_path, quality),
    ):
        atomic_write_immutable_bytes(
            path,
            (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
            artifact_kind="US adjustment research evidence",
        )
    created_at = datetime.now(UTC)
    if proposed_path.is_file():
        existing_manifest = DatasetManifest.model_validate_json(
            proposed_path.read_text(encoding="utf-8")
        )
        if (
            existing_manifest.dataset_id != dataset_id
            or existing_manifest.version != dataset_version
        ):
            raise ValueError("US adjustment proposed manifest identity conflicts")
        created_at = existing_manifest.created_at
    manifest = DatasetManifest(
        dataset_id=dataset_id,
        version=dataset_version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="US",
        provider="yfinance",
        provider_version="1.5.2",
        adapter_version="phase-2.11b",
        native_schema_version="daily-ohlcv-actions-v2",
        frequency="1d",
        schema_ref="quant-stack://schemas/us-equity-adjustment-shadow-v1",
        storage_uri=f"quant-data://{bars_path.relative_to(data_root).as_posix()}",
        created_at=created_at,
        available_at_policy="after_session_close_and_quality_acceptance",
        revision_policy="immutable",
        row_count=int(proof["row_count"]),
        sha256=digest(bars_path),
        quality_status="passed",
        quality=DatasetQualityMetadata(
            status="passed",
            checks=quality["checks"],
            warnings=quality["warnings"],
            report_uri=f"quant-data://{quality_path.relative_to(data_root).as_posix()}",
        ),
        license_scope="research",
        source_url="https://finance.yahoo.com/",
        raw_storage_uri=(f"quant-data://{source_report_path.relative_to(data_root).as_posix()}"),
        job_id=f"us-adjustment-shadow-{dataset_version}",
    )
    instrument_manifest = DatasetManifest(
        dataset_id="instruments_us",
        version=dataset_version,
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.INSTRUMENTS,
        market="US",
        provider="yfinance",
        provider_version="1.5.2",
        adapter_version="phase-2.11b",
        native_schema_version="us-core-instruments-v1",
        frequency="1d",
        schema_ref="quant-stack://schemas/instruments-v1",
        storage_uri=(f"quant-data://{instrument_parquet_path.relative_to(data_root).as_posix()}"),
        created_at=created_at,
        available_at_policy="after_session_close_and_quality_acceptance",
        revision_policy="immutable",
        row_count=len(instrument_rows),
        sha256=digest(instrument_parquet_path),
        quality_status="passed",
        quality=DatasetQualityMetadata(
            status="passed",
            checks=["instrument_identity_unique", "research_shadow_only"],
            warnings=quality["warnings"],
            report_uri=f"quant-data://{quality_path.relative_to(data_root).as_posix()}",
        ),
        license_scope="research",
        source_url="https://finance.yahoo.com/",
        raw_storage_uri=(f"quant-data://{source_report_path.relative_to(data_root).as_posix()}"),
        job_id=f"us-adjustment-shadow-instruments-{dataset_version}",
    )
    atomic_write_immutable_bytes(
        proposed_path,
        (json.dumps(manifest.model_dump(mode="json"), indent=2) + "\n").encode(),
        artifact_kind="US adjustment proposed manifest",
    )
    atomic_write_immutable_bytes(
        instrument_proposed_path,
        (json.dumps(instrument_manifest.model_dump(mode="json"), indent=2) + "\n").encode(),
        artifact_kind="US adjustment instrument proposed manifest",
    )

    def artifact(
        path: Path, kind: str, *, artifact_dataset_id: str = dataset_id
    ) -> PublicationArtifact:
        return PublicationArtifact(
            uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
            kind=kind,
            size_bytes=path.stat().st_size,
            sha256=digest(path),
            dataset_id=artifact_dataset_id,
            dataset_version=dataset_version,
        )

    policy = ConsumerEligibilityPolicy(
        route_class="canonical",
        shadow_allowed=True,
        candidate_allowed=candidate_release,
        reason=(
            "US adjusted bars passed the explicit-version SS candidate gate"
            if candidate_release
            else "US adjusted bars passed for explicit-version research shadow only"
        ),
    )
    prior_bundle = None
    bundles_root = data_root / "catalog/publication_bundles"
    if bundles_root.is_dir():
        for path in bundles_root.glob("*.json"):
            candidate = load_publication_bundle(path)
            if (candidate.dataset_id, candidate.dataset_version) == (
                dataset_id,
                dataset_version,
            ):
                prior_bundle = candidate
                break
    producer_environment = (
        prior_bundle.producer_environment
        if prior_bundle is not None
        else ProducerEnvironment.current(environment_id)
    )
    bundle = PublicationBundle(
        schema_version="1.1.0",
        task_id=f"us-adjustment-shadow-{dataset_version}",
        attempt_id="attempt-1",
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        provider="yfinance",
        endpoint="adjustment_shadow_derivative",
        artifacts=(
            artifact(source_report_path, "raw"),
            artifact(
                instrument_parquet_path,
                "instruments",
                artifact_dataset_id="instruments_us",
            ),
            artifact(bars_path, "bars"),
            artifact(quality_path, "quality"),
            artifact(proposed_path, "proposed_manifest"),
            artifact(
                instrument_proposed_path,
                "proposed_manifest",
                artifact_dataset_id="instruments_us",
            ),
        ),
        proposed_manifests=(manifest, instrument_manifest),
        created_at=prior_bundle.created_at if prior_bundle is not None else created_at,
        producer_environment=producer_environment,
        canonical_eligible=True,
        comparison_status="not_required",
        comparison_required=False,
        publication_gate="eligible",
        publication_state=publication_state_for(
            "eligible",
            quality_passed=True,
            comparison_status="not_required",
            consumer_eligibility=policy,
        ),
        consumer_eligibility=policy,
        canonical_source_provider="yfinance",
        canonical_source_endpoint="adjustment_shadow_derivative",
        canonical_selection_reason=(
            "action-complete US adjustment SS candidate derivative"
            if candidate_release
            else "action-complete US adjustment research derivative"
        ),
        selection_rule_id=(
            f"us-{adjustment}-ss-candidate-v1"
            if candidate_release
            else f"us-{adjustment}-research-shadow-v1"
        ),
    )
    publisher = CatalogPublisher(data_root)
    receipt = publisher.publish(bundle)
    replay = publisher.replay_receipt(receipt)
    if not replay.verified:
        raise ValueError("US adjustment publication receipt replay did not verify")
    return receipt, {
        **proof,
        "created": created,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "shadow_eligible": receipt.publication_state.shadow_eligible,
        "candidate_eligible": receipt.publication_state.candidate_eligible,
        "cutover_eligible": receipt.publication_state.cutover_eligible,
        "receipt_replay_verified": replay.verified,
        "production_path_changed": candidate_release,
    }
