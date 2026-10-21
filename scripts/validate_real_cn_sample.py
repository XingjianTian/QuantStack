from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from quant_stack.data.batch import BatchBarsQuery, CanonicalBatchRepository
from quant_stack.data.calendars import ObservedSessionCalendar
from quant_stack.data.evidence import (
    load_market_state_sample,
    load_official_calendar_evidence,
)
from quant_stack.data.ingestion import DataJob, IngestionService, RetryPolicy
from quant_stack.data.models import Instrument, Market
from quant_stack.data.providers.cn import (
    CNProviderDiscrepancyReport,
    ComparisonFailure,
    EvidenceMode,
    ProviderSourceIdentity,
    ProviderTransportIdentity,
)
from quant_stack.data.providers.cn.live import (
    BaoStockLiveProvider,
    TDXReferenceLiveProvider,
    normalize_baostock_live_bars,
    normalize_tdx_reference_live_bars,
)
from quant_stack.data.providers.cn.reliability import require_non_empty
from quant_stack.data.providers.cn.replay import compare_replay_bars
from quant_stack.data.publication import CatalogPublisher, build_publication_bundle
from quant_stack.data.quality import ProviderSchemaValidator
from quant_stack.data.storage import StagingFileSystemStorage


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


def _json_bytes(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()


class _CachedBaoStockProvider(BaoStockLiveProvider):
    def __init__(
        self,
        instruments: Sequence[Instrument],
        records: Sequence[Mapping[str, object]],
    ) -> None:
        super().__init__(instruments)
        self._records = tuple(records)

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        list(self.list_instruments(market, symbols))
        if frequency != "1d":
            raise ValueError("cached controlled provider supports only daily bars")
        yield from self._records


def _identity(
    *,
    provider: str,
    endpoint: str,
    provider_version: str,
    adapter_version: str,
    native_schema_version: str,
    raw_uri: str,
    raw_sha256: str,
    transport_identity: ProviderTransportIdentity | None = None,
) -> ProviderSourceIdentity:
    return ProviderSourceIdentity(
        provider=provider,
        endpoint=endpoint,
        provider_version=provider_version,
        adapter_version=adapter_version,
        native_schema_version=native_schema_version,
        raw_uri=raw_uri,
        raw_sha256=raw_sha256,
        transport_identity=transport_identity,
    )


def _failure_report(
    *,
    primary_identity: ProviderSourceIdentity,
    comparison_identity: ProviderSourceIdentity,
    generated_at: datetime,
    error_class: str,
    failure_stage: str,
) -> CNProviderDiscrepancyReport:
    return CNProviderDiscrepancyReport(
        report_id=(
            f"live-{primary_identity.provider}-vs-{comparison_identity.provider}-unavailable"
        ),
        generated_at=generated_at,
        evidence_mode=EvidenceMode.LIVE,
        fixture_only=False,
        primary_raw_sha256=primary_identity.raw_sha256,
        comparison_raw_sha256=comparison_identity.raw_sha256,
        primary_identity=primary_identity,
        comparison_identity=comparison_identity,
        status="unavailable",
        comparison_required=True,
        failure=ComparisonFailure(error_class=error_class, stage=failure_stage),
        canonical_selection_reason=(
            "required comparison unavailable; no dual-source agreement or source switch"
        ),
    )


def run_validation(
    data_root: Path,
    *,
    start: date,
    end: date,
    dataset_version: str,
    calendar_raw: Path,
    calendar_normalized: Path,
    calendar_raw_uri: str,
    calendar_normalized_uri: str,
    market_state_raw: Path,
    market_state_raw_uri: str,
) -> dict[str, object]:
    generated_at = datetime.now(UTC)
    calendar_evidence = load_official_calendar_evidence(
        raw_path=calendar_raw,
        normalized_path=calendar_normalized,
        observed_at=generated_at,
        raw_uri=calendar_raw_uri,
        normalized_uri=calendar_normalized_uri,
    )
    official_calendar = ObservedSessionCalendar(calendar_evidence)
    official_sessions = tuple(
        item.trading_date for item in official_calendar.sessions("CN", start, end)
    )
    market_state_sample = load_market_state_sample(
        raw_path=market_state_raw,
        raw_uri=market_state_raw_uri,
    )
    storage = StagingFileSystemStorage(data_root)
    baostock_instruments = (
        _instrument("XSHG.600000", "600000", "XSHG", "sh.600000", "sh"),
        _instrument("XSHE.000001", "000001", "XSHE", "sz.000001", "sz"),
    )
    live_baostock = BaoStockLiveProvider(baostock_instruments)
    primary_native_rows = list(
        live_baostock.fetch_bars(
            "CN",
            [item.instrument_id for item in baostock_instruments],
            start,
            end,
        )
    )
    ProviderSchemaValidator().validate(
        provider="baostock",
        records=primary_native_rows,
        schema=live_baostock.bar_schema,
    )
    confirmed_sessions = tuple(
        sorted({date.fromisoformat(str(row["date"])) for row in primary_native_rows})
    )
    if not set(confirmed_sessions).issubset(official_sessions):
        raise ValueError("BaoStock returned dates outside official exchange sessions")
    baostock = _CachedBaoStockProvider(baostock_instruments, primary_native_rows)
    result = IngestionService(
        providers={baostock.name: baostock},
        storage=storage,
        bar_normalizers={baostock.name: normalize_baostock_live_bars},
        provider_schemas={baostock.name: baostock.bar_schema},
        calendar=official_calendar,
        market_state_sample=market_state_sample,
    ).run(
        DataJob(
            dataset="cn_equity_daily",
            dataset_version=dataset_version,
            provider="baostock",
            provider_version="0.9.3",
            provider_endpoint="query_history_k_data_plus",
            adapter_version="real-cn-v1",
            native_schema_version="live-observed-2026-08-03-v1",
            market="CN",
            frequency="1d",
            execution_time=generated_at,
            retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0),
            symbols=[item.instrument_id for item in baostock_instruments],
            start=start,
            end=end,
        )
    )
    primary_rows = storage.read_dataset(result.bar_artifact.uri)

    tdx_instruments = (
        _instrument("XSHG.600000", "600000", "XSHG", "600000", "SH"),
        _instrument("XSHE.000001", "000001", "XSHE", "000001", "SZ"),
    )
    tdx_reference = TDXReferenceLiveProvider(
        tdx_instruments,
        verified_sessions=confirmed_sessions,
    )
    comparison_error_class: str | None = None
    comparison_failure_stage = "provider_fetch"
    comparison_rows: list[dict[str, object]] = []
    comparison_payload: dict[str, object]
    try:
        native_rows = list(
            tdx_reference.fetch_bars(
                "CN",
                [item.instrument_id for item in tdx_instruments],
                start,
                end,
            )
        )
        comparison_failure_stage = "empty_response_gate"
        require_non_empty(native_rows)
        comparison_failure_stage = "schema_validation"
        ProviderSchemaValidator().validate(
            provider="tdx_reference", records=native_rows, schema=tdx_reference.bar_schema
        )
        comparison_failure_stage = "normalization"
        comparison_dates = tuple(
            sorted({date.fromisoformat(str(row["datetime"])[:10]) for row in native_rows})
        )
        if not set(comparison_dates).issubset(official_sessions):
            raise ValueError("TDX returned dates outside official exchange sessions")
        comparison_rows = [
            dict(item)
            for item in normalize_tdx_reference_live_bars(
                native_rows,
                {item.symbol: item for item in tdx_instruments},
                official_calendar,
            )
        ]
        comparison_payload = {
            "provider": "tdx_reference",
            "endpoint": "standard_market_daily_bars",
            "provider_version": "0.2.7",
            "request": {
                "symbols": [item.instrument_id for item in tdx_instruments],
                "start": start.isoformat(),
                "end": end.isoformat(),
                "frequency": "daily",
                "adjustment": "none",
            },
            "transport_audit": tdx_reference.transport_audit(),
            "records": native_rows,
        }
    except Exception as exc:
        comparison_error_class = type(exc).__name__
        comparison_payload = {
            "provider": "tdx_reference",
            "endpoint": "standard_market_daily_bars",
            "provider_version": "0.2.7",
            "request": {
                "symbol_count": len(tdx_instruments),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "frequency": "daily",
                "adjustment": "none",
            },
            "outcome": "provider_unavailable",
            "error_class": comparison_error_class,
            "transport_audit": tdx_reference.transport_audit(),
        }

    comparison_artifact = storage.write_raw_object(
        Path("raw/CN/tdx_reference/cn_equity_daily")
        / f"execution_date={generated_at:%Y-%m-%d}"
        / "controlled-overlap.json",
        _json_bytes(comparison_payload),
    )
    primary_identity = _identity(
        provider="baostock",
        endpoint="query_history_k_data_plus",
        provider_version="0.9.3",
        adapter_version="real-cn-v1",
        native_schema_version="live-observed-2026-08-03-v1",
        raw_uri=result.raw_artifact.uri,
        raw_sha256=result.raw_artifact.sha256,
    )
    comparison_identity = _identity(
        provider="tdx_reference",
        endpoint="standard_market_daily_bars",
        provider_version="0.2.7",
        adapter_version="real-cn-tdx-v1",
        native_schema_version="live-observed-2026-08-03-v1",
        raw_uri=comparison_artifact.uri,
        raw_sha256=comparison_artifact.sha256,
        transport_identity=ProviderTransportIdentity.model_validate(
            tdx_reference.transport_audit()
        ),
    )
    for row in comparison_rows:
        lineage = dict(row.get("lineage") or {})
        row["lineage"] = {
            **lineage,
            "raw_uri": comparison_artifact.uri,
            "raw_sha256": comparison_artifact.sha256,
        }
    if comparison_error_class is None:
        overlap_ids = {item.instrument_id for item in baostock_instruments}
        overlap_comparison_rows = [
            row for row in comparison_rows if row.get("instrument_id") in overlap_ids
        ]
        discrepancy = compare_replay_bars(
            primary_rows,
            overlap_comparison_rows,
            primary_provider="baostock",
            comparison_provider="tdx_reference",
            primary_raw_uri=result.raw_artifact.uri,
            comparison_raw_uri=comparison_artifact.uri,
            dataset_partition="market=CN/frequency=1d/controlled-live-sample",
            generated_at=generated_at,
            evidence_mode=EvidenceMode.LIVE,
            primary_raw_sha256=result.raw_artifact.sha256,
            comparison_raw_sha256=comparison_artifact.sha256,
            primary_identity=primary_identity,
            comparison_identity=comparison_identity,
            comparison_required=True,
        )
    else:
        discrepancy = _failure_report(
            primary_identity=primary_identity,
            comparison_identity=comparison_identity,
            generated_at=generated_at,
            error_class=comparison_error_class,
            failure_stage=comparison_failure_stage,
        )
    discrepancy_artifact = storage.write_quality_report(
        Path("quality/cn_equity_daily")
        / f"version={dataset_version}"
        / "live-provider-discrepancy.json",
        _json_bytes(discrepancy.model_dump(mode="json")),
    )

    comparison_status = {
        "passed": "passed",
        "unavailable": "unavailable",
        "quarantined": (
            "source_identity_drift"
            if any("endpoint_source_drift" in item.differing_fields for item in discrepancy.records)
            else "serious_discrepancy"
        ),
    }[discrepancy.status]
    bundle = build_publication_bundle(
        result=result,
        task_id=f"real-cn-sample-{result.job_id}",
        attempt_id="controlled-live-1",
        endpoint="query_history_k_data_plus",
        created_at=generated_at,
        environment_id="controlled-real-cn-validation",
        discrepancy_report_uri=discrepancy_artifact.uri,
        comparison_status=comparison_status,
        comparison_required=True,
        canonical_selection_reason=(
            "BaoStock is the approved controlled XSHG/XSHE bootstrap route; "
            f"TDX required comparison outcome is {discrepancy.status} and cannot overwrite "
            "canonical"
        ),
        selection_rule_id="cn-xshg-xshe-bootstrap-baostock-tdx-v4",
    )
    receipt = None
    replay = None
    batch = None
    metadata = None
    staging_cleaned_before_replay = False
    if bundle.publication_state is not None and bundle.publication_state.catalog_published:
        publisher = CatalogPublisher(data_root)
        receipt = publisher.publish(bundle)
        staging_root = data_root / "staging"
        if staging_root.is_dir():
            shutil.rmtree(staging_root)
            staging_cleaned_before_replay = True
        replay = publisher.replay_receipt(receipt)
        repository = CanonicalBatchRepository(data_root)
        query = BatchBarsQuery(
            instrument_ids=tuple(item.instrument_id for item in baostock_instruments),
            start=start,
            end=end,
            frequency="1d",
            market="CN",
            dataset_id="cn_equity_daily",
            dataset_version=dataset_version,
            page_size=100,
        )
        plan = repository.prepare_bars(query)
        try:
            batch = repository.execute_bars(plan, query)
        finally:
            plan.close()
        metadata_plan = repository.prepare_metadata("cn_equity_daily", dataset_version)
        try:
            metadata = repository.metadata_response(metadata_plan)
        finally:
            metadata_plan.close()

    return {
        "schema_version": "1.2.0",
        "dataset_id": result.bar_manifest.dataset_id,
        "dataset_version": result.bar_manifest.version,
        "validation_scope": {
            "markets": ["XSHG", "XSHE"],
            "bjse_outside_publication_scope": True,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "full_market": False,
            "validation_stage": "bounded_sample_validated",
            "sample_only": True,
            "full_history_validated": False,
            "universe_validated": False,
            "production_ready": False,
        },
        "primary": {
            "provider": "baostock",
            "provider_version": "0.9.3",
            "endpoint": "query_history_k_data_plus",
            "native_schema_version": "live-observed-2026-08-03-v1",
            "volume_unit": "share",
            "volume_multiplier_to_shares": 1,
            "adjustment": "unadjusted",
            "timezone": "Asia/Shanghai",
            "rows": result.bar_manifest.row_count,
            "raw_uri": result.raw_artifact.uri,
            "raw_sha256": result.raw_artifact.sha256,
            "quality_status": result.quality_report.quality_status,
        },
        "comparison": {
            "provider": "tdx_reference",
            "provider_version": "0.2.7",
            "endpoint": "standard_market_daily_bars",
            "native_schema_version": "live-observed-2026-08-03-v1",
            "volume_unit": "lot",
            "volume_multiplier_to_shares": 100,
            "canonical_volume_type": "int64_shares",
            "volume_conversion": "Decimal(str(raw_vol))*100_then_exact_integral_check",
            "adjustment": "unadjusted",
            "rows": len(comparison_rows),
            "raw_uri": comparison_artifact.uri,
            "raw_sha256": comparison_artifact.sha256,
            "error_class": comparison_error_class,
            "failure_stage": (
                comparison_failure_stage if comparison_error_class is not None else None
            ),
            "contract_evidence": (
                "live_observed" if comparison_error_class is None else "docs_fixture_derived"
            ),
            "transport_audit": tdx_reference.transport_audit(),
            "explicitly_unsupported": ["BJSE"],
        },
        "optional_reference": {
            "provider": "akshare",
            "endpoint": "stock_zh_a_hist",
            "coverage_candidate": ["XSHG", "XSHE", "BJSE"],
            "role": "optional_reference_and_bjse_candidate",
            "required_for_xshg_xshe_publication": False,
            "bjse_publication_status": "blocked",
            "automatic_fallback": False,
        },
        "discrepancy": {
            "status": discrepancy.status,
            "record_count": len(discrepancy.records),
            "report_uri": discrepancy_artifact.uri,
            "report_sha256": discrepancy_artifact.sha256,
            "automatic_switching": False,
            "comparison_required": discrepancy.comparison_required,
        },
        "publication": {
            "gate": bundle.publication_gate,
            "state": (
                bundle.publication_state.model_dump(mode="json")
                if bundle.publication_state is not None
                else None
            ),
            "catalog_published": receipt is not None,
            "bundle_id": bundle.bundle_id,
            "bundle_uri": receipt.bundle_uri if receipt is not None else None,
            "bundle_sha256": receipt.bundle_sha256 if receipt is not None else None,
            "receipt_id": receipt.receipt_id if receipt is not None else None,
            "decision_uri": receipt.decision_uri if receipt is not None else None,
            "decision_sha256": receipt.decision_sha256 if receipt is not None else None,
            "publication_evidence_fingerprint": (
                receipt.publication_evidence_fingerprint if receipt is not None else None
            ),
            "manifest_count": len(receipt.manifest_uris) if receipt is not None else 0,
            "batch_rows": len(batch.items) if batch is not None else 0,
            "batch_volume_unit": "shares" if batch is not None else None,
            "snapshot_token_sha256": (
                hashlib.sha256(batch.snapshot_token.encode()).hexdigest()
                if batch is not None
                else None
            ),
            "canonical_source_provider": bundle.canonical_source_provider,
            "canonical_source_endpoint": bundle.canonical_source_endpoint,
            "canonical_selection_reason": bundle.canonical_selection_reason,
            "discrepancy_report_uri": bundle.discrepancy_report_uri,
            "discrepancy_report_sha256": bundle.discrepancy_report_sha256,
            "cutover_eligible": False,
            "staging_cleaned_before_replay": staging_cleaned_before_replay,
            "receipt_replay": (replay.model_dump(mode="json") if replay is not None else None),
            "metadata_etag": metadata.etag if metadata is not None else None,
            "metadata_publication_gate": (
                metadata.publication_gate if metadata is not None else None
            ),
            "metadata_comparison_complete": (
                metadata.comparison_complete if metadata is not None else None
            ),
            "metadata_candidate_eligible": (
                metadata.candidate_eligible if metadata is not None else None
            ),
        },
        "calendar": calendar_evidence.model_dump(mode="json"),
        "market_state_sample": {
            "version": market_state_sample.version,
            "finite_sample": market_state_sample.finite_sample,
            "raw_evidence_sha256": market_state_sample.raw_evidence_sha256,
            "status_counts": {
                status: sum(item.status == status for item in market_state_sample.evidence)
                for status in ("verified", "partial", "unavailable", "blocked")
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Controlled real CN sample validation")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 4, 29))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 5, 7))
    parser.add_argument("--dataset-version", default="2026-08-04.calendar-state.1")
    parser.add_argument("--calendar-raw", type=Path, required=True)
    parser.add_argument("--calendar-normalized", type=Path, required=True)
    parser.add_argument("--calendar-raw-uri", required=True)
    parser.add_argument("--calendar-normalized-uri", required=True)
    parser.add_argument("--market-state-raw", type=Path, required=True)
    parser.add_argument("--market-state-raw-uri", required=True)
    args = parser.parse_args()
    summary = run_validation(
        args.data_root,
        start=args.start,
        end=args.end,
        dataset_version=args.dataset_version,
        calendar_raw=args.calendar_raw,
        calendar_normalized=args.calendar_normalized,
        calendar_raw_uri=args.calendar_raw_uri,
        calendar_normalized_uri=args.calendar_normalized_uri,
        market_state_raw=args.market_state_raw,
        market_state_raw_uri=args.market_state_raw_uri,
    )
    summary_path = args.data_root / "validation-summary.json"
    StagingFileSystemStorage(args.data_root).write_quality_report(
        summary_path.relative_to(args.data_root), _json_bytes(summary)
    )
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    if not summary["publication"]["catalog_published"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
