from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

from ..calendars import SyntheticMarketCalendar
from ..interfaces import DataProvider, DataStorage, MarketCalendarProvider, StoredArtifact
from ..models import (
    CalendarEvidence,
    DatasetManifest,
    DatasetQualityMetadata,
    Instrument,
    MarketStateSample,
)
from ..normalizers import normalize_bars
from ..quality import (
    DataQualityError,
    DatasetQualityReport,
    ProviderNativeSchema,
    ProviderSchemaValidator,
    QualityChecker,
)
from ..semantics import DEFAULT_DATASET_SEMANTICS
from .contracts import ProviderContractMismatchError, ProviderRuntimeContract
from .jobs import DataJob, IngestionStatus

BarNormalizer = Callable[
    [list[Mapping[str, object]], Mapping[str, Instrument], MarketCalendarProvider],
    list[Mapping[str, object]],
]


@dataclass(frozen=True)
class IngestionResult:
    job_id: str
    status: IngestionStatus
    attempts: int
    raw_artifact: StoredArtifact
    instrument_artifact: StoredArtifact
    bar_artifact: StoredArtifact
    quality_artifact: StoredArtifact
    raw_manifest: DatasetManifest
    instrument_manifest: DatasetManifest
    bar_manifest: DatasetManifest
    quality_report: DatasetQualityReport
    manifest_paths: tuple[Path, Path, Path]


@dataclass(frozen=True)
class IngestionFailureContext:
    failure_stage: str
    provider: str
    endpoint: str
    job_id: str
    raw_uri: str | None = None
    raw_manifest_uri: str | None = None
    quality_report_uri: str | None = None
    proposed_manifest_uris: tuple[str, ...] = ()
    completed_artifact_uris: tuple[str, ...] = ()


class IngestionStageError(RuntimeError):
    """Preserves the original error and durable evidence accumulated so far."""

    def __init__(self, original_error: Exception, context: IngestionFailureContext) -> None:
        self.original_error = original_error
        self.failure_context = context
        super().__init__(str(original_error))


class IngestionService:
    """Executes declared jobs without owning provider, scheduler or storage implementations."""

    def __init__(
        self,
        *,
        providers: Mapping[str, DataProvider],
        storage: DataStorage,
        bar_normalizer: BarNormalizer = normalize_bars,
        bar_normalizers: Mapping[str, BarNormalizer] | None = None,
        calendar: MarketCalendarProvider | None = None,
        quality_checker: QualityChecker | None = None,
        provider_schemas: Mapping[str, ProviderNativeSchema] | None = None,
        market_state_sample: MarketStateSample | None = None,
    ) -> None:
        self.providers = dict(providers)
        self.storage = storage
        self.bar_normalizer = bar_normalizer
        self.bar_normalizers = dict(bar_normalizers or {})
        self.calendar = calendar or SyntheticMarketCalendar()
        self.quality_checker = quality_checker or QualityChecker()
        self.provider_schemas = dict(provider_schemas or {})
        self.provider_schema_validator = ProviderSchemaValidator()
        self.market_state_sample = market_state_sample

    def _provider_for_job(self, job: DataJob) -> DataProvider:
        try:
            return self.providers[job.provider]
        except KeyError as exc:
            raise ValueError(f"Unknown data provider: {job.provider}") from exc

    @staticmethod
    def _validate_provider_contract(
        job: DataJob, provider: DataProvider
    ) -> ProviderRuntimeContract | None:
        provider_name = getattr(provider, "name", None)
        if job.provider != provider_name:
            raise ProviderContractMismatchError("Provider runtime contract mismatch: provider")
        declared = getattr(provider, "runtime_contract", None)
        pins = {
            "provider": job.provider,
            "endpoint": job.provider_endpoint,
            "provider_version": job.provider_version,
            "adapter_version": job.adapter_version,
            "native_schema_version": job.native_schema_version,
        }
        has_explicit_pin = any(
            value is not None for key, value in pins.items() if key != "provider"
        )
        if declared is None:
            if has_explicit_pin:
                raise ProviderContractMismatchError(
                    f"Provider {job.provider} does not declare a runtime contract"
                )
            return None
        contract = ProviderRuntimeContract.model_validate(declared)
        mismatches = [
            field_name
            for field_name, pinned in pins.items()
            if pinned is not None and pinned != getattr(contract, field_name)
        ]
        if job.market not in contract.supported_markets:
            mismatches.append("market")
        if job.frequency not in contract.supported_frequencies:
            mismatches.append("frequency")
        if mismatches:
            raise ProviderContractMismatchError(
                "Provider runtime contract mismatch: " + ", ".join(sorted(set(mismatches)))
            )
        return contract

    def _fetch(
        self, job: DataJob, provider: DataProvider
    ) -> tuple[list[Instrument], list[Mapping[str, object]], int]:

        last_error: Exception | None = None
        for attempt in range(1, job.retry_policy.max_attempts + 1):
            try:
                instruments = list(provider.list_instruments(job.market, job.symbols))
                records = list(
                    provider.fetch_bars(job.market, job.symbols, job.start, job.end, job.frequency)
                )
                return instruments, records, attempt
            except Exception as exc:  # scheduler-independent retry boundary
                last_error = exc
        raise RuntimeError(
            f"Provider {job.provider} failed after {job.retry_policy.max_attempts} attempts"
        ) from last_error

    def run(self, job: DataJob) -> IngestionResult:
        endpoint = job.provider_endpoint or "unknown"

        def fail(
            stage: str,
            error: Exception,
            *,
            raw_artifact: StoredArtifact | None = None,
            raw_manifest_path: Path | None = None,
            quality_artifact: StoredArtifact | None = None,
            proposed_manifest_paths: tuple[Path, ...] = (),
            completed_artifacts: tuple[StoredArtifact, ...] = (),
        ) -> NoReturn:
            def uri_for(path: Path) -> str:
                return f"quant-data://{path.relative_to(self.storage.data_root).as_posix()}"

            context = IngestionFailureContext(
                failure_stage=stage,
                provider=job.provider,
                endpoint=endpoint,
                job_id=job.job_id,
                raw_uri=raw_artifact.uri if raw_artifact else None,
                raw_manifest_uri=(uri_for(raw_manifest_path) if raw_manifest_path else None),
                quality_report_uri=quality_artifact.uri if quality_artifact else None,
                proposed_manifest_uris=tuple(uri_for(path) for path in proposed_manifest_paths),
                completed_artifact_uris=tuple(item.uri for item in completed_artifacts),
            )
            try:
                error.failure_context = context  # type: ignore[attr-defined]
            except (AttributeError, TypeError):
                raise IngestionStageError(error, context) from error
            raise error

        try:
            provider = self._provider_for_job(job)
            validated_contract = self._validate_provider_contract(job, provider)
        except Exception as exc:
            fail("provider_contract_validation", exc)
        if validated_contract is not None:
            job = job.model_copy(
                update={
                    "provider": validated_contract.provider,
                    "provider_endpoint": validated_contract.endpoint,
                    "provider_version": validated_contract.provider_version,
                    "adapter_version": validated_contract.adapter_version,
                    "native_schema_version": validated_contract.native_schema_version,
                }
            )
            endpoint = validated_contract.endpoint

        if getattr(provider, "requires_verified_calendar", False):
            if not getattr(self.calendar, "verified_for_live_publication", False):
                fail(
                    "calendar_validation",
                    ValueError(
                        "live provider ingestion requires explicit verified calendar evidence"
                    ),
                )
            try:
                list(self.calendar.sessions(job.market, job.start, job.end))
            except Exception as exc:
                fail("calendar_validation", exc)

        try:
            instruments, raw_records, attempts = self._fetch(job, provider)
        except Exception as exc:
            fail("provider_fetch", exc)
        lookup = {item.instrument_id: item for item in instruments}
        lookup.update({item.symbol: item for item in instruments})
        raw_relative = (
            Path("raw")
            / job.market
            / job.provider
            / job.dataset
            / f"execution_date={job.execution_time.astimezone(UTC):%Y-%m-%d}"
            / f"{job.job_id}.json"
        )
        try:
            transport_audit = getattr(provider, "transport_audit", None)
            raw_payload = json.dumps(
                {
                    "job": job.model_dump(mode="json"),
                    "instruments": [item.model_dump(mode="json") for item in instruments],
                    "records": [dict(record) for record in raw_records],
                    **(
                        {"transport_audit": transport_audit()}
                        if callable(transport_audit)
                        else {}
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        except Exception as exc:
            fail("raw_persist", exc)
        try:
            raw_artifact = self.storage.write_raw_object(raw_relative, raw_payload)
        except Exception as exc:
            fail("raw_persist", exc)

        try:
            raw_manifest = self._manifest(
                job=job,
                dataset_id=f"{job.dataset}_raw",
                layer="raw",
                kind="market_bars",
                artifact=raw_artifact,
                row_count=len(raw_records),
                schema_ref=f"provider://{job.provider}/market_bars",
                provider=provider,
            )
        except Exception as exc:
            fail(
                "manifest_stage",
                exc,
                raw_artifact=raw_artifact,
                completed_artifacts=(raw_artifact,),
            )
        try:
            raw_manifest_path = self.storage.register_manifest(raw_manifest)
        except Exception as exc:
            fail("manifest_stage", exc, raw_artifact=raw_artifact)

        provider_schema = self.provider_schemas.get(job.provider) or getattr(
            provider, "bar_schema", None
        )
        if provider_schema is not None:
            try:
                self.provider_schema_validator.validate(
                    provider=job.provider,
                    records=raw_records,
                    schema=provider_schema,
                )
            except Exception as exc:
                fail(
                    "provider_schema_validation",
                    exc,
                    raw_artifact=raw_artifact,
                    raw_manifest_path=raw_manifest_path,
                    proposed_manifest_paths=(raw_manifest_path,),
                    completed_artifacts=(raw_artifact,),
                )

        normalizer = self.bar_normalizers.get(job.provider, self.bar_normalizer)
        try:
            normalized_records = normalizer(raw_records, lookup, self.calendar)
        except Exception as exc:
            fail(
                "normalization",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact,),
            )
        for record in normalized_records:
            lineage = record.get("lineage")
            contract_lineage = (
                {
                    "provider": validated_contract.provider,
                    "endpoint": validated_contract.endpoint,
                    "provider_version": validated_contract.provider_version,
                    "adapter_version": validated_contract.adapter_version,
                    "native_schema_version": validated_contract.native_schema_version,
                }
                if validated_contract is not None
                else {}
            )
            record["lineage"] = {
                **(lineage if isinstance(lineage, Mapping) else {}),
                **contract_lineage,
                "raw_uri": raw_artifact.uri,
                "raw_sha256": raw_artifact.sha256,
            }
        try:
            quality_report = self.quality_checker.check_bars(
                dataset_id=job.dataset,
                provider=job.provider,
                records=normalized_records,
                instruments=lookup,
            )
        except Exception as exc:
            fail(
                "quality_gate",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact,),
            )
        quality_relative = (
            Path("quality") / job.dataset / f"version={job.version}" / "bar_quality_report.json"
        )
        quality_payload = (
            json.dumps(quality_report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
        ).encode()
        try:
            quality_artifact = self.storage.write_quality_report(quality_relative, quality_payload)
        except Exception as exc:
            fail(
                "quality_gate",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact,),
            )
        if quality_report.quality_status == "failed":
            fail(
                "quality_gate",
                DataQualityError(quality_report, quality_artifact.uri),
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact, quality_artifact),
            )
        try:
            bars = sorted(
                self.quality_checker.materialize_bars(normalized_records),
                key=lambda item: (item.instrument_id, item.ts_event),
            )
        except Exception as exc:
            fail(
                "normalization",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact, quality_artifact),
            )

        instrument_relative = (
            Path("normalized/instruments")
            / f"market={job.market}"
            / f"version={job.version}"
            / "instruments.parquet"
        )
        try:
            instrument_artifact = self.storage.write_parquet_dataset(
                instrument_relative,
                [item.model_dump(mode="python") for item in instruments],
            )
        except Exception as exc:
            fail(
                "parquet_write",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact, quality_artifact),
            )
        bar_relative = (
            Path("normalized")
            / f"bars_{job.frequency}"
            / f"market={job.market}"
            / f"version={job.version}"
            / "bars.parquet"
        )
        try:
            bar_artifact = self.storage.write_parquet_dataset(
                bar_relative,
                [item.model_dump(mode="python") for item in bars],
            )
        except Exception as exc:
            fail(
                "parquet_write",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(raw_artifact, quality_artifact, instrument_artifact),
            )

        try:
            raw_version = f"{raw_manifest.dataset_id}@{raw_manifest.version}"
            instrument_manifest = self._manifest(
                job=job,
                dataset_id=f"instruments_{job.market.lower()}",
                layer="normalized",
                kind="instruments",
                artifact=instrument_artifact,
                row_count=len(instruments),
                schema_ref="schemas/data/instrument.schema.json",
                raw_version=raw_version,
                raw_storage_uri=raw_artifact.uri,
                provider=provider,
                quality=DatasetQualityMetadata(status="passed", checks=["instrument_schema_valid"]),
            )
            bar_manifest = self._manifest(
                job=job,
                dataset_id=job.dataset,
                layer="normalized",
                kind="market_bars",
                artifact=bar_artifact,
                row_count=len(bars),
                schema_ref="schemas/data/bar.schema.json",
                raw_version=raw_version,
                raw_storage_uri=raw_artifact.uri,
                provider=provider,
                quality=quality_report.manifest_metadata().model_copy(
                    update={"report_uri": quality_artifact.uri}
                ),
                source_timestamp=max((bar.ts_event for bar in bars), default=None),
                calendar_evidence=getattr(self.calendar, "evidence", None),
                market_state_sample=self.market_state_sample,
            )
            DEFAULT_DATASET_SEMANTICS.validate_manifest_if_registered(bar_manifest)
        except Exception as exc:
            fail(
                "manifest_stage",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=(raw_manifest_path,),
                completed_artifacts=(
                    raw_artifact,
                    quality_artifact,
                    instrument_artifact,
                    bar_artifact,
                ),
            )
        try:
            instrument_manifest_path = self.storage.register_manifest(instrument_manifest)
            bar_manifest_path = self.storage.register_manifest(bar_manifest)
        except Exception as exc:
            staged_paths = tuple(
                path
                for path in (
                    raw_manifest_path,
                    locals().get("instrument_manifest_path"),
                )
                if isinstance(path, Path)
            )
            fail(
                "manifest_stage",
                exc,
                raw_artifact=raw_artifact,
                raw_manifest_path=raw_manifest_path,
                quality_artifact=quality_artifact,
                proposed_manifest_paths=staged_paths,
                completed_artifacts=(
                    raw_artifact,
                    quality_artifact,
                    instrument_artifact,
                    bar_artifact,
                ),
            )
        manifest_paths = (raw_manifest_path, instrument_manifest_path, bar_manifest_path)
        return IngestionResult(
            job_id=job.job_id,
            status=IngestionStatus.COMPLETED,
            attempts=attempts,
            raw_artifact=raw_artifact,
            instrument_artifact=instrument_artifact,
            bar_artifact=bar_artifact,
            quality_artifact=quality_artifact,
            raw_manifest=raw_manifest,
            instrument_manifest=instrument_manifest,
            bar_manifest=bar_manifest,
            quality_report=quality_report,
            manifest_paths=manifest_paths,
        )

    @staticmethod
    def _manifest(
        *,
        job: DataJob,
        dataset_id: str,
        layer: str,
        kind: str,
        artifact: StoredArtifact,
        provider: DataProvider,
        row_count: int,
        schema_ref: str,
        raw_version: str | None = None,
        raw_storage_uri: str | None = None,
        quality: DatasetQualityMetadata | None = None,
        source_timestamp: datetime | None = None,
        calendar_evidence: CalendarEvidence | None = None,
        market_state_sample: MarketStateSample | None = None,
    ) -> DatasetManifest:
        return DatasetManifest(
            manifest_schema_version=(
                "1.3.0"
                if calendar_evidence is not None and market_state_sample is not None
                else "1.2.0"
            ),
            dataset_id=dataset_id,
            version=job.version,
            layer=layer,
            kind=kind,
            market=job.market,
            provider=job.provider,
            provider_version=job.provider_version,
            adapter_version=job.adapter_version,
            native_schema_version=job.native_schema_version,
            frequency=job.frequency,
            schema_ref=schema_ref,
            storage_uri=artifact.uri,
            created_at=job.execution_time.astimezone(UTC),
            available_at_policy=provider.available_at_policy,
            revision_policy="append_only" if layer == "raw" else "immutable",
            row_count=row_count,
            sha256=artifact.sha256,
            quality_status=(quality.status if quality else "passed"),
            quality=quality
            or DatasetQualityMetadata(status="passed", checks=["raw_payload_persisted"]),
            license_scope=provider.license_scope,
            source_url=provider.source_url,
            input_dataset_versions=[raw_version] if raw_version else [],
            raw_storage_uri=raw_storage_uri,
            job_id=job.job_id,
            source_timestamp=source_timestamp,
            available_at=job.execution_time.astimezone(UTC),
            calendar_evidence=calendar_evidence,
            market_state_sample=market_state_sample,
        )
