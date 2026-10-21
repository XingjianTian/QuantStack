from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime

from ..reconciliation import (
    CNDiscrepancyRecord,
    CNProviderDiscrepancyReport,
    DiscrepancyStatus,
    EvidenceMode,
    ProviderSourceIdentity,
)


def _key(record: Mapping[str, object]) -> tuple[str, date]:
    timestamp = record["ts_event"]
    if isinstance(timestamp, datetime):
        trading_date = timestamp.date()
    else:
        trading_date = datetime.fromisoformat(str(timestamp)).date()
    return str(record["instrument_id"]), trading_date


def compare_replay_bars(
    primary: Iterable[Mapping[str, object]],
    comparison: Iterable[Mapping[str, object]],
    *,
    primary_provider: str,
    comparison_provider: str,
    primary_raw_uri: str,
    comparison_raw_uri: str,
    dataset_partition: str = "market=CN/frequency=1d",
    price_tolerance: float = 0.01,
    volume_relative_tolerance: float = 0.005,
    amount_relative_tolerance: float = 0.005,
    generated_at: datetime | None = None,
    evidence_mode: EvidenceMode = EvidenceMode.FIXTURE,
    primary_raw_sha256: str | None = None,
    comparison_raw_sha256: str | None = None,
    primary_identity: ProviderSourceIdentity | None = None,
    comparison_identity: ProviderSourceIdentity | None = None,
    comparison_required: bool = False,
) -> CNProviderDiscrepancyReport:
    """Compare normalized feeds with explicit unit-aware tolerances and Raw lineage."""

    primary_rows = list(primary)
    comparison_rows = list(comparison)
    if not primary_rows or not comparison_rows:
        raise ValueError(
            "comparison requires observed rows from both sources; use unavailable evidence"
        )
    identity_envelope_drift = False
    if evidence_mode is EvidenceMode.LIVE:
        if primary_identity is None or comparison_identity is None:
            raise ValueError("live comparison requires exact source identities")
        identity_envelope_drift = any(
            (
                primary_identity.provider != primary_provider,
                comparison_identity.provider != comparison_provider,
                primary_identity.raw_uri != primary_raw_uri,
                comparison_identity.raw_uri != comparison_raw_uri,
                primary_identity.raw_sha256 != primary_raw_sha256,
                comparison_identity.raw_sha256 != comparison_raw_sha256,
                primary_identity.provider == comparison_identity.provider,
            )
        )
    primary_counts = Counter(_key(row) for row in primary_rows)
    comparison_counts = Counter(_key(row) for row in comparison_rows)
    primary_by_key = {_key(row): row for row in primary_rows}
    comparison_by_key = {_key(row): row for row in comparison_rows}
    differing: list[CNDiscrepancyRecord] = []
    tolerance = (
        f"ohlc_abs<={price_tolerance};volume_relative<={volume_relative_tolerance};"
        f"amount_relative<={amount_relative_tolerance}"
    )
    all_keys = sorted(set(primary_by_key) | set(comparison_by_key))
    for instrument_id, trading_date in all_keys:
        left = primary_by_key.get((instrument_id, trading_date))
        right = comparison_by_key.get((instrument_id, trading_date))
        fields: list[str] = []
        if (
            primary_counts[(instrument_id, trading_date)] > 1
            or comparison_counts[(instrument_id, trading_date)] > 1
        ):
            fields.append("duplicate")
        if left is None or right is None:
            fields.append("missing_date")
            existing = left if left is not None else right
            expected_identity = primary_identity if left is not None else comparison_identity
            if (
                evidence_mode is EvidenceMode.LIVE
                and existing is not None
                and expected_identity is not None
                and not _lineage_matches(dict(existing.get("lineage") or {}), expected_identity)
            ):
                fields.append("endpoint_source_drift")
        else:
            for field in ("open", "high", "low", "close"):
                if abs(float(left[field]) - float(right[field])) > price_tolerance:
                    fields.append(field)
            for field, relative_tolerance in (
                ("volume", volume_relative_tolerance),
                ("amount", amount_relative_tolerance),
            ):
                left_value = left.get(field)
                right_value = right.get(field)
                if left_value is None or right_value is None:
                    if left_value != right_value:
                        fields.append(field)
                    continue
                denominator = max(abs(float(left_value)), abs(float(right_value)), 1.0)
                if abs(float(left_value) - float(right_value)) / denominator > relative_tolerance:
                    fields.append(field)
            if (float(left.get("volume", 0)) == 0) != (float(right.get("volume", 0)) == 0):
                fields.append("suspension_semantics")
            left_lineage = dict(left.get("lineage") or {})
            right_lineage = dict(right.get("lineage") or {})
            if evidence_mode is EvidenceMode.LIVE:
                if primary_identity is None or comparison_identity is None:
                    fields.append("endpoint_source_drift")
                elif not _lineage_matches(left_lineage, primary_identity) or not (
                    _lineage_matches(right_lineage, comparison_identity)
                ):
                    fields.append("endpoint_source_drift")
            elif (
                primary_provider != str(left_lineage.get("provider", ""))
                or comparison_provider != str(right_lineage.get("provider", ""))
                or not left_lineage.get("endpoint")
                or not right_lineage.get("endpoint")
            ):
                fields.append("endpoint_source_drift")
        if identity_envelope_drift:
            fields.append("endpoint_source_drift")
        if fields:
            differing.append(
                CNDiscrepancyRecord(
                    dataset_partition=dataset_partition,
                    instrument_id=instrument_id,
                    trade_date=trading_date,
                    primary_provider=primary_provider,
                    comparison_provider=comparison_provider,
                    primary_raw_uri=primary_raw_uri,
                    comparison_raw_uri=comparison_raw_uri,
                    differing_fields=tuple(dict.fromkeys(fields)),
                    tolerance_policy=tolerance,
                    discrepancy_status=DiscrepancyStatus.OPEN,
                    created_at=generated_at or datetime.now(UTC),
                )
            )
    status = "quarantined" if differing else "passed"
    return CNProviderDiscrepancyReport(
        report_id=f"{evidence_mode.value}-{primary_provider}-vs-{comparison_provider}",
        generated_at=generated_at or datetime.now(UTC),
        evidence_mode=evidence_mode,
        fixture_only=evidence_mode is EvidenceMode.FIXTURE,
        primary_raw_sha256=primary_raw_sha256,
        comparison_raw_sha256=comparison_raw_sha256,
        primary_identity=primary_identity,
        comparison_identity=comparison_identity,
        status=status,
        comparison_required=comparison_required,
        canonical_selection_reason=(
            "both normalized replay sources agree within explicit tolerances"
            if status == "passed"
            else "serious replay discrepancy requires quarantine and human review"
        ),
        records=tuple(differing),
    )


def _lineage_matches(lineage: Mapping[str, object], identity: ProviderSourceIdentity) -> bool:
    expected = {
        "provider": identity.provider,
        "endpoint": identity.endpoint,
        "provider_version": identity.provider_version,
        "adapter_version": identity.adapter_version,
        "native_schema_version": identity.native_schema_version,
        "raw_uri": identity.raw_uri,
        "raw_sha256": identity.raw_sha256,
    }
    return all(lineage.get(key) == value for key, value in expected.items())
