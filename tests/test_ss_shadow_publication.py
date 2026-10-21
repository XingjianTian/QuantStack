from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from quant_stack.data.batch import (
    BatchBarsQuery,
    BatchDatasetNotFoundError,
    CanonicalBatchRepository,
)
from quant_stack.data.models import (
    DatasetKind,
    DatasetLayer,
    DatasetManifest,
    DatasetQualityMetadata,
)
from quant_stack.data.shadow_publication import (
    publish_qfq_ss_shadow_derivative,
    publish_unadjusted_ss_shadow_copy,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_manifest(data_root: Path) -> Path:
    bars = data_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    bars.parent.mkdir(parents=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "('XSHG.600000', '600000', 'XSHG', DATE '2026-07-29', "
            "TIMESTAMPTZ '2026-07-29 07:00:00+00', 10.0, 11.0, 9.0, 10.5, "
            "1000.0, 10500.0, 'CNY', 'none'), "
            "('XSHG.600000', '600000', 'XSHG', DATE '2026-07-30', "
            "TIMESTAMPTZ '2026-07-30 07:00:00+00', 10.5, 12.0, 10.0, 11.5, "
            "1200.0, 13800.0, 'CNY', 'none')) "
            "AS bars(instrument_id, symbol, venue, trading_date, ts_event, open, high, low, "
            "close, volume, amount, currency, adjustment)) TO ? (FORMAT PARQUET)",
            [str(bars)],
        )
    finally:
        connection.close()
    quality_path = bars.parent / "quality.json"
    quality_path.write_text('{"status":"passed"}\n', encoding="utf-8")
    manifest = DatasetManifest(
        manifest_schema_version="1.2.0",
        dataset_id="cn_equity_daily",
        version="raw-v1",
        layer=DatasetLayer.NORMALIZED,
        kind=DatasetKind.MARKET_BARS,
        market="CN",
        provider="baostock",
        provider_version="0.9.3",
        adapter_version="fixture-flat-adjustment-v1",
        native_schema_version="fixture-v1",
        frequency="1d",
        schema_ref="quant-stack://schemas/cn-equity-daily-unadjusted-v1",
        storage_uri="quant-data://canonical/cn_equity_daily/version=raw-v1/bars.parquet",
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
        available_at_policy="provider retrieval timestamp; no PIT membership claim",
        revision_policy="immutable",
        row_count=2,
        sha256=_sha256(bars),
        quality_status="passed",
        quality=DatasetQualityMetadata(
            status="passed",
            checks=["fixture"],
            report_uri="quant-data://canonical/cn_equity_daily/version=raw-v1/quality.json",
        ),
        license_scope="test fixture",
        input_dataset_versions=[],
    )
    path = data_root / "source-manifest.json"
    path.write_text(json.dumps(manifest.model_dump(mode="json")), encoding="utf-8")
    return path


def test_shadow_projection_is_immutable_batch_readable_and_candidate_blocked(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_manifest = _source_manifest(source_root)
    source_manifest_before = source_manifest.read_bytes()
    source_bars = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    source_bars_before = source_bars.read_bytes()

    receipt, report = publish_unadjusted_ss_shadow_copy(
        data_root=target_root,
        source_data_root=source_root,
        source_manifest_path=source_manifest,
        target_version="raw-v1.ss-shadow-v1",
        environment_id="test",
    )

    assert report["shadow_eligible"] is True
    assert report["candidate_eligible"] is False
    assert report["cutover_eligible"] is False
    assert report["receipt_replay_verified"] is True
    assert source_manifest.read_bytes() == source_manifest_before
    assert source_bars.read_bytes() == source_bars_before
    assert receipt.publication_gate == "eligible"

    shadow = CanonicalBatchRepository(target_root, consumer_access="shadow")
    query = BatchBarsQuery(
        instrument_ids=("XSHG.600000",),
        start=date(2026, 7, 29),
        end=date(2026, 7, 30),
        dataset_id="cn_equity_daily",
        dataset_version="raw-v1.ss-shadow-v1",
    )
    plan = shadow.prepare_bars(query)
    try:
        response = shadow.execute_bars(plan, query)
    finally:
        plan.close()
    assert len(response.items) == 2
    assert {item.adjustment_semantics for item in response.items} == {"none"}
    assert response.dataset_version == "raw-v1.ss-shadow-v1"

    candidate = CanonicalBatchRepository(target_root, consumer_access="candidate")
    with pytest.raises(BatchDatasetNotFoundError):
        candidate.prepare_metadata("cn_equity_daily", "raw-v1.ss-shadow-v1")

    replay_receipt, replay_report = publish_unadjusted_ss_shadow_copy(
        data_root=target_root,
        source_data_root=source_root,
        source_manifest_path=source_manifest,
        target_version="raw-v1.ss-shadow-v1",
        environment_id="test",
    )
    assert replay_receipt.receipt_id == receipt.receipt_id
    assert replay_report["created"] is False


def test_qfq_derivative_is_batch_readable_but_not_candidate(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_manifest = _source_manifest(source_root)
    raw = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    qfq = tmp_path / "qfq.parquet"
    qfq_sql = "'" + str(qfq).replace("'", "''") + "'"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id, symbol, venue, trading_date, ts_event, "
            "open, high, low, close, volume, amount, currency, "
            "struct_pack(adjustment := 'qfq') lineage FROM read_parquet(?)) "
            f"TO {qfq_sql} (FORMAT PARQUET)",
            [str(raw)],
        )
    finally:
        connection.close()

    receipt, report = publish_qfq_ss_shadow_derivative(
        data_root=target_root,
        source_manifest_path=source_manifest,
        source_bars_path=raw,
        qfq_bars_path=qfq,
        target_version="qfq-v1.ss-shadow-v1",
        environment_id="test",
    )

    assert receipt.publication_state.shadow_eligible is True
    assert report["candidate_eligible"] is False
    repository = CanonicalBatchRepository(target_root, consumer_access="shadow")
    query = BatchBarsQuery(
        instrument_ids=("XSHG.600000",),
        start=date(2026, 7, 29),
        end=date(2026, 7, 30),
        dataset_id="cn_equity_daily",
        dataset_version="qfq-v1.ss-shadow-v1",
    )
    plan = repository.prepare_bars(query)
    try:
        response = repository.execute_bars(plan, query)
    finally:
        plan.close()
    assert len(response.items) == 2
    assert {item.adjustment_semantics for item in response.items} == {"qfq"}


def test_qfq_candidate_requires_and_publishes_passing_ss_qualification(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_manifest = _source_manifest(source_root)
    raw = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    qfq = tmp_path / "qfq.parquet"
    qfq_sql = "'" + str(qfq).replace("'", "''") + "'"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id, symbol, venue, trading_date, ts_event, "
            "open, high, low, close, volume, amount, currency, "
            "struct_pack(adjustment := 'qfq') lineage FROM read_parquet(?)) "
            f"TO {qfq_sql} (FORMAT PARQUET)",
            [str(raw)],
        )
    finally:
        connection.close()
    qualification = tmp_path / "qualification.json"
    qualification.write_text(
        json.dumps(
            {
                "result": "PASS_QFQ_SHADOW_READY_FOR_CANDIDATE",
                "ss_runtime": {
                    "reports": 32,
                    "adjustment_compatible_reports": 32,
                    "candidate_failures": 0,
                    "comparison_failures": 0,
                    "report_failures": 0,
                    "worker_failures": 0,
                    "dropped": 0,
                },
                "price_comparison": {
                    "price_ratio_regime_changes": 0,
                    "close_return_drift_p95_max": 0.000001,
                },
                "indicator_and_screener_comparison": {
                    "scanner_categorical_change_symbols": 0,
                    "ma_score_change_symbols": 0,
                    "vcp_detection_change_symbols": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    receipt, report = publish_qfq_ss_shadow_derivative(
        data_root=target_root,
        source_manifest_path=source_manifest,
        source_bars_path=raw,
        qfq_bars_path=qfq,
        target_version="qfq-v1.ss-candidate-v1",
        qualification_evidence_path=qualification,
        environment_id="test",
    )

    assert receipt.publication_state.candidate_eligible is True
    assert report["candidate_eligible"] is True
    candidate = CanonicalBatchRepository(target_root, consumer_access="candidate")
    metadata = candidate.prepare_metadata("cn_equity_daily", "qfq-v1.ss-candidate-v1")
    try:
        assert candidate.metadata_response(metadata).candidate_eligible is True
    finally:
        metadata.close()


def test_qfq_publication_rejects_partial_source_universe(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_manifest = _source_manifest(source_root)
    raw = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    partial = tmp_path / "partial.parquet"
    partial_sql = "'" + str(partial).replace("'", "''") + "'"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id, symbol, venue, trading_date, ts_event, "
            "open, high, low, close, volume, amount, currency, "
            "struct_pack(adjustment := 'qfq') lineage FROM read_parquet(?) "
            f"LIMIT 1) TO {partial_sql} (FORMAT PARQUET)",
            [str(raw)],
        )
    finally:
        connection.close()

    with pytest.raises(ValueError, match="exactly cover"):
        publish_qfq_ss_shadow_derivative(
            data_root=tmp_path / "target",
            source_manifest_path=source_manifest,
            source_bars_path=raw,
            qfq_bars_path=partial,
            target_version="qfq-partial-v1",
            environment_id="test",
        )


def test_shadow_aggregate_proof_is_stable_when_copy_reorders_float_rows(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    source_manifest = _source_manifest(source_root)
    bars = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "('XSHG.600000', '600000', 'XSHG', DATE '2026-07-31', "
            "TIMESTAMPTZ '2026-07-31 07:00:00+00', 1e16, 1e16, 1e16, 1e16, "
            "1e16, 1e16, 'CNY', 'none'), "
            "('XSHG.600000', '600000', 'XSHG', DATE '2026-07-29', "
            "TIMESTAMPTZ '2026-07-29 07:00:00+00', 1.0, 1.0, 1.0, 1.0, "
            "1.0, 1.0, 'CNY', 'none'), "
            "('XSHG.600000', '600000', 'XSHG', DATE '2026-07-30', "
            "TIMESTAMPTZ '2026-07-30 07:00:00+00', 1.0, 1.0, 1.0, 1.0, "
            "1.0, 1.0, 'CNY', 'none')) "
            "AS bars(instrument_id, symbol, venue, trading_date, ts_event, open, high, low, "
            "close, volume, amount, currency, adjustment)) TO ? (FORMAT PARQUET)",
            [str(bars)],
        )
    finally:
        connection.close()
    manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    manifest["row_count"] = 3
    manifest["sha256"] = _sha256(bars)
    source_manifest.write_text(json.dumps(manifest), encoding="utf-8")

    _receipt, report = publish_unadjusted_ss_shadow_copy(
        data_root=tmp_path / "target",
        source_data_root=source_root,
        source_manifest_path=source_manifest,
        target_version="raw-v1.ss-shadow-v1",
        environment_id="test-reordered-floats",
    )

    assert report["row_count"] == 3


def test_shadow_projection_rejects_unsafe_target_version(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_manifest = _source_manifest(source_root)
    with pytest.raises(ValueError, match="unsafe path"):
        publish_unadjusted_ss_shadow_copy(
            data_root=tmp_path / "target",
            source_data_root=source_root,
            source_manifest_path=source_manifest,
            target_version="../escape",
            environment_id="test",
        )


def test_shadow_projection_requires_explicit_source_root(tmp_path: Path) -> None:
    source_manifest = _source_manifest(tmp_path / "source")
    with pytest.raises(TypeError, match="source_data_root"):
        publish_unadjusted_ss_shadow_copy(  # type: ignore[call-arg]
            data_root=tmp_path / "target",
            source_manifest_path=source_manifest,
            target_version="raw-v1.ss-shadow-v1",
        )


@pytest.mark.parametrize("relation", ["equal", "source-parent", "target-parent"])
def test_shadow_projection_rejects_equal_or_nested_roots(
    tmp_path: Path, relation: str
) -> None:
    source_root = tmp_path / "source"
    source_manifest = _source_manifest(source_root)
    if relation == "equal":
        target_root = source_root
    elif relation == "source-parent":
        target_root = source_root / "target"
    else:
        target_root = tmp_path
    with pytest.raises(ValueError, match="separate non-nested"):
        publish_unadjusted_ss_shadow_copy(
            data_root=target_root,
            source_data_root=source_root,
            source_manifest_path=source_manifest,
            target_version="raw-v1.ss-shadow-v1",
        )


def test_shadow_projection_rejects_source_manifest_escape(tmp_path: Path) -> None:
    actual_source = tmp_path / "actual-source"
    source_manifest = _source_manifest(actual_source)
    with pytest.raises(ValueError, match="manifest escapes"):
        publish_unadjusted_ss_shadow_copy(
            data_root=tmp_path / "target",
            source_data_root=tmp_path / "declared-source",
            source_manifest_path=source_manifest,
            target_version="raw-v1.ss-shadow-v1",
        )


def test_shadow_projection_can_read_from_separate_read_only_source_root(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_manifest = _source_manifest(source_root)
    source_bars = source_root / "canonical/cn_equity_daily/version=raw-v1/bars.parquet"
    source_manifest_before = source_manifest.read_bytes()
    source_bars_before = source_bars.read_bytes()

    receipt, report = publish_unadjusted_ss_shadow_copy(
        data_root=target_root,
        source_data_root=source_root,
        source_manifest_path=source_manifest,
        target_version="raw-v1.ss-shadow-v1",
        environment_id="test-separated-ro-source",
    )

    assert receipt.publication_state.shadow_eligible is True
    assert receipt.publication_state.candidate_eligible is False
    assert report["source_parquet_unchanged"] is True
    assert source_manifest.read_bytes() == source_manifest_before
    assert source_bars.read_bytes() == source_bars_before
    assert not (source_root / "normalized").exists()
    assert (
        target_root
        / "normalized/bars_1d/market=CN/version=raw-v1.ss-shadow-v1/bars.parquet"
    ).is_file()
