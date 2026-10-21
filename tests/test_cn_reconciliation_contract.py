from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from quant_stack.data.providers.cn import (
    CNDiscrepancyRecord,
    CNPartitionPromotion,
    PartitionStatus,
)


def test_discrepancy_contract_preserves_both_raw_sources_and_selection_reason() -> None:
    record = CNDiscrepancyRecord(
        dataset_partition="market=CN/venue=XSHG/year=2026",
        instrument_id="XSHG.600000",
        trade_date=date(2026, 7, 28),
        primary_provider="akshare",
        comparison_provider="baostock",
        primary_raw_uri="quant-data://raw/CN/akshare/primary.json",
        comparison_raw_uri="quant-data://raw/CN/baostock/comparison.json",
        differing_fields=("volume",),
        tolerance_policy="cn-daily-bars-v1",
        canonical_source_provider="akshare",
        canonical_selection_reason="locked daily-increment endpoint passed unit-aware checks",
        created_at=datetime(2026, 7, 29, tzinfo=UTC),
    )

    assert record.primary_raw_uri != record.comparison_raw_uri
    assert record.canonical_selection_reason


def test_quarantined_partition_cannot_enter_catalog() -> None:
    with pytest.raises(ValidationError, match="cannot enter the catalog"):
        CNPartitionPromotion(
            dataset_partition="market=CN/venue=XSHG/year=2026",
            dataset_version="2026-07-29.1",
            status=PartitionStatus.QUARANTINED,
            discrepancy_uris=("quant-data://quality/discrepancy-1.json",),
            catalog_eligible=True,
        )

    decision = CNPartitionPromotion(
        dataset_partition="market=CN/venue=XSHG/year=2026",
        dataset_version="2026-07-29.2",
        status=PartitionStatus.QUARANTINED,
        discrepancy_uris=("quant-data://quality/discrepancy-1.json",),
        catalog_eligible=False,
    )
    assert not decision.catalog_eligible
