import json
from datetime import date
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from quant_stack.data.models import DatasetManifest
from quant_stack.data.pipeline import run_synthetic_pipeline
from quant_stack.data.providers import SyntheticProvider

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize(
    "schema_name",
    [
        "instrument.schema.json",
        "bar.schema.json",
        "fundamental.schema.json",
        "corporate_action.schema.json",
        "dataset_manifest.schema.json",
    ],
)
def test_data_schemas_are_valid_json_schema(schema_name: str) -> None:
    schema = json.loads((ROOT / "schemas/data" / schema_name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)


def test_manifest_is_validated_against_data_schema(tmp_path: Path) -> None:
    result = run_synthetic_pipeline(
        tmp_path, market="CN", start=date(2026, 1, 1), end=date(2026, 1, 5)
    )
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    schema = json.loads(
        (ROOT / "schemas/data/dataset_manifest.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    DatasetManifest.model_validate(payload)


def test_manifest_rejects_point_in_time_availability_before_source(tmp_path: Path) -> None:
    result = run_synthetic_pipeline(
        tmp_path, market="US", start=date(2026, 1, 1), end=date(2026, 1, 5)
    )
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    payload["source_timestamp"] = "2026-01-05T21:00:00Z"
    payload["available_at"] = "2026-01-05T20:00:00Z"
    with pytest.raises(ValueError, match="available_at cannot be earlier"):
        DatasetManifest.model_validate(payload)


def test_synthetic_provider_supports_all_markets() -> None:
    provider = SyntheticProvider()
    for market in ("CN", "HK", "US"):
        instruments = list(provider.list_instruments(market))
        bars = list(
            provider.fetch_bars(market, [instruments[0].symbol], date(2026, 1, 1), date(2026, 1, 5))
        )
        assert instruments[0].market == market
        assert len(bars) == 3


def test_synthetic_pipeline_writes_parquet_and_duckdb_catalog(tmp_path: Path) -> None:
    result = run_synthetic_pipeline(
        tmp_path, market="US", start=date(2026, 1, 1), end=date(2026, 1, 9)
    )
    assert result.row_count == 7
    assert result.parquet_path.is_file()
    assert result.catalog_path.is_file()
    import duckdb

    connection = duckdb.connect(str(result.catalog_path), read_only=True)
    row = connection.execute(
        """SELECT dataset_id, row_count FROM datasets
           WHERE layer = 'normalized' AND kind = 'market_bars'"""
    ).fetchone()
    assert row == ("synthetic_us_equity_daily", 7)
    parquet_rows = connection.execute(
        f"SELECT count(*) FROM read_parquet('{result.parquet_path}')"
    ).fetchone()[0]
    assert parquet_rows == 7
    connection.close()
