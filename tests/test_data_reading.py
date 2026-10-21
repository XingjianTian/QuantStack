import hashlib
from pathlib import Path

from quant_stack.catalog import resolve_storage_uri, verify_dataset
from quant_stack.models import DatasetManifest, load_dataset_manifest

ROOT = Path(__file__).parents[1]


def test_catalog_uri_resolves_inside_root(tmp_path: Path) -> None:
    manifest = load_dataset_manifest(ROOT / "datasets/catalog/us_equity_daily_sample.yaml")
    assert resolve_storage_uri(manifest, tmp_path) == (
        tmp_path / "raw/us/fixture/bars_1d/year=2026/sample.csv"
    ).resolve()


def test_dataset_checksum_is_verified(tmp_path: Path) -> None:
    payload = b"date,close\n2026-01-02,100\n"
    target = tmp_path / "raw/us/test.csv"
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    manifest = DatasetManifest(
        schema_version="1.0.0",
        dataset_id="fixture",
        version="1",
        kind="market_bars",
        market="US",
        provider="fixture",
        frequency="1d",
        storage_uri="quant-data://raw/us/test.csv",
        format="csv",
        schema_ref="schemas/market_bar_v1.json",
        fetched_at="2026-01-03T00:00:00Z",
        available_at_policy="next_session",
        revision_policy="immutable_append",
        sha256=hashlib.sha256(payload).hexdigest(),
        row_count=1,
        quality_status="passed",
        license_scope="test",
        source_url="https://example.invalid/test",
    )
    assert verify_dataset(manifest, tmp_path) == target.resolve()
