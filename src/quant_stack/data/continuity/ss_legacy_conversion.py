from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from ..catalog.duckdb_catalog import manifest_sha256
from ..models import DataModel, DatasetManifest
from ..storage.atomic import atomic_write_immutable_bytes, atomic_write_immutable_file
from .ss_archive_migration import SSArchiveMigrationInput
from .ss_archive_models import SSArchiveFile, SSLegacyArchiveManifest
from .ss_archive_safety import ArchiveLayout
from .stock_screener import SS_LEGACY_EXPORT_DATASET_ID

CONVERTER_VERSION = "1.0.2"
CONVERSION_MANIFEST_SCHEMA_VERSION = "1.2.0"
MAPPING_CONTRACT_VERSION = "1.0.0"
UNIT_AUDIT_VERSION = "1.0.0"
ADJUSTMENT_AUDIT_VERSION = "1.0.0"

_SYMBOL_PATTERN = re.compile(r"^(?P<code>[0-9]{6})(?:\.(?P<suffix>SS|SZ|BJ))?$")
_EXCHANGE_TO_MIC = {
    "SSE": "XSHG",
    "XSHG": "XSHG",
    "SH": "XSHG",
    "SHSE": "XSHG",
    "SZSE": "XSHE",
    "XSHE": "XSHE",
    "SZ": "XSHE",
    "BJSE": "BJSE",
    "XBSE": "BJSE",
    "BSE": "BJSE",
    "BJ": "BJSE",
}
_SUFFIX_TO_MIC = {"SS": "XSHG", "SZ": "XSHE", "BJ": "BJSE"}


class SSLegacyMappingStatus(StrEnum):
    MAPPED = "mapped"
    AMBIGUOUS = "ambiguous"
    UNSUPPORTED_MARKET = "unsupported_market"
    MISSING_UNIVERSE = "missing_universe"
    INVALID_SYMBOL = "invalid_symbol"
    CONFLICT = "conflict"


class SSLegacyMappingRecord(DataModel):
    source_symbol: str
    market: str | None = None
    exchange: str | None = None
    local_code: str | None = None
    mapping_status: SSLegacyMappingStatus
    candidate_instrument_id: str | None = None
    canonical_mic: str | None = None
    mapping_evidence: tuple[str, ...]
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True


class SSLegacyConversionArtifact(DataModel):
    uri: str = Field(pattern=r"^quant-data://legacy/stock_screener/")
    kind: Literal[
        "instrument_mapping",
        "audited_prices",
        "mapping_report",
        "unit_audit_report",
        "volume_anomalies",
        "adjustment_audit_report",
        "coverage_report",
        "discrepancy_ready_manifest",
    ]
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    row_count: int | None = Field(default=None, ge=0)


class SSLegacySourcePriceFile(DataModel):
    logical_uri: str = Field(pattern=r"^archive://ss-legacy/")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    row_count: int = Field(ge=0)
    size_bytes: int = Field(gt=0)


class SSLegacyConversionVerification(DataModel):
    source_price_file_count: int = Field(ge=1)
    catalog_status: Literal[
        "canonical_catalog_absent", "canonical_catalog_verified"
    ]


class SSLegacyConversionManifest(DataModel):
    schema_version: Literal["1.2.0"] = CONVERSION_MANIFEST_SCHEMA_VERSION
    conversion_id: str = Field(pattern=r"^ssconv-[a-f0-9]{32}$")
    conversion_identity_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    converter_version: Literal["1.0.2"] = CONVERTER_VERSION
    mapping_contract_version: Literal["1.0.0"] = MAPPING_CONTRACT_VERSION
    unit_audit_version: Literal["1.0.0"] = UNIT_AUDIT_VERSION
    adjustment_audit_version: Literal["1.0.0"] = ADJUSTMENT_AUDIT_VERSION
    created_at: datetime
    migration_input: SSArchiveMigrationInput
    source_archive_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_price_file_inventory: tuple[SSLegacySourcePriceFile, ...] = Field(
        min_length=1
    )
    source_provider: Literal["stock_screener_legacy"] = "stock_screener_legacy"
    source_endpoint: Literal["stock_prices_export_v1"] = "stock_prices_export_v1"
    lineage_quality: Literal["legacy"] = "legacy"
    canonical_eligible: Literal[False] = False
    quarantined: Literal[True] = True
    artifacts: tuple[SSLegacyConversionArtifact, ...] = Field(min_length=8)
    mapped_symbols: int = Field(ge=0)
    unresolved_symbols: int = Field(ge=0)
    orphan_price_symbols: int = Field(ge=0)
    price_rows: int = Field(ge=0)
    volume_anomaly_rows: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_conversion(self) -> SSLegacyConversionManifest:
        if self.created_at.tzinfo is None:
            raise ValueError("conversion timestamp must be timezone-aware")
        kinds = {artifact.kind for artifact in self.artifacts}
        if len(kinds) != 8:
            raise ValueError("conversion artifact inventory is incomplete")
        if self.source_archive_manifest_sha256 != self.migration_input.manifest_sha256:
            raise ValueError("source archive manifest hash must match migration input")
        source_uris = [item.logical_uri for item in self.source_price_file_inventory]
        if len(source_uris) != len(set(source_uris)):
            raise ValueError("source price file inventory URIs must be unique")
        return self


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        + "\n"
    ).encode()


def _conversion_identity(migration_input: SSArchiveMigrationInput) -> tuple[str, str]:
    identity = {
        "archive_id": migration_input.archive_id,
        "archive_manifest_sha256": migration_input.manifest_sha256,
        "conversion_manifest_schema_version": CONVERSION_MANIFEST_SCHEMA_VERSION,
        "converter_version": CONVERTER_VERSION,
        "mapping_contract_version": MAPPING_CONTRACT_VERSION,
        "unit_audit_version": UNIT_AUDIT_VERSION,
        "adjustment_audit_version": ADJUSTMENT_AUDIT_VERSION,
    }
    identity_hash = hashlib.sha256(_canonical_json(identity)).hexdigest()
    return identity_hash, f"ssconv-{identity_hash[:32]}"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _archive_file_path(layout: ArchiveLayout, item: SSArchiveFile) -> Path:
    marker = "archive://ss-legacy/"
    if not item.logical_uri.startswith(marker):
        raise ValueError("legacy conversion input URI is outside the SS archive")
    return layout.resolve(item.logical_uri.removeprefix(marker))


def _resolve_mapping(rows: list[dict]) -> SSLegacyMappingRecord:
    first = rows[0]
    symbol = str(first.get("symbol") or "").strip().upper()
    if len(rows) != 1:
        return SSLegacyMappingRecord(
            source_symbol=symbol or "<empty>",
            mapping_status=SSLegacyMappingStatus.AMBIGUOUS,
            mapping_evidence=("duplicate frozen-universe symbol rows",),
        )
    market = str(first.get("market") or "").strip().upper() or None
    exchange = str(first.get("exchange") or "").strip().upper() or None
    local_code = str(first.get("local_code") or "").strip().upper() or None
    match = _SYMBOL_PATTERN.fullmatch(symbol)
    if match is None:
        return SSLegacyMappingRecord(
            source_symbol=symbol or "<empty>",
            market=market,
            exchange=exchange,
            local_code=local_code,
            mapping_status=SSLegacyMappingStatus.INVALID_SYMBOL,
            mapping_evidence=("symbol does not match explicit six-digit SS suffix contract",),
        )
    if market != "CN":
        return SSLegacyMappingRecord(
            source_symbol=symbol,
            market=market,
            exchange=exchange,
            local_code=local_code,
            mapping_status=SSLegacyMappingStatus.UNSUPPORTED_MARKET,
            mapping_evidence=(f"unsupported frozen-universe market={market or 'missing'}",),
        )

    suffix = match.group("suffix")
    exchange_mic = _EXCHANGE_TO_MIC.get(exchange or "")
    suffix_mic = _SUFFIX_TO_MIC.get(suffix or "")
    evidence: list[str] = []
    if exchange_mic:
        evidence.append(f"frozen_universe.exchange={exchange}->{exchange_mic}")
    if suffix_mic:
        evidence.append(f"source_symbol.suffix={suffix}->{suffix_mic}")
    if exchange and exchange_mic is None:
        evidence.append(f"unrecognized frozen_universe.exchange={exchange}")
    candidates = {value for value in (exchange_mic, suffix_mic) if value}
    if len(candidates) > 1:
        return SSLegacyMappingRecord(
            source_symbol=symbol,
            market=market,
            exchange=exchange,
            local_code=local_code,
            mapping_status=SSLegacyMappingStatus.CONFLICT,
            mapping_evidence=tuple(evidence),
        )
    if not candidates:
        return SSLegacyMappingRecord(
            source_symbol=symbol,
            market=market,
            exchange=exchange,
            local_code=local_code,
            mapping_status=SSLegacyMappingStatus.AMBIGUOUS,
            mapping_evidence=tuple(evidence or ["no explicit exchange or suffix evidence"]),
        )
    mic = candidates.pop()
    code = match.group("code")
    return SSLegacyMappingRecord(
        source_symbol=symbol,
        market=market,
        exchange=exchange,
        local_code=local_code,
        mapping_status=SSLegacyMappingStatus.MAPPED,
        candidate_instrument_id=f"{mic}.{code}",
        canonical_mic=mic,
        mapping_evidence=tuple(evidence),
    )


def _artifact(data_root: Path, path: Path, kind: str, row_count: int | None = None):
    return SSLegacyConversionArtifact(
        uri=f"quant-data://{path.relative_to(data_root).as_posix()}",
        kind=kind,
        size_bytes=path.stat().st_size,
        sha256=_sha256(path),
        row_count=row_count,
    )


def _write_json(path: Path, value: object, *, kind: str) -> None:
    atomic_write_immutable_bytes(path, _canonical_json(value), artifact_kind=kind)


def _write_parquet_query(connection, path: Path, query: str, *, kind: str) -> None:
    def write(temporary: Path) -> None:
        connection.execute(
            f"COPY ({query}) TO {_sql_string(str(temporary))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    atomic_write_immutable_file(
        path,
        write=write,
        validate=lambda candidate: connection.execute(
            f"SELECT count(*) FROM read_parquet({_sql_string(str(candidate))})"
        ).fetchone(),
        equivalent=lambda first, second: _sha256(first) == _sha256(second),
        artifact_kind=kind,
    )


def _contains_legacy_registration(
    value: object,
    *,
    conversion_id: str,
    legacy_prefix: str,
    artifact_uris: frozenset[str],
) -> bool:
    if isinstance(value, str):
        return (
            value == SS_LEGACY_EXPORT_DATASET_ID
            or conversion_id in value
            or value.startswith(legacy_prefix)
            or value in artifact_uris
        )
    if isinstance(value, dict):
        return any(
            _contains_legacy_registration(
                item,
                conversion_id=conversion_id,
                legacy_prefix=legacy_prefix,
                artifact_uris=artifact_uris,
            )
            for item in value.values()
        )
    if isinstance(value, (list, tuple)):
        return any(
            _contains_legacy_registration(
                item,
                conversion_id=conversion_id,
                legacy_prefix=legacy_prefix,
                artifact_uris=artifact_uris,
            )
            for item in value
        )
    return False


def _verify_catalog_exclusion(
    root: Path,
    manifest: SSLegacyConversionManifest,
    *,
    legacy_prefix: str,
) -> Literal["canonical_catalog_absent", "canonical_catalog_verified"]:
    import duckdb

    catalog_root = root / "catalog"
    if not catalog_root.exists():
        return "canonical_catalog_absent"

    artifact_uris = frozenset(item.uri for item in manifest.artifacts)
    pointer_path = catalog_root / "current-manifest-set.json"
    if not pointer_path.is_file():
        raise ValueError("canonical catalog pointer is missing")
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("canonical catalog pointer is invalid") from exc
    relatives = pointer.get("manifests")
    if (
        pointer.get("schema_version") != "1.0.0"
        or not isinstance(relatives, list)
        or len(relatives) != len(set(relatives))
    ):
        raise ValueError("canonical catalog pointer is invalid")

    pointer_rows: dict[tuple[str, str], str] = {}
    for relative in relatives:
        if not isinstance(relative, str):
            raise ValueError("canonical catalog pointer manifest path is invalid")
        path = (root / relative).resolve()
        if root not in path.parents or not path.is_file():
            raise ValueError("canonical catalog pointer manifest is missing")
        dataset_manifest = DatasetManifest.model_validate_json(
            path.read_text(encoding="utf-8")
        )
        if _contains_legacy_registration(
            dataset_manifest.model_dump(mode="json"),
            conversion_id=manifest.conversion_id,
            legacy_prefix=legacy_prefix,
            artifact_uris=artifact_uris,
        ):
            raise ValueError("legacy conversion was registered in the canonical pointer")
        key = (dataset_manifest.dataset_id, dataset_manifest.version)
        if key in pointer_rows:
            raise ValueError("canonical catalog pointer contains duplicate dataset versions")
        pointer_rows[key] = manifest_sha256(dataset_manifest)

    catalog_path = catalog_root / "quant.duckdb"
    if not catalog_path.is_file():
        raise ValueError("canonical DuckDB catalog is missing")
    try:
        connection = duckdb.connect(str(catalog_path), read_only=True)
        try:
            columns = (
                "dataset_id, version, storage_uri, raw_storage_uri, "
                "quality_report_uri, manifest_sha256"
            )
            rows = connection.execute(f"SELECT {columns} FROM datasets").fetchall()
        finally:
            connection.close()
    except Exception as exc:
        raise ValueError("canonical DuckDB catalog query failed") from exc
    catalog_rows: dict[tuple[str, str], str] = {}
    for row in rows:
        if _contains_legacy_registration(
            row,
            conversion_id=manifest.conversion_id,
            legacy_prefix=legacy_prefix,
            artifact_uris=artifact_uris,
        ):
            raise ValueError("legacy conversion was registered in canonical DuckDB")
        key = (row[0], row[1])
        if key in catalog_rows:
            raise ValueError("canonical DuckDB contains duplicate dataset versions")
        catalog_rows[key] = row[5]
    if catalog_rows != pointer_rows:
        raise ValueError("canonical pointer and DuckDB catalog are inconsistent")

    receipts_root = catalog_root / "publication_receipts"
    if receipts_root.exists():
        for path in sorted(receipts_root.glob("*.json")):
            try:
                receipt = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("canonical publication receipt is invalid") from exc
            if _contains_legacy_registration(
                receipt,
                conversion_id=manifest.conversion_id,
                legacy_prefix=legacy_prefix,
                artifact_uris=artifact_uris,
            ):
                raise ValueError(
                    "legacy conversion was registered in a publication receipt"
                )
    return "canonical_catalog_verified"


class SSLegacyOfflineConverter:
    """Convert one accepted frozen archive into a quarantined audit namespace."""

    def __init__(self, archive_root: str | Path, data_root: str | Path) -> None:
        self.archive_root = Path(archive_root)
        self.data_root = Path(data_root)

    def convert(
        self,
        migration_input: SSArchiveMigrationInput,
        *,
        created_at: datetime | None = None,
        fail_after_artifacts: bool = False,
    ) -> SSLegacyConversionManifest:
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq

        layout = ArchiveLayout(self.archive_root, migration_input.archive_id)
        manifest_path = layout.archive_path("frozen-exports", "manifest.json")
        archive_manifest = SSLegacyArchiveManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        if _sha256(manifest_path) != migration_input.manifest_sha256:
            raise ValueError("migration input no longer matches the frozen archive manifest")

        identity_hash, conversion_id = _conversion_identity(migration_input)
        output_root = (
            self.data_root
            / "legacy"
            / "stock_screener"
            / migration_input.archive_id
            / conversion_id
        )
        conversion_manifest_path = output_root / "conversion-manifest.json"
        checksum_path = output_root / "conversion-manifest.json.sha256"
        if conversion_manifest_path.exists():
            existing = SSLegacyConversionManifest.model_validate_json(
                conversion_manifest_path.read_text(encoding="utf-8")
            )
            expected_checksum = (
                f"{_sha256(conversion_manifest_path)}  conversion-manifest.json\n".encode()
            )
            atomic_write_immutable_bytes(
                checksum_path,
                expected_checksum,
                artifact_kind="SS legacy conversion manifest checksum",
            )
            verify_ss_legacy_conversion(
                self.data_root, existing, archive_root=self.archive_root
            )
            if existing.conversion_identity_sha256 != identity_hash:
                raise ValueError("immutable legacy conversion identity conflict")
            return existing

        universe_files = [
            item for item in archive_manifest.files if item.table_name == "stock_universe"
        ]
        price_files = [item for item in archive_manifest.files if item.table_name == "stock_prices"]
        if not universe_files or not price_files:
            raise ValueError("frozen archive is missing required conversion inputs")
        source_price_file_inventory = tuple(
            SSLegacySourcePriceFile(
                logical_uri=item.logical_uri,
                sha256=item.sha256,
                row_count=item.row_count,
                size_bytes=item.size_bytes,
            )
            for item in sorted(price_files, key=lambda value: value.logical_uri)
        )

        universe_rows: list[dict] = []
        for item in universe_files:
            universe_rows.extend(pq.read_table(_archive_file_path(layout, item)).to_pylist())
        universe_by_symbol: dict[str, list[dict]] = {}
        for row in universe_rows:
            universe_by_symbol.setdefault(str(row.get("symbol") or "").strip().upper(), []).append(
                row
            )

        connection = duckdb.connect(":memory:")
        try:
            price_paths = [_archive_file_path(layout, item) for item in price_files]
            price_scan = (
                "read_parquet(["
                + ",".join(_sql_string(str(path)) for path in price_paths)
                + "], filename=true, union_by_name=true)"
            )
            price_symbols = {
                str(row[0]).strip().upper()
                for row in connection.execute(
                    f"SELECT DISTINCT symbol FROM {price_scan} ORDER BY symbol"
                ).fetchall()
            }
            mappings = {
                symbol: _resolve_mapping(rows) for symbol, rows in universe_by_symbol.items()
            }
            for symbol in sorted(price_symbols - set(universe_by_symbol)):
                mappings[symbol] = SSLegacyMappingRecord(
                    source_symbol=symbol,
                    mapping_status=SSLegacyMappingStatus.MISSING_UNIVERSE,
                    mapping_evidence=("price symbol absent from frozen stock_universe",),
                )

            mapping_rows = [
                {
                    **record.model_dump(mode="json"),
                    "mapping_evidence": json.dumps(record.mapping_evidence),
                }
                for record in sorted(mappings.values(), key=lambda item: item.source_symbol)
            ]
            mapping_table = pa.Table.from_pylist(mapping_rows)
            connection.register("legacy_mapping", mapping_table)
            source_rows = [
                {
                    "filename": str(_archive_file_path(layout, item)),
                    "source_file_uri": item.logical_uri,
                    "source_file_sha256": item.sha256,
                }
                for item in price_files
            ]
            connection.register("legacy_source_files", pa.Table.from_pylist(source_rows))

            output_root.mkdir(parents=True, exist_ok=True)
            mapping_path = output_root / "instrument-mapping.parquet"
            atomic_write_immutable_file(
                mapping_path,
                write=lambda candidate: pq.write_table(
                    mapping_table, candidate, compression="zstd"
                ),
                validate=lambda candidate: pq.ParquetFile(candidate).metadata.num_rows,
                equivalent=lambda first, second: _sha256(first) == _sha256(second),
                artifact_kind="SS legacy instrument mapping",
            )

            audited_prices_path = output_root / "audited-prices.parquet"
            audited_query = f"""
                SELECT p.* EXCLUDE (filename),
                       p.volume AS original_volume,
                       'unknown_or_mixed' AS volume_unit,
                       CAST(NULL AS DOUBLE) AS volume_shares,
                       'preserved_unverified' AS volume_conversion_status,
                       'unverified' AS adjustment_semantics,
                       CASE WHEN p.adj_close IS DISTINCT FROM p.close THEN true ELSE false END
                           AS adj_close_difference_flag,
                       false AS adjusted_value_usable,
                       m.mapping_status,
                       m.candidate_instrument_id,
                       s.source_file_uri,
                       s.source_file_sha256,
                       {_sql_string(migration_input.archive_id)} AS archive_id,
                       'stock_screener_legacy' AS source_provider,
                       'stock_prices_export_v1' AS source_endpoint,
                       'legacy' AS lineage_quality,
                       false AS canonical_eligible,
                       true AS quarantined
                FROM {price_scan} p
                LEFT JOIN legacy_mapping m ON upper(trim(p.symbol)) = m.source_symbol
                JOIN legacy_source_files s ON p.filename = s.filename
                ORDER BY p.id
            """
            _write_parquet_query(
                connection,
                audited_prices_path,
                audited_query,
                kind="SS legacy audited prices",
            )

            anomalies_path = output_root / "volume-anomalies.parquet"
            anomalies_query = f"""
                WITH ordered AS (
                    SELECT symbol, date, volume AS original_volume,
                           lag(volume) OVER (
                               PARTITION BY symbol ORDER BY date, id
                           ) AS previous_volume
                    FROM {price_scan}
                ), ratios AS (
                    SELECT *, CASE
                        WHEN previous_volume > 0 AND original_volume > 0
                        THEN greatest(original_volume, previous_volume)::DOUBLE /
                             least(original_volume, previous_volume)::DOUBLE
                        ELSE NULL END AS jump_ratio
                    FROM ordered
                )
                SELECT symbol, date, original_volume, previous_volume, jump_ratio,
                       'legacy_volume_80_120x_jump' AS anomaly_flag
                FROM ratios
                WHERE jump_ratio BETWEEN 80.0 AND 120.0
                ORDER BY symbol, date
            """
            _write_parquet_query(
                connection,
                anomalies_path,
                anomalies_query,
                kind="SS legacy volume anomaly report",
            )
            anomaly_rows = connection.execute(
                f"SELECT count(*) FROM read_parquet({_sql_string(str(anomalies_path))})"
            ).fetchone()[0]
            adjustment_difference_rows = connection.execute(
                f"SELECT count(*) FROM read_parquet({_sql_string(str(audited_prices_path))}) "
                "WHERE adj_close_difference_flag"
            ).fetchone()[0]
        finally:
            connection.close()

        mapped = sum(
            record.mapping_status == SSLegacyMappingStatus.MAPPED for record in mappings.values()
        )
        unresolved = len(mappings) - mapped
        orphan_count = sum(
            record.mapping_status == SSLegacyMappingStatus.MISSING_UNIVERSE
            for record in mappings.values()
        )
        mapping_report_path = output_root / "mapping-report.json"
        unit_report_path = output_root / "unit-audit-report.json"
        adjustment_report_path = output_root / "adjustment-audit-report.json"
        coverage_report_path = output_root / "coverage-report.json"
        discrepancy_path = output_root / "discrepancy-ready-manifest.json"
        _write_json(
            mapping_report_path,
            {
                "contract_version": MAPPING_CONTRACT_VERSION,
                "mapped_symbols": mapped,
                "unresolved_symbols": unresolved,
                "orphan_price_symbols": orphan_count,
                "status_counts": {
                    status.value: sum(
                        record.mapping_status == status for record in mappings.values()
                    )
                    for status in SSLegacyMappingStatus
                },
                "canonical_eligible": False,
                "quarantined": True,
            },
            kind="SS legacy mapping report",
        )
        _write_json(
            unit_report_path,
            {
                "audit_version": UNIT_AUDIT_VERSION,
                "original_volume_preserved": True,
                "volume_unit": "unknown_or_mixed",
                "volume_shares": None,
                "global_multiplier_applied": False,
                "anomaly_rule": "bidirectional adjacent-day ratio between 80 and 120",
                "anomaly_rows": anomaly_rows,
            },
            kind="SS legacy unit audit report",
        )
        _write_json(
            adjustment_report_path,
            {
                "audit_version": ADJUSTMENT_AUDIT_VERSION,
                "adj_close_preserved": True,
                "adjustment_semantics": "unverified",
                "adjusted_value_usable": False,
                "factor_generated": False,
                "difference_rows": adjustment_difference_rows,
            },
            kind="SS legacy adjustment audit report",
        )
        _write_json(
            coverage_report_path,
            {
                "archive_id": migration_input.archive_id,
                "stock_prices_rows": archive_manifest.coverage.stock_prices_row_count,
                "stock_universe_rows": archive_manifest.coverage.stock_universe_row_count,
                "price_symbols": archive_manifest.coverage.distinct_price_symbols,
                "min_date": archive_manifest.coverage.min_date,
                "max_date": archive_manifest.coverage.max_date,
                "orphan_price_symbols": archive_manifest.coverage.orphan_price_symbols,
                "source_history_export": archive_manifest.coverage.source_history_export,
                "market_history_completeness": "not_claimed",
            },
            kind="SS legacy coverage report",
        )
        _write_json(
            discrepancy_path,
            {
                "schema_version": "1.0.0",
                "conversion_id": conversion_id,
                "candidate_source": "none",
                "ready_for_future_cross_validation": True,
                "source_selection_performed": False,
                "canonical_eligible": False,
                "quarantined": True,
            },
            kind="SS legacy discrepancy-ready manifest",
        )

        artifacts = (
            _artifact(self.data_root, mapping_path, "instrument_mapping", len(mapping_rows)),
            _artifact(
                self.data_root,
                audited_prices_path,
                "audited_prices",
                migration_input.stock_prices_rows,
            ),
            _artifact(self.data_root, mapping_report_path, "mapping_report"),
            _artifact(self.data_root, unit_report_path, "unit_audit_report"),
            _artifact(self.data_root, anomalies_path, "volume_anomalies", anomaly_rows),
            _artifact(self.data_root, adjustment_report_path, "adjustment_audit_report"),
            _artifact(self.data_root, coverage_report_path, "coverage_report"),
            _artifact(self.data_root, discrepancy_path, "discrepancy_ready_manifest"),
        )
        result = SSLegacyConversionManifest(
            conversion_id=conversion_id,
            conversion_identity_sha256=identity_hash,
            created_at=created_at or datetime.now(UTC),
            migration_input=migration_input,
            source_archive_manifest_sha256=migration_input.manifest_sha256,
            source_price_file_inventory=source_price_file_inventory,
            artifacts=artifacts,
            mapped_symbols=mapped,
            unresolved_symbols=unresolved,
            orphan_price_symbols=orphan_count,
            price_rows=migration_input.stock_prices_rows,
            volume_anomaly_rows=anomaly_rows,
        )
        if fail_after_artifacts:
            raise RuntimeError("simulated crash after conversion artifacts")
        _write_json(
            conversion_manifest_path,
            result.model_dump(mode="json"),
            kind="SS legacy conversion manifest",
        )
        atomic_write_immutable_bytes(
            checksum_path,
            f"{_sha256(conversion_manifest_path)}  conversion-manifest.json\n".encode(),
            artifact_kind="SS legacy conversion manifest checksum",
        )
        return result


def verify_ss_legacy_conversion(
    data_root: str | Path,
    manifest: SSLegacyConversionManifest,
    *,
    archive_root: str | Path,
) -> SSLegacyConversionVerification:
    import duckdb
    import pyarrow.parquet as pq

    root = Path(data_root).resolve()
    archive_layout = ArchiveLayout(archive_root, manifest.migration_input.archive_id)
    source_manifest_path = archive_layout.archive_path(
        "frozen-exports", "manifest.json"
    )
    if not source_manifest_path.is_file():
        raise ValueError("hash-locked source archive manifest is missing")
    source_manifest_sha256 = _sha256(source_manifest_path)
    if source_manifest_sha256 != manifest.source_archive_manifest_sha256:
        raise ValueError("source archive manifest hash does not match conversion evidence")
    source_manifest = SSLegacyArchiveManifest.model_validate_json(
        source_manifest_path.read_text(encoding="utf-8")
    )
    if source_manifest.archive_id != manifest.migration_input.archive_id:
        raise ValueError("source archive manifest identifies a different archive")
    source_price_files = tuple(
        sorted(
            (item for item in source_manifest.files if item.table_name == "stock_prices"),
            key=lambda value: value.logical_uri,
        )
    )
    expected_source_inventory = tuple(
        SSLegacySourcePriceFile(
            logical_uri=item.logical_uri,
            sha256=item.sha256,
            row_count=item.row_count,
            size_bytes=item.size_bytes,
        )
        for item in source_price_files
    )
    if manifest.source_price_file_inventory != expected_source_inventory:
        raise ValueError("source price file inventory does not match hash-locked archive")
    for item in source_price_files:
        source_path = _archive_file_path(archive_layout, item)
        if (
            not source_path.is_file()
            or source_path.stat().st_size != item.size_bytes
            or _sha256(source_path) != item.sha256
            or pq.ParquetFile(source_path).metadata.num_rows != item.row_count
        ):
            raise ValueError("source price file evidence does not match archive payload")

    identity_hash, conversion_id = _conversion_identity(manifest.migration_input)
    if manifest.schema_version != CONVERSION_MANIFEST_SCHEMA_VERSION:
        raise ValueError("legacy conversion manifest schema version is unsupported")
    if manifest.converter_version != CONVERTER_VERSION:
        raise ValueError("legacy conversion converter version is unsupported")
    if manifest.conversion_identity_sha256 != identity_hash:
        raise ValueError("legacy conversion identity hash does not match its inputs")
    if manifest.conversion_id != conversion_id:
        raise ValueError("legacy conversion namespace does not match its identity")
    expected_prefix = (
        f"quant-data://legacy/stock_screener/{manifest.migration_input.archive_id}/"
        f"{manifest.conversion_id}/"
    )
    conversion_root = (
        root
        / "legacy"
        / "stock_screener"
        / manifest.migration_input.archive_id
        / manifest.conversion_id
    ).resolve()
    manifest_path = conversion_root / "conversion-manifest.json"
    checksum_path = conversion_root / "conversion-manifest.json.sha256"
    if not manifest_path.is_file() or not checksum_path.is_file():
        raise ValueError("legacy conversion manifest or checksum is missing")
    persisted = SSLegacyConversionManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if persisted != manifest:
        raise ValueError("legacy conversion manifest argument does not match persisted evidence")
    expected_checksum = f"{_sha256(manifest_path)}  conversion-manifest.json\n"
    if checksum_path.read_text(encoding="ascii") != expected_checksum:
        raise ValueError("legacy conversion manifest checksum does not match")

    artifacts_by_kind: dict[str, Path] = {}
    for artifact in manifest.artifacts:
        if not artifact.uri.startswith(expected_prefix):
            raise ValueError("legacy conversion artifact escaped its immutable namespace")
        relative = Path(artifact.uri.removeprefix("quant-data://"))
        path = (root / relative).resolve()
        if root not in path.parents or not path.is_file():
            raise ValueError("legacy conversion artifact is missing")
        if path.stat().st_size != artifact.size_bytes or _sha256(path) != artifact.sha256:
            raise ValueError("legacy conversion artifact hash evidence does not match")
        artifacts_by_kind[artifact.kind] = path
    if len(artifacts_by_kind) != 8:
        raise ValueError("legacy conversion artifact kinds are not unique and complete")

    mapping_path = artifacts_by_kind["instrument_mapping"]
    prices_path = artifacts_by_kind["audited_prices"]
    anomalies_path = artifacts_by_kind["volume_anomalies"]
    connection = duckdb.connect(":memory:")
    try:
        price_sql = f"read_parquet({_sql_string(str(prices_path))})"
        price_evidence = connection.execute(
            f"""
            SELECT count(*) AS rows,
                   count(id) AS non_null_ids,
                   count(DISTINCT id) AS unique_ids,
                   count(*) FILTER (WHERE original_volume IS DISTINCT FROM volume)
                       AS changed_original_volume,
                   count(*) FILTER (WHERE volume_unit != 'unknown_or_mixed') AS bad_unit,
                   count(*) FILTER (WHERE volume_shares IS NOT NULL) AS shares_populated,
                   count(*) FILTER (WHERE adjustment_semantics != 'unverified')
                       AS bad_adjustment,
                   count(*) FILTER (
                       WHERE source_file_uri IS NULL
                          OR NOT starts_with(
                              source_file_uri,
                              {_sql_string(f"archive://ss-legacy/frozen-exports/{manifest.migration_input.archive_id}/stock_prices/")}
                          )
                          OR source_file_sha256 IS NULL
                          OR NOT regexp_full_match(source_file_sha256, '[0-9a-f]{{64}}')
                   ) AS bad_source_evidence,
                   count(*) FILTER (
                       WHERE source_provider != 'stock_screener_legacy'
                          OR source_endpoint != 'stock_prices_export_v1'
                          OR archive_id != {_sql_string(manifest.migration_input.archive_id)}
                          OR lineage_quality != 'legacy'
                          OR volume_conversion_status != 'preserved_unverified'
                          OR adjusted_value_usable IS DISTINCT FROM false
                          OR canonical_eligible IS DISTINCT FROM false
                          OR quarantined IS DISTINCT FROM true
                   ) AS bad_lineage,
                   count(*) FILTER (WHERE adj_close_difference_flag) AS adjustment_diffs
            FROM {price_sql}
            """
        ).fetchone()
        if price_evidence[0] != manifest.price_rows:
            raise ValueError("audited price row count does not match conversion evidence")
        if price_evidence[1] != price_evidence[0] or price_evidence[2] != price_evidence[0]:
            raise ValueError("audited price primary key is null or non-unique")
        if any(price_evidence[index] for index in range(3, 9)):
            raise ValueError("audited price lineage, unit, or adjustment evidence is invalid")
        adjustment_diffs = price_evidence[9]
        actual_source_contributions = {
            (row[0], row[1]): row[2]
            for row in connection.execute(
                f"""
                SELECT source_file_uri, source_file_sha256, count(*)
                FROM {price_sql}
                GROUP BY source_file_uri, source_file_sha256
                """
            ).fetchall()
        }
        expected_source_contributions = {
            (item.logical_uri, item.sha256): item.row_count
            for item in manifest.source_price_file_inventory
        }
        if actual_source_contributions != expected_source_contributions:
            raise ValueError(
                "audited price source file contributions do not match archive inventory"
            )
        source_price_scan = (
            "read_parquet(["
            + ",".join(
                _sql_string(str(_archive_file_path(archive_layout, item)))
                for item in source_price_files
            )
            + "], union_by_name=true)"
        )
        source_adjustment_diffs = connection.execute(
            f"SELECT count(*) FROM {source_price_scan} "
            "WHERE adj_close IS DISTINCT FROM close"
        ).fetchone()[0]
        if adjustment_diffs != source_adjustment_diffs:
            raise ValueError(
                "audited adjustment differences do not match source archive evidence"
            )

        mapping_sql = f"read_parquet({_sql_string(str(mapping_path))})"
        mapping_evidence = connection.execute(
            f"""
            SELECT count(*) AS rows,
                   count(DISTINCT source_symbol) AS unique_sources,
                   count(*) FILTER (WHERE mapping_status = 'mapped') AS mapped,
                   count(*) FILTER (WHERE mapping_status != 'mapped') AS unresolved,
                   count(*) FILTER (WHERE mapping_status = 'missing_universe') AS orphaned,
                   count(candidate_instrument_id) FILTER (WHERE mapping_status = 'mapped')
                       AS mapped_ids,
                   count(DISTINCT candidate_instrument_id)
                       FILTER (WHERE mapping_status = 'mapped') AS unique_mapped_ids,
                   count(*) FILTER (
                       WHERE mapping_status = 'mapped'
                             AND NOT regexp_full_match(
                                 candidate_instrument_id, '(XSHG|XSHE|BJSE)[.][0-9]{{6}}'
                             )
                   ) AS invalid_candidate_ids,
                   count(*) FILTER (
                           WHERE mapping_status = 'mapped'
                             AND NOT starts_with(
                                 candidate_instrument_id, canonical_mic || '.'
                             )
                   ) AS inconsistent_mics,
                   count(*) FILTER (
                       WHERE mapping_status != 'mapped'
                         AND candidate_instrument_id IS NOT NULL
                   ) AS unresolved_candidate_ids
            FROM {mapping_sql}
            """
        ).fetchone()
        if mapping_evidence[0] != mapping_evidence[1]:
            raise ValueError("legacy mapping source symbols are not unique")
        if mapping_evidence[5] != mapping_evidence[6]:
            raise ValueError("legacy mapped instrument IDs are not unique")
        if any(mapping_evidence[index] for index in range(7, 10)):
            raise ValueError("legacy mapping candidate ID or MIC evidence is inconsistent")
        if tuple(mapping_evidence[2:5]) != (
            manifest.mapped_symbols,
            manifest.unresolved_symbols,
            manifest.orphan_price_symbols,
        ):
            raise ValueError("legacy mapping counts do not match manifest evidence")
        mapping_status_counts = {
            row[0]: row[1]
            for row in connection.execute(
                f"SELECT mapping_status, count(*) FROM {mapping_sql} "
                "GROUP BY mapping_status"
            ).fetchall()
        }
        anomaly_rows = connection.execute(
            f"SELECT count(*) FROM read_parquet({_sql_string(str(anomalies_path))})"
        ).fetchone()[0]
        if anomaly_rows != manifest.volume_anomaly_rows:
            raise ValueError("legacy volume anomaly count does not match manifest evidence")
    finally:
        connection.close()

    mapping_report = json.loads(
        artifacts_by_kind["mapping_report"].read_text(encoding="utf-8")
    )
    expected_status_counts = {
        status.value: mapping_status_counts.get(status.value, 0)
        for status in SSLegacyMappingStatus
    }
    if (
        mapping_report.get("contract_version") != MAPPING_CONTRACT_VERSION
        or mapping_report.get("mapped_symbols") != manifest.mapped_symbols
        or mapping_report.get("unresolved_symbols") != manifest.unresolved_symbols
        or mapping_report.get("orphan_price_symbols") != manifest.orphan_price_symbols
        or mapping_report.get("status_counts") != expected_status_counts
        or mapping_report.get("canonical_eligible") is not False
        or mapping_report.get("quarantined") is not True
    ):
        raise ValueError("legacy mapping report does not match verified mapping")
    unit_report = json.loads(
        artifacts_by_kind["unit_audit_report"].read_text(encoding="utf-8")
    )
    if (
        unit_report.get("audit_version") != UNIT_AUDIT_VERSION
        or unit_report.get("original_volume_preserved") is not True
        or unit_report.get("volume_unit") != "unknown_or_mixed"
        or unit_report.get("volume_shares") is not None
        or unit_report.get("global_multiplier_applied") is not False
        or unit_report.get("anomaly_rule")
        != "bidirectional adjacent-day ratio between 80 and 120"
        or unit_report.get("anomaly_rows") != manifest.volume_anomaly_rows
    ):
        raise ValueError("legacy unit audit report does not match audited prices")
    adjustment_report = json.loads(
        artifacts_by_kind["adjustment_audit_report"].read_text(encoding="utf-8")
    )
    if (
        adjustment_report.get("audit_version") != ADJUSTMENT_AUDIT_VERSION
        or adjustment_report.get("adjustment_semantics") != "unverified"
        or adjustment_report.get("adjusted_value_usable") is not False
        or adjustment_report.get("factor_generated") is not False
        or adjustment_report.get("difference_rows") != adjustment_diffs
    ):
        raise ValueError("legacy adjustment report does not match audited prices")
    coverage_report = json.loads(
        artifacts_by_kind["coverage_report"].read_text(encoding="utf-8")
    )
    if (
        coverage_report.get("archive_id") != manifest.migration_input.archive_id
        or coverage_report.get("stock_prices_rows") != manifest.price_rows
        or coverage_report.get("stock_universe_rows")
        != manifest.migration_input.stock_universe_rows
        or coverage_report.get("price_symbols")
        != source_manifest.coverage.distinct_price_symbols
        or coverage_report.get("min_date")
        != (
            source_manifest.coverage.min_date.isoformat()
            if source_manifest.coverage.min_date is not None
            else None
        )
        or coverage_report.get("max_date")
        != (
            source_manifest.coverage.max_date.isoformat()
            if source_manifest.coverage.max_date is not None
            else None
        )
        or coverage_report.get("orphan_price_symbols")
        != source_manifest.coverage.orphan_price_symbols
        or coverage_report.get("source_history_export")
        != source_manifest.coverage.source_history_export
        or coverage_report.get("market_history_completeness") != "not_claimed"
    ):
        raise ValueError("legacy coverage report does not match migration evidence")
    discrepancy = json.loads(
        artifacts_by_kind["discrepancy_ready_manifest"].read_text(encoding="utf-8")
    )
    if (
        discrepancy.get("conversion_id") != manifest.conversion_id
        or discrepancy.get("ready_for_future_cross_validation") is not True
        or discrepancy.get("candidate_source") != "none"
        or discrepancy.get("source_selection_performed") is not False
        or discrepancy.get("canonical_eligible") is not False
        or discrepancy.get("quarantined") is not True
    ):
        raise ValueError("legacy discrepancy evidence incorrectly names a canonical source")

    catalog_status = _verify_catalog_exclusion(
        root, manifest, legacy_prefix=expected_prefix
    )
    return SSLegacyConversionVerification(
        source_price_file_count=len(source_price_files),
        catalog_status=catalog_status,
    )
