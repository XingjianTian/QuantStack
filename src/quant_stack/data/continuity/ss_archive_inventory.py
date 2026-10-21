from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .ss_archive_connection import (
    exact_pg_environment,
    postgres_connection_fingerprint,
    resolved_connection_parameters,
)
from .ss_archive_models import (
    EvidenceStatus,
    SnapshotConsistency,
    SourceHistoryStatus,
    SSArchiveColumn,
    SSArchiveConstraint,
    SSArchiveIndex,
    SSArchiveTableInventory,
    SSArchiveTableSummary,
    SSCoverageSummary,
    SSDatabaseInventory,
    SSDataQualityInventory,
    SSMarketCoverage,
)
from .ss_archive_safety import assert_read_only_sql, validate_identifier


@dataclass(frozen=True)
class SnapshotMetadata:
    opened_at: datetime
    raw_snapshot_id: str
    snapshot_fingerprint: str
    transaction_marker: str
    database_fingerprint: str
    connection_fingerprint: str
    postgresql_version: str
    encoding: str
    timezone: str
    consistency: SnapshotConsistency = SnapshotConsistency.EXPORTED_SNAPSHOT


class PostgresSnapshot(AbstractContextManager["PostgresSnapshot"]):
    """Own one exported repeatable-read, read-only PostgreSQL snapshot."""

    def __init__(
        self,
        dsn: str,
        *,
        application_name: str = "quantstack_phase22_ss_archive",
        lock_timeout_seconds: int = 5,
        statement_timeout_seconds: int = 300,
        snapshot_timeout_seconds: int = 1800,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self.dsn = dsn
        self.application_name = application_name
        self.lock_timeout_seconds = lock_timeout_seconds
        self.statement_timeout_seconds = statement_timeout_seconds
        self.snapshot_timeout_seconds = snapshot_timeout_seconds
        self._connect = connect
        self.connection: Any | None = None
        self.connection_parameters: dict[str, str] | None = None
        self.metadata: SnapshotMetadata | None = None

    def __enter__(self) -> PostgresSnapshot:
        if self._connect is None:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:  # pragma: no cover - optional environment
                raise RuntimeError("PostgreSQL inventory requires the 'archive' extra") from exc
            self._connect = psycopg.connect
            self.connection = self._connect(self.dsn, row_factory=dict_row)
        else:
            self.connection = self._connect(self.dsn)
        try:
            cursor = self.connection.cursor()
            statements = (
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
                f"SET LOCAL lock_timeout = '{self.lock_timeout_seconds}s'",
                f"SET LOCAL statement_timeout = '{self.statement_timeout_seconds}s'",
                "SET LOCAL idle_in_transaction_session_timeout = "
                f"'{self.snapshot_timeout_seconds}s'",
                f"SET LOCAL application_name = '{self.application_name}'",
            )
            for sql in statements:
                assert_read_only_sql(sql)
                cursor.execute(sql)
            cursor.execute(
                """
                SELECT
                    pg_export_snapshot() AS snapshot_id,
                    txid_current_snapshot()::text AS transaction_marker,
                    current_database() AS database_name,
                    current_setting('server_version') AS postgresql_version,
                    current_setting('server_encoding') AS encoding,
                    current_setting('TimeZone') AS timezone,
                    clock_timestamp() AS opened_at
                """
            )
            row = cursor.fetchone()
            raw_snapshot = str(row["snapshot_id"])
            database_name = str(row["database_name"])
            connection_parameters = resolved_connection_parameters(self.connection, self.dsn)
            connection_parameters["dbname"] = database_name
            self.connection_parameters = connection_parameters
            self.metadata = SnapshotMetadata(
                opened_at=row["opened_at"].astimezone(UTC),
                raw_snapshot_id=raw_snapshot,
                snapshot_fingerprint=hashlib.sha256(raw_snapshot.encode()).hexdigest(),
                transaction_marker=str(row["transaction_marker"]),
                database_fingerprint=hashlib.sha256(database_name.encode()).hexdigest(),
                connection_fingerprint=postgres_connection_fingerprint(connection_parameters),
                postgresql_version=str(row["postgresql_version"]),
                encoding=str(row["encoding"]),
                timezone=str(row["timezone"]),
            )
            return self
        except Exception:
            if self.connection is not None:
                self.connection.rollback()
                self.connection.close()
            self.connection = None
            self.connection_parameters = None
            raise

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.connection is not None:
            self.connection.rollback()
            self.connection.close()
            self.connection = None

    def pg_environment(self, inherited: Mapping[str, str]) -> dict[str, str]:
        if self.connection_parameters is None:
            raise RuntimeError("snapshot connection parameters are unavailable")
        return exact_pg_environment(self.connection_parameters, inherited)

    def follower_connection(self):
        if self.metadata is None or self._connect is None:
            raise RuntimeError("snapshot is not open")
        try:
            from psycopg.rows import dict_row

            connection = self._connect(self.dsn, row_factory=dict_row)
        except (ImportError, TypeError):
            connection = self._connect(self.dsn)
        try:
            cursor = connection.cursor()
            for statement in (
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
                f"SET TRANSACTION SNAPSHOT '{self.metadata.raw_snapshot_id}'",
                f"SET LOCAL lock_timeout = '{self.lock_timeout_seconds}s'",
                f"SET LOCAL statement_timeout = '{self.statement_timeout_seconds}s'",
                "SET LOCAL idle_in_transaction_session_timeout = "
                f"'{self.snapshot_timeout_seconds}s'",
                f"SET LOCAL application_name = '{self.application_name}_reader'",
            ):
                assert_read_only_sql(statement)
                cursor.execute(statement)
            return connection
        except Exception:
            connection.rollback()
            connection.close()
            raise


class SSInventoryCollector:
    REQUIRED_TABLES = ("stock_prices", "stock_universe")

    def __init__(self, connection: Any, metadata: SnapshotMetadata):
        self.connection = connection
        self.metadata = metadata

    def _rows(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[Mapping[str, Any]]:
        assert_read_only_sql(sql)
        cursor = self.connection.cursor()
        cursor.execute(sql, parameters)
        columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
        values = cursor.fetchall()
        if values and isinstance(values[0], Mapping):
            return list(values)
        return [dict(zip(columns, row, strict=True)) for row in values]

    def _row(self, sql: str, parameters: tuple[Any, ...] = ()) -> Mapping[str, Any]:
        rows = self._rows(sql, parameters)
        if len(rows) != 1:
            raise ValueError("inventory query did not return exactly one row")
        return rows[0]

    def discover_table_names(self) -> tuple[str, ...]:
        rows = self._rows(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        names = tuple(str(row["table_name"]) for row in rows)
        missing = set(self.REQUIRED_TABLES) - set(names)
        if missing:
            raise ValueError(f"missing required legacy table(s): {', '.join(sorted(missing))}")
        return names

    def _table_inventory(self, table_name: str) -> SSArchiveTableInventory:
        validate_identifier(table_name)
        columns = self._rows(
            """
            SELECT ordinal_position, column_name, data_type, is_nullable,
                   column_default, domain_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        enum_rows = self._rows(
            """
            SELECT a.attname AS column_name, e.enumlabel
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = 'public' AND c.relname = %s
            ORDER BY a.attname, e.enumsortorder
            """,
            (table_name,),
        )
        enum_values: dict[str, list[str]] = {}
        for row in enum_rows:
            enum_values.setdefault(str(row["column_name"]), []).append(str(row["enumlabel"]))
        constraint_rows = self._rows(
            """
            SELECT con.conname,
                   con.contype,
                   ARRAY(
                     SELECT att.attname FROM unnest(con.conkey) WITH ORDINALITY keys(attnum, ord)
                     JOIN pg_attribute att ON att.attrelid = con.conrelid
                                           AND att.attnum = keys.attnum
                     ORDER BY keys.ord
                   ) AS columns,
                   ref.relname AS referenced_table,
                   CASE WHEN con.confkey IS NULL THEN ARRAY[]::text[] ELSE ARRAY(
                     SELECT att.attname FROM unnest(con.confkey) WITH ORDINALITY keys(attnum, ord)
                     JOIN pg_attribute att ON att.attrelid = con.confrelid
                                           AND att.attnum = keys.attnum
                     ORDER BY keys.ord
                   ) END AS referenced_columns
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = rel.relnamespace
            LEFT JOIN pg_class ref ON ref.oid = con.confrelid
            WHERE n.nspname = 'public' AND rel.relname = %s
            ORDER BY con.conname
            """,
            (table_name,),
        )
        kind = {"p": "primary_key", "u": "unique", "f": "foreign_key", "c": "check"}
        constraints = tuple(
            SSArchiveConstraint(
                name=str(row["conname"]),
                kind=kind[str(row["contype"])],
                columns=tuple(row["columns"] or ()),
                referenced_table=row["referenced_table"],
                referenced_columns=tuple(row["referenced_columns"] or ()),
            )
            for row in constraint_rows
            if str(row["contype"]) in kind
        )
        index_rows = self._rows(
            """
            SELECT idx.relname AS index_name, i.indisunique, i.indisprimary,
                   pg_relation_size(idx.oid) AS size_bytes,
                   ARRAY(
                     SELECT att.attname FROM unnest(i.indkey) WITH ORDINALITY keys(attnum, ord)
                     JOIN pg_attribute att ON att.attrelid = i.indrelid
                                           AND att.attnum = keys.attnum
                     WHERE keys.attnum > 0 ORDER BY keys.ord
                   ) AS columns
            FROM pg_index i
            JOIN pg_class rel ON rel.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = rel.relnamespace
            JOIN pg_class idx ON idx.oid = i.indexrelid
            WHERE n.nspname = 'public' AND rel.relname = %s
            ORDER BY idx.relname
            """,
            (table_name,),
        )
        indexes = tuple(
            SSArchiveIndex(
                name=str(row["index_name"]),
                columns=tuple(row["columns"] or ()),
                unique=bool(row["indisunique"]),
                primary=bool(row["indisprimary"]),
                size_bytes=int(row["size_bytes"]),
            )
            for row in index_rows
        )
        counts = self._row(
            f"""
            SELECT count(*)::bigint AS exact_row_count,
                   pg_table_size('public.{table_name}'::regclass)::bigint AS table_size_bytes,
                   pg_indexes_size('public.{table_name}'::regclass)::bigint AS index_size_bytes,
                   (SELECT reltuples::bigint FROM pg_class
                    WHERE oid = 'public.{table_name}'::regclass) AS estimated_row_count
            FROM public.{table_name}
            """
        )
        primary_key = next(
            (constraint.columns for constraint in constraints if constraint.kind == "primary_key"),
            (),
        )
        return SSArchiveTableInventory(
            schema_name="public",
            table_name=table_name,
            estimated_row_count=max(0, int(counts["estimated_row_count"])),
            exact_row_count=int(counts["exact_row_count"]),
            table_size_bytes=int(counts["table_size_bytes"]),
            index_size_bytes=int(counts["index_size_bytes"]),
            columns=tuple(
                SSArchiveColumn(
                    ordinal_position=int(row["ordinal_position"]),
                    name=str(row["column_name"]),
                    data_type=str(row["data_type"]),
                    nullable=row["is_nullable"] == "YES",
                    default_expression=row["column_default"],
                    domain_name=row["domain_name"],
                    enum_values=tuple(enum_values.get(str(row["column_name"]), ())),
                )
                for row in columns
            ),
            constraints=constraints,
            indexes=indexes,
            primary_key=primary_key,
        )

    def per_symbol_coverage(self) -> Iterator[Mapping[str, Any]]:
        rows = self._rows(
            """
            SELECT COALESCE(u.market, 'unknown') AS market, p.symbol,
                   min(p.date) AS min_date, max(p.date) AS max_date,
                   count(*)::bigint AS row_count
            FROM public.stock_prices p
            LEFT JOIN public.stock_universe u ON u.symbol = p.symbol
            GROUP BY COALESCE(u.market, 'unknown'), p.symbol
            ORDER BY COALESCE(u.market, 'unknown'), p.symbol
            """
        )
        yield from rows

    def collect(
        self,
        *,
        archive_id: str,
        source_commit: str,
        retention_policy: str,
        cleanup_evidence: str,
        per_symbol_coverage_uri: str,
        source_history_export: SourceHistoryStatus = SourceHistoryStatus.UNKNOWN,
    ) -> SSDatabaseInventory:
        table_names = self.discover_table_names()
        # Inventory every public base table so the archive retains the source schema,
        # size, constraint, and row-count evidence without exporting unrelated data.
        # Only the two required market-data tables are eligible for frozen export.
        target_tables = tuple(self._table_inventory(name) for name in table_names)
        schema_revision = self._row(
            """
            SELECT CASE WHEN to_regclass('public.alembic_version') IS NULL
                        THEN 'not_observed'
                        ELSE (SELECT version_num FROM public.alembic_version LIMIT 1)
                   END AS revision
            """
        )["revision"]
        coverage_row = self._row(
            """
            SELECT count(*)::bigint AS row_count,
                   count(DISTINCT symbol)::bigint AS distinct_symbols,
                   min(date) AS min_date, max(date) AS max_date
            FROM public.stock_prices
            """
        )
        markets = tuple(
            SSMarketCoverage(
                market=str(row["market"]),
                row_count=int(row["row_count"]),
                distinct_symbols=int(row["distinct_symbols"]),
                min_date=row["min_date"],
                max_date=row["max_date"],
            )
            for row in self._rows(
                """
                SELECT COALESCE(u.market, 'unknown') AS market,
                       count(*)::bigint AS row_count,
                       count(DISTINCT p.symbol)::bigint AS distinct_symbols,
                       min(p.date) AS min_date, max(p.date) AS max_date
                FROM public.stock_prices p
                LEFT JOIN public.stock_universe u ON u.symbol = p.symbol
                GROUP BY COALESCE(u.market, 'unknown') ORDER BY market
                """
            )
        )
        universe = self._row(
            """
            SELECT count(*) FILTER (WHERE is_active IS TRUE)::bigint AS active,
                   count(*) FILTER (WHERE is_active IS NOT TRUE)::bigint AS non_active,
                   count(*) FILTER (WHERE NOT EXISTS (
                     SELECT 1 FROM public.stock_prices p WHERE p.symbol = u.symbol
                   ))::bigint AS without_prices
            FROM public.stock_universe u
            """
        )
        orphan = self._row(
            """
            SELECT count(*)::bigint AS count FROM (
              SELECT DISTINCT p.symbol FROM public.stock_prices p
              LEFT JOIN public.stock_universe u ON u.symbol = p.symbol
              WHERE u.symbol IS NULL
            ) values_without_universe
            """
        )
        quality_row = self._row(
            """
            SELECT count(*) FILTER (WHERE id IS NULL)::bigint AS pk_null,
                   count(*) - count(DISTINCT id) AS pk_duplicate,
                   count(*) FILTER (WHERE open IS NULL)::bigint AS open_null,
                   count(*) FILTER (WHERE high IS NULL)::bigint AS high_null,
                   count(*) FILTER (WHERE low IS NULL)::bigint AS low_null,
                   count(*) FILTER (WHERE close IS NULL)::bigint AS close_null,
                   count(*) FILTER (WHERE volume IS NULL)::bigint AS volume_null,
                   count(*) FILTER (WHERE adj_close IS NULL)::bigint AS adj_close_null,
                   count(*) FILTER (WHERE open < 0 OR high < 0 OR low < 0 OR close < 0
                                          OR adj_close < 0)::bigint AS negative_price,
                   count(*) FILTER (WHERE volume < 0)::bigint AS negative_volume,
                   count(*) FILTER (WHERE volume = 0)::bigint AS zero_volume,
                   count(*) FILTER (
                     WHERE adj_close IS NOT NULL AND close IS DISTINCT FROM adj_close
                   )
                     ::bigint AS adj_close_difference,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY volume)
                     FILTER (WHERE volume IS NOT NULL) AS volume_p50,
                   percentile_cont(0.9) WITHIN GROUP (ORDER BY volume)
                     FILTER (WHERE volume IS NOT NULL) AS volume_p90,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY volume)
                     FILTER (WHERE volume IS NOT NULL) AS volume_p99
            FROM public.stock_prices
            """
        )
        duplicate = self._row(
            """
            SELECT COALESCE(sum(row_count - 1), 0)::bigint AS duplicate_count FROM (
              SELECT count(*)::bigint AS row_count FROM public.stock_prices
              GROUP BY symbol, date HAVING count(*) > 1
            ) duplicates
            """
        )
        jumps = self._row(
            """
            WITH ordered AS (
              SELECT symbol, date, volume,
                     lag(volume) OVER (PARTITION BY symbol ORDER BY date, id) AS previous_volume
              FROM public.stock_prices WHERE volume IS NOT NULL AND volume > 0
            )
            SELECT count(*) FILTER (
              WHERE previous_volume > 0 AND
                    greatest(volume::numeric / previous_volume,
                             previous_volume::numeric / volume) BETWEEN 80 AND 120
            )::bigint AS suspicious_count
            FROM ordered
            """
        )
        delisted = (
            EvidenceStatus.PRESENT if int(universe["non_active"]) > 0 else EvidenceStatus.UNKNOWN
        )
        coverage = SSCoverageSummary(
            stock_prices_row_count=int(coverage_row["row_count"]),
            stock_universe_row_count=next(
                table.exact_row_count
                for table in target_tables
                if table.table_name == "stock_universe"
            ),
            distinct_price_symbols=int(coverage_row["distinct_symbols"]),
            min_date=coverage_row["min_date"],
            max_date=coverage_row["max_date"],
            markets=markets,
            active_universe_symbols=int(universe["active"]),
            non_active_universe_symbols=int(universe["non_active"]),
            orphan_price_symbols=int(orphan["count"]),
            universe_symbols_without_prices=int(universe["without_prices"]),
            delisted_coverage=delisted,
            source_history_export=source_history_export,
            quality=SSDataQualityInventory(
                primary_key_null_count=int(quality_row["pk_null"]),
                primary_key_duplicate_count=int(quality_row["pk_duplicate"]),
                symbol_date_duplicate_count=int(duplicate["duplicate_count"]),
                null_counts={
                    "open": int(quality_row["open_null"]),
                    "high": int(quality_row["high_null"]),
                    "low": int(quality_row["low_null"]),
                    "close": int(quality_row["close_null"]),
                    "volume": int(quality_row["volume_null"]),
                    "adj_close": int(quality_row["adj_close_null"]),
                },
                negative_price_count=int(quality_row["negative_price"]),
                negative_volume_count=int(quality_row["negative_volume"]),
                zero_volume_count=int(quality_row["zero_volume"]),
                adj_close_null_count=int(quality_row["adj_close_null"]),
                adj_close_difference_count=int(quality_row["adj_close_difference"]),
                volume_quantiles={
                    "p50": quality_row["volume_p50"],
                    "p90": quality_row["volume_p90"],
                    "p99": quality_row["volume_p99"],
                },
                suspicious_volume_jump_count=int(jumps["suspicious_count"]),
            ),
            per_symbol_coverage_uri=per_symbol_coverage_uri,
        )
        type_dependencies = {
            f"{table.schema_name}.{table.table_name}.{column.name}": column.enum_values
            for table in target_tables
            for column in table.columns
            if column.enum_values
        }
        return SSDatabaseInventory(
            archive_id=archive_id,
            database_fingerprint=self.metadata.database_fingerprint,
            connection_fingerprint=self.metadata.connection_fingerprint,
            postgresql_version=self.metadata.postgresql_version,
            encoding=self.metadata.encoding,
            timezone=self.metadata.timezone,
            snapshot_timestamp=self.metadata.opened_at,
            snapshot_fingerprint=self.metadata.snapshot_fingerprint,
            transaction_marker=self.metadata.transaction_marker,
            consistency=self.metadata.consistency,
            source_commit=source_commit,
            source_schema_revision=str(schema_revision or "not_observed"),
            all_table_names=table_names,
            all_table_summaries=tuple(
                SSArchiveTableSummary(
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    estimated_row_count=table.estimated_row_count,
                    exact_row_count=table.exact_row_count,
                    table_size_bytes=table.table_size_bytes,
                    index_size_bytes=table.index_size_bytes,
                )
                for table in target_tables
            ),
            type_dependencies=type_dependencies,
            tables=target_tables,
            coverage=coverage,
            retention_policy=retention_policy,
            cleanup_evidence=cleanup_evidence,
            inventory_uri=(f"archive://ss-legacy/reports/{archive_id}/inventory.json"),
        )
