# Data management

## NAS layout

```text
quant-data/
├── raw/{CN,HK,US}/{provider}/{dataset}/execution_date=YYYY-MM-DD/
├── normalized/{bars_1d,instruments,corporate_actions,fundamentals_pit}/market=XX/version=V/
├── features/{factors,labels,universes}/
├── experiments/<experiment_id>/
├── backtests/<strategy_id>/<strategy_version>/
├── reports/
├── logs/
├── catalog/quant.duckdb
├── catalog/manifests/<dataset_id>@<version>.json
└── backups/
```

Stocks and ETFs use market-specific partitions. Fundamentals must preserve
filing/release time and revisions. News/text preserves source, publication time,
retrieval time, and license scope. Factors are derived and must reference their
input dataset versions. Backtest results are experiment artifacts, not datasets
to overwrite.

The Quant Workspace PostgreSQL database and Redis cache are operational stores,
not the authoritative research data lake. Preserve them for application
continuity and back them up before upgrades, but do not cite their mutable tables
as an experiment dataset version. Research promoted from the workspace must be
exported through a Quant Stack adapter into a versioned dataset manifest or an
experiment bundle with source revision and retrieval time recorded.

## Version model

- Raw/Bronze is append-only. Never replace a response in place.
- Normalized/Silver is rebuilt from explicit raw versions and schema versions.
- Features/Gold records code commit, parameters, input manifests, and availability
  cutoffs.
- Dataset manifests are tracked in Git; payload URIs use `quant-data://`.
- A manifest version is immutable. Routine CN/US Session-Parquet records semantic identity, rows,
  instruments, session date and duplicate-key results without hashing the Parquet payload. Use a
  content checksum only when a cross-trust transfer, manual disaster recovery or historical evidence
  contract explicitly requires it.
- `available_at <= decision_time` is mandatory for point-in-time backtests.

Recommended dataset version: `YYYY-MM-DD.sequence` for snapshots or a provider
revision identifier when stable. Parquet is the default tabular format; JSONL is
appropriate for immutable text events; DuckDB is the catalog/query layer.

## Safety and backup

The data root is injected with `QUANT_DATA_ROOT`, never hard-coded. NAS RAID does
not replace backup. Keep a second private backup of catalog, manifests, strategy
repository, and irreplaceable raw data. Validate restore procedures quarterly.

Preview the data-lake directories before creating them:

```bash
uv run python scripts/init_data_lake.py /path/to/quant-data
uv run python scripts/init_data_lake.py /path/to/quant-data --apply
```

For a NAS deployment, use a private host path such as `/srv/quant-data` and map
it to the container data root. Keep the real mount path and ownership details in
ignored runtime configuration. An administrator creates the root once and grants
it to the project service account; platform scripts maintain its children.
