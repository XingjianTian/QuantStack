# QuantStack maintenance

The single maintained repository is https://github.com/XingjianTian/QuantStack. Historical
operational records, prior Git history and unmerged work are held in a private
backup and must not be merged or pushed back into this repository. Continue new
work from the current main branch; recreate older work as reviewed source changes.

Deployment-specific values are supplied by the ignored local `.env`, loaded with
`uv run --env-file .env`. Render JSON runtime files and gateway configuration into
the ignored paths described in `docs/ENVIRONMENT.md`. Keys stay on the server.

The replay OHLCV corpus is deterministic synthetic data. It proves fixture
integrity and offline behavior, not actual provider acquisition or production
acceptance. Optional external integrations and absent historical evidence have
explicit test skips and must be reported separately from passing checks.

Obsolete one-off image/dependency planning tools tied to historical commit IDs
were retired. Dependency lockfile hashes, container digests and market-data
integrity checks remain part of the supported build and data contracts.
