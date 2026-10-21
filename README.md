# QuantStack

Personal quantitative research infrastructure with typed market-data contracts,
offline replay, data-quality checks, a research API, NautilusTrader integration,
and optional Stock Screener / Vibe-Trading adapters.

This is a research-only project. It provides no live broker execution.
The brokerage sample uses fictional data and non-executable proposals.

## Development

Python 3.12–3.14, `uv`, and Gitleaks 8.25+ are required for the full offline test
suite, including privacy-check integration tests. The suite uses the
optional data, archive, brokerage, CN-provider and Nautilus dependencies:

```sh
uv sync --frozen --all-extras
uv run --no-env-file pytest
```

No Provider collection, production deployment or external service login is needed
for the default suite. Tests that require omitted historical operations evidence,
optional upstream checkouts, or a Lua runtime report an
explicit skip. Their assertions are retained; these skips do not claim those
integration or historical acceptance checks passed.

## Private configuration

```sh
cp .env.example .env
chmod 600 .env
# Fill .env with independent locally generated keys and deployment values.
uv run --env-file .env quant-stack-api
```

See [environment configuration](docs/ENVIRONMENT.md) for JSON templates, private
gateway rendering, Docker secret preparation and Compose validation. The real
`.env`, rendered files, credentials, local databases and build outputs are excluded
from both Git and Docker build contexts. Never use frontend `VITE_*` variables for
secrets.

Public-source defaults are examples, container service names and loopback
addresses. Personal hostnames, ports, proxy URLs and filesystem roots belong in
the local `.env`. No existing NAS or other service is configured by cloning this
repository.

## Source boundaries

- Development conversations, historical handoffs, production receipts and private
  acceptance evidence are retained in a separate private backup.
- The public replay corpus contains deterministic synthetic OHLCV, not downloaded
  market prices. Its compatibility labels do not establish real provider behavior
  or actual acquisition provenance. Regenerate it with
  `uv run --extra data python scripts/generate_public_replay_fixture.py`.
- Source-provider URLs and API protocol identifiers describe public upstream
  services. Provider libraries still require their own terms and access review
  before live data acquisition.
- Optional upstream sources are pinned in `VERSION_LOCK.json`. See
  [third-party notices](THIRD_PARTY_NOTICES.md) and the included upstream license
  copies. This repository does not grant a new license for the project's own code;
  public visibility and open-source licensing are separate decisions.

Useful documentation: [architecture](docs/ARCHITECTURE.md),
[market-data API](docs/MARKET_DATA_BATCH_API.md),
[fictional brokerage sample](docs/SCHWAB_READONLY_SAMPLE.md), and
[configuration](docs/ENVIRONMENT.md).

## Privacy before committing

The maintained repository is [QuantStack](https://github.com/XingjianTian/QuantStack).
After cloning, install Gitleaks 8.25+, configure your GitHub noreply email and run
`make privacy-setup`. Commits then check the complete staged snapshot, private
paths and credentials. See [privacy checks](docs/PRIVACY.md) for local policy and
verification limits.
