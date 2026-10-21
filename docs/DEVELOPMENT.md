# Development

## Requirements

- Python 3.12 (managed by `uv`)
- uv 0.9.28 or a compatible newer client
- Git 2.40+
- Docker with Compose v2+
- Rust only when building NautilusTrader from source

## Bootstrap

```bash
cp .env.example .env
uv python install 3.12
uv sync --frozen
uv run pytest
uv run ruff check .
```

### macOS: external Python 3.12 environment

For long-lived worktrees, keep the virtual environment outside the repository
and expose it through the conventional `.venv` link. This prevents each
worktree from carrying another large environment while preserving normal editor
and `uv` discovery.

```bash
pyenv install 3.12.13
mkdir -p "${HOME}/.venvs"

quantstack_venv="${HOME}/.venvs/quantstack-ss-kline-viewport-paging-py312"
uv venv --python 3.12 "${quantstack_venv}"
UV_PROJECT_ENVIRONMENT="${quantstack_venv}" uv sync --frozen --all-extras
ln -s "${quantstack_venv}" .venv
```

Before creating the link, preserve or remove an existing `.venv` deliberately;
do not overwrite it. Use `--all-extras` when restoring the full development
environment: a plain `uv sync --frozen` intentionally prunes packages that are
present only through optional extras.

The host setup verified on 2026-09-04 is:

- pyenv CPython: `3.12.13` at `~/.pyenv/versions/3.12.13`;
- external environment: `~/.venvs/quantstack-ss-kline-viewport-paging-py312`;
- repository link: `.venv` points to that external environment;
- environment interpreter: uv-managed CPython `3.12.12`;
- all project extras restored and imports verified for DuckDB, PyArrow, pandas,
  NautilusTrader, AKShare, and BaoStock.

In this host's shell, `python` and explicit `python3.12` select Python 3.12.
Homebrew's unversioned `python3` may select a newer interpreter, so repository
commands should use `uv run`, `.venv/bin/python`, or `python3.12`.

Optional integration dependencies are installed deliberately:

```bash
uv sync --extra data
uv sync --extra nautilus
```

Start the Nautilus research API and the independent Web clients during UI
development:

```bash
uv sync --frozen --extra nautilus --extra data
uv run quant-stack-api

# In upstream/Vibe-Trading/frontend
VITE_QUANT_API_URL=http://127.0.0.1:8900 npm run dev

# In QuantStack/apps/nautilus-lab
npm ci
npm run dev
```

Develop and verify the Quant Center frontend separately:

```bash
cd apps/quant-center
npm ci
npm test -- --run
npm run lint
npm run build
npm audit --omit=dev --registry=https://registry.npmjs.org
```

The final Quant Center image contains only compiled static assets and Nginx. Its
Node, TypeScript, test, and lint packages remain in the build stage.

The NAS Compose file attaches only `quant-api` to the existing Stock Screener
network so service health can be checked without routing private API keys through
the browser. Override `QUANT_SCREENER_NETWORK` when the upstream Compose project
name changes.

The cloned NautilusTrader `develop` source and the PyPI integration package may
have different versions. `VERSION_LOCK.json` records both; do not infer that an
unreleased source version is installable.

Do not add Vibe-Trading as a Python dependency. It exchanges files through the
research contract. Do not add credentials to `.env.example` or tracked YAML.

## Change discipline

- add or change a schema before changing an adapter contract;
- add a migration note for breaking schema changes;
- pin dependencies through `uv.lock`;
- run tests and lint before each local commit;
- keep generated data and experiments under the NAS root;
- never enable a broker connection in test or research configuration.

## Docker

```bash
docker compose -f docker/compose.yaml config
docker compose -f docker/compose.yaml build
docker compose -f docker/compose.yaml run --rm quant-dev
```

`docker/compose.yaml` is the development baseline. `docker/compose.nas.yaml`
is the hardened Quant API, Quant Center, and Vibe-Trading NAS contract; see
`DEPLOYMENT_NAS.md`.
