# Private environment configuration

Copy `.env.example` to `.env`, set mode `0600`, and fill in deployment values.
The real file is excluded from Git and Docker build contexts. Never put API
credentials in a `VITE_*` variable, browser bundle, checked-in JSON, or URL.

```sh
cp .env.example .env
chmod 600 .env
# Generate independent values for QUANT_API_KEY and VIBE_API_AUTH_KEY:
openssl rand -hex 32
```

Only edit `.env` with the generated values; do not paste them into commands,
documentation, screenshots, or commits. The local preparation checkout may
already have an initialized private `.env`; copying is only for new checkouts.

## Local execution

`uv` loads the file explicitly, including `${VARIABLE}` references. Existing
process environment variables take precedence. Run from the repository root:

```sh
uv run --env-file .env quant-stack-api
uv run --env-file .env quant-stack validate-strategy strategy_spec.yaml
```

The application continues to use its existing environment-variable interfaces;
importing the Python package does not silently load a file from a parent folder.
Tests use `uv run --no-env-file pytest` so local credentials are not test inputs.

## JSON job configuration

Deployment-specific paths, proxy URLs and downstream API addresses in
`config/*.example.json` use environment placeholders. Render a private runtime
file before invoking an existing job:

```sh
uv run --env-file .env python -m quant_stack.configuration json \
  config/us_equity_daily_nas.example.json config/us_equity_daily_nas.local.json
```

The renderer expands JSON string values without executing shell text, preserves
JSON types, refuses missing/empty variables, and writes atomically with mode
`0600`. Generated `*.local.json` files are excluded from Git and build contexts.
Set `QUANT_STACK_ROOT`, data/runtime roots, the snapshot path, proxy and screener
URL in `.env` for the actual host. No job or Provider request is started by rendering.

## Docker and the browser gateway

```sh
make configure
docker compose --env-file .env -f docker/compose.nas.yaml config --quiet
```

Compose receives only each service's declared environment variables. QuantStack
and Vibe require separate nonempty authentication keys. QuantStack's key is also
rendered into an ignored server-side Docker secret for the frontend Nginx proxy;
it is never copied into static frontend files. On the deployment host, install
that secret with group `QUANT_SECRET_GID` and mode `0640` if the non-root proxy
needs group access. The private source `.env` stays `0600`.

The gateway renderer writes configuration under `docker/generated/openresty/`.
It takes the domain, external port, upstream host and service ports from `.env`.
Install these generated files using your gateway's existing deployment process;
the checked-in `docker/openresty/` tree is the public example, not a configured
production gateway. The browser gateway requires sibling service subdomains
under `QUANT_PUBLIC_DOMAIN`. IPv4/DNS hostnames are supported by this renderer;
invalid hostnames and ports fail before output is written. Existing TLS, cookie,
session and service allowlist rules are preserved.

The optional upstream Vibe `.env` file can still be selected with
`QUANT_VIBE_ENV_FILE` for upstream-specific provider configuration. Those private
values are not part of the public source. Container-internal paths, loopback
health checks, upstream documentation URLs and synthetic test addresses remain
public reference values; they are not copied from a personal deployment.

Use `config --quiet` when validating real configuration: normal `docker compose
config` output may contain expanded secret values. Rendered files and logs can
contain private addresses and must remain excluded from publication.

## Frontend development

Both Vite development servers read the repository-root `.env` only in their Node
configuration. `QUANT_DEV_API_URL`, the development listen address/ports and the
brokerage target are configurable there. The API key is inserted into the server
proxy request header; it is not defined as a frontend environment variable.
Production browser builds use relative API paths and the server-side Nginx proxy.
