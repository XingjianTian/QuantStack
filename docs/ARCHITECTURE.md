# Architecture

## Repository boundary

```text
Quant/
├── upstream/nautilus_trader/  # independent Git repository; official remote = upstream
├── upstream/Vibe-Trading/     # independent Git repository; official remote = upstream
├── upstream/stock-screener/   # independent Git repository; browser research workspace
└── QuantStack/                # independent, user-owned Git repository
```

NautilusTrader is the eventual deterministic event-driven backtest and execution
core. Vibe-Trading is the AI research assistant. Stock Screener is the human-facing
screening, watchlist, chart, and market-review workspace. Quant Stack owns
contracts, adapters, strategies, dataset metadata, reproducibility evidence,
tests, and deployment. The NAS owns large and private payloads.

## Phase 1 data flow

```text
Vibe-Trading or human research
  -> Quant Workspace selection/watchlist
  -> research output
  -> validated strategy_spec.yaml
  -> Quant Stack Nautilus adapter
  -> NautilusTrader backtest
  -> immutable experiment bundle
  -> research feedback
```

The durable interface remains file-based and one-way at each step. A narrow
read/run HTTP API exposes strategy metadata and backtest evidence to the
independent Nautilus Lab Web UI; the API does not expose broker connections or
order routes. Vibe-Trading exchanges research output through the versioned
`strategy_spec.yaml` contract instead of owning a Nautilus route. Its shared
top-navigation entry for Nautilus is an external cross-service link only: VT
contains no Nautilus page, API client, execution state, or strategy runtime.

## Web integration boundary

```text
Browser
  -> OpenResty :11443
     -> quant.example.com -> Quant Center Web :18920
        -> /api/* -> Quant Stack API :18900
                      -> NautilusTrader BacktestEngine
                      -> NAS experiment and navigation state
     -> vibe-trading.example.com -> Vibe-Trading Web :18899
     -> nautilus.example.com -> Nautilus Lab Web :18930
        -> /api/* -> Quant Stack API :18900
     -> stock-screener.example.com -> Stock Screener Web :18910
        -> isolated backend/PostgreSQL/Redis
```

Quant Center owns the stable platform navigation but not upstream feature code.
Vibe-Trading owns AI research presentation. Quant Stack owns the independent
Nautilus Lab, API, strategy contracts, experiment writer, navigation registry,
and Nautilus adapter.
OpenResty injects service credentials, so no API key is shipped in the browser
bundle.

Stock Screener remains an isolated upstream application with its own PostgreSQL
and Redis stores. Phase 1 integration is intentionally narrow: selected symbols
and research metadata cross into Quant Stack through documented APIs/MCP or
exported files. Stock Screener never places orders and never receives broker
credentials.

For server-side health checks only, the Quant API joins the external Stock
Screener Docker network and probes its frontend by service DNS. The response
contains state, latency, and timestamp but never exposes the internal target URL.
This network name is deployment configuration, not an application dependency.

## Design decisions

1. Independent repositories, never submodules: upstream histories remain easy to fetch
   and inspect while Quant Stack does not encode developer-specific paths.
2. `local/main` represents a reviewed local baseline. Official remotes are named
   `upstream`; optional private backup remotes will be named `origin`.
3. Contracts precede adapters. YAML is human/AI-readable; versioned schema and
   Pydantic validation make the boundary executable.
4. The NAS is a data lake, not a source repository. Git tracks metadata and tiny
   golden fixtures only.
5. No live mode in Phase 1. Configuration defaults are fail-closed.
6. The Quant Workspace starts CN-only and without third-party LLM keys. Its AI
   surface is enabled only after a reviewed VT/Codex adapter exists.
7. Web applications keep independent sibling hostnames behind one gateway and
   one shared authentication boundary. A single `quant.example.com/<service>`
   origin would simplify cookies and visual continuity, but VT and Stock
   Screener currently assume root-relative `/assets`, `/api`, WebSocket, OAuth,
   and SPA routes. Rewriting those contracts would couple Quant Stack to each
   upstream release and create path collisions. Shared navigation and SSO
   provide the unified experience without that coupling. Reconsider path-based
   hosting only when every upstream application natively supports a configurable
   base path and its complete route/API test suite passes under that prefix.
