#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
uv sync --frozen
uv run quant-stack validate-strategy strategy_spec.yaml
uv run pytest
