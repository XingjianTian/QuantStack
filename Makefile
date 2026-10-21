.PHONY: sync test lint validate configure api

sync:
	uv sync --frozen

test:
	uv run --no-env-file pytest

lint:
	uv run ruff check .

validate:
	uv run quant-stack validate-strategy strategy_spec.yaml

configure:
	uv run --env-file .env python -m quant_stack.configuration gateway docker/openresty docker/generated/openresty
	uv run --env-file .env python -m quant_stack.configuration secret QUANT_API_KEY docker/secrets/quant-api-key

api:
	uv run --env-file .env quant-stack-api

.PHONY: privacy-setup privacy-check
privacy-setup:
	git config --local core.hooksPath .githooks

privacy-check:
	python3 scripts/check_privacy.py
