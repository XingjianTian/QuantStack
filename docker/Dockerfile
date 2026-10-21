FROM ghcr.io/astral-sh/uv:0.9.28@sha256:59240a65d6b57e6c507429b45f01b8f2c7c0bbeee0fb697c41a39c6a8e3a4cfb AS uv
FROM python:3.12-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

COPY --from=uv /uv /uvx /bin/
WORKDIR /workspace
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY schemas ./schemas
COPY strategy_spec.yaml ./
RUN uv sync --frozen --no-dev

ENV PATH="/workspace/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
CMD ["quant-stack", "validate-strategy", "strategy_spec.yaml"]
