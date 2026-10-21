from __future__ import annotations

import os
import secrets
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from starlette.middleware.gzip import GZipMiddleware

from ..center import CenterRepository, NavigationConfig
from ..data.cn_qfq_dataset import warm_materialized_cn_qfq_runtime
from ..data.interfaces import DataStorage
from ..data.semantics import DEFAULT_DATASET_SEMANTICS, DatasetSemanticsRegistry
from ..data.storage import LocalFileSystemStorage
from ..nautilus_runner import run_synthetic_backtest
from ..platform import PlatformRepository
from .market import create_market_data_router


def _require_api_key(x_quant_api_key: Annotated[str | None, Header()] = None) -> None:
    configured = os.environ.get("QUANT_API_KEY")
    if configured and (
        not x_quant_api_key or not secrets.compare_digest(x_quant_api_key, configured)
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


def create_app(
    repository: PlatformRepository | None = None,
    data_storage: DataStorage | None = None,
    dataset_semantics: DatasetSemanticsRegistry = DEFAULT_DATASET_SEMANTICS,
) -> FastAPI:
    repo = repository or PlatformRepository()
    center = CenterRepository(repo.root, repo.data_root)
    storage = data_storage
    if storage is None and repo.data_root is not None:
        storage = LocalFileSystemStorage(repo.data_root)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if storage is not None:
            await run_in_threadpool(warm_materialized_cn_qfq_runtime)
        yield

    app = FastAPI(
        title="Quant Stack API",
        version="0.6.0",
        description="Research-only QuantStack platform and market data service",
        lifespan=lifespan,
    )
    app.add_middleware(GZipMiddleware, minimum_size=1_024)

    @app.middleware("http")
    async def limit_batch_request_body(request: Request, call_next):
        if request.url.path in {"/market/batch/instruments", "/market/batch/bars"}:
            maximum = int(os.environ.get("QUANT_BATCH_MAX_REQUEST_BYTES", "1048576"))
            content_length = request.headers.get("content-length")
            if content_length is not None:
                try:
                    too_large = int(content_length) > maximum
                except ValueError:
                    too_large = True
                if too_large:
                    return JSONResponse(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        content={"detail": "batch request size limit exceeded"},
                    )
            chunks: list[bytes] = []
            actual_size = 0
            async for chunk in request.stream():
                actual_size += len(chunk)
                if actual_size > maximum:
                    return JSONResponse(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        content={"detail": "batch request size limit exceeded"},
                    )
                chunks.append(chunk)
            body = b"".join(chunks)
            request._body = body  # noqa: SLF001
        return await call_next(request)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "quant-stack-api"}

    @app.get("/v1/overview", dependencies=[Depends(_require_api_key)])
    def overview() -> dict:
        return repo.overview()

    @app.get("/v1/center/bootstrap", dependencies=[Depends(_require_api_key)])
    def center_bootstrap() -> dict:
        return {
            "overview": repo.overview(),
            "catalog": center.catalog(),
            "navigation": center.navigation().model_dump(),
            "persistence": {
                "configured": center.navigation_path is not None,
                "available": bool(repo.data_root and repo.data_root.is_dir()),
            },
        }

    @app.put("/v1/center/navigation", dependencies=[Depends(_require_api_key)])
    def update_center_navigation(config: NavigationConfig) -> dict:
        try:
            saved = center.save_navigation(config)
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=str(exc),
            ) from exc
        return saved.model_dump()

    @app.get("/v1/strategies", dependencies=[Depends(_require_api_key)])
    def strategies() -> list[dict]:
        return repo.strategies()

    @app.get("/v1/experiments", dependencies=[Depends(_require_api_key)])
    def experiments(limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
        return repo.experiments(limit=limit)

    @app.get("/v1/datasets", dependencies=[Depends(_require_api_key)])
    def datasets() -> list[dict]:
        return repo.datasets()

    @app.get("/v1/center/services", dependencies=[Depends(_require_api_key)])
    def center_services() -> list[dict]:
        overview_payload = repo.overview()
        return center.service_statuses(overview_payload["engine"]["available"])

    @app.post("/v1/backtests/synthetic", dependencies=[Depends(_require_api_key)])
    async def synthetic_backtest() -> dict:
        strategy_path = repo.root / "strategies/examples/sma_cross/strategy_spec.yaml"
        data_manifest = repo.root / "datasets/catalog/us_equity_daily_sample.yaml"
        return await run_in_threadpool(
            run_synthetic_backtest,
            strategy_path=strategy_path,
            data_manifest_path=data_manifest,
            experiment_root=repo.experiment_root,
            repo_root=repo.root,
            upstream_versions=repo.upstream_versions(),
        )

    app.include_router(
        create_market_data_router(storage, dataset_semantics),
        dependencies=[Depends(_require_api_key)],
    )
    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run(
        "quant_stack.api:app",
        host=os.environ.get("QUANT_API_HOST", "127.0.0.1"),
        port=int(os.environ.get("QUANT_API_PORT", "8900")),
        reload=False,
    )


__all__ = ["app", "create_app", "run"]
