from __future__ import annotations

import os
import secrets
from datetime import date
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ..data.batch import (
    BatchBarsQuery,
    BatchBarsResponse,
    BatchDatasetMetadata,
    BatchDatasetNotFoundError,
    BatchError,
    BatchInstrumentQuery,
    BatchInstrumentResponse,
    BatchPayloadError,
    BatchRequestError,
    BatchSnapshotError,
    CanonicalBatchRepository,
)
from ..data.interfaces import DataStorage
from ..data.models import Bar, DatasetManifest, Instrument, Market
from ..data.query import MarketDataRepository
from ..data.semantics import DEFAULT_DATASET_SEMANTICS, DatasetSemanticsRegistry
from ..data.us_market_indexes import read_index_snapshot


def _require_batch_api_key(
    x_quant_api_key: Annotated[str | None, Header()] = None,
) -> None:
    configured = os.environ.get("QUANT_API_KEY")
    if not configured:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API authentication is not configured",
        )
    if not x_quant_api_key or not secrets.compare_digest(x_quant_api_key, configured):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )


def create_market_data_router(
    storage: DataStorage | None,
    semantics_registry: DatasetSemanticsRegistry = DEFAULT_DATASET_SEMANTICS,
) -> APIRouter:
    router = APIRouter(tags=["market-data"])

    def repository() -> MarketDataRepository:
        if storage is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="QUANT_DATA_ROOT is not configured",
            )
        return MarketDataRepository(storage)

    def batch_repository() -> CanonicalBatchRepository:
        if storage is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="QUANT_DATA_ROOT is not configured",
            )
        consumer_access = os.environ.get("QUANT_BATCH_CONSUMER_ACCESS", "candidate").strip().lower()
        if consumer_access not in {"candidate", "shadow"}:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Batch consumer access is not configured",
            )
        return CanonicalBatchRepository(
            storage.data_root,
            semantics_registry=semantics_registry,
            consumer_access=consumer_access,
        )

    def response_headers(plan) -> dict[str, str]:
        return {
            "ETag": plan.etag,
            "Cache-Control": "private, must-revalidate",
            "X-Quant-Snapshot-Token": plan.snapshot_token,
            **plan.metrics.response_headers(),
        }

    def not_modified(plan):
        try:
            return Response(
                status_code=status.HTTP_304_NOT_MODIFIED,
                headers=response_headers(plan),
            )
        finally:
            plan.close()

    def json_response(plan, payload):
        try:
            return JSONResponse(content=jsonable_encoder(payload), headers=response_headers(plan))
        finally:
            plan.close()

    def batch_error(exc: BatchError) -> HTTPException:
        if isinstance(exc, BatchDatasetNotFoundError):
            return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=exc.detail)
        if isinstance(exc, BatchRequestError):
            return HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=exc.detail
            )
        if isinstance(exc, BatchPayloadError):
            return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=exc.detail)
        if isinstance(exc, BatchSnapshotError):
            return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.detail)
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="canonical batch request failed",
        )

    def unexpected_batch_error(exc: Exception) -> HTTPException:
        # The exception itself can contain SQL, a physical path, or a credential.
        # Keep it in server logs only as a class name.
        import logging

        logging.getLogger(__name__).error(
            "unexpected canonical batch failure error_class=%s", type(exc).__name__
        )
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="canonical batch request failed",
        )

    @router.get("/market/instruments", response_model=list[Instrument])
    def instruments(market: Market | None = None) -> list[Instrument]:
        return repository().instruments(market)

    @router.get("/market/{symbol}/bars", response_model=list[Bar])
    def bars(
        symbol: str,
        market: Market,
        frequency: str = "1d",
        start: date | None = None,
        end: date | None = None,
        limit: Annotated[int, Query(ge=1, le=100_000)] = 10_000,
    ) -> list[Bar]:
        try:
            return repository().bars(
                symbol,
                market=market,
                frequency=frequency,
                start=start,
                end=end,
                limit=limit,
            )
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @router.get("/datasets", response_model=list[DatasetManifest])
    def datasets() -> list[DatasetManifest]:
        return repository().datasets()

    @router.get(
        "/market/index-snapshot",
        dependencies=[Depends(_require_batch_api_key)],
    )
    def index_snapshot(
        market: str = "US",
        as_of_date: date | None = None,
        points: Annotated[int, Query(ge=2, le=120)] = 30,
    ) -> dict[str, object]:
        if market.upper() != "US":
            return {"market": market.upper(), "key_markets": []}
        configured = os.environ.get("QUANT_US_INDEX_ROOT", "").strip()
        if not configured:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="US index dataset is not configured",
            )
        return {
            "market": "US",
            "key_markets": read_index_snapshot(
                Path(configured), as_of_date=as_of_date, points=points
            ),
        }

    @router.post(
        "/market/batch/instruments",
        response_model=BatchInstrumentResponse,
        dependencies=[Depends(_require_batch_api_key)],
    )
    def batch_instruments(
        query: BatchInstrumentQuery,
        if_none_match: Annotated[str | None, Header()] = None,
    ):
        plan = None
        try:
            repository = batch_repository()
            plan = repository.prepare_instruments(query)
            if if_none_match == plan.etag:
                return not_modified(plan)
            payload = repository.execute_instruments(plan, query)
            return json_response(plan, payload)
        except HTTPException:
            raise
        except BatchError as exc:
            raise batch_error(exc) from exc
        except Exception as exc:
            raise unexpected_batch_error(exc) from exc
        finally:
            if plan is not None:
                plan.close()

    @router.post(
        "/market/batch/bars",
        response_model=BatchBarsResponse,
        dependencies=[Depends(_require_batch_api_key)],
    )
    def batch_bars(
        query: BatchBarsQuery,
        if_none_match: Annotated[str | None, Header()] = None,
    ):
        plan = None
        try:
            repository = batch_repository()
            plan = repository.prepare_bars(query)
            if if_none_match == plan.etag:
                return not_modified(plan)
            payload = repository.execute_bars(plan, query)
            return json_response(plan, payload)
        except HTTPException:
            raise
        except BatchError as exc:
            raise batch_error(exc) from exc
        except Exception as exc:
            raise unexpected_batch_error(exc) from exc
        finally:
            if plan is not None:
                plan.close()

    @router.get(
        "/market/datasets/{dataset_id}/metadata",
        response_model=BatchDatasetMetadata,
        dependencies=[Depends(_require_batch_api_key)],
    )
    def dataset_metadata(
        dataset_id: str,
        version: str = "latest",
        if_none_match: Annotated[str | None, Header()] = None,
    ):
        plan = None
        try:
            repository = batch_repository()
            plan = repository.prepare_metadata(dataset_id, version)
            if if_none_match == plan.etag:
                return not_modified(plan)
            payload = repository.metadata_response(plan)
            return json_response(plan, payload)
        except HTTPException:
            raise
        except BatchError as exc:
            raise batch_error(exc) from exc
        except Exception as exc:
            raise unexpected_batch_error(exc) from exc
        finally:
            if plan is not None:
                plan.close()

    return router
