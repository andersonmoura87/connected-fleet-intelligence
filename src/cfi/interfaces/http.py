"""Read-only HTTP application.

Handlers call the Gold reader. They do not import DuckDB, Parquet, Bronze,
Silver, or the Gold publication command, and they do not create files.
"""

from pathlib import Path
from typing import Annotated, Any

from fastapi import FastAPI, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from starlette.requests import Request

from cfi.application.published_read import PublishedSummary, ReadFailure, read_published
from cfi.interfaces.schemas import ApiError, LiveStatus, TripSummary


def create_app(data_root: Path) -> FastAPI:
    """Return an application bound to a data root. Do not create that root."""

    app = FastAPI(title="Connected Fleet Intelligence", version="0.1.0a0")

    @app.exception_handler(ReadFailure)
    def _read_failed(_request: Request, exc: ReadFailure) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"code": exc.code})

    @app.get("/health/live", response_model=LiveStatus)
    def live() -> LiveStatus:
        return LiveStatus(status="live")

    @app.get(
        "/v1/trips/summary",
        response_model=TripSummary,
        responses={
            404: {"model": ApiError},
            422: {"model": ApiError},
            500: {"model": ApiError},
            503: {"model": ApiError},
        },
    )
    def trips_summary(
        source_id: Annotated[str | None, Query()] = None,
        dataset_version: Annotated[str | None, Query()] = None,
    ) -> TripSummary:
        if source_id is None or dataset_version is None:
            raise ReadFailure("missing_parameter", 422)
        return _view(read_published(data_root, source_id, dataset_version))

    def openapi() -> dict[str, Any]:
        cached = app.openapi_schema
        if cached is not None:
            return cached
        schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
        parameters = schema["paths"]["/v1/trips/summary"]["get"]["parameters"]
        for parameter in parameters:
            parameter["required"] = True
        app.openapi_schema = schema
        return schema

    app.openapi = openapi  # type: ignore[method-assign]
    return app


def _view(summary: PublishedSummary) -> TripSummary:
    return TripSummary(
        source_id=summary.source_id,
        dataset_version=summary.dataset_version,
        input_sha256=summary.input_sha256,
        storage_key=summary.storage_key,
        contract_version=summary.contract_version,
        transform_version=summary.transform_version,
        gold_contract_version=summary.gold_contract_version,
        aggregate_version=summary.aggregate_version,
        trip_count=summary.trip_count,
        total_distance_km=summary.total_distance_km,
        average_duration_seconds=summary.average_duration_seconds,
        rejected_count=summary.rejected_count,
    )
