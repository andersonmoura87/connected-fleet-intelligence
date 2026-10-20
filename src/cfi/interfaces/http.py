"""Read-only HTTP application.

Handlers call the Gold reader. They do not import DuckDB, Parquet, Bronze,
Silver, or the Gold publication command, and they do not create files.
"""

import json
import time
from collections.abc import Awaitable, Callable, MutableMapping
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from starlette.requests import Request

from cfi.application.published_read import PublishedSummary, ReadFailure, read_published
from cfi.domain.identifiers import validate_identifier
from cfi.interfaces.schemas import ApiError, LiveStatus, TripSummary
from cfi.observability import elapsed_ms, emit

_Scope = MutableMapping[str, Any]
_Message = MutableMapping[str, Any]
_Receive = Callable[[], Awaitable[_Message]]
_Send = Callable[[_Message], Awaitable[None]]
_ERROR_BODY_LIMIT = 512


def create_app(data_root: Path) -> FastAPI:
    """Return an application bound to a data root. Do not create that root."""

    app = FastAPI(title="Connected Fleet Intelligence", version="0.1.0a0")
    app.add_middleware(RequestEventMiddleware)

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


class RequestEventMiddleware:
    """Emit one http_request event after a response. Do not read Gold again."""

    def __init__(self, app: Callable[[_Scope, _Receive, _Send], Awaitable[None]]) -> None:
        self.app = app

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter_ns()
        status: int | None = None
        error_body = bytearray()
        overflow = False

        async def forward(message: _Message) -> None:
            nonlocal status, overflow
            if message.get("type") == "http.response.start":
                raw = message.get("status")
                if isinstance(raw, int) and not isinstance(raw, bool):
                    status = raw
            elif (
                message.get("type") == "http.response.body"
                and status is not None
                and status >= 400
                and not overflow
            ):
                piece = message.get("body", b"")
                if isinstance(piece, bytes):
                    if len(error_body) + len(piece) > _ERROR_BODY_LIMIT:
                        overflow = True
                        error_body.clear()
                    else:
                        error_body.extend(piece)
            await send(message)

        try:
            await self.app(scope, receive, forward)
        finally:
            _emit_request(scope, status, bytes(error_body), elapsed_ms(started))


def _emit_request(scope: _Scope, status: int | None, body: bytes, duration_ms: int) -> None:
    fields: dict[str, object] = {
        "duration_ms": duration_ms,
        "event_name": "http_request",
        "route": _route(scope),
        "stage": "serve",
        "status": "succeeded" if status is not None and 200 <= status < 300 else "rejected",
    }
    if status is not None:
        fields["http_status"] = status
    code = _error_code(status, body)
    if code is not None:
        fields["error_code"] = code
    fields.update(_identifiers(scope))
    emit(fields)


def _route(scope: _Scope) -> str:
    path = getattr(scope.get("route"), "path", None)
    if isinstance(path, str) and path.startswith("/") and "?" not in path and ".." not in path:
        return path
    return "unmatched"


def _error_code(status: int | None, body: bytes) -> str | None:
    if status is None or status < 400 or not body:
        return None
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or list(parsed) != ["code"]:
        return None
    code = parsed.get("code")
    if isinstance(code, str):
        return code
    return None


def _identifiers(scope: _Scope) -> dict[str, str]:
    raw = scope.get("query_string", b"")
    if not isinstance(raw, bytes | bytearray):
        return {}
    parsed = parse_qs(bytes(raw).decode("ascii", errors="ignore"), keep_blank_values=False)
    found: dict[str, str] = {}
    for key in ("dataset_version", "source_id"):
        values = parsed.get(key, [])
        if len(values) != 1:
            continue
        try:
            found[key] = validate_identifier(values[0])
        except ValueError:
            continue
    return found


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
