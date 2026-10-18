"""HTTP response documents for the read-only trip summary.

Decimal metrics are strings. The empty average is null. These models do not
import FastAPI.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LiveStatus(BaseModel):
    """Process liveness. This document does not claim data integrity."""

    model_config = ConfigDict(extra="forbid", title="live_status")

    status: Literal["live"]


class ApiError(BaseModel):
    """Stable error body. It carries no path, traceback, or file bytes."""

    model_config = ConfigDict(
        extra="forbid",
        title="api_error",
        json_schema_extra={"contract_name": "api_error", "contract_version": "1"},
    )

    code: str


class TripSummary(BaseModel):
    """Metrics from one validated Gold snapshot."""

    model_config = ConfigDict(
        extra="forbid",
        title="trip_summary",
        json_schema_extra={"contract_name": "trip_summary", "contract_version": "1"},
    )

    source_id: str
    dataset_version: str
    input_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    storage_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_version: str
    transform_version: str
    gold_contract_version: str
    aggregate_version: str
    trip_count: int = Field(ge=0)
    total_distance_km: str
    average_duration_seconds: str | None
    rejected_count: int = Field(ge=0)
