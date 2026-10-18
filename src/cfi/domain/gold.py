"""Gold aggregate and versioned snapshot contracts.

The arithmetic does not read the process-wide Decimal context. Distances are
summed in a local context. The average is an integer half-even rounding of
microseconds, then shown as seconds with six decimal places.
"""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, Inexact, InvalidOperation, Rounded, localcontext
from typing import Annotated

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    WithJsonSchema,
    field_serializer,
    field_validator,
    model_validator,
)

from cfi.domain.identifiers import validate_identifier
from cfi.domain.silver import TRANSFORM_VERSION, storage_key_for
from cfi.domain.trip import CONTRACT_VERSION

GOLD_CONTRACT_VERSION = "1"
AGGREGATE_VERSION = "1"
SNAPSHOT_PREFIX = "cfi-gold-snapshot-v1"
POINTER_PREFIX = "cfi-gold-pointer-v1"
_DISTANCE_QUANTUM = Decimal("0.001")
_DECIMAL_SCHEMA = {"type": "string", "pattern": r"^-?\d+(\.\d+)?$"}
_OPTIONAL_DECIMAL_SCHEMA = {"anyOf": [_DECIMAL_SCHEMA, {"type": "null"}]}


def _decimal_text(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        try:
            return Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("decimal") from exc
    raise ValueError("decimal")


def _optional_decimal_text(value: object) -> Decimal | None:
    if value is None:
        return None
    return _decimal_text(value)


DistanceKm = Annotated[Decimal, BeforeValidator(_decimal_text), WithJsonSchema(_DECIMAL_SCHEMA)]
AverageSeconds = Annotated[
    Decimal | None,
    BeforeValidator(_optional_decimal_text),
    WithJsonSchema(_OPTIONAL_DECIMAL_SCHEMA),
]
_SHA256 = r"^[0-9a-f]{64}$"
_CREATED_AT = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
_DISTANCE_CONTEXT_PRECISION = 40


class AggregationError(ValueError):
    """A Silver row cannot be aggregated without rounding or dropping it."""


@dataclass(frozen=True, slots=True)
class TripMetric:
    trip_id: str
    pickup_at: datetime
    dropoff_at: datetime
    trip_distance: Decimal
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class GoldMetrics:
    trip_count: int
    total_distance_km: Decimal
    average_duration_seconds: Decimal | None
    rejected_count: int


def gold_storage_key_for(
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str = CONTRACT_VERSION,
    transform_version: str = TRANSFORM_VERSION,
    gold_contract_version: str = GOLD_CONTRACT_VERSION,
    aggregate_version: str = AGGREGATE_VERSION,
) -> str:
    """Hash the Gold snapshot identity. Each field is one LF-terminated line."""

    _identity(source_id, dataset_version, contract_version, transform_version)
    validate_identifier(gold_contract_version)
    validate_identifier(aggregate_version)
    _sha256(input_sha256)
    lines = (
        SNAPSHOT_PREFIX,
        source_id,
        dataset_version,
        input_sha256,
        contract_version,
        transform_version,
        gold_contract_version,
        aggregate_version,
    )
    return _digest(lines)


def pointer_key_for(source_id: str, dataset_version: str) -> str:
    """Hash the pointer identity. It does not include a checksum or a run id."""

    validate_identifier(source_id)
    validate_identifier(dataset_version)
    return _digest((POINTER_PREFIX, source_id, dataset_version))


def aggregate_trips(
    rows: tuple[TripMetric, ...] | list[TripMetric], rejected_count: int
) -> GoldMetrics:
    """Sum logical Silver trips. duplicate_count is not a weight."""

    if rejected_count < 0:
        raise AggregationError("rejected_count")
    ordered = tuple(sorted(rows, key=lambda row: row.trip_id))
    distances: list[Decimal] = []
    total_microseconds = 0
    for row in ordered:
        if row.duplicate_count < 1:
            raise AggregationError("duplicate_count")
        pickup = _require_utc(row.pickup_at)
        dropoff = _require_utc(row.dropoff_at)
        if dropoff < pickup:
            raise AggregationError("duration")
        if not isinstance(row.trip_distance, Decimal):
            raise AggregationError("distance")
        distances.append(row.trip_distance)
        total_microseconds += _microseconds(pickup, dropoff)
    count = len(ordered)
    average = None if count == 0 else _average_seconds(total_microseconds, count)
    return GoldMetrics(
        trip_count=count,
        total_distance_km=_sum_distances(distances),
        average_duration_seconds=average,
        rejected_count=rejected_count,
    )


def format_decimal(value: Decimal) -> str:
    return format(value, "f")


class GoldSummary(BaseModel):
    """Logical Gold metrics. This document has no clock and no run id."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="gold_summary",
        json_schema_extra={"contract_name": "gold_summary", "contract_version": "1"},
    )

    summary_version: str
    source_id: str
    dataset_version: str
    input_sha256: str = Field(pattern=_SHA256)
    contract_version: str
    transform_version: str
    silver_storage_key: str = Field(pattern=_SHA256)
    gold_contract_version: str
    aggregate_version: str
    trip_count: int = Field(ge=0)
    total_distance_km: DistanceKm
    average_duration_seconds: AverageSeconds
    rejected_count: int = Field(ge=0)

    @field_serializer("total_distance_km", "average_duration_seconds", when_used="json")
    def _decimal_json(self, value: Decimal | None) -> str | None:
        if value is None:
            return None
        return format_decimal(value)

    @field_validator("summary_version")
    @classmethod
    def _summary_version(cls, value: str) -> str:
        if value != "1":
            raise ValueError("summary_version")
        return value

    @field_validator(
        "source_id",
        "dataset_version",
        "contract_version",
        "transform_version",
        "gold_contract_version",
        "aggregate_version",
    )
    @classmethod
    def _identifiers(cls, value: str) -> str:
        return validate_identifier(value)

    @model_validator(mode="after")
    def _sealed(self) -> "GoldSummary":
        if self.gold_contract_version != GOLD_CONTRACT_VERSION:
            raise ValueError("gold_contract_version")
        if self.aggregate_version != AGGREGATE_VERSION:
            raise ValueError("aggregate_version")
        expected_silver = storage_key_for(
            self.source_id,
            self.dataset_version,
            self.input_sha256,
            self.contract_version,
            self.transform_version,
        )
        if self.silver_storage_key != expected_silver:
            raise ValueError("silver_storage_key")
        if (self.trip_count == 0) != (self.average_duration_seconds is None):
            raise ValueError("average")
        if self.total_distance_km < 0:
            raise ValueError("distance")
        if self.average_duration_seconds is not None and self.average_duration_seconds < 0:
            raise ValueError("average")
        return self


class GoldGeneration(BaseModel):
    """Seal for one immutable Gold snapshot directory."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="gold_generation",
        json_schema_extra={"contract_name": "gold_generation", "contract_version": "1"},
    )

    manifest_version: str
    source_id: str
    dataset_version: str
    input_sha256: str = Field(pattern=_SHA256)
    contract_version: str
    transform_version: str
    silver_storage_key: str = Field(pattern=_SHA256)
    gold_contract_version: str
    aggregate_version: str
    storage_key: str = Field(pattern=_SHA256)
    pointer_key: str = Field(pattern=_SHA256)
    bronze_run_id: str
    run_id: str
    created_at: str = Field(pattern=_CREATED_AT)
    status: str
    summary_sha256: str = Field(pattern=_SHA256)
    summary_size_bytes: int = Field(ge=0)
    parquet_reader: str
    parquet_reader_version: str
    trip_count: int = Field(ge=0)
    total_distance_km: DistanceKm
    average_duration_seconds: AverageSeconds
    rejected_count: int = Field(ge=0)

    @field_serializer("total_distance_km", "average_duration_seconds", when_used="json")
    def _decimal_json(self, value: Decimal | None) -> str | None:
        if value is None:
            return None
        return format_decimal(value)

    @field_validator("manifest_version")
    @classmethod
    def _manifest_version(cls, value: str) -> str:
        if value != "1":
            raise ValueError("manifest_version")
        return value

    @field_validator(
        "source_id",
        "dataset_version",
        "contract_version",
        "transform_version",
        "gold_contract_version",
        "aggregate_version",
        "bronze_run_id",
        "run_id",
    )
    @classmethod
    def _identifiers(cls, value: str) -> str:
        return validate_identifier(value)

    @model_validator(mode="after")
    def _sealed(self) -> "GoldGeneration":
        if self.status != "published" or self.parquet_reader != "duckdb":
            raise ValueError("status")
        if self.gold_contract_version != GOLD_CONTRACT_VERSION:
            raise ValueError("gold_contract_version")
        if self.aggregate_version != AGGREGATE_VERSION:
            raise ValueError("aggregate_version")
        summary = GoldSummary(
            summary_version="1",
            source_id=self.source_id,
            dataset_version=self.dataset_version,
            input_sha256=self.input_sha256,
            contract_version=self.contract_version,
            transform_version=self.transform_version,
            silver_storage_key=self.silver_storage_key,
            gold_contract_version=self.gold_contract_version,
            aggregate_version=self.aggregate_version,
            trip_count=self.trip_count,
            total_distance_km=self.total_distance_km,
            average_duration_seconds=self.average_duration_seconds,
            rejected_count=self.rejected_count,
        )
        expected = gold_storage_key_for(
            self.source_id,
            self.dataset_version,
            self.input_sha256,
            self.contract_version,
            self.transform_version,
            self.gold_contract_version,
            self.aggregate_version,
        )
        if self.storage_key != expected or self.pointer_key != pointer_key_for(
            self.source_id, self.dataset_version
        ):
            raise ValueError("storage_key")
        if summary.silver_storage_key != self.silver_storage_key:
            raise ValueError("silver_storage_key")
        return self


class GoldPointer(BaseModel):
    """Mutable pointer to one intact Gold snapshot for a source identity."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="gold_pointer",
        json_schema_extra={"contract_name": "gold_pointer", "contract_version": "1"},
    )

    pointer_version: str
    source_id: str
    dataset_version: str
    storage_key: str = Field(pattern=_SHA256)
    summary_sha256: str = Field(pattern=_SHA256)
    status: str

    @field_validator("pointer_version")
    @classmethod
    def _pointer_version(cls, value: str) -> str:
        if value != "1":
            raise ValueError("pointer_version")
        return value

    @field_validator("source_id", "dataset_version")
    @classmethod
    def _identifiers(cls, value: str) -> str:
        return validate_identifier(value)

    @model_validator(mode="after")
    def _sealed(self) -> "GoldPointer":
        if self.status != "published":
            raise ValueError("status")
        return self


def _identity(*values: str) -> None:
    for value in values:
        validate_identifier(value)


def _sha256(value: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("input_sha256")


def _digest(lines: tuple[str, ...]) -> str:
    payload = "".join(f"{line}\n" for line in lines).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise AggregationError("timestamp")
    return value.astimezone(UTC)


def _microseconds(start: datetime, end: datetime) -> int:
    delta = end - start
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _sum_distances(values: list[Decimal]) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = _DISTANCE_CONTEXT_PRECISION
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        total = Decimal("0.000")
        for value in values:
            total += value
        try:
            return total.quantize(_DISTANCE_QUANTUM, rounding=ROUND_HALF_EVEN)
        except (Inexact, Rounded) as exc:
            raise AggregationError("distance") from exc


def _average_seconds(total_microseconds: int, count: int) -> Decimal:
    if total_microseconds < 0 or count <= 0:
        raise AggregationError("duration")
    quotient, remainder = divmod(total_microseconds, count)
    twice = remainder * 2
    if twice > count or (twice == count and quotient % 2 == 1):
        quotient += 1
    whole, frac = divmod(quotient, 1_000_000)
    return Decimal(f"{whole}.{frac:06d}")
