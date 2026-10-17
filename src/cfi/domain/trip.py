"""Executable trip_record contract, version 1.0.0.

The model validates one row. It does not ingest files, detect duplicates,
or compute fleet aggregates.
"""

import re
from datetime import UTC, datetime
from decimal import Decimal, Inexact, InvalidOperation, Rounded, localcontext
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from pydantic.json_schema import WithJsonSchema
from pydantic_core import ErrorDetails

from cfi.domain.reasons import ReasonCode

CONTRACT_NAME = "trip_record"
CONTRACT_VERSION = "1.0.0"
DISTANCE_UNIT = "km"
DISTANCE_SCALE = 3

_NONFINITE_TOKENS = frozenset({"NaN", "Infinity", "-Infinity"})
_NONNEGATIVE_LITERAL = re.compile(r"^(0|[1-9]\d*)(\.\d+)?$")
_NEGATIVE_LITERAL = re.compile(r"^-(0|[1-9]\d*)(\.\d+)?$")
_DISTANCE_DESCRIPTION = (
    "Kilometers as an integer, Decimal, or canonical decimal string. "
    "The supplied fractional scale must be 3 or less. A finer literal is rejected "
    "and is not rounded."
)

_DATETIME_PARSE_ERRORS = frozenset(
    {
        "datetime_parsing",
        "datetime_from_date_parsing",
        "date_from_datetime_parsing",
        "time_parsing",
    }
)


class _ContractError(ValueError):
    def __init__(self, reason: ReasonCode) -> None:
        super().__init__(reason.value)
        self.reason = reason


def _exact_decimal(literal: str) -> Decimal:
    with localcontext() as ctx:
        ctx.traps[Inexact] = True
        ctx.traps[Rounded] = True
        try:
            return Decimal(literal)
        except (Inexact, InvalidOperation, Rounded) as exc:
            raise _ContractError(ReasonCode.SCHEMA_VIOLATION) from exc


def _parse_distance(value: object) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise _ContractError(ReasonCode.SCHEMA_VIOLATION)
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int):
        number = _exact_decimal(str(value))
    elif isinstance(value, str):
        if value in _NONFINITE_TOKENS:
            raise _ContractError(ReasonCode.NONFINITE_DISTANCE)
        if _NONNEGATIVE_LITERAL.fullmatch(value) or _NEGATIVE_LITERAL.fullmatch(value):
            number = _exact_decimal(value)
        else:
            raise _ContractError(ReasonCode.SCHEMA_VIOLATION)
    else:
        raise _ContractError(ReasonCode.SCHEMA_VIOLATION)

    if not number.is_finite():
        raise _ContractError(ReasonCode.NONFINITE_DISTANCE)
    exponent = number.as_tuple().exponent
    if not isinstance(exponent, int):
        raise _ContractError(ReasonCode.NONFINITE_DISTANCE)
    scale = -exponent if exponent < 0 else 0
    if scale > DISTANCE_SCALE:
        raise _ContractError(ReasonCode.DISTANCE_SCALE_EXCEEDED)
    if number < 0:
        raise _ContractError(ReasonCode.NEGATIVE_DISTANCE)
    return number


class TripRejection:
    """One or more reason codes for a single rejected row."""

    __slots__ = ("reasons",)

    def __init__(self, reasons: tuple[ReasonCode, ...]) -> None:
        if not reasons:
            raise ValueError("a rejection contains at least one reason")
        self.reasons = reasons

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TripRejection):
            return NotImplemented
        return self.reasons == other.reasons

    def __repr__(self) -> str:
        return f"TripRejection(reasons={self.reasons!r})"


class TripRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="trip_record",
        json_schema_extra={
            "contract_version": CONTRACT_VERSION,
            "distance_unit": DISTANCE_UNIT,
            "distance_scale": DISTANCE_SCALE,
        },
    )

    trip_id: str = Field(
        description=(
            "Non-blank identifier. A value made only of spaces is rejected. "
            "Any other character, including surrounding spaces, is preserved."
        )
    )
    pickup_at: datetime = Field(description="Timezone-aware pickup instant, stored in UTC.")
    dropoff_at: datetime = Field(
        description="Timezone-aware dropoff instant, stored in UTC. An earlier instant is rejected."
    )
    trip_distance: Annotated[
        Decimal,
        WithJsonSchema(
            {
                "anyOf": [{"type": "integer"}, {"type": "string"}],
                "description": _DISTANCE_DESCRIPTION,
            }
        ),
    ] = Field(description=_DISTANCE_DESCRIPTION)
    source_zone: str | None = Field(default=None, description="Optional zone identifier.")
    destination_zone: str | None = Field(default=None, description="Optional zone identifier.")

    @field_validator("trip_id")
    @classmethod
    def _trip_id_not_blank(cls, value: str) -> str:
        if value.strip() == "":
            raise _ContractError(ReasonCode.BLANK_TRIP_ID)
        return value

    @field_validator("pickup_at", "dropoff_at", mode="before")
    @classmethod
    def _timestamp_input(cls, value: object) -> object:
        if isinstance(value, datetime | str):
            return value
        raise _ContractError(ReasonCode.SCHEMA_VIOLATION)

    @field_validator("pickup_at", "dropoff_at")
    @classmethod
    def _normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise _ContractError(ReasonCode.TIMESTAMP_MISSING_TIMEZONE)
        return value.astimezone(UTC)

    @field_validator("trip_distance", mode="before")
    @classmethod
    def _distance(cls, value: object) -> Decimal:
        return _parse_distance(value)

    @model_validator(mode="after")
    def _dropoff_not_before_pickup(self) -> Self:
        if self.dropoff_at < self.pickup_at:
            raise _ContractError(ReasonCode.END_BEFORE_START)
        return self


def _ordered(reasons: set[ReasonCode]) -> tuple[ReasonCode, ...]:
    rank = {reason: index for index, reason in enumerate(ReasonCode)}
    return tuple(sorted(reasons, key=rank.__getitem__))


def _reason_from_error(error: ErrorDetails) -> ReasonCode:
    ctx = error.get("ctx")
    if isinstance(ctx, dict):
        original = ctx.get("error")
        if isinstance(original, _ContractError):
            return original.reason
    field = error["loc"][0] if error["loc"] else None
    if field in {"pickup_at", "dropoff_at"} and error["type"] in _DATETIME_PARSE_ERRORS:
        return ReasonCode.INVALID_TIMESTAMP
    return ReasonCode.SCHEMA_VIOLATION


def validate_trip_record(payload: object) -> TripRecord | TripRejection:
    """Validate one row. Duplicate detection is intentionally absent."""

    try:
        return TripRecord.model_validate(payload)
    except ValidationError as exc:
        reasons = {_reason_from_error(error) for error in exc.errors()}
        if not reasons:
            reasons.add(ReasonCode.SCHEMA_VIOLATION)
        return TripRejection(_ordered(reasons))
