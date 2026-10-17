"""Synthetic fixtures and the metrics they specify. Gold is not implemented here."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from cfi.domain.reasons import ReasonCode
from cfi.domain.trip import TripRecord, TripRejection, validate_trip_record

FIXTURES = Path("tests/fixtures/synthetic")

VALID_TRIP_COUNT = 2
VALID_TOTAL_DISTANCE_KM = Decimal("12.750")
VALID_AVERAGE_DURATION_SECONDS = Decimal("1500")

CONFLICT_LOGICAL_TRIP_COUNT = 1
CONFLICT_TOTAL_DISTANCE_KM = Decimal("1.000")
CONFLICT_AVERAGE_DURATION_SECONDS = Decimal("600")
CONFLICT_REJECTED_ROWS = 2

INVALID_REASONS: tuple[tuple[ReasonCode, ...], ...] = (
    (ReasonCode.BLANK_TRIP_ID,),
    (ReasonCode.BLANK_TRIP_ID,),
    (ReasonCode.INVALID_TIMESTAMP,),
    (ReasonCode.TIMESTAMP_MISSING_TIMEZONE,),
    (ReasonCode.END_BEFORE_START,),
    (ReasonCode.END_BEFORE_START,),
    (ReasonCode.NEGATIVE_DISTANCE,),
    (ReasonCode.NONFINITE_DISTANCE,),
    (ReasonCode.NONFINITE_DISTANCE,),
    (ReasonCode.NONFINITE_DISTANCE,),
    (ReasonCode.DISTANCE_SCALE_EXCEEDED,),
    (ReasonCode.SCHEMA_VIOLATION,),
    (ReasonCode.SCHEMA_VIOLATION,),
    (ReasonCode.SCHEMA_VIOLATION,),
)


def _load_jsonl(path: Path) -> list[object]:
    return [
        json.loads(line, parse_float=Decimal)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _duration_seconds(record: TripRecord) -> Decimal:
    delta = record.dropoff_at - record.pickup_at
    total_microseconds = (
        delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    )
    return Decimal(total_microseconds) / Decimal(1_000_000)


def test_valid_fixture_matches_specified_metrics() -> None:
    rows = _load_jsonl(FIXTURES / "trips_valid.jsonl")
    records = [validate_trip_record(row) for row in rows]

    assert all(isinstance(record, TripRecord) for record in records)
    accepted = [record for record in records if isinstance(record, TripRecord)]
    total_distance = sum((record.trip_distance for record in accepted), start=Decimal(0))
    total_duration = sum((_duration_seconds(record) for record in accepted), start=Decimal(0))
    average_duration = total_duration / Decimal(len(accepted))
    second = accepted[1]

    assert len(accepted) == VALID_TRIP_COUNT
    assert total_distance == VALID_TOTAL_DISTANCE_KM
    assert average_duration == VALID_AVERAGE_DURATION_SECONDS
    assert second.pickup_at == datetime(2024, 6, 1, 13, 0, tzinfo=UTC)
    assert second.source_zone is None
    assert second.destination_zone is None


def test_invalid_fixture_reasons() -> None:
    rows = _load_jsonl(FIXTURES / "trips_invalid.jsonl")

    assert len(rows) == len(INVALID_REASONS)
    for row, expected in zip(rows, INVALID_REASONS, strict=True):
        result = validate_trip_record(row)
        assert isinstance(result, TripRejection)
        assert result.reasons == expected


def test_conflict_fixture_specifies_silver_outcome_without_applying_it() -> None:
    rows = _load_jsonl(FIXTURES / "trips_conflict.jsonl")
    identical_payload, repeated_payload, left_payload, right_payload = rows
    validated = [validate_trip_record(row) for row in rows]

    assert identical_payload == repeated_payload
    assert all(isinstance(record, TripRecord) for record in validated)
    identical, repeated, left, right = (
        record for record in validated if isinstance(record, TripRecord)
    )
    assert identical == repeated
    assert left.trip_id == right.trip_id
    assert left.trip_distance != right.trip_distance
    assert CONFLICT_LOGICAL_TRIP_COUNT == 1
    assert CONFLICT_TOTAL_DISTANCE_KM == identical.trip_distance
    assert CONFLICT_AVERAGE_DURATION_SECONDS == _duration_seconds(identical)
    assert CONFLICT_REJECTED_ROWS == 2
