"""Row-level rules for trip_record v1."""

from datetime import UTC, datetime
from decimal import Decimal

from cfi.domain.reasons import ReasonCode
from cfi.domain.trip import TripRecord, TripRejection, validate_trip_record


def _row(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "trip_id": "trip-001",
        "pickup_at": "2024-06-01T12:00:00Z",
        "dropoff_at": "2024-06-01T12:20:00Z",
        "trip_distance": "4.500",
    }
    payload.update(overrides)
    return payload


def _accepted(**overrides: object) -> TripRecord:
    result = validate_trip_record(_row(**overrides))
    assert isinstance(result, TripRecord)
    return result


def _rejected(**overrides: object) -> TripRejection:
    result = validate_trip_record(_row(**overrides))
    assert isinstance(result, TripRejection)
    return result


def test_normalizes_offset_to_utc() -> None:
    record = _accepted(
        pickup_at="2024-06-01T08:00:00-05:00",
        dropoff_at="2024-06-01T14:00:00Z",
    )

    assert record.pickup_at == datetime(2024, 6, 1, 13, 0, tzinfo=UTC)
    assert record.pickup_at.tzinfo is UTC


def test_equal_instants_are_accepted() -> None:
    record = _accepted(dropoff_at="2024-06-01T12:00:00Z")

    assert record.dropoff_at == record.pickup_at


def test_cross_offset_reversal_is_end_before_start() -> None:
    rejection = _rejected(
        pickup_at="2024-06-01T10:00:00-03:00",
        dropoff_at="2024-06-01T12:30:00Z",
    )

    assert rejection.reasons == (ReasonCode.END_BEFORE_START,)


def test_blank_and_preserved_trip_id() -> None:
    assert _rejected(trip_id="").reasons == (ReasonCode.BLANK_TRIP_ID,)
    assert _rejected(trip_id=" \t ").reasons == (ReasonCode.BLANK_TRIP_ID,)
    assert _accepted(trip_id=" trip-001 ").trip_id == " trip-001 "


def test_fractional_timestamp_precision_is_strict() -> None:
    kept = _accepted(pickup_at="2024-06-01T12:00:00.123456Z")
    padded = _accepted(pickup_at="2024-06-01T12:00:00.1Z")
    offset = _accepted(
        pickup_at="2024-06-01T07:00:00.123456-05:00",
        dropoff_at="2024-06-01T12:00:00.123456Z",
    )

    assert kept.pickup_at.microsecond == 123456
    assert padded.pickup_at.microsecond == 100000
    assert offset.pickup_at == datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=UTC)
    assert offset.dropoff_at == offset.pickup_at
    assert _rejected(pickup_at="2024-06-01T12:00:00.1234560Z").reasons == (
        ReasonCode.TIMESTAMP_PRECISION_EXCEEDED,
    )
    assert _rejected(pickup_at="2024-06-01T12:00:00.1234561Z").reasons == (
        ReasonCode.TIMESTAMP_PRECISION_EXCEEDED,
    )
    assert _rejected(
        dropoff_at="2024-06-01T11:00:00.1234560Z",
    ).reasons == (ReasonCode.TIMESTAMP_PRECISION_EXCEEDED,)


def test_timestamp_failures() -> None:
    assert _rejected(pickup_at="not-a-timestamp").reasons == (ReasonCode.INVALID_TIMESTAMP,)
    assert _rejected(pickup_at="2024-06-01T12:00:00").reasons == (
        ReasonCode.TIMESTAMP_MISSING_TIMEZONE,
    )
    assert _rejected(pickup_at=1_700_000_000).reasons == (ReasonCode.SCHEMA_VIOLATION,)


def test_distance_scale_is_not_rounded() -> None:
    kept = _accepted(trip_distance="1.230")

    assert kept.trip_distance == Decimal("1.230")
    assert format(kept.trip_distance, "f") == "1.230"
    assert _accepted(trip_distance="1.234").trip_distance == Decimal("1.234")
    assert _rejected(trip_distance="1.2340").reasons == (ReasonCode.DISTANCE_SCALE_EXCEEDED,)
    assert _rejected(trip_distance="1.2345").reasons == (ReasonCode.DISTANCE_SCALE_EXCEEDED,)
    assert _rejected(trip_distance="-1.2345").reasons == (ReasonCode.DISTANCE_SCALE_EXCEEDED,)


def test_distance_sign_and_nonfinite_values() -> None:
    assert _rejected(trip_distance="-1.250").reasons == (ReasonCode.NEGATIVE_DISTANCE,)
    assert _rejected(trip_distance=-2).reasons == (ReasonCode.NEGATIVE_DISTANCE,)
    assert _rejected(trip_distance="NaN").reasons == (ReasonCode.NONFINITE_DISTANCE,)
    assert _rejected(trip_distance="Infinity").reasons == (ReasonCode.NONFINITE_DISTANCE,)
    assert _rejected(trip_distance="-Infinity").reasons == (ReasonCode.NONFINITE_DISTANCE,)
    assert _rejected(trip_distance=Decimal("NaN")).reasons == (ReasonCode.NONFINITE_DISTANCE,)
    assert _accepted(trip_distance="-0.000").trip_distance == Decimal(0)
    assert _accepted(trip_distance=0).trip_distance == Decimal(0)


def test_float_and_noncanonical_distance_are_schema_violations() -> None:
    assert _rejected(trip_distance=1.5).reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert _rejected(trip_distance=True).reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert _rejected(trip_distance="1e-3").reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert _rejected(trip_distance="+1.0").reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert _rejected(trip_distance="01.20").reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert _rejected(trip_distance=" 1.250").reasons == (ReasonCode.SCHEMA_VIOLATION,)


def test_unexpected_and_missing_fields() -> None:
    extra = _row(fare="9.000")
    missing = _row()
    del missing["trip_distance"]

    extra_result = validate_trip_record(extra)
    missing_result = validate_trip_record(missing)
    null_zones = _accepted(source_zone=None, destination_zone=None)

    assert isinstance(extra_result, TripRejection)
    assert extra_result.reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert isinstance(missing_result, TripRejection)
    assert missing_result.reasons == (ReasonCode.SCHEMA_VIOLATION,)
    assert null_zones.source_zone is None
    assert null_zones.destination_zone is None


def test_multiple_field_errors_keep_table_order() -> None:
    rejection = _rejected(trip_id=" ", trip_distance="-1.000")

    assert rejection.reasons == (ReasonCode.BLANK_TRIP_ID, ReasonCode.NEGATIVE_DISTANCE)
