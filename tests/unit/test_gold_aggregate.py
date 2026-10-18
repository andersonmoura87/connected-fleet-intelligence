"""Gold arithmetic is integer half-even and ignores the global Decimal context."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext

import pytest

from cfi.domain.gold import (
    AggregationError,
    TripMetric,
    aggregate_trips,
    gold_storage_key_for,
    pointer_key_for,
)

_START = datetime(2024, 6, 1, tzinfo=UTC)


def _trip(
    trip_id: str,
    *,
    seconds: int = 0,
    microseconds: int = 0,
    distance: str = "0.000",
    duplicate_count: int = 1,
) -> TripMetric:
    end = _START + timedelta(seconds=seconds, microseconds=microseconds)
    return TripMetric(trip_id, _START, end, Decimal(distance), duplicate_count)


def test_average_uses_integer_half_even_and_ignores_global_precision() -> None:
    previous = getcontext().prec
    getcontext().prec = 6
    try:
        one_over_three = aggregate_trips(
            (_trip("a", microseconds=1), _trip("b"), _trip("c")),
            0,
        )
        two_over_three = aggregate_trips(
            (_trip("a", microseconds=2), _trip("b"), _trip("c")),
            0,
        )
        exact_half_even = aggregate_trips((_trip("a", microseconds=1), _trip("b")), 0)
        exact_half_odd = aggregate_trips(
            (_trip("a", microseconds=1), _trip("b", microseconds=2)),
            0,
        )
        exact = aggregate_trips(
            (_trip("a", microseconds=1), _trip("b", microseconds=2), _trip("c")),
            0,
        )
        distances = aggregate_trips(
            (
                _trip("b", seconds=1800, distance="8.250"),
                _trip("a", seconds=1200, distance="4.500"),
            ),
            0,
        )
    finally:
        getcontext().prec = previous

    assert one_over_three.average_duration_seconds == Decimal("0.000000")
    assert two_over_three.average_duration_seconds == Decimal("0.000001")
    assert exact_half_even.average_duration_seconds == Decimal("0.000000")
    assert exact_half_odd.average_duration_seconds == Decimal("0.000002")
    assert exact.average_duration_seconds == Decimal("0.000001")
    assert distances.trip_count == 2
    assert distances.total_distance_km == Decimal("12.750")
    assert distances.average_duration_seconds == Decimal("1500.000000")
    assert format(distances.total_distance_km, "f") == "12.750"
    assert format(distances.average_duration_seconds, "f") == "1500.000000"


def test_empty_dataset_and_unweighted_duplicate_count() -> None:
    empty = aggregate_trips((), 14)
    weighted = aggregate_trips(
        (_trip("trip-100", seconds=600, distance="1.000", duplicate_count=5),), 2
    )

    assert empty.trip_count == 0
    assert empty.total_distance_km == Decimal("0.000")
    assert empty.average_duration_seconds is None
    assert empty.rejected_count == 14
    assert format(empty.total_distance_km, "f") == "0.000"
    assert weighted.trip_count == 1
    assert weighted.total_distance_km == Decimal("1.000")
    assert weighted.average_duration_seconds == Decimal("600.000000")
    assert weighted.rejected_count == 2


def test_distance_scale_stays_exact_for_thousandths() -> None:
    rows = tuple(_trip(f"t{index}", distance="0.001") for index in range(10))

    assert aggregate_trips(rows, 0).total_distance_km == Decimal("0.010")


def test_invalid_rows_are_rejected() -> None:
    with pytest.raises(AggregationError):
        aggregate_trips((_trip("a", duplicate_count=0),), 0)
    backwards = TripMetric("a", _START, _START - timedelta(seconds=1), Decimal("0.000"), 1)
    with pytest.raises(AggregationError):
        aggregate_trips((backwards,), 0)


def test_pointer_key_ignores_checksum_and_snapshot_key_includes_versions() -> None:
    sha_a = "a" * 64
    sha_b = "b" * 64

    assert pointer_key_for("source", "v1") == pointer_key_for("source", "v1")
    assert pointer_key_for("source", "v1") != pointer_key_for("source", "v2")
    assert gold_storage_key_for("source", "v1", sha_a) != gold_storage_key_for(
        "source", "v1", sha_b
    )
    assert gold_storage_key_for("source", "v1", sha_a) != gold_storage_key_for(
        "source",
        "v1",
        sha_a,
        aggregate_version="2",
    )
