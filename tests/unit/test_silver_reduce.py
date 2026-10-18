"""Deterministic reduction of Bronze bytes. No filesystem and no DuckDB."""

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from cfi.domain.silver import reduce_bronze, split_physical_lines, storage_key_for

FIXTURES = {
    "valid": Path("tests/fixtures/synthetic/trips_valid.jsonl"),
    "invalid": Path("tests/fixtures/synthetic/trips_invalid.jsonl"),
    "conflict": Path("tests/fixtures/synthetic/trips_conflict.jsonl"),
}


def _bytes(name: str) -> bytes:
    return FIXTURES[name].read_bytes()


def _line(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _trip(trip_id: str, distance: str, **extra: object) -> dict[str, object]:
    row: dict[str, object] = {
        "trip_id": trip_id,
        "pickup_at": "2024-06-01T12:00:00Z",
        "dropoff_at": "2024-06-01T12:10:00Z",
        "trip_distance": distance,
    }
    row.update(extra)
    return row


def test_fixture_counts_reconcile() -> None:
    expected = {
        "valid": (2, 2, 0, 2, 0, 0, 0),
        "invalid": (14, 0, 14, 0, 0, 0, 14),
        "conflict": (4, 4, 0, 1, 1, 2, 2),
    }
    for name, counts in expected.items():
        batch = reduce_bronze(_bytes(name))
        assert batch.counts.reconciled()
        assert (
            batch.counts.input_line_count,
            batch.counts.valid_record_count,
            batch.counts.invalid_record_count,
            batch.counts.silver_record_count,
            batch.counts.identical_duplicate_extra_count,
            batch.counts.conflict_rejection_count,
            batch.counts.quarantine_row_count,
        ) == counts


def test_conflict_fixture_keeps_one_identical_trip() -> None:
    batch = reduce_bronze(_bytes("conflict"))

    assert len(batch.silver) == 1
    assert batch.silver[0].trip_id == "trip-100"
    assert batch.silver[0].trip_distance == Decimal("1.000")
    assert batch.silver[0].duplicate_count == 2
    assert batch.silver[0].line_number == 1
    assert [row.line_number for row in batch.quarantine] == [3, 4]
    assert {row.trip_id for row in batch.quarantine} == {"trip-200"}
    assert {row.reason_codes for row in batch.quarantine} == {("duplicate_conflict",)}


def test_semantic_equality_collapses_scale_and_null_zone() -> None:
    first = _line(_trip("trip-a", "1.5"))
    second = _line(_trip("trip-a", "1.500", source_zone=None))
    batch = reduce_bronze(first + b"\n" + second + b"\n")

    assert batch.counts.silver_record_count == 1
    assert batch.counts.identical_duplicate_extra_count == 1
    assert batch.silver[0].duplicate_count == 2
    assert batch.silver[0].trip_distance.as_tuple() == Decimal("1.5").as_tuple()
    assert batch.silver[0].source_zone is None
    assert batch.quarantine == ()


def test_three_occurrences_and_mixed_invalid_do_not_cross() -> None:
    valid = _line(_trip("trip-a", "1.000"))
    invalid = _line(_trip("trip-a", "-1.000"))
    payload = b"\n".join((valid, valid, invalid, valid))
    batch = reduce_bronze(payload)

    assert batch.silver[0].duplicate_count == 3
    assert batch.silver[0].line_number == 1
    assert batch.counts.invalid_record_count == 1
    assert batch.counts.conflict_rejection_count == 0
    assert batch.quarantine[0].failure_class == "semantic"
    assert batch.quarantine[0].reason_codes == ("negative_distance",)
    assert batch.quarantine[0].trip_id == "trip-a"
    assert batch.counts.reconciled()


def test_conflict_quarantines_every_valid_occurrence() -> None:
    left = _line(_trip("trip-a", "1.000"))
    right = _line(_trip("trip-a", "2.000"))
    batch = reduce_bronze(b"\n".join((left, left, right)))

    assert batch.silver == ()
    assert batch.counts.conflict_rejection_count == 3
    assert [row.line_number for row in batch.quarantine] == [1, 2, 3]
    assert batch.counts.reconciled()


def test_reverse_order_sorts_silver_by_trip_id() -> None:
    later = _line(_trip("trip-b", "2.000"))
    earlier = _line(_trip("trip-a", "1.000"))
    batch = reduce_bronze(later + b"\n" + earlier)

    assert [row.trip_id for row in batch.silver] == ["trip-a", "trip-b"]
    assert [row.line_number for row in batch.silver] == [2, 1]


def test_precision_rows_do_not_collapse_into_one_instant() -> None:
    accepted = _line(_trip("trip-a", "1.000", pickup_at="2024-06-01T12:00:00.123456Z"))
    zeros = _line(_trip("trip-a", "1.000", pickup_at="2024-06-01T12:00:00.1234560Z"))
    ones = _line(_trip("trip-b", "1.000", pickup_at="2024-06-01T12:00:00.1234561Z"))
    batch = reduce_bronze(b"\n".join((accepted, zeros, ones)))

    assert batch.silver[0].pickup_at == datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=UTC)
    assert batch.counts.silver_record_count == 1
    assert [row.reason_codes for row in batch.quarantine] == [
        ("timestamp_precision_exceeded",),
        ("timestamp_precision_exceeded",),
    ]
    assert batch.quarantine[0].trip_id == "trip-a"
    assert batch.quarantine[1].trip_id == "trip-b"


def test_equivalent_offset_is_one_duplicate() -> None:
    zulu = _line(
        _trip(
            "trip-a",
            "1.000",
            pickup_at="2024-06-01T12:00:00.123456Z",
            dropoff_at="2024-06-01T12:10:00Z",
        )
    )
    offset = _line(
        _trip(
            "trip-a",
            "1.000",
            pickup_at="2024-06-01T07:00:00.123456-05:00",
            dropoff_at="2024-06-01T07:10:00-05:00",
        )
    )
    batch = reduce_bronze(zulu + b"\n" + offset)

    assert batch.counts.identical_duplicate_extra_count == 1
    assert batch.silver[0].pickup_at.microsecond == 123456


def test_line_identity_and_parse_failures() -> None:
    assert split_physical_lines(b"") == ()
    assert split_physical_lines(b"a\n") == (b"a",)
    assert split_physical_lines(b"\n") == (b"",)
    assert split_physical_lines(b"a\r\nb") == (b"a", b"b")
    assert split_physical_lines(b"a\rb") == (b"a\rb",)

    payload = b"\n" + b"\xff" + b"\n{\n[]\n"
    batch = reduce_bronze(payload)
    assert [row.reason_codes[0] for row in batch.quarantine] == [
        "line_empty",
        "invalid_utf8",
        "invalid_json",
        "json_not_object",
    ]
    assert batch.quarantine[1].line_sha256 == hashlib.sha256(b"\xff").hexdigest()
    assert batch.quarantine[1].line_size_bytes == 1
    assert batch.quarantine[1].trip_id is None
    assert batch.counts.input_line_count == 4


def test_storage_key_changes_with_identity() -> None:
    digest = "a" * 64
    key = storage_key_for("source", "v1", digest)
    other = storage_key_for("source", "v2", digest)

    assert key != other
    assert len(key) == 64
    with pytest.raises(ValueError):
        storage_key_for("../source", "v1", digest)
