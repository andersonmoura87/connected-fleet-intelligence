"""Pure Silver reduction. This module does not touch the filesystem or DuckDB."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from cfi.domain.identifiers import validate_identifier
from cfi.domain.reasons import ReasonCode
from cfi.domain.trip import CONTRACT_VERSION, TripRecord, TripRejection, validate_trip_record

TRANSFORM_VERSION = "1"
STORAGE_KEY_PREFIX = "cfi-silver-generation-v1"
_PARSE_REASONS = frozenset({"line_empty", "invalid_utf8", "invalid_json", "json_not_object"})
_SCHEMA_REASON = ReasonCode.SCHEMA_VIOLATION.value


@dataclass(frozen=True, slots=True)
class Counts:
    input_line_count: int
    valid_record_count: int
    invalid_record_count: int
    silver_record_count: int
    identical_duplicate_extra_count: int
    conflict_rejection_count: int
    quarantine_row_count: int

    def reconciled(self) -> bool:
        lines = self.valid_record_count + self.invalid_record_count
        valid = (
            self.silver_record_count
            + self.identical_duplicate_extra_count
            + self.conflict_rejection_count
        )
        quarantined = self.invalid_record_count + self.conflict_rejection_count
        return (
            self.input_line_count == lines
            and self.valid_record_count == valid
            and self.quarantine_row_count == quarantined
        )


@dataclass(frozen=True, slots=True)
class SilverLogical:
    trip_id: str
    pickup_at: datetime
    dropoff_at: datetime
    trip_distance: Decimal
    source_zone: str | None
    destination_zone: str | None
    line_number: int
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class QuarantineLogical:
    line_number: int
    line_sha256: str
    line_size_bytes: int
    failure_class: str
    reason_codes: tuple[str, ...]
    trip_id: str | None


@dataclass(frozen=True, slots=True)
class LogicalBatch:
    silver: tuple[SilverLogical, ...]
    quarantine: tuple[QuarantineLogical, ...]
    counts: Counts


@dataclass(frozen=True, slots=True)
class _Valid:
    line_number: int
    content: bytes
    record: TripRecord


@dataclass(frozen=True, slots=True)
class _Rejected:
    line_number: int
    content: bytes
    failure_class: str
    reason_codes: tuple[str, ...]
    trip_id: str | None


def storage_key_for(
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str = CONTRACT_VERSION,
    transform_version: str = TRANSFORM_VERSION,
) -> str:
    """Hash the canonical generation identity. Each field is one LF-terminated line."""

    validate_identifier(source_id)
    validate_identifier(dataset_version)
    validate_identifier(contract_version)
    validate_identifier(transform_version)
    if len(input_sha256) != 64 or any(char not in "0123456789abcdef" for char in input_sha256):
        raise ValueError("input_sha256")
    lines = (
        STORAGE_KEY_PREFIX,
        source_id,
        dataset_version,
        input_sha256,
        contract_version,
        transform_version,
    )
    payload = "".join(f"{line}\n" for line in lines).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def split_physical_lines(payload: bytes) -> tuple[bytes, ...]:
    """Split on LF. A CRLF delimiter does not leave the CR in the line. A bare CR stays."""

    if payload == b"":
        return ()
    lines: list[bytes] = []
    start = 0
    index = 0
    size = len(payload)
    while index < size:
        if payload[index] == 0x0A:
            end = index
            if end > start and payload[end - 1] == 0x0D:
                end -= 1
            lines.append(payload[start:end])
            start = index + 1
        index += 1
    if start < size:
        lines.append(payload[start:])
    return tuple(lines)


def reduce_bronze(payload: bytes) -> LogicalBatch:
    """Validate and deduplicate one Bronze original. Invalid lines do not join groups."""

    valid: list[_Valid] = []
    rejected: list[_Rejected] = []
    for number, content in enumerate(split_physical_lines(payload), start=1):
        item = _classify(number, content)
        if isinstance(item, _Valid):
            valid.append(item)
        else:
            rejected.append(item)

    grouped: dict[str, list[_Valid]] = {}
    for item in valid:
        grouped.setdefault(item.record.trip_id, []).append(item)

    silver: list[SilverLogical] = []
    conflicts: list[_Rejected] = []
    for trip_id in grouped:
        group = grouped[trip_id]
        first = group[0].record
        if all(member.record == first for member in group):
            representative = min(group, key=lambda member: member.line_number)
            silver.append(
                SilverLogical(
                    trip_id=trip_id,
                    pickup_at=representative.record.pickup_at,
                    dropoff_at=representative.record.dropoff_at,
                    trip_distance=representative.record.trip_distance,
                    source_zone=representative.record.source_zone,
                    destination_zone=representative.record.destination_zone,
                    line_number=representative.line_number,
                    duplicate_count=len(group),
                )
            )
            continue
        for member in group:
            conflicts.append(
                _Rejected(
                    line_number=member.line_number,
                    content=member.content,
                    failure_class="conflict",
                    reason_codes=("duplicate_conflict",),
                    trip_id=trip_id,
                )
            )

    quarantine = [
        QuarantineLogical(
            line_number=item.line_number,
            line_sha256=hashlib.sha256(item.content).hexdigest(),
            line_size_bytes=len(item.content),
            failure_class=item.failure_class,
            reason_codes=item.reason_codes,
            trip_id=item.trip_id,
        )
        for item in (*rejected, *conflicts)
    ]
    quarantine.sort(key=lambda item: item.line_number)
    silver.sort(key=lambda item: item.trip_id)
    identical_extra = sum(row.duplicate_count - 1 for row in silver)
    counts = Counts(
        input_line_count=len(valid) + len(rejected),
        valid_record_count=len(valid),
        invalid_record_count=len(rejected),
        silver_record_count=len(silver),
        identical_duplicate_extra_count=identical_extra,
        conflict_rejection_count=len(conflicts),
        quarantine_row_count=len(quarantine),
    )
    if not counts.reconciled():
        raise RuntimeError("counts")
    return LogicalBatch(silver=tuple(silver), quarantine=tuple(quarantine), counts=counts)


def _classify(number: int, content: bytes) -> _Valid | _Rejected:
    if content == b"":
        return _reject(number, content, "parse", ("line_empty",), None)
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return _reject(number, content, "parse", ("invalid_utf8",), None)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return _reject(number, content, "parse", ("invalid_json",), None)
    if not isinstance(value, dict):
        return _reject(number, content, "parse", ("json_not_object",), None)
    raw_trip_id = value.get("trip_id")
    trip_id = raw_trip_id if isinstance(raw_trip_id, str) else None
    result = validate_trip_record(value)
    if isinstance(result, TripRecord):
        return _Valid(number, content, result)
    if not isinstance(result, TripRejection):
        return _reject(number, content, "schema", (_SCHEMA_REASON,), trip_id)
    reasons = tuple(reason.value for reason in result.reasons)
    failure = "schema" if _SCHEMA_REASON in reasons else "semantic"
    if reasons and reasons[0] in _PARSE_REASONS:
        failure = "parse"
    return _reject(number, content, failure, reasons, trip_id)


def _reject(
    number: int,
    content: bytes,
    failure_class: str,
    reason_codes: tuple[str, ...],
    trip_id: str | None,
) -> _Rejected:
    return _Rejected(number, content, failure_class, reason_codes, trip_id)
