"""Rejection codes for one trip_record v1 row.

Batch duplicate policy is not represented here. Identical and conflicting
duplicates are a Silver concern and are specified in the contract README.
"""

from enum import StrEnum


class ReasonCode(StrEnum):
    SCHEMA_VIOLATION = "schema_violation"
    BLANK_TRIP_ID = "blank_trip_id"
    INVALID_TIMESTAMP = "invalid_timestamp"
    TIMESTAMP_MISSING_TIMEZONE = "timestamp_missing_timezone"
    END_BEFORE_START = "end_before_start"
    NONFINITE_DISTANCE = "nonfinite_distance"
    DISTANCE_SCALE_EXCEEDED = "distance_scale_exceeded"
    NEGATIVE_DISTANCE = "negative_distance"
