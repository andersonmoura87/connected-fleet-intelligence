"""Domain contracts for Connected Fleet Intelligence."""

from cfi.domain.reasons import ReasonCode
from cfi.domain.trip import (
    CONTRACT_NAME,
    CONTRACT_VERSION,
    DISTANCE_SCALE,
    DISTANCE_UNIT,
    TripRecord,
    TripRejection,
    validate_trip_record,
)

__all__ = [
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "DISTANCE_SCALE",
    "DISTANCE_UNIT",
    "ReasonCode",
    "TripRecord",
    "TripRejection",
    "validate_trip_record",
]
