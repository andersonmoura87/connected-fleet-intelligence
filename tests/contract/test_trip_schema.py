"""trip_record v1.0.0 stays frozen and v1.1.0 matches the executable model."""

import json
from pathlib import Path

from cfi.domain.trip import CONTRACT_VERSION, DISTANCE_SCALE, DISTANCE_UNIT, TripRecord

SCHEMA_V1 = Path("contracts/trip_record/v1/schema.json")
SCHEMA_V1_0_0 = Path("contracts/trip_record/v1.0.0/schema.json")
SCHEMA_V1_1_0 = Path("contracts/trip_record/v1.1.0/schema.json")
README_V1 = Path("contracts/trip_record/v1/README.md")
README_V1_1_0 = Path("contracts/trip_record/v1.1.0/README.md")


def test_frozen_v1_schema_is_not_the_current_model() -> None:
    frozen = json.loads(SCHEMA_V1.read_text(encoding="utf-8"))
    historical = json.loads(SCHEMA_V1_0_0.read_text(encoding="utf-8"))

    assert frozen == historical
    assert frozen["contract_version"] == "1.0.0"
    assert frozen != TripRecord.model_json_schema()


def test_v1_1_0_schema_matches_model() -> None:
    committed = json.loads(SCHEMA_V1_1_0.read_text(encoding="utf-8"))
    schema = TripRecord.model_json_schema()

    assert committed == schema
    assert schema["contract_version"] == CONTRACT_VERSION == "1.1.0"
    assert schema["distance_unit"] == DISTANCE_UNIT
    assert schema["distance_scale"] == DISTANCE_SCALE
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["trip_id", "pickup_at", "dropoff_at", "trip_distance"]
    assert schema["properties"]["trip_distance"]["anyOf"] == [
        {"type": "integer"},
        {"type": "string"},
    ]


def test_contract_readme_marks_fixtures_as_synthetic() -> None:
    readme = README_V1.read_text(encoding="utf-8")
    current = README_V1_1_0.read_text(encoding="utf-8")

    assert (
        "These fixtures are synthetic. They are not a NYC TLC sample "
        "and they are not observed fleet telemetry."
    ) in readme
    assert "timestamp_precision_exceeded" in current
    assert "1.1.0" in current
