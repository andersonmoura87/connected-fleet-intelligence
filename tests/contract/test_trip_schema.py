"""The committed trip_record schema is the model export."""

import json
from pathlib import Path

from cfi.domain.trip import CONTRACT_VERSION, DISTANCE_SCALE, DISTANCE_UNIT, TripRecord

SCHEMA_PATH = Path("contracts/trip_record/v1/schema.json")
README_PATH = Path("contracts/trip_record/v1/README.md")


def test_committed_schema_matches_model() -> None:
    committed = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    assert committed == TripRecord.model_json_schema()


def test_schema_records_version_and_forbids_extra_fields() -> None:
    schema = TripRecord.model_json_schema()

    assert schema["contract_version"] == CONTRACT_VERSION
    assert schema["distance_unit"] == DISTANCE_UNIT
    assert schema["distance_scale"] == DISTANCE_SCALE
    distance = schema["properties"]["trip_distance"]

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["trip_id", "pickup_at", "dropoff_at", "trip_distance"]
    assert distance["anyOf"] == [{"type": "integer"}, {"type": "string"}]


def test_contract_readme_marks_fixtures_as_synthetic() -> None:
    readme = README_PATH.read_text(encoding="utf-8")

    assert (
        "These fixtures are synthetic. They are not a NYC TLC sample "
        "and they are not observed fleet telemetry."
    ) in readme
