"""The HTTP trip summary stays aligned with its committed schema."""

import json
from pathlib import Path

from cfi.interfaces.schemas import TripSummary

SCHEMA = Path("contracts/trip_summary/v1/schema.json")


def test_trip_summary_schema_matches_the_model() -> None:
    committed = json.loads(SCHEMA.read_text(encoding="utf-8"))

    assert committed == TripSummary.model_json_schema()
    assert committed["properties"]["total_distance_km"]["type"] == "string"
    average = committed["properties"]["average_duration_seconds"]
    assert "number" not in json.dumps(average)
    assert "generated_at" not in committed["properties"]
    assert "total_distance" not in committed["properties"]
    assert committed["contract_version"] == "1"
