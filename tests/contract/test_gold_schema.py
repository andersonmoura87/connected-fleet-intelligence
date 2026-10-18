"""Gold contracts stay aligned with the executable models."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from cfi.domain.gold import GoldGeneration, GoldPointer, GoldSummary

SUMMARY = Path("contracts/gold_summary/v1/schema.json")
GENERATION = Path("contracts/gold_generation/v1/schema.json")
POINTER = Path("contracts/gold_pointer/v1/schema.json")


def test_gold_schemas_match_the_models() -> None:
    assert json.loads(SUMMARY.read_text(encoding="utf-8")) == GoldSummary.model_json_schema()
    assert json.loads(GENERATION.read_text(encoding="utf-8")) == GoldGeneration.model_json_schema()
    assert json.loads(POINTER.read_text(encoding="utf-8")) == GoldPointer.model_json_schema()
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    assert summary["properties"]["total_distance_km"]["type"] == "string"
    assert "number" not in json.dumps(summary["properties"]["average_duration_seconds"])
    assert "created_at" not in summary["properties"]
    assert "run_id" not in summary["properties"]
    generation = json.loads(GENERATION.read_text(encoding="utf-8"))
    assert "artifact_path" not in generation["properties"]
    assert generation["properties"]["parquet_reader"]["type"] == "string"


def test_json_numbers_are_not_decimals() -> None:
    payload = {
        "summary_version": "1",
        "source_id": "source",
        "dataset_version": "v1",
        "input_sha256": "a" * 64,
        "contract_version": "1.1.0",
        "transform_version": "1",
        "silver_storage_key": "b" * 64,
        "gold_contract_version": "1",
        "aggregate_version": "1",
        "trip_count": 1,
        "total_distance_km": 1.5,
        "average_duration_seconds": "1.000000",
        "rejected_count": 0,
    }

    with pytest.raises(ValidationError):
        GoldSummary.model_validate(payload)
    with pytest.raises(ValidationError):
        GoldPointer.model_validate(
            {
                "pointer_version": "1",
                "source_id": "source",
                "dataset_version": "v1",
                "storage_key": "c" * 64,
                "summary_sha256": "d" * 64,
                "status": "draft",
            }
        )
