"""Silver output contracts stay aligned with the executable column lists."""

import json
from pathlib import Path

from cfi.domain.generation import QUARANTINE_COLUMNS, SILVER_COLUMNS, GenerationManifest

SILVER_SCHEMA = Path("contracts/silver_trip/v1/schema.json")
QUARANTINE_SCHEMA = Path("contracts/quarantine_record/v1/schema.json")
GENERATION_SCHEMA = Path("contracts/silver_generation/v1/schema.json")


def _columns(path: Path) -> tuple[tuple[str, str], ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return tuple((column["name"], column["logical_type"]) for column in payload["columns"])


def test_silver_and_quarantine_columns_match_code() -> None:
    silver = json.loads(SILVER_SCHEMA.read_text(encoding="utf-8"))
    quarantine = json.loads(QUARANTINE_SCHEMA.read_text(encoding="utf-8"))

    assert _columns(SILVER_SCHEMA) == SILVER_COLUMNS
    assert _columns(QUARANTINE_SCHEMA) == QUARANTINE_COLUMNS
    assert silver["contract_name"] == "silver_trip"
    assert silver["contract_version"] == "1"
    assert quarantine["contract_name"] == "quarantine_record"
    assert "payload" not in quarantine


def test_generation_manifest_schema_matches_model() -> None:
    committed = json.loads(GENERATION_SCHEMA.read_text(encoding="utf-8"))

    assert committed == GenerationManifest.model_json_schema()
    assert committed["contract_version"] == "1"
    assert "artifact_path" not in committed["properties"]
    assert "total_distance" not in committed["properties"]
