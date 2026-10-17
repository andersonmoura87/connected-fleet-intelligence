"""The committed Bronze manifest schema is the model export."""

import json
from pathlib import Path

from cfi.domain.bronze import BronzeManifest

SCHEMA_PATH = Path("contracts/bronze_manifest/v1/schema.json")
README_PATH = Path("contracts/bronze_manifest/v1/README.md")


def test_committed_schema_matches_model() -> None:
    committed = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    assert committed == BronzeManifest.model_json_schema()


def test_schema_has_no_paths_or_trip_fields() -> None:
    schema = BronzeManifest.model_json_schema()
    properties = schema["properties"]

    assert schema["contract_version"] == "1"
    assert schema["additionalProperties"] is False
    assert "artifact_path" not in properties
    assert "trip_id" not in properties
    assert set(schema["required"]) == {
        "manifest_version",
        "source_id",
        "dataset_version",
        "input_sha256",
        "input_size_bytes",
        "run_id",
        "created_at",
        "status",
    }


def test_contract_readme_excludes_trip_metrics() -> None:
    readme = README_PATH.read_text(encoding="utf-8")

    assert "It does not describe trips, Silver metrics, or Gold metrics." in readme
