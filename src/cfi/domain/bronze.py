"""Bronze manifest v1 and the pure publish decision.

This module does not touch the filesystem and does not validate trip rows.
"""

import re
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cfi.domain.identifiers import validate_identifier

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CREATED_AT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class ObservationKind(StrEnum):
    ABSENT = "absent"
    SEALED = "sealed"
    INCOMPLETE = "incomplete"
    MISMATCH = "mismatch"


class IngestAction(StrEnum):
    PUBLISH = "publish"
    NOOP = "noop"
    CONFLICT = "identity_conflict"
    INCOMPLETE = "integrity_incomplete"
    MISMATCH = "integrity_mismatch"


class BronzeManifest(BaseModel):
    """Sealed description of one published Bronze directory."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="bronze_manifest",
        json_schema_extra={"contract_name": "bronze_manifest", "contract_version": "1"},
    )

    manifest_version: str = Field(
        description="Schema version of this manifest. Only '1' is accepted."
    )
    source_id: str
    dataset_version: str
    input_sha256: str = Field(description="Lowercase SHA-256 of the original bytes.")
    input_size_bytes: int = Field(ge=0)
    run_id: str = Field(
        description="Execution that created the artifact. Not part of logical identity."
    )
    created_at: str = Field(description="UTC timestamp of the creating execution, with a Z suffix.")
    status: str = Field(description="Published artifacts use the status published.")

    @field_validator("manifest_version")
    @classmethod
    def _version(cls, value: str) -> str:
        if value != "1":
            raise ValueError("manifest_version")
        return value

    @field_validator("source_id", "dataset_version", "run_id")
    @classmethod
    def _identifiers(cls, value: str) -> str:
        return validate_identifier(value)

    @field_validator("input_sha256")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if _SHA256.fullmatch(value) is None:
            raise ValueError("input_sha256")
        return value

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: str) -> str:
        if _CREATED_AT.fullmatch(value) is None:
            raise ValueError("created_at")
        return value

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        if value != "published":
            raise ValueError("status")
        return value


def decide(kind: ObservationKind, sealed_sha256: str | None, incoming_sha256: str) -> IngestAction:
    """Choose an action from the state observed after the source lock is held."""

    if kind is ObservationKind.ABSENT:
        return IngestAction.PUBLISH
    if kind is ObservationKind.INCOMPLETE:
        return IngestAction.INCOMPLETE
    if kind is ObservationKind.MISMATCH:
        return IngestAction.MISMATCH
    if sealed_sha256 == incoming_sha256:
        return IngestAction.NOOP
    return IngestAction.CONFLICT
