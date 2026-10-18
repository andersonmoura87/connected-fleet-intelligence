"""Silver generation manifest v1. Paths are not stored."""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cfi.domain.identifiers import validate_identifier
from cfi.domain.silver import Counts, storage_key_for

_SHA256 = r"^[0-9a-f]{64}$"
_CREATED_AT = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

SILVER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("trip_id", "utf8"),
    ("pickup_at", "timestamp_utc_us"),
    ("dropoff_at", "timestamp_utc_us"),
    ("trip_distance", "decimal(18,3)"),
    ("source_zone", "utf8_nullable"),
    ("destination_zone", "utf8_nullable"),
    ("source_id", "utf8"),
    ("dataset_version", "utf8"),
    ("input_sha256", "utf8"),
    ("contract_version", "utf8"),
    ("transform_version", "utf8"),
    ("bronze_run_id", "utf8"),
    ("line_number", "int64"),
    ("duplicate_count", "int64"),
)

QUARANTINE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_id", "utf8"),
    ("dataset_version", "utf8"),
    ("input_sha256", "utf8"),
    ("contract_version", "utf8"),
    ("transform_version", "utf8"),
    ("bronze_run_id", "utf8"),
    ("line_number", "int64"),
    ("line_sha256", "utf8"),
    ("line_size_bytes", "int64"),
    ("failure_class", "utf8"),
    ("reason_codes", "utf8_list"),
    ("trip_id", "utf8_nullable"),
)


class ColumnSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    logical_type: str


class GenerationManifest(BaseModel):
    """Seal for one Silver generation directory."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        title="silver_generation",
        json_schema_extra={"contract_name": "silver_generation", "contract_version": "1"},
    )

    manifest_version: str
    source_id: str
    dataset_version: str
    input_sha256: str = Field(pattern=_SHA256)
    contract_name: str
    contract_version: str
    transform_version: str
    storage_key: str = Field(pattern=_SHA256)
    bronze_run_id: str
    run_id: str
    created_at: str = Field(pattern=_CREATED_AT)
    status: str
    silver_sha256: str = Field(pattern=_SHA256)
    silver_size_bytes: int = Field(ge=0)
    quarantine_sha256: str = Field(pattern=_SHA256)
    quarantine_size_bytes: int = Field(ge=0)
    silver_schema: tuple[ColumnSpec, ...]
    quarantine_schema: tuple[ColumnSpec, ...]
    parquet_writer: str
    parquet_writer_version: str
    parquet_compression: str
    input_line_count: int = Field(ge=0)
    valid_record_count: int = Field(ge=0)
    invalid_record_count: int = Field(ge=0)
    silver_record_count: int = Field(ge=0)
    identical_duplicate_extra_count: int = Field(ge=0)
    conflict_rejection_count: int = Field(ge=0)
    quarantine_row_count: int = Field(ge=0)

    @field_validator("manifest_version")
    @classmethod
    def _manifest_version(cls, value: str) -> str:
        if value != "1":
            raise ValueError("manifest_version")
        return value

    @field_validator(
        "source_id",
        "dataset_version",
        "contract_version",
        "transform_version",
        "bronze_run_id",
        "run_id",
    )
    @classmethod
    def _identifiers(cls, value: str) -> str:
        return validate_identifier(value)

    @model_validator(mode="after")
    def _sealed(self) -> "GenerationManifest":
        if self.contract_name != "trip_record":
            raise ValueError("contract_name")
        if self.parquet_writer != "duckdb" or self.parquet_compression != "zstd":
            raise ValueError("writer")
        if self.status != "published":
            raise ValueError("status")
        expected_key = storage_key_for(
            self.source_id,
            self.dataset_version,
            self.input_sha256,
            self.contract_version,
            self.transform_version,
        )
        if self.storage_key != expected_key:
            raise ValueError("storage_key")
        if _pairs(self.silver_schema) != SILVER_COLUMNS:
            raise ValueError("silver_schema")
        if _pairs(self.quarantine_schema) != QUARANTINE_COLUMNS:
            raise ValueError("quarantine_schema")
        counts = Counts(
            input_line_count=self.input_line_count,
            valid_record_count=self.valid_record_count,
            invalid_record_count=self.invalid_record_count,
            silver_record_count=self.silver_record_count,
            identical_duplicate_extra_count=self.identical_duplicate_extra_count,
            conflict_rejection_count=self.conflict_rejection_count,
            quarantine_row_count=self.quarantine_row_count,
        )
        if not counts.reconciled():
            raise ValueError("counts")
        return self


def column_specs(columns: tuple[tuple[str, str], ...]) -> tuple[ColumnSpec, ...]:
    return tuple(ColumnSpec(name=name, logical_type=logical_type) for name, logical_type in columns)


def _pairs(columns: tuple[ColumnSpec, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((column.name, column.logical_type) for column in columns)
