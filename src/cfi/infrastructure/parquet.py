"""DuckDB Parquet read and write for one Silver generation.

Instants are stored as DuckDB TIMESTAMP. The components are UTC and the
resolution is microseconds. TIMESTAMP WITH TIME ZONE is not used: DuckDB
1.5.6 refuses to materialize that type in Python without pytz, while TIMESTAMP
round-trips the same microseconds exactly.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from cfi.domain.generation import QUARANTINE_COLUMNS, SILVER_COLUMNS
from cfi.domain.silver import QuarantineLogical, SilverLogical

_DUCKDB_SQL = {
    "utf8": "VARCHAR",
    "utf8_nullable": "VARCHAR",
    "timestamp_utc_us": "TIMESTAMP",
    "decimal(18,3)": "DECIMAL(18,3)",
    "int64": "BIGINT",
    "utf8_list": "VARCHAR[]",
}
_WRITER = "duckdb"
_COMPRESSION = "zstd"


class ParquetRoundtripError(RuntimeError):
    """A Parquet value read back did not match the value written."""


def writer_version() -> str:
    version = getattr(duckdb, "__version__", "")
    if not isinstance(version, str) or version == "":
        raise ParquetRoundtripError("duckdb_version")
    return version


def write_generation_parquet(
    silver_path: Path,
    quarantine_path: Path,
    silver_rows: Sequence[SilverLogical],
    quarantine_rows: Sequence[QuarantineLogical],
    *,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str,
    transform_version: str,
    bronze_run_id: str,
) -> None:
    connection = duckdb.connect(":memory:")
    try:
        _create(connection, "silver_trip", SILVER_COLUMNS)
        if silver_rows:
            connection.executemany(
                _insert_sql("silver_trip", len(SILVER_COLUMNS)),
                [
                    _silver_parameters(
                        row,
                        source_id=source_id,
                        dataset_version=dataset_version,
                        input_sha256=input_sha256,
                        contract_version=contract_version,
                        transform_version=transform_version,
                        bronze_run_id=bronze_run_id,
                    )
                    for row in silver_rows
                ],
            )
        _copy(connection, "silver_trip", silver_path)
        _create(connection, "quarantine_record", QUARANTINE_COLUMNS)
        if quarantine_rows:
            connection.executemany(
                _insert_sql("quarantine_record", len(QUARANTINE_COLUMNS)),
                [
                    _quarantine_parameters(
                        row,
                        source_id=source_id,
                        dataset_version=dataset_version,
                        input_sha256=input_sha256,
                        contract_version=contract_version,
                        transform_version=transform_version,
                        bronze_run_id=bronze_run_id,
                    )
                    for row in quarantine_rows
                ],
            )
        _copy(connection, "quarantine_record", quarantine_path)
    finally:
        connection.close()


def read_generation_parquet(
    silver_path: Path,
    quarantine_path: Path,
) -> tuple[tuple[tuple[object, ...], ...], tuple[tuple[object, ...], ...]]:
    connection = duckdb.connect(":memory:")
    try:
        _expect_schema(connection, silver_path, SILVER_COLUMNS)
        _expect_schema(connection, quarantine_path, QUARANTINE_COLUMNS)
        silver = tuple(
            _normalize_row(row)
            for row in connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY trip_id",
                [str(silver_path)],
            ).fetchall()
        )
        quarantine = tuple(
            _normalize_row(row)
            for row in connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY line_number",
                [str(quarantine_path)],
            ).fetchall()
        )
    finally:
        connection.close()
    return silver, quarantine


def expected_silver_row(
    row: SilverLogical,
    *,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str,
    transform_version: str,
    bronze_run_id: str,
) -> tuple[object, ...]:
    return (
        row.trip_id,
        _as_utc(row.pickup_at),
        _as_utc(row.dropoff_at),
        row.trip_distance,
        row.source_zone,
        row.destination_zone,
        source_id,
        dataset_version,
        input_sha256,
        contract_version,
        transform_version,
        bronze_run_id,
        row.line_number,
        row.duplicate_count,
    )


def expected_quarantine_row(
    row: QuarantineLogical,
    *,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str,
    transform_version: str,
    bronze_run_id: str,
) -> tuple[object, ...]:
    return (
        source_id,
        dataset_version,
        input_sha256,
        contract_version,
        transform_version,
        bronze_run_id,
        row.line_number,
        row.line_sha256,
        row.line_size_bytes,
        row.failure_class,
        list(row.reason_codes),
        row.trip_id,
    )


def _silver_parameters(
    row: SilverLogical,
    *,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str,
    transform_version: str,
    bronze_run_id: str,
) -> tuple[object, ...]:
    values = expected_silver_row(
        row,
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=input_sha256,
        contract_version=contract_version,
        transform_version=transform_version,
        bronze_run_id=bronze_run_id,
    )
    pickup = values[1]
    dropoff = values[2]
    if not isinstance(pickup, datetime) or not isinstance(dropoff, datetime):
        raise ParquetRoundtripError("timestamp")
    return (values[0], pickup.replace(tzinfo=None), dropoff.replace(tzinfo=None), *values[3:])


def _quarantine_parameters(
    row: QuarantineLogical,
    *,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    contract_version: str,
    transform_version: str,
    bronze_run_id: str,
) -> tuple[object, ...]:
    return expected_quarantine_row(
        row,
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=input_sha256,
        contract_version=contract_version,
        transform_version=transform_version,
        bronze_run_id=bronze_run_id,
    )


def _create(
    connection: duckdb.DuckDBPyConnection, name: str, columns: tuple[tuple[str, str], ...]
) -> None:
    body = ", ".join(f"{column} {_DUCKDB_SQL[logical]}" for column, logical in columns)
    connection.execute(f"CREATE TABLE {name} ({body})")


def _insert_sql(name: str, width: int) -> str:
    placeholders = ", ".join("?" for _ in range(width))
    return f"INSERT INTO {name} VALUES ({placeholders})"


def _copy(connection: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    destination = path.as_posix().replace("'", "''")
    connection.execute(f"COPY {name} TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def _expect_schema(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    columns: tuple[tuple[str, str], ...],
) -> None:
    described = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    actual = tuple((str(row[0]), str(row[1])) for row in described)
    expected = tuple((name, _DUCKDB_SQL[logical]) for name, logical in columns)
    if actual != expected:
        raise ParquetRoundtripError("schema")


def _normalize_row(row: tuple[object, ...]) -> tuple[object, ...]:
    normalized: list[object] = []
    for value in row:
        if isinstance(value, datetime):
            if value.tzinfo is not None and value.utcoffset() is not None:
                value = value.astimezone(UTC)
            else:
                value = value.replace(tzinfo=UTC)
            normalized.append(value)
            continue
        if isinstance(value, Decimal):
            normalized.append(value)
            continue
        if isinstance(value, list):
            normalized.append([str(item) for item in value])
            continue
        normalized.append(value)
    return tuple(normalized)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ParquetRoundtripError("timestamp")
    return value.astimezone(UTC)
