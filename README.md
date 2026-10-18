# Connected Fleet Intelligence

Reference platform for fleet data intelligence: ingest and validate trip records, produce traceable analytical datasets, and expose operational metrics through a small, operable service.

Independent project. Not affiliated with Platform Science. Not an operational fleet product. Results in this repository are local engineering evidence.

## Status

Python package `cfi` `0.1.0a0`.

Present in the tree:

- INC-001: Python 3.12 toolchain with uv, Ruff, mypy, and pytest.
- INC-002: frozen `trip_record` v1.0.0 and synthetic fixtures.
- INC-003: local Bronze ingestion, manifest v1, and `cfi ingest`.
- INC-004: `trip_record` v1.1.0, Bronze to Silver transformation, quarantine, and `cfi transform`.
- INC-005: Gold publication from one Silver generation, immutable snapshot, and `cfi gold`.

The read API, container images, and cloud resources are not implemented. This is not a production platform.

INC-003, INC-004, and INC-005 were exercised on Windows 10.0.26200 with Python 3.12.15, and on WSL2 Linux (kernel `5.15.167.4-microsoft-standard-WSL2`, ext4, Python 3.12.3) using the versions pinned in `uv.lock`. DuckDB 1.5.6 reads and writes the Parquet files. The Docker engine was not running, so there is no container evidence. Details and limits are in the vertical slice.

## Bronze ingest

```text
cfi ingest --input <path> --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]
```

`CFI_DATA_ROOT` defaults to `data`, which is gitignored. `CFI_MAX_INPUT_BYTES` defaults to 10 MiB. `CFI_LOCK_TIMEOUT_SECONDS` defaults to 30 seconds.

## Silver transform

```text
cfi transform --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]
```

The command reads one published Bronze directory. It does not take a file path and it does not modify Bronze. `contract_version` is `1.1.0` and `transform_version` is `1`; neither is a flag. `--dry-run` returns counts and the decision without creating a directory, lock, temporary file, or persistent log.

## Gold publication

```text
cfi gold --source-id <id> --dataset-version <version> --input-sha256 <sha256> --run-id <id> [--dry-run] [--promote]
```

The command opens `silver/generations/{storage_key}/` and does not read Bronze. `storage_key` is derived with the Silver versions fixed in code: trip_record `1.1.0` and transform version `1`. Contract and transform versions are not flags.

DuckDB reads the two Parquet files. The totals are Python decimal arithmetic, not a SQL average. `trip_count` is the number of Silver rows. `total_distance_km` is their distance sum at scale 3. `average_duration_seconds` is the half-even mean of durations in microseconds, written with six decimal places. It is null when `trip_count` is 0. `rejected_count` is the Silver `quarantine_row_count`. `duplicate_count` is not a weight.

One pointer, `gold/published/{pointer_key}/current.json`, exists per `(source_id, dataset_version)`. The first intact publication creates it. `--promote` replaces an intact pointer that names a different intact snapshot. `--promote` does not repair a corrupt pointer, a missing snapshot, or a hash mismatch. `--dry-run` returns the decision and writes nothing.

## Toolchain

The project targets Python 3.12 through uv. uv keeps that interpreter in its own cache and does not replace the system Python. Direct dependencies are declared in `pyproject.toml` and pinned in `uv.lock`. The runtime dependencies are Pydantic v2 and DuckDB. Pandas and PyArrow are not direct dependencies.

Configured checks:

- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy src`
- `uv run pytest -q`

## Start here

- [Product specification](docs/product/product-spec-v0.2.md)
- [System design](docs/architecture/system-design-v0.2.md)
- [Roadmap](docs/architecture/implementation-roadmap-v0.2.md)
- [First vertical slice](docs/product/vertical-slice-001-v0.1.md)

All performance, reliability, and cost figures are hypotheses until measured. Public trip records and synthetic telemetry are separate datasets and are never presented as the same source.
