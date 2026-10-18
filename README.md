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
- INC-006: in-process read-only FastAPI routes over a published Gold pointer.

Container images and cloud resources are not implemented. There is no listening server and no deployment in this repository. This is not a production platform.

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

## Read API

`create_app(data_root)` returns a FastAPI application. Tests call it with `TestClient`. `cfi serve` is not implemented, and these tests do not bind a port.

`GET /health/live` returns `{"status": "live"}` when the application answers. It does not look for Bronze, Silver, Gold, or `current.json`, and it does not report readiness or data integrity.

`GET /v1/trips/summary` requires `source_id` and `dataset_version`. The handler reads one `gold/published/{pointer_key}/current.json`, then `summary.json` and `manifest.json` under `gold/snapshots/{storage_key}/`. It does not call `cfi gold`, does not aggregate, does not open Bronze, Silver, or DuckDB, and does not create files or locks.

The pointer bytes are captured once. A promotion that replaces `current.json` afterward can leave the response on the previous intact snapshot. The handler does not read the pointer again, and that promotion is not an error.

An HTTP 200 body contains `source_id`, `dataset_version`, `input_sha256`, `storage_key`, `contract_version`, `transform_version`, `gold_contract_version`, `aggregate_version`, `trip_count`, `total_distance_km`, `average_duration_seconds`, and `rejected_count`. The two decimal fields are JSON strings. The average is null when `trip_count` is 0. Zero trips are still HTTP 200. The body has no `generated_at` and no `total_distance` field.

| Status | Code | Condition |
|---|---|---|
| 200 | | Intact publication, including zero trips. |
| 422 | `missing_parameter` | A required query parameter is absent. |
| 422 | `invalid_identifier` | An identifier fails the storage rule. |
| 404 | `not_published` | `current.json` is absent for that origin. |
| 503 | `pointer_invalid` | The pointer fails JSON, schema, or identity checks. |
| 503 | `snapshot_absent` | The snapshot named by the captured pointer is absent. |
| 503 | `integrity_mismatch` | Hash, size, identity, or metrics disagree. |
| 503 | `schema_mismatch` | The summary or the manifest fails its Gold schema. |
| 500 | `path_limit`, `reparse_point`, `path_escape`, `storage_error` | Path limit, reparse point, escape, or another filesystem failure. |

Error bodies are `{"code": "<code>"}`. They do not include a filesystem path or a traceback. A corrupt pointer or snapshot is not reported as 404.

## Toolchain

The project targets Python 3.12 through uv. uv keeps that interpreter in its own cache and does not replace the system Python. Direct dependencies are declared in `pyproject.toml` and pinned in `uv.lock`. The runtime dependencies are Pydantic v2, DuckDB, and FastAPI. HTTPX is a development dependency used by `TestClient`. Pandas and PyArrow are not direct dependencies. The HTTP modules do not import DuckDB.

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
