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
- INC-007: one CLI-to-HTTP integration test and the reproduction steps below.
- OPS-001: a Linux container image definition and `cfi serve`.
- OPS-002: a GitHub Actions workflow for quality, tests, and a Linux container smoke test. That workflow has not been executed on GitHub.
- OPS-003: one JSON operational event per command boundary and per completed HTTP request, written to stderr. There is no metrics endpoint and no external collector.
- OPS-004: `cfi diagnose` reads one publication identity and writes nothing. Recovery of a durable Gold snapshot still uses `cfi gold`. See `docs/operations/diagnostics.md`.

The image has not been built or run in this repository's recorded evidence: the Docker engine was not available. This is not a production platform.

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

The command reads one published Bronze directory. It does not take a file path and it does not modify Bronze. `contract_version` is `1.1.0` and `transform_version` is `1`; neither is a flag. `--dry-run` returns counts and the decision without creating a directory, lock, temporary file, or persistent log. An event with `status` `succeeded` and `dry_run` `true` records that decision. It does not mean the dataset was stored.

## Gold publication

```text
cfi gold --source-id <id> --dataset-version <version> --input-sha256 <sha256> --run-id <id> [--dry-run] [--promote]
```

The command opens `silver/generations/{storage_key}/` and does not read Bronze. `storage_key` is derived with the Silver versions fixed in code: trip_record `1.1.0` and transform version `1`. Contract and transform versions are not flags.

DuckDB reads the two Parquet files. The totals are Python decimal arithmetic, not a SQL average. `trip_count` is the number of Silver rows. `total_distance_km` is their distance sum at scale 3. `average_duration_seconds` is the half-even mean of durations in microseconds, written with six decimal places. It is null when `trip_count` is 0. `rejected_count` is the Silver `quarantine_row_count`. `duplicate_count` is not a weight.

One pointer, `gold/published/{pointer_key}/current.json`, exists per `(source_id, dataset_version)`. The first intact publication creates it. `--promote` replaces an intact pointer that names a different intact snapshot. `--promote` does not repair a corrupt pointer, a missing snapshot, or a hash mismatch. `--dry-run` returns the decision and writes nothing.

A later `cfi gold` for the same identity and checksum does not rewrite a sealed snapshot. If that snapshot is intact and `current.json` is absent, the existing publication protocol creates the pointer. If the pointer already names that snapshot, the command is a no-op.

## Diagnosis

```text
cfi diagnose --source-id <id> --dataset-version <version> [--input-sha256 <sha256>]
```

The command reads one identity and writes nothing. It does not create a data root, directory, lock, or pointer, and it does not open DuckDB. It does not publish or promote. Exit 0 and `status` `reported` mean the inspection finished, including when an artifact is absent or invalid. Exit 2 is an invalid argument. Exit 4 is a path or storage failure, and then the report is not written. Absence and hash disagreement are fields in the report, not exit codes.

`bronze_state` is `absent`, `intact`, `incomplete`, or `mismatch`. `pointer_state` is `absent`, `intact`, `pointer_invalid`, `snapshot_absent`, `integrity_mismatch`, or `schema_mismatch`. `current.json` is read once. The command does not claim one transactional view of Bronze, Silver, and Gold together.

`input_sha256`, `silver_state`, and `snapshot_state` appear only when the checksum flag is present. Those states are `absent`, `intact`, `incomplete`, or `mismatch`. The snapshot check compares `summary.json` with `manifest.json`. It does not recompute the aggregate. The operational procedure for each state is in `docs/operations/diagnostics.md`.

## Read API

`create_app(data_root)` returns a FastAPI application. Tests call it with `TestClient`.

```text
cfi serve
```

`cfi serve` reads `CFI_DATA_ROOT`, `CFI_HTTP_HOST`, and `CFI_HTTP_PORT`. The host defaults to `127.0.0.1`. The port defaults to `8000`. It builds `create_app(data_root)` and listens with Uvicorn. It does not create the data root and it does not run ingest, transform, or gold. `GET /health/live` stays `{"status": "live"}` when Gold is absent. The summary route then returns 404 `not_published`. Set `CFI_HTTP_HOST=0.0.0.0` only for a container that publishes the port.

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

## Operational events

Stdout remains the machine result: one JSON object for `cfi ingest`, `cfi transform`, and `cfi gold`, and the HTTP body for the API. Stderr is a JSON Lines log for operators. The process does not open a log file. These lines are not distributed traces, and they are not a latency percentile or an SLO.

Each line has `timestamp` (UTC, `Z` suffix), `event_name`, `stage`, and `status`. `duration_ms` is whole milliseconds for one execution, measured with `perf_counter_ns`. It is one sample. A batch command writes `command_started` and then `command_finished`. `cfi serve` writes one `http_request` when a response finishes. `stage` is `ingest`, `transform`, `gold`, `serve`, or `diagnose`. `status` is `started`, `succeeded`, `noop`, `rejected`, or `partial`.

Optional fields appear only when that execution already knows them: `run_id`, `source_id`, `dataset_version`, `duration_ms`, `error_code`, `exit_code`, `dry_run`, record counts already present on the result, `snapshot_state`, `pointer_state`, `snapshot_durable`, `pointer_replaced`, `route`, and `http_status`. An unknown HTTP path uses the route `unmatched`. The event omits trip payloads, coordinates, absolute paths, the data root, exception text, stack traces, environment variables, secrets, the query string, and `input_sha256`.

`duration_ms` does not establish a service level. Counts are the integers the command already computed. `partial` means a Gold snapshot is durable and the pointer was not replaced. HTTP 200 on `/v1/trips/summary` means that pointer read succeeded. `GET /health/live` does not consult Gold. There is no `/metrics` endpoint and no collector in this process.

`status` `succeeded` with `dry_run` `true` means the command finished its decision and wrote nothing. Stdout is unchanged: an ingest or transform dry-run can still report `status` `published` and `dry_run` `true` while the data root stays absent. That pair is not a stored publication. Exit codes stay the same. `cfi diagnose` has no dry-run. Its event status `succeeded` means the inspection finished. The stdout field `status` `reported` is the machine result and can still describe an absent or invalid artifact.

Batch commands write only application events to stderr. `cfi serve` also writes Uvicorn lifecycle lines to stderr through the standard-library logger. A lifecycle line has `logger` `uvicorn` and `event`. It does not have `event_name`, so it is not an application event. The listening line does not include the bind address or the port. Exception text, paths, and query strings are omitted. A server failure remains visible as `server_error`, `startup_failed`, or `shutdown_failed`, with `error_type` and `errno` when the exception has them. Access logs stay off. One request still produces one `http_request` line.

```text
{"dataset_version": "v1", "dry_run": false, "duration_ms": 12, "event_name": "command_finished", "exit_code": 0, "run_id": "bronze-001", "source_id": "synthetic.trips.v1", "stage": "ingest", "status": "succeeded", "timestamp": "2026-10-20T04:16:00.000000Z"}
```

The example is one finished ingest. A real line also includes the timestamp of that process. Keys are sorted. Replay uses `status` `noop`. A classified failure uses `rejected` and the same `error_code` as stdout. Gold uses `partial` when the snapshot remains and the pointer does not move.

## Reproduce the synthetic slice

Use Python 3.12 and uv. Install the locked dependencies once:

```text
uv sync
```

Point `CFI_DATA_ROOT` at an empty temporary directory. These commands do not open a port.

PowerShell:

```powershell
$root = Join-Path $env:TEMP "cfi-demo"
New-Item -ItemType Directory -Force -Path $root | Out-Null
$env:CFI_DATA_ROOT = $root
uv run cfi ingest --input tests/fixtures/synthetic/trips_valid.jsonl --source-id synthetic.trips.v1 --dataset-version v1 --run-id bronze-001
```

The ingest command prints one JSON object. Copy `input_sha256` from that object, then run:

```powershell
uv run cfi transform --source-id synthetic.trips.v1 --dataset-version v1 --run-id silver-001
uv run cfi gold --source-id synthetic.trips.v1 --dataset-version v1 --input-sha256 <input_sha256> --run-id gold-001
uv run python -c "from pathlib import Path; import os; from fastapi.testclient import TestClient; from cfi.interfaces.http import create_app; client = TestClient(create_app(Path(os.environ['CFI_DATA_ROOT']))); body = client.get('/v1/trips/summary', params={'source_id': 'synthetic.trips.v1', 'dataset_version': 'v1'}).json(); print(body['trip_count'], body['total_distance_km'], body['average_duration_seconds'], body['rejected_count'])"
```

bash:

```bash
root=$(mktemp -d)
export CFI_DATA_ROOT="$root"
uv run cfi ingest --input tests/fixtures/synthetic/trips_valid.jsonl --source-id synthetic.trips.v1 --dataset-version v1 --run-id bronze-001
uv run cfi transform --source-id synthetic.trips.v1 --dataset-version v1 --run-id silver-001
uv run cfi gold --source-id synthetic.trips.v1 --dataset-version v1 --input-sha256 <input_sha256> --run-id gold-001
uv run python -c 'from pathlib import Path; import os; from fastapi.testclient import TestClient; from cfi.interfaces.http import create_app; client = TestClient(create_app(Path(os.environ["CFI_DATA_ROOT"]))); body = client.get("/v1/trips/summary", params={"source_id": "synthetic.trips.v1", "dataset_version": "v1"}).json(); print(body["trip_count"], body["total_distance_km"], body["average_duration_seconds"], body["rejected_count"])'
```

The printed line is `2 12.750 1500.000000 0`. Repeating the three commands with new run ids leaves the input in place: ingest and transform report `"status": "noop"`, and gold reports `"code": "noop"`. The HTTP body stays the same. `created_at` on the manifests is the clock of the first publication.

Remove the temporary directory when finished. PowerShell: `Remove-Item -Recurse -Force $root`. bash: `rm -rf "$root"`.

## Linux container

The image is `linux/amd64`. The base is the official glibc image `python:3.12-slim`, pinned to the amd64 manifest `sha256:2b4f19dae3a777dfc3b76730bda1e82e1f66ab2a2686fa93ca78edbfb4f04ffe`. That digest was read from the Docker registry index `sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1` for the tag `3.12-slim`. The builder copies uv `0.13.1` at `sha256:9a853e40f2eb00f980d265a84fba919dc38ff17750197e7f13c055e4e2f12edd` and runs `uv sync --frozen --no-dev --no-editable`. Development tools are not in the runtime stage. The process user is `cfi`, uid `10001`, gid `10001`. Application code lives under `/opt/cfi`. Data lives only at `/data`.

These commands are the lab procedure. They were not executed here because the Docker engine was unavailable. They are not a production deployment, and they do not show durability across power loss.

```text
docker build --platform linux/amd64 -t cfi:0.1.0a0 .
```

Prerequisites for a lab run are a Docker engine that can build `linux/amd64` and a local volume on the same filesystem as the container writes. Do not mount NFS or CIFS. The host checks use Python 3.12 and uv and do not need the engine.

Place the fixture on the volume as uid `10001` before batch. This does not change the publication commands:

```text
docker run --rm --user 10001:10001 --entrypoint /bin/sh -v cfi-lab-data:/data -v ${PWD}/tests/fixtures/synthetic:/fixtures:ro cfi:0.1.0a0 -c "mkdir -p /data/inbox && cp /fixtures/trips_valid.jsonl /data/inbox/trips_valid.jsonl"
```

Batch uses a short container, `--network none`, a read-only root filesystem, `/tmp` on tmpfs, and the writable data volume. Temporary files and final paths both stay under `/data`:

```text
docker run --rm --network none --read-only --tmpfs /tmp --memory 512m --cpus 1 --user 10001:10001 -e CFI_DATA_ROOT=/data -v cfi-lab-data:/data cfi:0.1.0a0 ingest --input /data/inbox/trips_valid.jsonl --source-id synthetic.trips.v1 --dataset-version v1 --run-id bronze-001
```

Repeat `transform` and `gold` as new containers against the same volume. A second run of the same three commands is replay: ingest and transform report `noop`, and gold reports `noop`. Removing and recreating the container leaves the named volume in place. `docker volume rm cfi-lab-data` deletes that lab volume and nothing else.

The API is a different container. It mounts `/data` read-only and publishes the port only on the host loopback. `CFI_HTTP_HOST=0.0.0.0` is the bind inside the container. Outside a container the default remains `127.0.0.1`. The default port is `8000`.

```text
docker run --rm --name cfi-lab-api --read-only --tmpfs /tmp --memory 256m --cpus 1 --user 10001:10001 -e CFI_DATA_ROOT=/data -e CFI_HTTP_HOST=0.0.0.0 -e CFI_HTTP_PORT=8000 -v cfi-lab-data:/data:ro -p 127.0.0.1:8000:8000 cfi:0.1.0a0 serve
```

The liveness check is `GET /health/live`. It does not read Gold. The procedure expects HTTP 200 `{"status":"live"}` when the process is up, including before publication. The summary route then expects HTTP 404 `not_published`. After the synthetic publication it expects HTTP 200 with 2 trips, `12.750`, `1500.000000`, and 0 rejected. Those HTTP results were not executed in this increment. `docker stop cfi-lab-api` sends SIGTERM. Uvicorn stops listening and then re-raises that signal, so the process status is the signal. The API does not rewrite the volume. Do not use the root user or mode `777` to fix a volume. A read-only data mount on a batch command is expected to fail the command and publish nothing.

Cleanup of this lab only:

```text
docker rm -f cfi-lab-api
docker volume rm cfi-lab-data
docker rmi cfi:0.1.0a0
```

## Continuous integration

`.github/workflows/ci.yml` runs on a push to `main` and on pull requests. The runner is `ubuntu-24.04`. The workflow permission is `contents: read`. It does not publish an image and it does not use a secret. Three jobs run independently:

- `quality` runs `uv sync --frozen`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy src`.
- `test` runs `uv sync --frozen` and `uv run pytest -q`.
- `container` builds the `linux/amd64` image and runs ingest, transform, and gold on a volume named for that run. It then requests `GET /health/live` and `GET /v1/trips/summary`.

A non-zero exit fails the job. The workflow does not retry a failed test. Host jobs resolve dependencies with `uv sync --frozen` from `uv.lock`. The uv download cache is off, because saving it would require an Actions write permission. Ubuntu does not create Windows directory junctions, so those tests skip. A Windows run remains local evidence. This section describes the workflow file. It is not a record of a completed GitHub Actions run.

## Toolchain

The project targets Python 3.12 through uv. uv keeps that interpreter in its own cache and does not replace the system Python. Direct dependencies are declared in `pyproject.toml` and pinned in `uv.lock`. The runtime dependencies are Pydantic v2, DuckDB, FastAPI, and Uvicorn. HTTPX is a development dependency used by `TestClient`. Pandas and PyArrow are not direct dependencies. The HTTP modules do not import DuckDB.

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
