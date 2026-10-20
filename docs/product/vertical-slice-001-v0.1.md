# Vertical Slice 001 v0.1 — Public Trips to Read-only Metrics

Status: Bronze ingestion, Silver transformation, Gold publication, and the read-only FastAPI routes are implemented. Bronze, Silver, and Gold were exercised on Windows 10.0.26200 (Python 3.12.15) and on WSL2 Linux (kernel `5.15.167.4-microsoft-standard-WSL2`, ext4, Python 3.12.3, dependencies from `uv.lock`, DuckDB 1.5.6). The HTTP routes are called in-process with `TestClient`. One integration test runs `cfi ingest`, `cfi transform`, and `cfi gold` through the CLI and then reads the summary with `TestClient`. The command sequence is in the README. These runs are local evidence, not a production certification. The Docker engine was not running. There is no listening server.

## Goal
From a small pinned synthetic trip fixture, validate records, publish immutable Bronze and typed Silver, compute a Gold summary, and serve it through a read-only FastAPI endpoint. A licensed public sample is a later, separate input. No network access is required for tests or dry run.

## Prerequisites
- Python environment and pinned dependencies; no cloud credentials.
- Locally committed tiny synthetic test fixture that conforms to the canonical internal contract. Download a real public sample only after license and source review, through a versioned field mapping. The fixture remains synthetic and is never described as a public dataset.
- Writable approved workspace root; input size limits configured.

## Commands
Implemented:

- `cfi ingest --input <path> --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]`
- `cfi transform --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]`
- `cfi gold --source-id <id> --dataset-version <version> --input-sha256 <sha256> --run-id <id> [--dry-run] [--promote]`
- `cfi serve` reads `CFI_DATA_ROOT`, `CFI_HTTP_HOST`, and `CFI_HTTP_PORT`. It listens with Uvicorn and does not publish data.

`--dataset-version` is required. It has no default. `--dry-run` evaluates the decision and creates no directory, lock, temporary file, or persistent log. An event `status` of `succeeded` with `dry_run` `true` is that decision, not a stored publication. Stdout is one JSON object and keeps its existing status. Stderr is a JSON Lines operational log and is not a second copy of the result. Transform does not accept `contract_version` or `transform_version` as flags. Those values come from the code: `1.1.0` and `1`. Gold does not accept them either, and it does not accept `silver_storage_key`. The caller passes `input_sha256`. Gold derives the Silver directory from that checksum and the versions in code. It does not open Bronze.

Exit codes for `cfi ingest` and `cfi transform`:

| Code | Meaning |
|---|---|
| 0 | `published` or `noop` |
| 2 | usage, invalid identifier, or rejected input |
| 3 | identity conflict, incomplete artifact, checksum mismatch, unstable input, absent Bronze, or a Silver generation that does not match the recomputed rows |
| 4 | storage failure, cross-device rename, reparse point, path escape, path length, or Parquet round-trip failure |
| 5 | lock unavailable |

Exit codes for `cfi gold`:

| Code | Meaning |
|---|---|
| 0 | snapshot published or already current, including an explicit promotion |
| 2 | usage, invalid identifier, or invalid SHA-256 |
| 3 | Silver absent or invalid, snapshot conflict, publication conflict, or a pointer that is corrupt, missing its snapshot, or hash-mismatched |
| 4 | storage failure, cross-device replace, reparse point, path escape, or path length |
| 5 | snapshot lock or pointer lock unavailable |

A Gold result names `snapshot_state` and `pointer_state` separately. `snapshot_durable` is true only when this command observed the final snapshot directory and its schema, hash, size, and metrics checked out. `pointer_replaced` is true only after `os.replace` returns in that process. If the snapshot phase has finished and the pointer lock or the pointer replace then fails, the status is `partial`: the snapshot is not described as absent and it is not deleted. A snapshot-lock timeout reports an already valid snapshot as `snapshot_noop` or `snapshot_conflict` with `snapshot_durable` true. It does not publish and it does not change the pointer. `--promote` does not change a pointer that fails validation. An injected exception after `os.replace` is not turned into a success result.

## Minimal data contract
Required: trip_id (nonblank string), pickup_at (timezone-aware ISO8601, at most six fractional digits), dropoff_at (same timestamp rule), trip_distance (finite number >= 0, fractional scale at most 3). Optional: source_zone, destination_zone. Provenance: source_id, dataset_version, input_sha256, run_id. Reject end before start, nonfinite numbers, duplicate conflicting trip IDs, invalid timestamps, and timestamp literals with more than six fractional digits. The executable row contract is `1.1.0`. Real public dataset field mapping must be versioned and documented; this is a canonical internal contract, not a claim about NYC TLC columns.

## Artifact identity
`source_identity` is `(source_id, dataset_version)`. Bronze logical identity is `(source_id, dataset_version, input_sha256)`. One published directory exists for each `source_identity`. `input_sha256` is the SHA-256 of the stored bytes and is sealed in the manifest. It does not create a second published directory.

`run_id` records the execution that created the artifact. Another `run_id` does not fork storage identity. Replay of the same bytes is a no-op and does not rewrite the bytes, the manifest, or `created_at`. A different checksum for the same `source_identity` fails closed and writes nothing. A new `dataset_version` is a new artifact even when the bytes match. The same bytes under another `source_id` are another artifact.

Silver logical identity is `(source_id, dataset_version, input_sha256, contract_version, transform_version)`. The current transform uses trip_record `1.1.0` and transform version `1`. The directory name is the SHA-256 of those fields, not the fields themselves. `run_id` is the creator recorded in the manifest and is not a path segment.

Gold snapshot identity adds `gold_contract_version` `1` and `aggregate_version` `1`. `run_id`, `created_at`, the absolute path, and the DuckDB version are lineage, not identity. The pointer identity is only `(source_id, dataset_version)`.

Bronze stores the original bytes, including rows that fail trip validation. Ingestion does not accept or reject trips.

## Concurrency
Bronze, Silver, and Gold use separate locks. The Gold snapshot lock is released before the Gold pointer lock is acquired.

INC-003 holds an OS-level exclusive lock for one `source_identity`. The operating system releases that lock when the holding process exits, including abrupt termination. Acquisition waits up to `CFI_LOCK_TIMEOUT_SECONDS`, initially 30 seconds. A waiter does not take over a lock because of its age. After acquisition, the process reads the source state again and only then publishes, replays, or fails closed. Linux uses `fcntl.flock`. Windows uses `msvcrt.locking`. Both come from the Python standard library. The lock excludes other processes that use it. Threads in one process, and programs that ignore the lock, are outside that guarantee. The marker file is not deleted on release or after a crash.

Process tests for abrupt exit, timeout, and two concurrent publishers passed on Windows and on the WSL2 ext4 run. `fcntl.flock` was executed only in that Linux run. `msvcrt.locking` was executed only on Windows.

INC-004 holds a separate OS-level lock at `silver/locks/{storage_key}.lock`. It uses the same lock primitive and the same timeout. It is not the Bronze marker and it is not a Gold pointer lock. Transform does not take the Bronze lock.

INC-005 holds `gold/locks/{storage_key}.lock` while publishing one snapshot, then releases it. It holds `gold/locks/pointer-{pointer_key}.lock` while reading and replacing `current.json`. Those locks are not the Bronze lock and not the Silver lock. Two explicit promotions are serialized by the pointer lock. The last one to pass revalidation sets the target. `created_at` and `run_id` are not a business order.

## Publication and durability
Bronze publication renames a complete temporary directory onto a final path that does not already exist. The temporary directory already contains the original bytes and the manifest, both flushed and fsynced. The rename does not replace an existing published directory, including an empty one. If the final path exists, the rename fails and the published artifact stays in place.

On Windows the implementation uses `os.rename`, which was observed to refuse an existing destination. On other platforms it uses `renameat2(RENAME_NOREPLACE)` and does not fall back to `os.rename`. If `renameat2` is missing or the kernel returns an unsupported-operation error, publication fails closed. That Linux path was executed on the WSL2 ext4 run, where `os.rename` replaced an empty directory and `renameat2(RENAME_NOREPLACE)` did not.

Reads and writes of Bronze bytes use binary mode. On Windows, `O_BINARY` is required so the C runtime does not translate newlines.

The artifact is published when that rename succeeds and an immediate integrity read of the final directory matches the manifest. A crash after the rename can report an error while the directory is already visible. A later ingest verifies the bytes and treats a match as replay.

A second read of the input detects some concurrent modifications. It does not guarantee an atomic snapshot of a source that changes during the read.

File fsync before the rename is required. Directory fsync after the rename is attempted and can fail on Windows. These steps are not evidence of durability across power loss.

Dry-run creates no directory, lock, temporary file, or persistent log. Identifiers used as path segments stay inside a safe alphabet. Writes stay under the resolved data root. Reparse points on the write path are rejected. Symlink and junction inputs are rejected. Cleanup deletes only the temporary directory created by that execution, and it refuses a reparse point instead of following it. A published directory is never deleted by cleanup.

Symlink rejection and cleanup-without-following were executed on the WSL2 run. On Windows, creating a file symlink was not permitted, so that case was skipped; a directory junction on the write path was rejected there. The junction case is skipped on Linux.

## Bronze storage layout

`CFI_DATA_ROOT` defaults to `data`. The default root is gitignored.

```text
{CFI_DATA_ROOT}/bronze/locks/{source_id}={dataset_version}.lock
{CFI_DATA_ROOT}/bronze/.incoming/{run_id}-{token}/original
{CFI_DATA_ROOT}/bronze/.incoming/{run_id}-{token}/manifest.json
{CFI_DATA_ROOT}/bronze/sources/{source_id}/{dataset_version}/original
{CFI_DATA_ROOT}/bronze/sources/{source_id}/{dataset_version}/manifest.json
```

The lock separator is `=`, which is outside the identifier alphabet. `.incoming` holds the temporary directory on the same volume as the final path. Publication is the rename of that complete directory onto `sources/{source_id}/{dataset_version}` when that final path is absent. `original` is the exact input bytes. `manifest.json` is Bronze manifest v1.

Manifest v1 fields are `manifest_version` (`1`), `source_id`, `dataset_version`, `input_sha256`, `input_size_bytes`, `run_id`, `created_at`, and `status` (`published`). Paths are not stored. Trip fields and Silver or Gold metrics are not stored. The contract is `contracts/bronze_manifest/v1/`.

Identifiers match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`, reject a trailing dot, and reject Windows device names. The initial input limit is 10 MiB (`CFI_MAX_INPUT_BYTES`).

## Silver storage layout

Transform locates Bronze only by `source_id` and `dataset_version`. It checks manifest v1, `published`, the original file, size, and SHA-256. It hashes the original again before the directory rename. That second read is not an atomic snapshot. Bronze bytes are not modified.

Physical lines split on LF. A CRLF delimiter drops the CR. A bare CR stays in the line. An empty file has zero lines. A final delimiter does not add a line. Each line is decoded as UTF-8 without replacement. Invalid UTF-8, invalid JSON, a non-object, and an empty line are individual quarantine rows. JSON objects use the default parser and `validate_trip_record` at contract `1.1.0`. A timestamp literal with seven or more fractional digits is rejected before parsing, including when the extra digits are zero. The code is `timestamp_precision_exceeded`.

Valid rows group by `trip_id`. Equal `TripRecord` values become one Silver row. `duplicate_count` is the group size and `line_number` is the earliest physical line. Any difference quarantines every valid row of that `trip_id` with `duplicate_conflict`. Invalid rows do not enter the group. Silver rows are ordered by `trip_id`. Quarantine rows are ordered by `line_number`. Quarantine stores the line hash and size, not the rejected payload.

```text
{CFI_DATA_ROOT}/silver/generations/{storage_key}/silver.parquet
{CFI_DATA_ROOT}/silver/generations/{storage_key}/quarantine.parquet
{CFI_DATA_ROOT}/silver/generations/{storage_key}/manifest.json
{CFI_DATA_ROOT}/silver/locks/{storage_key}.lock
{CFI_DATA_ROOT}/silver/.incoming/{token}/
```

`storage_key` is the SHA-256 of six UTF-8 lines, each ending in newline: `cfi-silver-generation-v1`, `source_id`, `dataset_version`, `input_sha256`, `contract_version`, `transform_version`. The manifest repeats the full identity. Before any write, the longest absolute path that publication would use must be at most 240 characters. There is no current-Silver pointer.

DuckDB 1.5.6 writes both Parquet files from an in-memory connection, using ZSTD. The connection is closed before fsync and rename. `trip_distance` is `DECIMAL(18,3)`. `pickup_at` and `dropoff_at` are normalized to UTC before they are written. The physical type is DuckDB `TIMESTAMP` with microsecond precision and no time zone. Readers must interpret those components as UTC. `TIMESTAMP WITH TIME ZONE` is not used, because materializing it required pytz in this DuckDB version, and pytz is not a dependency. A value that does not read back equal to the value written fails the transform before rename. `Decimal('1.5')` reads back as `Decimal('1.500')`, which compares equal. A distance that already has scale 3 keeps that scale. Empty outputs are Parquet files with the schema and zero rows.

A sealed generation whose rows, hashes, schema, and counts match the recomputation returns `noop` and does not rewrite files or `created_at`. A sealed generation with different logical rows returns `transform_conflict` and leaves the directory in place. An incomplete directory is not repaired or replaced. Parquet bytes are not required to be identical across two separate writes. DuckDB version and compression are lineage in the manifest, not part of `storage_key`.

Counts on every success satisfy `input_line_count = valid_record_count + invalid_record_count`, `valid_record_count = silver_record_count + identical_duplicate_extra_count + conflict_rejection_count`, and `quarantine_row_count = invalid_record_count + conflict_rejection_count`.

Contracts: `contracts/trip_record/v1.1.0/`, `contracts/silver_trip/v1/`, `contracts/quarantine_record/v1/`, and `contracts/silver_generation/v1/`. The v1.0.0 trip schema remains frozen.

## Gold publication

```text
{CFI_DATA_ROOT}/gold/snapshots/{storage_key}/summary.json
{CFI_DATA_ROOT}/gold/snapshots/{storage_key}/manifest.json
{CFI_DATA_ROOT}/gold/locks/{storage_key}.lock
{CFI_DATA_ROOT}/gold/.incoming/{token}/
{CFI_DATA_ROOT}/gold/published/{pointer_key}/current.json
{CFI_DATA_ROOT}/gold/published/{pointer_key}/.incoming/{token}.json
{CFI_DATA_ROOT}/gold/locks/pointer-{pointer_key}.lock
```

`storage_key` is the SHA-256 of eight UTF-8 lines, each ending in newline: `cfi-gold-snapshot-v1`, `source_id`, `dataset_version`, `input_sha256`, `contract_version`, `transform_version`, `gold_contract_version`, `aggregate_version`. `pointer_key` is the SHA-256 of three UTF-8 lines: `cfi-gold-pointer-v1`, `source_id`, `dataset_version`. The same 240-character absolute-path budget is checked before any Gold write.

Gold reads one published Silver generation. It checks the manifest, both Parquet hashes and sizes, the physical schemas, the reconciled counts, the contract and transform versions, and the UTC timestamp convention. Quarantine Parquet is read for that seal and its row count. Its payloads are not aggregated. Bronze is not opened.

DuckDB 1.5.6 is the Parquet reader. Distance is summed in a local decimal context of precision 40. The average converts each duration to integer microseconds, divides with half-even rounding, and formats six fractional digits. It does not use `timedelta.total_seconds()` and it does not use the process-wide decimal precision. An empty Silver generation has `trip_count` 0, `total_distance_km` `0.000`, and `average_duration_seconds` null.

`summary.json` is canonical JSON: UTF-8, sorted keys, two-space indent, trailing newline. Decimals are strings. Null is JSON null. The summary has no clock and no run id, so a replay can match it. `manifest.json` repeats the metrics and records `summary_sha256`, `created_at`, `run_id`, `bronze_run_id`, and the DuckDB reader version. The reader version does not change the storage key and does not authorize a rewrite.

The snapshot directory is published by the same exclusive rename used for Silver. An equal snapshot is `snapshot_noop` and is not rewritten. A logically different sealed snapshot is `snapshot_conflict` and is not replaced. An incomplete snapshot, or a manifest whose hash does not match `summary.json`, is left in place and is not repaired.

`current.json` is replaced with `os.replace` of one file on the same volume. Directory publication does not use `os.replace`. Readers that open the pointer, read all bytes, and close it see either the previous JSON or the new JSON. This is not a power-loss guarantee. On Windows, a reader that holds the file open without share-delete can make the replace fail. The previous pointer then stays in place, the snapshot remains, and the command exits 4.

Contracts: `contracts/gold_summary/v1/`, `contracts/gold_generation/v1/`, and `contracts/gold_pointer/v1/`.

## Recovery

A failure before the rename removes that execution's temporary directory when the process reaches cleanup. If the process is killed first, the unpublished temporary directory can remain under `.incoming`. A later ingest does not publish it and does not delete it.

An existing final path, including an empty directory, is left in place. A published directory with a missing file, an invalid manifest, or a checksum mismatch fails closed. It is not repaired and it is not deleted.

A crash after the rename can report an error while the directory is already visible. The next ingest of the same bytes verifies the directory and returns `noop`. Replay does not rewrite `original`, `manifest.json`, or `created_at`.

## Invariants
- Dry-run makes no filesystem changes, writes no persistent log, and never creates a cloud client.
- Bronze original bytes are immutable; identical replay is a no-op; the same source identity with a changed checksum fails closed.
- A published Bronze directory is not silently replaced.
- One Silver generation directory is renamed into place only when that path is absent. It contains both Parquet files and the manifest. Invalid rows are quarantined with reason and lineage, without the rejected payload.
- One Gold snapshot is renamed into place only when that path is absent. The pointer is a separate file. `--promote` is required to move an intact pointer from one intact snapshot to another.
- `GET /v1/trips/summary` reads the captured pointer and that snapshot. It does not republish Gold, and a later promotion of the pointer is not an error.
- Bronze ingests of one source identity are serialized by the OS lock. Silver publication of one generation identity uses its own lock. Gold uses a snapshot lock and, after releasing it, a pointer lock.
- No personal data or secrets in logs.

## API contract
`GET /health/live` returns 200 `{"status": "live"}` when the application answers. It does not check Bronze, Silver, Gold, or `current.json`.

`GET /health`, `GET /health/ready`, and `GET /metrics` are not implemented. `cfi serve` listens on the two routes above.

`GET /v1/trips/summary?source_id=<id>&dataset_version=<version>` returns the metrics in [trip_summary v1](../../contracts/trip_summary/v1/README.md). Decimal metrics are JSON strings. The average is null when there are zero trips. The body includes `input_sha256` and `storage_key` and does not include `generated_at`.

The handler captures one version of `current.json` and then validates the snapshot it names. It does not read the pointer again. HTTP 404 means that pointer file is absent. A corrupt pointer or snapshot is HTTP 503. A filesystem, path-limit, or reparse failure is HTTP 500. Error bodies are `{"code": "<code>"}`.

## Failure scenarios and tests
1. Valid two-trip fixture -> two Silver rows and one Gold snapshot: `trip_count` 2, `total_distance_km` `12.750`, `average_duration_seconds` `1500.000000`, `rejected_count` 0. The invalid fixture publishes `trip_count` 0, `total_distance_km` `0.000`, a null average, and `rejected_count` 14. The conflict fixture publishes one trip, `1.000` km, average `600.000000`, and `rejected_count` 2. `duplicate_count` is not a weight.
2. Invalid timestamp or negative distance -> one quarantine row per line, with the reason code.
3. Conflicting duplicate ID -> every valid row of that `trip_id` is quarantined. The published generation is not replaced by a later conflicting recomputation.
4. Interrupted write -> a crash before rename leaves no generation; a crash after rename leaves a generation that replays as `noop`.
5. Repeated transform of the same generation -> `noop`, without rewriting the Parquet files or `created_at`. Two fresh writes are not required to be byte-identical.
6. Missing or corrupt Bronze -> nonzero exit and no new generation.
7. Dry run -> no filesystem mutation and no persistent log.
8. Missing `current.json` -> HTTP 404 `not_published`. A missing snapshot, a bad pointer, a hash mismatch, or a schema mismatch -> HTTP 503. The handler does not create the missing file.
9. Malformed JSONL, invalid UTF-8, and an oversized absolute Silver path -> bounded failure and no published generation.
10. Fixture tests do not call a network API. A separate network-blocked run was not executed.

On Windows, creating a file or directory symlink was not permitted, so those ingest, transform, and Gold cases were skipped. Directory junctions on the Silver generation parent and on the Gold snapshot parent were rejected. On WSL2 those symlink cases ran, and the junction cases were skipped. The Windows path-length refusal and the Linux `renameat2(RENAME_NOREPLACE)` behavior were each executed on their own platform. Directory fsync is still not evidence of durability across power loss. `os.replace` of the Gold pointer was executed on both platforms. It is not evidence of durability across power loss.

Bronze file failures are separate from row validation. An oversized, unreadable, or unstable input fails before publication and leaves any previous artifact intact. A file whose rows violate the trip contract remains valid Bronze when its bytes are stored unchanged.

## Definition of done
Ruff, mypy, pytest pass; unit, contract and integration tests; test fixture documented; no cloud costs; README quickstart; example CLI output; manifest/checksum evidence; threat model note; clean Git diff; no commit until reviewed.

## Out of scope
Kubernetes, streaming broker, Databricks, real-time vehicle location, ML models, multi-account AWS, paid cloud deployment.
