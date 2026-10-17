# Vertical Slice 001 v0.1 — Public Trips to Read-only Metrics

Status: Bronze ingestion is implemented. It was exercised on Windows 10.0.26200 (Python 3.12.15) and on WSL2 Linux (kernel `5.15.167.4-microsoft-standard-WSL2`, ext4, Python 3.12.3, dependencies from `uv.lock`). Silver, Gold, and the API are not implemented. These runs are local evidence, not a production certification. The Docker engine was not running.

## Goal
From a small pinned synthetic trip fixture, validate records, publish immutable Bronze and typed Silver, compute a Gold summary, and serve it through a read-only FastAPI endpoint. A licensed public sample is a later, separate input. No network access is required for tests or dry run.

## Prerequisites
- Python environment and pinned dependencies; no cloud credentials.
- Locally committed tiny synthetic test fixture that conforms to the canonical internal contract. Download a real public sample only after license and source review, through a versioned field mapping. The fixture remains synthetic and is never described as a public dataset.
- Writable approved workspace root; input size limits configured.

## Commands
Implemented:

- `cfi ingest --input <path> --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]`

Still proposed, and not implemented:

- `cfi transform --run-id <id> [--dry-run]`
- `cfi serve --host 127.0.0.1 --port 8000`

`--dataset-version` is required. It has no default. `--dry-run` evaluates the ingest decision and creates no directory, lock, temporary file, or persistent log. Stdout is one JSON object.

Exit codes for `cfi ingest`:

| Code | Meaning |
|---|---|
| 0 | `published` or `noop` |
| 2 | usage, invalid identifier, or rejected input |
| 3 | identity conflict, incomplete artifact, checksum mismatch, or unstable input |
| 4 | storage failure, cross-device rename, reparse point, or path escape |
| 5 | lock unavailable |

## Minimal data contract
Required: trip_id (nonblank string), pickup_at (timezone-aware ISO8601), dropoff_at (timezone-aware ISO8601), trip_distance (finite number >= 0). Optional: source_zone, destination_zone. Provenance: source_id, dataset_version, input_sha256, run_id. Reject end before start, nonfinite numbers, duplicate conflicting trip IDs and invalid timestamps. Real public dataset field mapping must be versioned and documented; this is a canonical internal contract, not a claim about NYC TLC columns.

## Artifact identity
`source_identity` is `(source_id, dataset_version)`. Bronze logical identity is `(source_id, dataset_version, input_sha256)`. One published directory exists for each `source_identity`. `input_sha256` is the SHA-256 of the stored bytes and is sealed in the manifest. It does not create a second published directory.

`run_id` records the execution that created the artifact. Another `run_id` does not fork storage identity. Replay of the same bytes is a no-op and does not rewrite the bytes, the manifest, or `created_at`. A different checksum for the same `source_identity` fails closed and writes nothing. A new `dataset_version` is a new artifact even when the bytes match. The same bytes under another `source_id` are another artifact.

Silver, quarantine, and Gold logical identity is the Bronze identity plus `contract_version` and `transform_version`.

Bronze stores the original bytes, including rows that fail trip validation. Ingestion does not accept or reject trips.

## Concurrency
Two locks cover different critical sections.

INC-003 holds an OS-level exclusive lock for one `source_identity`. The operating system releases that lock when the holding process exits, including abrupt termination. Acquisition waits up to `CFI_LOCK_TIMEOUT_SECONDS`, initially 30 seconds. A waiter does not take over a lock because of its age. After acquisition, the process reads the source state again and only then publishes, replays, or fails closed. Linux uses `fcntl.flock`. Windows uses `msvcrt.locking`. Both come from the Python standard library. The lock excludes other processes that use it. Threads in one process, and programs that ignore the lock, are outside that guarantee. The marker file is not deleted on release or after a crash.

Process tests for abrupt exit, timeout, and two concurrent publishers passed on Windows and on the WSL2 ext4 run. `fcntl.flock` was executed only in that Linux run. `msvcrt.locking` was executed only on Windows.

INC-005 holds a separate exclusive lock around replacement of the Gold snapshot pointer for the data root. Readers of the snapshot do not take that write lock. A contending publisher fails closed and leaves the previous pointer in place. The Gold pointer lock is not the Bronze source lock.

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

## Recovery

A failure before the rename removes that execution's temporary directory when the process reaches cleanup. If the process is killed first, the unpublished temporary directory can remain under `.incoming`. A later ingest does not publish it and does not delete it.

An existing final path, including an empty directory, is left in place. A published directory with a missing file, an invalid manifest, or a checksum mismatch fails closed. It is not repaired and it is not deleted.

A crash after the rename can report an error while the directory is already visible. The next ingest of the same bytes verifies the directory and returns `noop`. Replay does not rewrite `original`, `manifest.json`, or `created_at`.

## Invariants
- Dry-run makes no filesystem changes, writes no persistent log, and never creates a cloud client.
- Bronze original bytes are immutable; identical replay is a no-op; the same source identity with a changed checksum fails closed.
- A published Bronze directory is not silently replaced.
- Silver writes are atomic; invalid rows quarantined with reason and lineage.
- Gold totals are deterministic; no double-count on replay; empty input produces explicit empty state.
- API never serves a partially committed dataset; missing dataset returns documented 503 (not fabricated zero values).
- Bronze ingests of one source identity are serialized by the OS lock. Gold pointer publication uses its own lock.
- No personal data or secrets in logs.

## API contract (proposed)
`GET /health/live` -> 200 when process running.
`GET /health/ready` -> 200 only when published Gold snapshot exists, otherwise 503.
`GET /v1/trips/summary` -> `{dataset_version, trip_count, total_distance, average_duration_seconds, rejected_count, generated_at}`; use a fixed or injected clock for deterministic tests.

## Failure scenarios and tests
1. Valid two-trip fixture -> expected count/distance/duration.
2. Invalid timestamp or negative distance -> quarantine with reason.
3. Conflicting duplicate ID -> fail/quarantine according to explicit contract, never silently overwrite.
4. Interrupted write -> previous published snapshot intact.
5. Repeated ingest/transform -> byte-identical outputs or semantically identical manifests with stable logical identity.
6. Missing input -> nonzero exit and no checkpoint advancement.
7. Dry run -> no filesystem mutation and no persistent log.
8. API with missing snapshot -> 503; with published snapshot -> validated 200.
9. Malformed or oversized file -> bounded failure and no partial artifacts.
10. Network blocked -> all fixture-based tests pass.

Bronze file failures are separate from row validation. An oversized, unreadable, or unstable input fails before publication and leaves any previous artifact intact. A file whose rows violate the trip contract remains valid Bronze when its bytes are stored unchanged.

## Definition of done
Ruff, mypy, pytest pass; unit, contract and integration tests; test fixture documented; no cloud costs; README quickstart; example CLI output; manifest/checksum evidence; threat model note; clean Git diff; no commit until reviewed.

## Out of scope
Kubernetes, streaming broker, Databricks, real-time vehicle location, ML models, multi-account AWS, paid cloud deployment.
