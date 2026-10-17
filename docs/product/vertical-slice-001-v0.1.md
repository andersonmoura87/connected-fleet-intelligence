# Vertical Slice 001 v0.1 — Public Trips to Read-only Metrics

Status: data path specified, not implemented. The Python package foundation does not implement this slice.

## Goal
From a small pinned synthetic trip fixture, validate records, publish immutable Bronze and typed Silver, compute a Gold summary, and serve it through a read-only FastAPI endpoint. A licensed public sample is a later, separate input. No network access is required for tests or dry run.

## Prerequisites
- Python environment and pinned dependencies; no cloud credentials.
- Locally committed tiny synthetic test fixture that conforms to the canonical internal contract. Download a real public sample only after license and source review, through a versioned field mapping. The fixture remains synthetic and is never described as a public dataset.
- Writable approved workspace root; input size limits configured.

## Commands (proposed CLI contract, not yet implemented)
- `cfi ingest --input <path> --source-id <id> --run-id <id> --dry-run`
- `cfi ingest --input <path> --source-id <id> --run-id <id>`
- `cfi transform --run-id <id> --dry-run`
- `cfi transform --run-id <id>`
- `cfi serve --host 127.0.0.1 --port 8000`

## Minimal data contract
Required: trip_id (nonblank string), pickup_at (timezone-aware ISO8601), dropoff_at (timezone-aware ISO8601), trip_distance (finite number >= 0). Optional: source_zone, destination_zone. Provenance: source_id, dataset_version, input_sha256, run_id. Reject end before start, nonfinite numbers, duplicate conflicting trip IDs and invalid timestamps. Real public dataset field mapping must be versioned and documented; this is a canonical internal contract, not a claim about NYC TLC columns.

## Artifact identity
Bronze logical identity is `(source_id, dataset_version, input_sha256)`. Silver, quarantine, and Gold logical identity is the Bronze identity plus `contract_version` and `transform_version`. `run_id` records one execution attempt and does not create another artifact identity. Identical logical identity is a no-op. The same source identity with a different `input_sha256` fails closed and writes nothing.

## Publication concurrency
A data root has one active publisher. The publisher holds an exclusive lock from generation creation through the atomic pointer replace, or through a failed attempt that leaves the previous pointer unchanged. A contending publisher fails closed, does not move the published pointer, and does not write into a generation owned by the lock holder. Readers do not take the write lock; they follow the pointer to immutable files. This control is required in the first increment that publishes a snapshot. It is not implemented in the Python foundation.

## Invariants
- Dry run makes no writes and never creates a cloud client.
- Bronze original bytes are immutable; identical replay is a no-op; same identity with changed checksum fails closed.
- Silver writes are atomic; invalid rows quarantined with reason and lineage.
- Gold totals are deterministic; no double-count on replay; empty input produces explicit empty state.
- API never serves a partially committed dataset; missing dataset returns documented 503 (not fabricated zero values).
- Concurrent publication cannot interleave generations or replace a newer published pointer with an older one.
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
7. Dry run -> no output mutation.
8. API with missing snapshot -> 503; with published snapshot -> validated 200.
9. Malformed or oversized file -> bounded failure and no partial artifacts.
10. Network blocked -> all fixture-based tests pass.

## Definition of done
Ruff, mypy, pytest pass; unit, contract and integration tests; test fixture documented; no cloud costs; README quickstart; example CLI output; manifest/checksum evidence; threat model note; clean Git diff; no commit until reviewed.

## Out of scope
Kubernetes, streaming broker, Databricks, real-time vehicle location, ML models, multi-account AWS, paid cloud deployment.
