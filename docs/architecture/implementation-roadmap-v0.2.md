# Implementation Roadmap v0.2

Indicative 16-week delivery sequence, assuming approximately 8 hours/week. Re-plan when capacity or evidence changes. Results in this repository are local engineering evidence. They are not a record of production operation. The phase table is the original sketch. It is not a list of approved commitments. The increment table below is the record of what is in the tree.

| Phase | Weeks | Deliverables | Exit gate |
|---|---|---|---|
| 1 Foundation | 1-3 | VS-001 local reference: versioned contracts, synthetic fixture, Bronze/Silver/Gold, FastAPI, and quality gates. A container image follows the slice. A real public sample stays behind license and source review. | deterministic fixture replay, executed tests, reproducible local run |
| 2 Lakehouse & AI | 4-6 | optional Databricks Free Edition comparison against the local reference; separate ML baseline and leakage checks | documented comparison and limits; the API remains served from the local Gold snapshot |
| 3 Developer Platform | 7-10 | kind/k3d, Argo CD, and rollback remain unapproved. GitHub Actions `quality`, `test`, and `container` are already recorded under OPS-002 and are not this phase. | deployment and fault recovery evidenced |
| 4 Cloud Foundation & SRE | 11-14 | Terraform modules, IAM/network policy tests, backup/restore, observability, FinOps | no-cost validation and measured recovery; cloud only if approved |
| 5 Staff-style review | 15-16 | measured local baseline, threat model, ADR review, trade-offs, lab incident notes, demo | independently reproducible review packet |

## Vertical Slice 001 increment sequence

Phase 1 builds the local reference in this order. INC-001 through INC-007 are present in the repository. INC-008 and later rows are specified and not implemented. OPS-002 adds the CI workflow. OPS-003 adds stderr JSON Lines for batch commands and HTTP requests. OPS-004 adds a read-only `cfi diagnose` command and an operational runbook. OPS-005 records publication behavior when a process ends before or after the exclusive rename. OPS-006 classifies allowlisted write and directory errno values as `storage_error`. OPS-007 is this documentation alignment and is in progress; it is not a completed increment. Phase 3 in the table above still names kind and Argo CD. Those are not approved work. Phase 4 still names a measured observability platform. OPS-003 and OPS-004 are not that platform. GitHub Actions `quality`, `test`, and `container` landed with OPS-002. On commit `631e65e6b88fd71c4f2c74727dc6974c58181804`, workflow run `39699852347` completed successfully. That run does not close phase 3.

| Increment | Scope | Notes |
|---|---|---|
| INC-001 | Python foundation and quality tooling | Python 3.12, uv, Ruff, mypy, pytest. No data dependencies. |
| INC-002 | Data contracts and deterministic fixtures | Canonical internal contract and synthetic fixture only. |
| INC-003 | Bronze ingestion | Implemented. Immutable original bytes, checksum, replay, and an OS-held lock per source identity. Publication renames a complete directory onto an absent final path. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-004 | Silver transformation and quarantine | Implemented. DuckDB 1.5.6 writes Silver and quarantine Parquet. Generation identity includes trip_record `1.1.0` and transform version `1`. Replay of an equal generation is a no-op. A divergent generation is a conflict and is not replaced. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-005 | Gold operational metrics | Implemented. DuckDB reads Silver Parquet. Python decimal arithmetic writes one JSON snapshot. Exclusive directory rename publishes the snapshot. `os.replace` publishes one pointer per `(source_id, dataset_version)`. `--promote` moves an intact pointer and does not repair a corrupt one. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-006 | FastAPI read endpoints | Implemented. `GET /health/live` and `GET /v1/trips/summary` read a published pointer in-process. DuckDB stays off the request path. The listener arrived in OPS-001. |
| INC-007 | Integration, failure, and reproducibility tests | Implemented. One test runs the public CLI and then `TestClient`. Replay, a same-pointer promotion, and a durable snapshot without `current.json` are included. Per-stage lock tests stay in their increments. A container image is not part of this increment. |
| OPS-001 | Linux runtime and one container image | Image definition and `cfi serve` are in the tree. The image is `linux/amd64`, non-root, and built from the locked runtime dependencies. The CI `container` job in run `39699852347` built and ran that image for the synthetic slice. NFS and CIFS are out of scope. The run is not a power-loss test. |
| OPS-002 | GitHub Actions continuous integration | Workflow for `quality`, `test`, and `container` on push to `main` and on pull requests. The container job builds `linux/amd64` and runs one synthetic smoke test. Ubuntu does not replace the Windows gate. Run `39699852347` on commit `631e65e6b88fd71c4f2c74727dc6974c58181804` finished with those three jobs successful. One run is not continuous production operation. |
| OPS-003 | Observability foundations | Stderr JSON Lines from the standard library: `command_started`, `command_finished`, and `http_request`. Uvicorn lifecycle lines use the same library and omit the bind address. Stdout stays the machine result. `succeeded` with `dry_run` true is not a stored publication. No log file, metrics endpoint, collector, or distributed trace. `duration_ms` is one sample, not an SLO. |
| OPS-004 | Operational diagnosis | `cfi diagnose` reads one identity and writes nothing. A sealed Gold snapshot with no pointer is recovered by running `cfi gold` again. No repair command and no hand-edited `current.json`. |
| OPS-005 | Failure behavior around publication | A process that ends before the exclusive rename leaves the final directory absent. A process that ends after publication leaves that artifact intact, and replay does not rewrite it. A Gold snapshot can exist without `current.json`; a later `cfi gold` creates the pointer. A directory under `.incoming` and a lock marker may remain. Process exit is not power loss. |
| OPS-006 | Storage failure classification | ENOSPC, EIO, EACCES, and EROFS on temporary directory creation, a write open, a write, or a file sync are `storage_error`, exit 4. Gold is `rejected` with `snapshot_durable` false before the snapshot exists, and `partial` with `snapshot_durable` true after it exists and the pointer update fails. Other errno values, DuckDB Parquet write errors, and directory sync failure stay outside this classification. |
| OPS-007 | Documentation alignment | In progress. Entry documents are being updated to match OPS-001 through OPS-006. This row does not mark the increment complete. |

### Artifact identity

`source_identity` is `(source_id, dataset_version)`. Bronze logical identity is `(source_id, dataset_version, input_sha256)`. One published directory exists per `source_identity`. The checksum is sealed in the manifest and is not a second published directory. `run_id` is the lineage of the creating execution. Replay is a no-op and writes nothing. A changed checksum for the same `source_identity` fails closed. Silver, quarantine, and Gold add `contract_version` and `transform_version`.

### Concurrency and durability

INC-003 locks one `source_identity` with an OS-held lock: `fcntl.flock` on Linux and `msvcrt.locking` on Windows. The operating system releases the lock when the process exits. The acquisition timeout starts at 30 seconds. A lock is not taken over because a timestamp aged out. The process revalidates the source identity after it acquires the lock. Process exclusion, abrupt release, and timeout were executed on Windows with `msvcrt.locking` and on WSL2 ext4 with `fcntl.flock`.

INC-005 locks one Gold snapshot, releases that lock, then locks the pointer for that `(source_id, dataset_version)`. Those locks are independent of the Bronze source lock and the Silver generation lock. A contending publisher leaves an intact pointer in place unless the operator passes `--promote`. A corrupt pointer is left in place either way.

Bronze becomes visible when a complete temporary directory is renamed onto a final path that does not exist. An existing final path is not replaced, including an empty directory. Windows uses `os.rename`, which refused an existing destination in the local run. Other platforms use `renameat2(RENAME_NOREPLACE)` and do not fall back to `os.rename`. That distinction was executed on WSL2 ext4. File contents are fsynced before the rename. This design has no measured durability across power loss. A second read of the input does not guarantee an atomic view of a source that changes during the read. Dry-run creates no directory, lock, temporary file, or persistent log.

### DuckDB

DuckDB entered in INC-004. The pinned version is 1.5.6. It writes ZSTD Parquet from an in-memory connection that is closed before publication. `DECIMAL(18,3)` and timezone-naive `TIMESTAMP` values at microsecond resolution were read back in the local tests. Those components are UTC. `TIMESTAMP WITH TIME ZONE` is not used. INC-005 uses the same engine to read those Parquet files. The Gold totals are Python decimal arithmetic, not a SQL average. The INC-006 reader does not import DuckDB and does not open those Parquet files.

## Verification loop

Each increment implements one contract-backed change, includes a failure test, and retains sanitized logs, manifests, and checksums. The system design still lists ADR-0001 through ADR-0007 as records to create. This roadmap does not treat those records as written. Third-party behavior is checked against current official documentation. Public records, synthetic events, and model predictions remain separate datasets.

## Provenance

Reuse third-party snippets only after license and compatibility review, and record provenance in the change. Independently verified IRRAH and Stack Overflow patterns may be reused with that provenance. Unreviewed copies are out of policy.

## Honesty constraint

Local experiments, Free Edition trials, and lab clusters stay labeled as such. Professional production experience is not inferred from this roadmap.
