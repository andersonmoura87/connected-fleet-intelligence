# Implementation Roadmap v0.2

Indicative 16-week delivery sequence, assuming approximately 8 hours/week. Re-plan when capacity or evidence changes. Results in this repository are local engineering evidence. They are not a record of production operation.

| Phase | Weeks | Deliverables | Exit gate |
|---|---|---|---|
| 1 Foundation | 1-3 | VS-001 local reference: versioned contracts, synthetic fixture, Bronze/Silver/Gold, FastAPI, and quality gates. A container image follows the slice. A real public sample stays behind license and source review. | deterministic fixture replay, executed tests, reproducible local run |
| 2 Lakehouse & AI | 4-6 | optional Databricks Free Edition comparison against the local reference; separate ML baseline and leakage checks | documented comparison and limits; the API remains served from the local Gold snapshot |
| 3 Developer Platform | 7-10 | GitHub Actions, container security, kind/k3d, Argo CD, rollback | deployment and fault recovery evidenced |
| 4 Cloud Foundation & SRE | 11-14 | Terraform modules, IAM/network policy tests, backup/restore, observability, FinOps | no-cost validation and measured recovery; cloud only if approved |
| 5 Staff-style review | 15-16 | measured local baseline, threat model, ADR review, trade-offs, lab incident notes, demo | independently reproducible review packet |

## Vertical Slice 001 increment sequence

Phase 1 builds the local reference in this order. INC-001, INC-002, INC-003, INC-004, INC-005, and INC-006 are present in the repository. INC-007 and later rows are specified and not implemented.

| Increment | Scope | Notes |
|---|---|---|
| INC-001 | Python foundation and quality tooling | Python 3.12, uv, Ruff, mypy, pytest. No data dependencies. |
| INC-002 | Data contracts and deterministic fixtures | Canonical internal contract and synthetic fixture only. |
| INC-003 | Bronze ingestion | Implemented. Immutable original bytes, checksum, replay, and an OS-held lock per source identity. Publication renames a complete directory onto an absent final path. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-004 | Silver transformation and quarantine | Implemented. DuckDB 1.5.6 writes Silver and quarantine Parquet. Generation identity includes trip_record `1.1.0` and transform version `1`. Replay of an equal generation is a no-op. A divergent generation is a conflict and is not replaced. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-005 | Gold operational metrics | Implemented. DuckDB reads Silver Parquet. Python decimal arithmetic writes one JSON snapshot. Exclusive directory rename publishes the snapshot. `os.replace` publishes one pointer per `(source_id, dataset_version)`. `--promote` moves an intact pointer and does not repair a corrupt one. Exercised on Windows and on WSL2 ext4. No power-loss durability claim. |
| INC-006 | FastAPI read endpoints | Implemented. `GET /health/live` and `GET /v1/trips/summary` read a published pointer in-process. DuckDB stays off the request path. `cfi serve` is not implemented. |
| INC-007 | Integration, failure, and reproducibility tests | Includes contended publication. The container image follows this increment and is not part of its definition of done. |

### Artifact identity

`source_identity` is `(source_id, dataset_version)`. Bronze logical identity is `(source_id, dataset_version, input_sha256)`. One published directory exists per `source_identity`. The checksum is sealed in the manifest and is not a second published directory. `run_id` is the lineage of the creating execution. Replay is a no-op and writes nothing. A changed checksum for the same `source_identity` fails closed. Silver, quarantine, and Gold add `contract_version` and `transform_version`.

### Concurrency and durability

INC-003 locks one `source_identity` with an OS-held lock: `fcntl.flock` on Linux and `msvcrt.locking` on Windows. The operating system releases the lock when the process exits. The acquisition timeout starts at 30 seconds. A lock is not taken over because a timestamp aged out. The process revalidates the source identity after it acquires the lock. Process exclusion, abrupt release, and timeout were executed on Windows with `msvcrt.locking` and on WSL2 ext4 with `fcntl.flock`.

INC-005 locks one Gold snapshot, releases that lock, then locks the pointer for that `(source_id, dataset_version)`. Those locks are independent of the Bronze source lock and the Silver generation lock. A contending publisher leaves an intact pointer in place unless the operator passes `--promote`. A corrupt pointer is left in place either way.

Bronze becomes visible when a complete temporary directory is renamed onto a final path that does not exist. An existing final path is not replaced, including an empty directory. Windows uses `os.rename`, which refused an existing destination in the local run. Other platforms use `renameat2(RENAME_NOREPLACE)` and do not fall back to `os.rename`. That distinction was executed on WSL2 ext4. File contents are fsynced before the rename. This design has no measured durability across power loss. A second read of the input does not guarantee an atomic view of a source that changes during the read. Dry-run creates no directory, lock, temporary file, or persistent log.

### DuckDB

DuckDB entered in INC-004. The pinned version is 1.5.6. It writes ZSTD Parquet from an in-memory connection that is closed before publication. `DECIMAL(18,3)` and timezone-naive `TIMESTAMP` values at microsecond resolution were read back in the local tests. Those components are UTC. `TIMESTAMP WITH TIME ZONE` is not used. INC-005 uses the same engine to read those Parquet files. The Gold totals are Python decimal arithmetic, not a SQL average. The INC-006 reader does not import DuckDB and does not open those Parquet files.

## Verification loop

Each increment implements one contract-backed change, includes a failure test, and retains sanitized logs, manifests, and checksums. Trade-offs are recorded in an ADR. Third-party behavior is checked against current official documentation. Public records, synthetic events, and model predictions remain separate datasets.

## Provenance

Reuse third-party snippets only after license and compatibility review, and record provenance in the change. Independently verified IRRAH and Stack Overflow patterns may be reused with that provenance. Unreviewed copies are out of policy.

## Honesty constraint

Local experiments, Free Edition trials, and lab clusters stay labeled as such. Professional production experience is not inferred from this roadmap.
