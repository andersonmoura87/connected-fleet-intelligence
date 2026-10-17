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

Phase 1 builds the local reference in this order. Later rows are planned and are not implemented with INC-001.

| Increment | Scope | Notes |
|---|---|---|
| INC-001 | Python foundation and quality tooling | Python 3.12, uv, Ruff, mypy, pytest. No data dependencies. |
| INC-002 | Data contracts and deterministic fixtures | Canonical internal contract and synthetic fixture only. |
| INC-003 | Bronze ingestion | Immutable original bytes, checksum, replay. |
| INC-004 | Silver transformation and quarantine | DuckDB is introduced here as the Parquet engine for Silver and quarantine. |
| INC-005 | Gold operational metrics | Reuses the DuckDB engine from INC-004. Adds the published pointer and the exclusive publication lock. |
| INC-006 | FastAPI read endpoints | Reads the published snapshot. DuckDB stays off the request path. |
| INC-007 | Integration, failure, and reproducibility tests | Includes contended publication. The container image follows this increment and is not part of its definition of done. |

### Artifact identity

Bronze logical identity is `(source_id, dataset_version, input_sha256)`. Silver, quarantine, and Gold logical identity is that tuple plus `contract_version` and `transform_version`. `run_id` is execution lineage. A new `run_id` does not fork storage identity. Replay of an existing logical identity is a no-op. A changed checksum for an existing source identity fails closed.

### Publication concurrency

Atomic file replace keeps a reader from observing a torn pointer. It does not serialize two publishers. The first increment that publishes a snapshot, INC-005, must take an exclusive lock on the data root for the whole publication transaction: generation write and pointer replace. A publisher that cannot acquire the lock exits nonzero, leaves the previous pointer in place, and does not write into a generation owned by the lock holder. INC-001 does not implement this lock.

### DuckDB

DuckDB enters in INC-004, when the first Parquet artifacts are written. INC-005 uses that same engine for Gold aggregates and does not add a second analytical library. The read API does not query DuckDB.

## Verification loop

Each increment implements one contract-backed change, includes a failure test, and retains sanitized logs, manifests, and checksums. Trade-offs are recorded in an ADR. Third-party behavior is checked against current official documentation. Public records, synthetic events, and model predictions remain separate datasets.

## Provenance

Reuse third-party snippets only after license and compatibility review, and record provenance in the change. Independently verified IRRAH and Stack Overflow patterns may be reused with that provenance. Unreviewed copies are out of policy.

## Honesty constraint

Local experiments, Free Edition trials, and lab clusters stay labeled as such. Professional production experience is not inferred from this roadmap.
