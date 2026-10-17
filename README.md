# Connected Fleet Intelligence

Reference platform for fleet data intelligence: ingest and validate trip records, produce traceable analytical datasets, and expose operational metrics through a small, operable service.

Independent project. Not affiliated with Platform Science. Not an operational fleet product. Results in this repository are local engineering evidence.

## Status

Python package `cfi` `0.1.0a0`.

Present in the tree:

- INC-001: Python 3.12 toolchain with uv, Ruff, mypy, and pytest.
- INC-002: `trip_record` v1 and synthetic fixtures.
- INC-003: local Bronze ingestion, manifest v1, and `cfi ingest`.

Silver, Gold, the read API, container images, and cloud resources are not implemented. This is not a production platform.

INC-003 was exercised on Windows 10.0.26200 with Python 3.12.15, and on WSL2 Linux (kernel `5.15.167.4-microsoft-standard-WSL2`, ext4, Python 3.12.3) using the versions pinned in `uv.lock`. The Docker engine was not running, so there is no container evidence. Details and limits are in the vertical slice.

## Bronze ingest

```text
cfi ingest --input <path> --source-id <id> --dataset-version <version> --run-id <id> [--dry-run]
```

`CFI_DATA_ROOT` defaults to `data`, which is gitignored. `CFI_MAX_INPUT_BYTES` defaults to 10 MiB. `CFI_LOCK_TIMEOUT_SECONDS` defaults to 30 seconds.

## Toolchain

The project targets Python 3.12 through uv. uv keeps that interpreter in its own cache and does not replace the system Python. Direct dependencies are declared in `pyproject.toml` and pinned in `uv.lock`. The runtime dependency is Pydantic v2.

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
