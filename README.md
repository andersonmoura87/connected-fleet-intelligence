# Connected Fleet Intelligence

Reference platform for fleet data intelligence: ingest and validate trip records, produce traceable analytical datasets, and expose operational metrics through a small, operable service.

Independent project. Not affiliated with Platform Science. Not an operational fleet product.

## Status

Python package `cfi` `0.1.0a0`, with Ruff, mypy, and pytest configured. Ingestion, Bronze, Silver, Gold, the API, container images, and cloud resources are not implemented. This document does not claim that the quality checks have passed.

## Toolchain

The project targets Python 3.12 through uv. uv keeps that interpreter in its own cache and does not replace the system Python. Direct dependencies are declared in `pyproject.toml`. `uv.lock` pins them after resolution; until that file is present, the set is not pinned.

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
