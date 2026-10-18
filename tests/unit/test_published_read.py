"""Gold reader failures that do not need a published pipeline."""

import ast
from pathlib import Path

from cfi.application.published_read import ReadFailure, read_published
from cfi.interfaces.http import create_app

ROOT = Path("src/cfi")


def test_invalid_identifier_and_missing_pointer_do_not_create_files(tmp_path: Path) -> None:
    try:
        read_published(tmp_path, "bad id", "v1")
    except ReadFailure as exc:
        assert exc.code == "invalid_identifier"
        assert exc.status_code == 422
    else:
        raise AssertionError("invalid identifier was accepted")

    try:
        read_published(tmp_path, "synthetic.trips.v1", "v1")
    except ReadFailure as exc:
        assert exc.code == "not_published"
        assert exc.status_code == 404
    else:
        raise AssertionError("missing pointer was accepted")
    assert not (tmp_path / "gold").exists()
    assert not list(tmp_path.iterdir())


def test_path_limit_does_not_create_a_directory(tmp_path: Path) -> None:
    root = tmp_path / ("p" * 300)
    try:
        read_published(root, "synthetic.trips.v1", "v1")
    except ReadFailure as exc:
        assert exc.code == "path_limit"
        assert exc.status_code == 500
    else:
        raise AssertionError("long path was accepted")
    assert ("p" * 300) not in {child.name for child in tmp_path.iterdir()}


def test_http_modules_do_not_import_duckdb_or_the_publisher() -> None:
    forbidden = {"duckdb", "cfi.infrastructure.parquet", "cfi.application.gold"}
    for name in ("application/published_read.py", "interfaces/http.py", "interfaces/schemas.py"):
        tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.add(node.module)
        assert imported.isdisjoint(forbidden)
    assert create_app.__module__ == "cfi.interfaces.http"
