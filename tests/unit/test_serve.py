"""Configuration for the read-only server. These tests do not bind a port."""

import ast
from pathlib import Path

from pytest import MonkeyPatch

from cfi.application.errors import IngestFailure
from cfi.interfaces.cli import main
from cfi.interfaces.serve import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, serve, serve_from_env


def test_defaults_do_not_create_a_data_root(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CFI_DATA_ROOT", raising=False)
    monkeypatch.delenv("CFI_HTTP_HOST", raising=False)
    monkeypatch.delenv("CFI_HTTP_PORT", raising=False)
    settings = serve_from_env()
    assert settings.data_root == Path("data")
    assert settings.host == DEFAULT_HTTP_HOST
    assert settings.port == DEFAULT_HTTP_PORT
    assert settings.host == "127.0.0.1"
    assert not (tmp_path / "data").exists()


def test_explicit_bind_and_rejected_values(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", "custom-root")
    monkeypatch.setenv("CFI_HTTP_HOST", "0.0.0.0")
    monkeypatch.setenv("CFI_HTTP_PORT", "8000")
    settings = serve_from_env()
    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert not Path("custom-root").exists()

    monkeypatch.setenv("CFI_HTTP_HOST", "../data")
    try:
        serve_from_env()
    except IngestFailure as exc:
        assert exc.code == "invalid_host"
        assert exc.exit_code == 2
    else:
        raise AssertionError("invalid host was accepted")

    monkeypatch.setenv("CFI_HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("CFI_HTTP_PORT", "0")
    try:
        serve_from_env()
    except IngestFailure as exc:
        assert exc.code == "usage"
        assert exc.exit_code == 2
    else:
        raise AssertionError("port 0 was accepted")

    monkeypatch.setenv("CFI_HTTP_PORT", "65536")
    try:
        serve_from_env()
    except IngestFailure as exc:
        assert exc.code == "usage"
    else:
        raise AssertionError("port 65536 was accepted")

    monkeypatch.setenv("CFI_HTTP_PORT", "8000")
    monkeypatch.setenv("CFI_HTTP_HOST", "")
    try:
        serve_from_env()
    except IngestFailure as exc:
        assert exc.code == "invalid_host"
    else:
        raise AssertionError("empty host was accepted")


def test_cli_serve_starts_without_writing_or_publishing(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    root = tmp_path / "absent"
    seen: dict[str, object] = {}

    def fake_run(
        app: object,
        host: str,
        port: int,
        access_log: bool,
        log_config: dict[str, object],
    ) -> None:
        seen["app"] = app
        seen["host"] = host
        seen["port"] = port
        seen["access_log"] = access_log
        seen["log_config"] = log_config

    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    monkeypatch.setenv("CFI_HTTP_HOST", "127.0.0.1")
    monkeypatch.setenv("CFI_HTTP_PORT", "8000")
    monkeypatch.setattr("cfi.interfaces.serve.uvicorn.run", fake_run)
    assert main(["serve"]) == 0
    app = seen["app"]
    assert getattr(app, "title", "") == "Connected Fleet Intelligence"
    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 8000
    assert seen["access_log"] is False
    log_config = seen["log_config"]
    assert isinstance(log_config, dict)
    handlers = log_config["handlers"]
    assert isinstance(handlers, dict)
    assert "access" not in handlers
    assert not root.exists()


def test_cli_rejects_a_bad_port_without_creating_files(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: object,
) -> None:
    del capsys
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "absent"))
    monkeypatch.setenv("CFI_HTTP_PORT", "nope")
    assert main(["serve"]) == 2
    assert not tmp_path.joinpath("absent").exists()
    assert not list(tmp_path.iterdir())


def test_serve_module_does_not_import_the_publisher() -> None:
    tree = ast.parse(Path("src/cfi/interfaces/serve.py").read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    assert modules.isdisjoint(
        {
            "duckdb",
            "cfi.application.gold",
            "cfi.application.ingest",
            "cfi.application.transform",
            "cfi.infrastructure.parquet",
        }
    )
    assert callable(serve)
