"""Read-only diagnosis without a DuckDB publication."""

import ast
import errno
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

from pytest import CaptureFixture, MonkeyPatch

from cfi.application.diagnose import diagnose
from cfi.application.published_read import read_bounded
from cfi.domain.gold import pointer_key_for
from cfi.interfaces.cli import main

_SOURCE = "synthetic.trips.v1"
_VERSION = "v1"
_OTHER = "ab" * 32
_ROOT = Path("src/cfi")


def _events(stderr: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in stderr.splitlines() if line.startswith("{")]


def _tree(root: Path) -> dict[str, bytes]:
    if not root.exists():
        return {}
    found: dict[str, bytes] = {}
    for path in root.rglob("*"):
        if path.is_file():
            found[path.relative_to(root).as_posix()] = path.read_bytes()
    return found


def _pointer_file(root: Path, payload: bytes) -> Path:
    path = root / "gold" / "published" / pointer_key_for(_SOURCE, _VERSION) / "current.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    return path


def _pointer_document(storage_key: str) -> bytes:
    body = {
        "dataset_version": _VERSION,
        "pointer_version": "1",
        "source_id": _SOURCE,
        "status": "published",
        "storage_key": storage_key,
        "summary_sha256": "cd" * 32,
    }
    return (json.dumps(body, sort_keys=True) + "\n").encode("utf-8")


def _bronze(root: Path, payload: bytes, *, sha: str | None = None, size: int | None = None) -> None:
    final = root / "bronze" / "sources" / _SOURCE / _VERSION
    final.mkdir(parents=True, exist_ok=True)
    (final / "original").write_bytes(payload)
    body = {
        "created_at": "2026-10-20T00:00:00Z",
        "dataset_version": _VERSION,
        "input_sha256": sha if sha is not None else hashlib.sha256(payload).hexdigest(),
        "input_size_bytes": len(payload) if size is None else size,
        "manifest_version": "1",
        "run_id": "run-001",
        "source_id": _SOURCE,
        "status": "published",
    }
    (final / "manifest.json").write_bytes((json.dumps(body, sort_keys=True) + "\n").encode("utf-8"))


def test_missing_data_root_is_absent_and_creates_nothing(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    report = diagnose(missing, _SOURCE, _VERSION, None)
    assert report["status"] == "reported"
    assert report["bronze_state"] == "absent"
    assert report["pointer_state"] == "absent"
    assert "input_sha256" not in report
    assert "silver_state" not in report
    assert "snapshot_state" not in report
    assert "pointer_sha256" not in report
    assert not missing.exists()


def test_checksum_on_a_missing_root_reports_silver_and_snapshot_absent(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    report = diagnose(missing, _SOURCE, _VERSION, _OTHER)
    assert report["input_sha256"] == _OTHER
    assert report["silver_state"] == "absent"
    assert report["snapshot_state"] == "absent"
    assert "silver_storage_key" not in report
    assert "snapshot_storage_key" not in report
    assert not missing.exists()


def test_invalid_identifier_exits_before_any_write(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path))
    code = main(["diagnose", "--source-id", "bad id", "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    assert code == 2
    assert json.loads(captured.out) == {
        "code": "invalid_identifier",
        "exit_code": 2,
        "status": "rejected",
    }
    assert list(tmp_path.iterdir()) == []
    finished = _events(captured.err)[1]
    assert finished["status"] == "rejected"
    assert finished["error_code"] == "invalid_identifier"
    assert "dry_run" not in finished


def test_invalid_checksum_and_missing_arguments_exit_2(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path))
    code = main(
        ["diagnose", "--source-id", _SOURCE, "--dataset-version", _VERSION, "--input-sha256", "AB"]
    )
    captured = capsys.readouterr()
    assert code == 2
    assert json.loads(captured.out)["code"] == "invalid_sha256"
    assert "AB" not in captured.err
    code = main(["diagnose", "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 2
    assert payload == {"code": "usage", "exit_code": 2, "status": "rejected"}
    assert _events(captured.err)[1]["error_code"] == "usage"
    assert list(tmp_path.iterdir()) == []


def test_bronze_intact_pointer_absent_and_events_omit_the_checksum(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    payload = b"abc"
    _bronze(tmp_path, payload)
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path))
    before = _tree(tmp_path)
    code = main(
        [
            "diagnose",
            "--source-id",
            _SOURCE,
            "--dataset-version",
            _VERSION,
            "--input-sha256",
            hashlib.sha256(payload).hexdigest(),
        ]
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert code == 0
    assert report["bronze_state"] == "intact"
    assert report["pointer_state"] == "absent"
    assert report["silver_state"] == "absent"
    assert report["snapshot_state"] == "absent"
    assert _tree(tmp_path) == before
    events = _events(captured.err)
    assert [event["event_name"] for event in events] == ["command_started", "command_finished"]
    assert events[0]["stage"] == events[1]["stage"] == "diagnose"
    assert events[1]["status"] == "succeeded"
    assert events[1]["exit_code"] == 0
    assert "dry_run" not in events[1]
    assert hashlib.sha256(payload).hexdigest() not in captured.err
    assert str(tmp_path) not in captured.err
    assert "input_sha256" not in captured.err


def test_bronze_incomplete_and_mismatch_stay_in_the_report(tmp_path: Path) -> None:
    final = tmp_path / "bronze" / "sources" / _SOURCE / _VERSION
    final.mkdir(parents=True)
    (final / "original").write_bytes(b"abc")
    incomplete = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert incomplete["bronze_state"] == "incomplete"
    _bronze(tmp_path, b"abc", sha="ab" * 32)
    mismatched = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert mismatched["bronze_state"] == "mismatch"
    assert mismatched["pointer_state"] == "absent"


def test_invalid_pointer_snapshot_absent_and_schema_mismatch(tmp_path: Path) -> None:
    path = _pointer_file(tmp_path, b"{")
    invalid = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert invalid["pointer_state"] == "pointer_invalid"
    assert invalid["pointer_sha256"] == hashlib.sha256(b"{").hexdigest()
    assert "published_storage_key" not in invalid

    document = _pointer_document(_OTHER)
    path.write_bytes(document)
    absent = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert absent["pointer_state"] == "snapshot_absent"
    assert absent["pointer_sha256"] == hashlib.sha256(document).hexdigest()
    assert "published_storage_key" not in absent

    snapshot = tmp_path / "gold" / "snapshots" / _OTHER
    snapshot.mkdir(parents=True)
    (snapshot / "summary.json").write_bytes(b"{")
    (snapshot / "manifest.json").write_bytes(b"{")
    schema = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert schema["pointer_state"] == "schema_mismatch"
    assert schema["status"] == "reported"


def test_pointer_bytes_are_captured_once(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    document = _pointer_document(_OTHER)
    path = _pointer_file(tmp_path, document)
    real = read_bounded

    def wrapped(
        target: Path,
        max_bytes: int,
        *,
        missing_code: str = "not_published",
        missing_status: int = 404,
    ) -> bytes:
        data = real(target, max_bytes, missing_code=missing_code, missing_status=missing_status)
        if target.name == "current.json":
            target.write_bytes(b"{")
        return data

    monkeypatch.setattr("cfi.application.diagnose.read_bounded", wrapped)
    report = diagnose(tmp_path, _SOURCE, _VERSION, None)
    assert report["pointer_state"] == "snapshot_absent"
    assert report["pointer_sha256"] == hashlib.sha256(document).hexdigest()
    assert path.read_bytes() == b"{"


def test_a_file_above_the_read_bound_is_storage_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    from cfi.application import diagnose as diagnose_module
    from cfi.application.gold import _MANIFEST_MAX_BYTES, _PARQUET_MAX_BYTES
    from cfi.application.published_read import _MAX_BYTES

    assert diagnose_module._JSON_MAX_BYTES == _MAX_BYTES == _MANIFEST_MAX_BYTES == 1024 * 1024
    assert diagnose_module._BLOB_MAX_BYTES == _PARQUET_MAX_BYTES == 64 * 1024 * 1024

    _bronze(tmp_path, b"abc")
    before = _tree(tmp_path)
    monkeypatch.setattr(diagnose_module, "_BLOB_MAX_BYTES", 2)
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path))
    code = main(["diagnose", "--source-id", _SOURCE, "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    assert code == 4
    assert json.loads(captured.out) == {
        "code": "storage_error",
        "exit_code": 4,
        "status": "rejected",
    }
    assert "mismatch" not in captured.out
    assert _tree(tmp_path) == before

    json_root = tmp_path / "json-bound"
    _pointer_file(json_root, b"{}xx")
    before_json = _tree(json_root)
    monkeypatch.setattr(diagnose_module, "_JSON_MAX_BYTES", 2)
    monkeypatch.setenv("CFI_DATA_ROOT", str(json_root))
    code = main(["diagnose", "--source-id", _SOURCE, "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    assert code == 4
    assert json.loads(captured.out)["code"] == "storage_error"
    assert "pointer_invalid" not in captured.out
    assert _tree(json_root) == before_json


def test_path_limit_and_read_failure_do_not_report_corruption(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    long_root = tmp_path / ("p" * 300)
    monkeypatch.setenv("CFI_DATA_ROOT", str(long_root))
    code = main(["diagnose", "--source-id", _SOURCE, "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    assert code == 4
    assert json.loads(captured.out)["code"] == "path_limit"
    assert ("p" * 300) not in os.listdir(tmp_path)

    _pointer_file(tmp_path, b"{}")
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path))

    def refuse(path: object, flags: int, *args: object, **kwargs: object) -> int:
        del path, flags, args, kwargs
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr("cfi.application.published_read.os.open", refuse)
    code = main(["diagnose", "--source-id", _SOURCE, "--dataset-version", _VERSION])
    captured = capsys.readouterr()
    assert code == 4
    assert json.loads(captured.out) == {
        "code": "storage_error",
        "exit_code": 4,
        "status": "rejected",
    }
    assert "mismatch" not in captured.out
    assert "Read-only" not in captured.out
    assert "Read-only" not in captured.err
    assert str(tmp_path) not in captured.err
    assert "Traceback" not in captured.err


def test_a_readable_tree_is_not_opened_for_writing(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    _bronze(tmp_path, b"abc")
    before = _tree(tmp_path)
    real_open = os.open

    def opener(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if flags & (os.O_APPEND | os.O_CREAT | os.O_RDWR | os.O_WRONLY):
            raise AssertionError(path)
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    def refuse_mkdir(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("mkdir")

    monkeypatch.setattr("cfi.infrastructure.storage.os.open", opener)
    monkeypatch.setattr("cfi.application.published_read.os.open", opener)
    monkeypatch.setattr("cfi.infrastructure.lock.os.open", opener)
    monkeypatch.setattr(Path, "mkdir", refuse_mkdir)
    report = diagnose(tmp_path, _SOURCE, _VERSION, hashlib.sha256(b"abc").hexdigest())
    assert report["bronze_state"] == "intact"
    assert report["pointer_state"] == "absent"
    assert _tree(tmp_path) == before
    assert not (tmp_path / "gold" / "locks").exists()
    assert not (tmp_path / "bronze" / "locks").exists()


def test_modules_do_not_import_duckdb_or_the_publisher() -> None:
    forbidden = {
        "cfi.application.gold",
        "cfi.application.ingest",
        "cfi.application.transform",
        "cfi.infrastructure.lock",
        "cfi.infrastructure.parquet",
        "duckdb",
    }
    tree = ast.parse((_ROOT / "application" / "diagnose.py").read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    assert imported.isdisjoint(forbidden)
    http_tree = ast.parse((_ROOT / "interfaces" / "http.py").read_text(encoding="utf-8"))
    http_imports: set[str] = set()
    for node in ast.walk(http_tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            http_imports.add(node.module)
    assert "cfi.application.diagnose" not in http_imports
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import cfi.application.diagnose; "
            "raise SystemExit(0 if 'duckdb' not in sys.modules else 1)",
        ],
        check=False,
        cwd=Path.cwd(),
        env={**os.environ, "PYTHONPATH": "src"},
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
