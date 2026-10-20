"""Classified storage failures for a read-only or forbidden filesystem."""

import errno
import json
import os
from pathlib import Path

from pytest import MonkeyPatch

from cfi.infrastructure.lock import LockPathDenied, source_lock
from cfi.infrastructure.storage import StorageFailure, create_directory
from cfi.interfaces.cli import main


def test_erofs_while_creating_the_lock_directory(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    def denied(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        del self, mode, parents, exist_ok
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(Path, "mkdir", denied)
    marker = tmp_path / "bronze" / "locks" / "source=v1.lock"
    try:
        with source_lock(marker, 1):
            raise AssertionError("lock acquired")
    except LockPathDenied as exc:
        assert exc.code == "storage_error"
        assert isinstance(exc.__cause__, OSError)
        assert exc.__cause__.errno == errno.EROFS
    else:
        raise AssertionError("EROFS was not classified")
    assert not (tmp_path / "bronze").exists()


def test_eacces_while_creating_the_lock_directory(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    def denied(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        del self, mode, parents, exist_ok
        raise OSError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(Path, "mkdir", denied)
    marker = tmp_path / "bronze" / "locks" / "source=v1.lock"
    try:
        with source_lock(marker, 1):
            raise AssertionError("lock acquired")
    except LockPathDenied as exc:
        assert exc.code == "storage_error"
        assert isinstance(exc.__cause__, OSError)
        assert exc.__cause__.errno == errno.EACCES
    else:
        raise AssertionError("EACCES was not classified")
    assert not marker.exists()


def test_erofs_while_opening_the_lock(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    def denied(path: str | Path, flags: int, mode: int = 0o777) -> int:
        del path, flags, mode
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr("cfi.infrastructure.lock.os.open", denied)
    marker = tmp_path / "locks" / "source=v1.lock"
    try:
        with source_lock(marker, 1):
            raise AssertionError("lock acquired")
    except LockPathDenied as exc:
        assert exc.code == "storage_error"
        assert isinstance(exc.__cause__, OSError)
        assert exc.__cause__.errno == errno.EROFS
    else:
        raise AssertionError("EROFS on open was not classified")
    assert not marker.exists()


def test_unexpected_lock_errors_stay_unclassified(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    def denied(path: str | Path, flags: int, mode: int = 0o777) -> int:
        del path, flags, mode
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr("cfi.infrastructure.lock.os.open", denied)
    try:
        with source_lock(tmp_path / "locks" / "source=v1.lock", 1):
            raise AssertionError("lock acquired")
    except LockPathDenied:
        raise AssertionError("unexpected errno became storage_error") from None
    except OSError as exc:
        assert exc.errno == errno.EIO
    else:
        raise AssertionError("unexpected errno was ignored")


def test_eacces_on_open_is_not_a_storage_denial(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    def denied(path: str | Path, flags: int, mode: int = 0o777) -> int:
        del path, flags, mode
        raise OSError(errno.EACCES, "Permission denied")

    monkeypatch.setattr("cfi.infrastructure.lock.os.open", denied)
    try:
        with source_lock(tmp_path / "locks" / "source=v1.lock", 1):
            raise AssertionError("lock acquired")
    except LockPathDenied:
        raise AssertionError("open EACCES was classified") from None
    except OSError as exc:
        assert exc.errno == errno.EACCES
    else:
        raise AssertionError("open EACCES was ignored")


def test_eacces_while_creating_a_publication_directory(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    create_directory(tmp_path / "bronze" / ".incoming" / "run")
    assert (tmp_path / "bronze" / ".incoming" / "run").is_dir()

    def denied(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        del mode, parents, exist_ok
        raise OSError(errno.EACCES, "Permission denied", str(self))

    monkeypatch.setattr(Path, "mkdir", denied)
    try:
        create_directory(tmp_path / "bronze" / "sources" / "other" / "v1")
    except StorageFailure as exc:
        assert exc.code == "storage_error"
        assert isinstance(exc.__cause__, OSError)
        assert exc.__cause__.errno == errno.EACCES
    else:
        raise AssertionError("EACCES was not classified")
    assert not (tmp_path / "bronze" / "sources").exists()
    assert (tmp_path / "bronze" / ".incoming" / "run").is_dir()


def test_cli_reports_lock_erofs_without_publishing(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: object,
) -> None:
    root, sealed = _published(tmp_path, monkeypatch, capsys)

    def denied(path: str | Path, flags: int, mode: int = 0o777) -> int:
        text = os.fsdecode(path)
        if text.endswith("synthetic.trips.ro=v1.lock"):
            raise OSError(errno.EROFS, "Read-only file system", text)
        return os.open(path, flags, mode)

    monkeypatch.setattr("cfi.infrastructure.lock.os.open", denied)
    code = main(
        [
            "ingest",
            "--input",
            str(tmp_path / "input.bin"),
            "--source-id",
            "synthetic.trips.ro",
            "--dataset-version",
            "v1",
            "--run-id",
            "bronze-ro",
        ]
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    assert code == 4
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "code": "storage_error",
        "dry_run": False,
        "exit_code": 4,
        "status": "rejected",
    }
    assert sealed == _sealed(root, "synthetic.trips.v1")
    assert not (root / "bronze" / "sources" / "synthetic.trips.ro").exists()


def test_cli_reports_publication_eacces_without_replacing_artifacts(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: object,
) -> None:
    root, sealed = _published(tmp_path, monkeypatch, capsys)
    real = Path.mkdir

    def denied(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if ".incoming" in self.parts:
            raise OSError(errno.EACCES, "Permission denied", str(self))
        return real(self, mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", denied)
    code = main(
        [
            "ingest",
            "--input",
            str(tmp_path / "input.bin"),
            "--source-id",
            "synthetic.trips.denied",
            "--dataset-version",
            "v1",
            "--run-id",
            "bronze-denied",
        ]
    )
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    assert code == 4
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "code": "storage_error",
        "dry_run": False,
        "exit_code": 4,
        "status": "rejected",
    }
    assert sealed == _sealed(root, "synthetic.trips.v1")
    assert not (root / "bronze" / "sources" / "synthetic.trips.denied").exists()
    incoming = root / "bronze" / ".incoming"
    assert not incoming.exists() or list(incoming.iterdir()) == []


def _published(
    tmp_path: Path, monkeypatch: MonkeyPatch, capsys: object
) -> tuple[Path, tuple[bytes, bytes]]:
    source = tmp_path / "input.bin"
    source.write_bytes(b"same-bytes")
    root = tmp_path / "data"
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    code = main(
        [
            "ingest",
            "--input",
            str(source),
            "--source-id",
            "synthetic.trips.v1",
            "--dataset-version",
            "v1",
            "--run-id",
            "bronze-001",
        ]
    )
    assert code == 0
    capsys.readouterr()  # type: ignore[attr-defined]
    return root, _sealed(root, "synthetic.trips.v1")


def _sealed(root: Path, source_id: str) -> tuple[bytes, bytes]:
    final = root / "bronze" / "sources" / source_id / "v1"
    return (final / "original").read_bytes(), (final / "manifest.json").read_bytes()
