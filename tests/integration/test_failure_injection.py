"""Observed behavior under injected I/O errors and abrupt process death.

ENOSPC and EIO on a temporary write or directory are raised as OSError. That is
the current behavior, not a desired operator contract. Rename and pointer
replacement classify those errno values. Nothing here claims durability after
power loss.
"""

import errno
import os
import stat
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cfi.application.diagnose import diagnose
from cfi.application.errors import IngestFailure
from cfi.application.gold import GoldFailure, GoldRequest, GoldSettings, gold
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
from cfi.application.transform import TransformRequest, TransformSettings, transform
from cfi.domain.gold import gold_storage_key_for, pointer_key_for
from cfi.domain.silver import storage_key_for
from cfi.infrastructure.lock import lock_path_for, source_lock
from cfi.interfaces.http import create_app

FIXTURE = Path("tests/fixtures/synthetic/trips_valid.jsonl")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"
_ERRNOS = (errno.ENOSPC, errno.EIO)
_READY_TIMEOUT_SECONDS = 60.0
_EXIT_TIMEOUT_SECONDS = 10.0
_DIRECTORY_FD = 1_000_000

_CHILD = """
import sys
from pathlib import Path


def hold() -> None:
    sys.stdout.write("ready\\n")
    sys.stdout.flush()
    sys.stdin.readline()


mode = sys.argv[1]
hook = sys.argv[2]
root = Path(sys.argv[3])
extra = sys.argv[4]
source = "synthetic.trips.v1"
version = "v1"

if mode == "bronze":
    from cfi.application.ingest import IngestHooks, IngestRequest, IngestSettings, ingest

    ingest(
        IngestSettings(data_root=root),
        IngestRequest(
            input_path=Path(extra),
            source_id=source,
            dataset_version=version,
            run_id="bronze-001",
        ),
        IngestHooks(**{hook: hold}),
    )
elif mode == "silver":
    from cfi.application.transform import (
        TransformHooks,
        TransformRequest,
        TransformSettings,
        transform,
    )

    transform(
        TransformSettings(data_root=root),
        TransformRequest(source_id=source, dataset_version=version, run_id="silver-001"),
        TransformHooks(**{hook: hold}),
    )
else:
    from cfi.application.gold import GoldHooks, GoldRequest, GoldSettings, gold

    gold(
        GoldSettings(data_root=root),
        GoldRequest(
            source_id=source,
            dataset_version=version,
            input_sha256=extra,
            run_id="gold-001",
        ),
        GoldHooks(**{hook: hold}),
    )
"""


def _payload() -> bytes:
    return FIXTURE.read_bytes()


def _bronze(root: Path) -> Path:
    return root / "bronze" / "sources" / SOURCE / VERSION


def _input(root: Path) -> Path:
    path = root / "input.bin"
    path.write_bytes(_payload())
    return path


def _ingest(root: Path, run_id: str = "bronze-001") -> str:
    result = ingest(
        IngestSettings(data_root=root),
        IngestRequest(
            input_path=_input(root),
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id=run_id,
        ),
    )
    assert result.status == "published"
    return result.input_sha256


def _transform(root: Path, run_id: str = "silver-001") -> str:
    result = transform(
        TransformSettings(data_root=root),
        TransformRequest(source_id=SOURCE, dataset_version=VERSION, run_id=run_id),
    )
    return result.storage_key


def _prepare_silver(root: Path) -> str:
    sha = _ingest(root)
    _transform(root)
    return sha


def _gold(root: Path, sha: str, run_id: str = "gold-002"):
    return gold(
        GoldSettings(data_root=root),
        GoldRequest(
            source_id=SOURCE,
            dataset_version=VERSION,
            input_sha256=sha,
            run_id=run_id,
        ),
    )


def _snapshot(root: Path, sha: str) -> Path:
    return root / "gold" / "snapshots" / gold_storage_key_for(SOURCE, VERSION, sha)


def _pointer(root: Path) -> Path:
    return root / "gold" / "published" / pointer_key_for(SOURCE, VERSION) / "current.json"


def _files(root: Path) -> dict[str, bytes]:
    found: dict[str, bytes] = {}
    for path in root.rglob("*"):
        if path.is_file():
            found[path.relative_to(root).as_posix()] = path.read_bytes()
    return found


def _acquire(marker: Path) -> None:
    with source_lock(marker, timeout_seconds=5):
        held = True
    assert held
    assert marker.is_file()


def _io_error(code: int) -> OSError:
    return OSError(code, os.strerror(code))


def _refuse_incoming_mkdir(real: object, code: int):
    def mkdir(self: Path, *args: object, **kwargs: object) -> None:
        if ".incoming" in self.as_posix():
            raise _io_error(code)
        return real(self, *args, **kwargs)  # type: ignore[operator]

    return mkdir


def _refuse_storage_write(code: int):
    def write(fd: int, data: object) -> int:
        del fd, data
        raise _io_error(code)

    return write


def _refuse_rename(code: int):
    def rename(source: Path, destination: Path) -> None:
        del source, destination
        raise _io_error(code)

    return rename


def _script(root: Path) -> Path:
    path = root / "hold_child.py"
    path.write_text(_CHILD, encoding="utf-8")
    return path


def _wait_ready(proc: subprocess.Popen[str]) -> None:
    if proc.stdout is None:
        raise AssertionError("child stdout is closed")
    found: list[str] = []

    def read() -> None:
        found.append(proc.stdout.readline() if proc.stdout is not None else "")

    waiter = threading.Thread(target=read, daemon=True)
    waiter.start()
    waiter.join(_READY_TIMEOUT_SECONDS)
    if not found or found[0].strip() != "ready":
        detail = found[0] if found else "no ready line before the timeout"
        raise AssertionError(detail)


def _stop(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.kill()
    try:
        proc.wait(timeout=_EXIT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_EXIT_TIMEOUT_SECONDS)


def _kill_at_hook(root: Path, mode: str, hook: str, extra: str) -> None:
    script = _script(root)
    error_log = root / "child.err"
    with error_log.open("w", encoding="utf-8") as errors:
        proc = subprocess.Popen(
            [sys.executable, str(script), mode, hook, str(root), extra],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            cwd=Path.cwd(),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        failed: AssertionError | None = None
        try:
            _wait_ready(proc)
            assert proc.poll() is None
        except AssertionError as exc:
            failed = exc
        finally:
            _stop(proc)
            if proc.stdin is not None:
                proc.stdin.close()
            if proc.stdout is not None:
                proc.stdout.close()
    if failed is not None:
        detail = error_log.read_text(encoding="utf-8")
        raise AssertionError(f"{failed}\n{detail}") from failed
    assert proc.poll() is not None


def _summary(root: Path) -> tuple[int, dict[str, object]]:
    with TestClient(create_app(root)) as client:
        response = client.get(
            "/v1/trips/summary",
            params={"source_id": SOURCE, "dataset_version": VERSION},
        )
    body = response.json()
    assert isinstance(body, dict)
    assert str(root) not in response.text
    assert "Traceback" not in response.text
    return response.status_code, body


@pytest.mark.parametrize("code", _ERRNOS)
def test_temporary_write_errors_stay_unclassified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-10. copy_hashed and write_bytes_fsync do not map ENOSPC or EIO."""

    monkeypatch.setattr("cfi.infrastructure.storage.os.write", _refuse_storage_write(code))
    with pytest.raises(OSError) as caught:
        ingest(
            IngestSettings(data_root=tmp_path),
            IngestRequest(
                input_path=_input(tmp_path),
                source_id=SOURCE,
                dataset_version=VERSION,
                run_id="bronze-001",
            ),
        )
    assert caught.value.errno == code
    assert not isinstance(caught.value, IngestFailure)
    assert not _bronze(tmp_path).exists()


@pytest.mark.parametrize("code", _ERRNOS)
def test_bronze_temp_directory_errors_stay_unclassified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-10. create_directory maps EROFS and EACCES only."""

    real = Path.mkdir
    monkeypatch.setattr(Path, "mkdir", _refuse_incoming_mkdir(real, code))
    with pytest.raises(OSError) as caught:
        ingest(
            IngestSettings(data_root=tmp_path),
            IngestRequest(
                input_path=_input(tmp_path),
                source_id=SOURCE,
                dataset_version=VERSION,
                run_id="bronze-001",
            ),
        )
    assert caught.value.errno == code
    assert not _bronze(tmp_path).exists()


@pytest.mark.parametrize("code", _ERRNOS)
def test_gold_temp_directory_errors_stay_unclassified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-10. The Gold snapshot temp directory uses Path.mkdir, not create_directory."""

    sha = _prepare_silver(tmp_path)
    real = Path.mkdir
    monkeypatch.setattr(Path, "mkdir", _refuse_incoming_mkdir(real, code))
    with pytest.raises(OSError) as caught:
        _gold(tmp_path, sha, "gold-001")
    assert caught.value.errno == code
    assert not isinstance(caught.value, GoldFailure)
    assert not _snapshot(tmp_path, sha).exists()
    assert not _pointer(tmp_path).exists()


@pytest.mark.parametrize("code", _ERRNOS)
def test_silver_manifest_write_errors_stay_unclassified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-10. The Silver manifest write uses write_bytes_fsync."""

    sha = _ingest(tmp_path)
    key = storage_key_for(SOURCE, VERSION, sha)
    monkeypatch.setattr("cfi.infrastructure.storage.os.write", _refuse_storage_write(code))
    with pytest.raises(OSError) as caught:
        _transform(tmp_path)
    assert caught.value.errno == code
    assert not (tmp_path / "silver" / "generations" / key).exists()
    assert (_bronze(tmp_path) / "original").read_bytes() == _payload()


@pytest.mark.parametrize("code", _ERRNOS)
def test_bronze_rename_errors_are_classified_and_replay_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-11. The ingest caller maps rename OSError to storage_error."""

    source = _input(tmp_path)
    monkeypatch.setattr("cfi.application.ingest.rename_exclusive", _refuse_rename(code))
    with pytest.raises(IngestFailure) as caught:
        ingest(
            IngestSettings(data_root=tmp_path),
            IngestRequest(
                input_path=source,
                source_id=SOURCE,
                dataset_version=VERSION,
                run_id="bronze-001",
            ),
        )
    assert caught.value.code == "storage_error"
    assert caught.value.exit_code == 4
    assert not _bronze(tmp_path).exists()
    monkeypatch.undo()
    replay = ingest(
        IngestSettings(data_root=tmp_path),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id="bronze-002",
        ),
    )
    assert replay.status == "published"
    assert (_bronze(tmp_path) / "original").read_bytes() == _payload()


@pytest.mark.parametrize("code", _ERRNOS)
def test_gold_rename_errors_are_classified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-11. A failed snapshot rename does not leave the final directory."""

    sha = _prepare_silver(tmp_path)
    monkeypatch.setattr("cfi.application.gold.rename_exclusive", _refuse_rename(code))
    with pytest.raises(GoldFailure) as caught:
        _gold(tmp_path, sha, "gold-001")
    assert caught.value.code == "storage_error"
    assert caught.value.exit_code == 4
    assert caught.value.result.status == "rejected"
    assert not _snapshot(tmp_path, sha).exists()
    assert not _pointer(tmp_path).exists()


@pytest.mark.parametrize("code", _ERRNOS)
def test_pointer_replace_errors_are_partial_and_replay_keeps_the_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    """FI-11. os.replace maps ENOSPC and EIO to storage_error after a durable snapshot."""

    sha = _prepare_silver(tmp_path)

    def fail_replace(source: Path, destination: Path) -> None:
        del source, destination
        raise _io_error(code)

    monkeypatch.setattr("cfi.infrastructure.storage.os.replace", fail_replace)
    with pytest.raises(GoldFailure) as caught:
        _gold(tmp_path, sha, "gold-001")
    assert caught.value.code == "storage_error"
    assert caught.value.exit_code == 4
    assert caught.value.result.status == "partial"
    assert caught.value.result.snapshot_durable is True
    assert caught.value.result.pointer_replaced is False
    sealed = (_snapshot(tmp_path, sha) / "summary.json").read_bytes()
    manifest = (_snapshot(tmp_path, sha) / "manifest.json").read_bytes()
    assert not _pointer(tmp_path).exists()
    monkeypatch.undo()
    recovered = _gold(tmp_path, sha, "gold-002")
    assert recovered.code == "published"
    assert recovered.pointer_state == "pointer_created"
    assert (_snapshot(tmp_path, sha) / "summary.json").read_bytes() == sealed
    assert (_snapshot(tmp_path, sha) / "manifest.json").read_bytes() == manifest


def test_process_death_before_bronze_rename_publishes_on_replay(tmp_path: Path) -> None:
    """FI-12. The final Bronze directory is still absent and the lock can be taken."""

    source = _input(tmp_path)
    _kill_at_hook(tmp_path, "bronze", "before_rename", str(source))
    assert not _bronze(tmp_path).exists()
    _acquire(lock_path_for(tmp_path, SOURCE, VERSION))
    replay = ingest(
        IngestSettings(data_root=tmp_path),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id="bronze-002",
        ),
    )
    assert replay.status == "published"
    assert (_bronze(tmp_path) / "original").read_bytes() == _payload()


def test_process_death_after_bronze_rename_replays_without_rewriting(tmp_path: Path) -> None:
    """FI-13. Bytes published before the process died stay in place."""

    source = _input(tmp_path)
    _kill_at_hook(tmp_path, "bronze", "after_rename", str(source))
    sealed = (_bronze(tmp_path) / "original").read_bytes()
    assert sealed == _payload()
    _acquire(lock_path_for(tmp_path, SOURCE, VERSION))
    replay = ingest(
        IngestSettings(data_root=tmp_path),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id="bronze-002",
        ),
    )
    assert replay.status == "noop"
    assert (_bronze(tmp_path) / "original").read_bytes() == sealed


def test_process_death_before_silver_rename_keeps_bronze(tmp_path: Path) -> None:
    """FI-12. Silver is absent, Bronze is unchanged, and transform can publish."""

    sha = _ingest(tmp_path)
    bronze = (_bronze(tmp_path) / "original").read_bytes()
    key = storage_key_for(SOURCE, VERSION, sha)
    _kill_at_hook(tmp_path, "silver", "before_rename", sha)
    assert not (tmp_path / "silver" / "generations" / key).exists()
    assert (_bronze(tmp_path) / "original").read_bytes() == bronze
    _acquire(tmp_path / "silver" / "locks" / f"{key}.lock")
    assert _transform(tmp_path, "silver-002") == key
    replay = transform(
        TransformSettings(data_root=tmp_path),
        TransformRequest(source_id=SOURCE, dataset_version=VERSION, run_id="silver-003"),
    )
    assert replay.status == "noop"
    assert replay.storage_key == key


def test_process_death_after_silver_rename_replays_without_rewriting(tmp_path: Path) -> None:
    """FI-13. The Silver generation published before death is not replaced."""

    sha = _ingest(tmp_path)
    bronze = (_bronze(tmp_path) / "original").read_bytes()
    key = storage_key_for(SOURCE, VERSION, sha)
    _kill_at_hook(tmp_path, "silver", "after_rename", sha)
    generation = tmp_path / "silver" / "generations" / key
    sealed = (generation / "silver.parquet").read_bytes()
    manifest = (generation / "manifest.json").read_bytes()
    _acquire(tmp_path / "silver" / "locks" / f"{key}.lock")
    replay = transform(
        TransformSettings(data_root=tmp_path),
        TransformRequest(source_id=SOURCE, dataset_version=VERSION, run_id="silver-002"),
    )
    assert replay.status == "noop"
    assert (generation / "silver.parquet").read_bytes() == sealed
    assert (generation / "manifest.json").read_bytes() == manifest
    assert (_bronze(tmp_path) / "original").read_bytes() == bronze


def test_process_death_before_gold_snapshot_rename(tmp_path: Path) -> None:
    """FI-12. No snapshot or pointer exists, and the next gold command publishes both."""

    sha = _prepare_silver(tmp_path)
    key = gold_storage_key_for(SOURCE, VERSION, sha)
    _kill_at_hook(tmp_path, "gold", "before_snapshot_rename", sha)
    assert not _snapshot(tmp_path, sha).exists()
    assert not _pointer(tmp_path).exists()
    _acquire(tmp_path / "gold" / "locks" / f"{key}.lock")
    report = diagnose(tmp_path, SOURCE, VERSION, sha)
    assert report["snapshot_state"] == "absent"
    assert report["pointer_state"] == "absent"
    assert report["silver_state"] == "intact"
    published = _gold(tmp_path, sha, "gold-002")
    assert published.code == "published"
    assert _pointer(tmp_path).is_file()
    status, body = _summary(tmp_path)
    assert status == 200
    assert body["trip_count"] == 2
    assert body["total_distance_km"] == "12.750"
    assert body["rejected_count"] == 0


def test_process_death_after_snapshot_rename_recovers_the_pointer(tmp_path: Path) -> None:
    """FI-13. The snapshot remains byte for byte and gold creates only the pointer."""

    sha = _prepare_silver(tmp_path)
    key = gold_storage_key_for(SOURCE, VERSION, sha)
    _kill_at_hook(tmp_path, "gold", "after_snapshot_rename", sha)
    snapshot = _snapshot(tmp_path, sha)
    summary = (snapshot / "summary.json").read_bytes()
    manifest = (snapshot / "manifest.json").read_bytes()
    assert not _pointer(tmp_path).exists()
    _acquire(tmp_path / "gold" / "locks" / f"{key}.lock")
    report = diagnose(tmp_path, SOURCE, VERSION, sha)
    assert report["snapshot_state"] == "intact"
    assert report["pointer_state"] == "absent"
    assert report["snapshot_storage_key"] == key
    assert _summary(tmp_path)[0] == 404
    recovered = _gold(tmp_path, sha, "gold-002")
    assert recovered.pointer_state == "pointer_created"
    assert (snapshot / "summary.json").read_bytes() == summary
    assert (snapshot / "manifest.json").read_bytes() == manifest
    status, body = _summary(tmp_path)
    assert status == 200
    assert body["average_duration_seconds"] == "1500.000000"
    assert body["rejected_count"] == 0


def test_process_death_after_pointer_replace_replays_without_rewriting(tmp_path: Path) -> None:
    """FI-13. Pointer and snapshot bytes survive process death and a later gold command."""

    sha = _prepare_silver(tmp_path)
    _kill_at_hook(tmp_path, "gold", "after_pointer_replace", sha)
    snapshot = _snapshot(tmp_path, sha)
    summary = (snapshot / "summary.json").read_bytes()
    manifest = (snapshot / "manifest.json").read_bytes()
    pointer = _pointer(tmp_path).read_bytes()
    _acquire(tmp_path / "gold" / "locks" / f"pointer-{pointer_key_for(SOURCE, VERSION)}.lock")
    report = diagnose(tmp_path, SOURCE, VERSION, sha)
    assert report["pointer_state"] == "intact"
    assert report["snapshot_state"] == "intact"
    assert report["same_generation"] is True
    replay = _gold(tmp_path, sha, "gold-002")
    assert replay.code == "noop"
    assert replay.pointer_replaced is False
    assert (snapshot / "summary.json").read_bytes() == summary
    assert (snapshot / "manifest.json").read_bytes() == manifest
    assert _pointer(tmp_path).read_bytes() == pointer
    status, body = _summary(tmp_path)
    assert status == 200
    assert body["trip_count"] == 2
    assert body["total_distance_km"] == "12.750"


def test_directory_fsync_failure_does_not_reject_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FI-14. File sync still runs. Directory sync failure is ignored.

    A published result is not evidence that the bytes would survive power loss.
    """

    real_open = os.open
    real_fsync = os.fsync
    real_close = os.close
    seen = {"directory": 0}

    def open_directory(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if flags == os.O_RDONLY:
            return _DIRECTORY_FD
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    def fsync(fd: int) -> None:
        if fd == _DIRECTORY_FD:
            seen["directory"] += 1
            raise OSError(errno.EIO, "Input/output error")
        real_fsync(fd)

    def close(fd: int) -> None:
        if fd == _DIRECTORY_FD:
            return
        real_close(fd)

    monkeypatch.setattr("cfi.infrastructure.storage.os.open", open_directory)
    monkeypatch.setattr("cfi.infrastructure.storage.os.fsync", fsync)
    monkeypatch.setattr("cfi.infrastructure.storage.os.close", close)
    result = ingest(
        IngestSettings(data_root=tmp_path),
        IngestRequest(
            input_path=_input(tmp_path),
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id="bronze-001",
        ),
    )
    assert seen["directory"] >= 1
    assert result.status == "published"
    assert (_bronze(tmp_path) / "original").read_bytes() == _payload()
    assert stat.S_ISREG((_bronze(tmp_path) / "original").stat().st_mode)


def test_http_erofs_is_storage_error_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FI-16. A read-only open failure stays on the existing 500 storage_error body."""

    sha = _prepare_silver(tmp_path)
    _gold(tmp_path, sha, "gold-001")
    before = _files(tmp_path)

    def refuse(path: object, flags: int, *args: object, **kwargs: object) -> int:
        del path, flags, args, kwargs
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr("cfi.application.published_read.os.open", refuse)
    status, body = _summary(tmp_path)
    assert status == 500
    assert body == {"code": "storage_error"}
    assert "Read-only" not in str(body)
    assert _files(tmp_path) == before
