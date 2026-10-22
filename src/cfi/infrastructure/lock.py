"""OS-held exclusive lock for one Bronze source identity.

The marker file is only a stable name. Exclusion is the operating-system lock
on the open descriptor, released when that process exits. The marker is not
removed on release or after a crash.
"""

import errno
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from cfi.infrastructure.storage import StorageFailure, storage_io_errno

_BUSY = {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}
if hasattr(errno, "EDEADLK"):
    _BUSY.add(errno.EDEADLK)


class LockUnavailable(TimeoutError):
    """The source lock was still held when the wait expired."""


class LockPathDenied(StorageFailure):
    """The lock directory or marker cannot be created on this filesystem."""

    def __init__(self) -> None:
        super().__init__("storage_error")


def lock_path_for(data_root: Path, source_id: str, dataset_version: str) -> Path:
    """Return the stable marker path. The separator is outside the identifier alphabet."""

    return data_root / "bronze" / "locks" / f"{source_id}={dataset_version}.lock"


@contextmanager
def source_lock(path: Path, timeout_seconds: float) -> Iterator[int]:
    """Hold the OS lock for this process. Do not unlink the shared marker."""

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        if storage_io_errno(exc.errno):
            raise LockPathDenied() from exc
        raise
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOINHERIT"):
        flags |= os.O_NOINHERIT
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        if exc.errno == errno.EROFS:
            raise LockPathDenied() from exc
        raise
    acquired = False
    try:
        _acquire(fd, timeout_seconds)
        acquired = True
        yield fd
    finally:
        if acquired:
            _release(fd)
        os.close(fd)


def _acquire(fd: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            _try_lock(fd)
            return
        except OSError as exc:
            if exc.errno not in _BUSY:
                raise
            if time.monotonic() >= deadline:
                raise LockUnavailable("lock_unavailable") from exc
            time.sleep(0.05)


def _try_lock(fd: int) -> None:
    if os.name == "nt":
        _windows_lock(fd, "LK_NBLCK")
        return
    _flock(fd, _posix_flag("LOCK_EX") | _posix_flag("LOCK_NB"))


def _release(fd: int) -> None:
    if os.name == "nt":
        _windows_lock(fd, "LK_UNLCK")
        return
    _flock(fd, _posix_flag("LOCK_UN"))


def _windows_lock(fd: int, mode_name: str) -> None:
    import msvcrt

    mode = getattr(msvcrt, mode_name)
    if not isinstance(mode, int):
        raise OSError(errno.EINVAL, mode_name)
    os.lseek(fd, 0, os.SEEK_SET)
    lock = getattr(msvcrt, "locking")  # noqa: B009
    lock(fd, mode, 1)


def _posix_flag(name: str) -> int:
    import fcntl

    value = getattr(fcntl, name)
    if not isinstance(value, int):
        raise OSError(errno.EINVAL, name)
    return value


def _flock(fd: int, operation: int) -> None:
    import fcntl

    lock = getattr(fcntl, "flock")  # noqa: B009
    lock(fd, operation)
