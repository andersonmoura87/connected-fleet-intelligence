"""Filesystem helpers for publication.

Directory publication uses a rename onto a path that does not exist.
On Windows, os.rename fails with FileExistsError when the destination exists,
including when that destination is an empty directory. On other platforms,
renameat2(RENAME_NOREPLACE) is required because os.rename can replace an empty
directory. os.replace is used only by replace_file, which publishes one file.
"""

import ctypes
import ctypes.util
import errno
import hashlib
import os
import stat
from pathlib import Path
from typing import NoReturn

_CHUNK = 1024 * 1024
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


def _read_flags() -> int:
    """Read bytes unchanged. Windows os.open translates newlines unless O_BINARY is set."""

    flags = os.O_RDONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _write_flags() -> int:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    return flags


class StorageFailure(OSError):
    """A filesystem operation failed without replacing a published artifact."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


_STORAGE_IO_ERRNOS = frozenset({errno.ENOSPC, errno.EIO, errno.EACCES, errno.EROFS})
_CLEANUP_SECURITY_CODES = frozenset({"path_escape", "reparse_point"})


def storage_io_errno(code: int | None) -> bool:
    """Return whether this errno is classified at a write or directory boundary."""

    return code in _STORAGE_IO_ERRNOS


def _raise_storage_io(exc: OSError) -> NoReturn:
    """Map an allowlisted write errno to storage_error. Leave every other errno unchanged."""

    if isinstance(exc, StorageFailure):
        raise exc
    if exc.errno in _STORAGE_IO_ERRNOS:
        raise StorageFailure("storage_error") from exc
    raise exc


def _close_fd(fd: int, pending: Exception | None) -> None:
    """Close one descriptor. A close error must not replace a write or read error."""

    try:
        os.close(fd)
    except OSError:
        if pending is None:
            raise


def retain_primary(
    primary: Exception | None,
    cleanup: Exception,
    *,
    decided: bool,
) -> Exception | None:
    """Choose the failure that remains after an unpublished temporary is removed.

    path_escape and reparse_point stay visible. A common cleanup error does not
    replace a primary failure or a decided publication result. When cleanup is
    the only failure, that error is returned.
    """

    security = isinstance(cleanup, StorageFailure) and cleanup.code in _CLEANUP_SECURITY_CODES
    if security:
        if primary is not None:
            primary.__context__ = cleanup
            return primary
        return cleanup
    if primary is not None or decided:
        return primary
    return cleanup


def remove_unpublished(
    path: Path,
    ancestor: Path,
    primary: Exception | None,
    *,
    decided: bool,
) -> Exception | None:
    """Remove one unpublished temporary. Keep the publication outcome when removal fails."""

    if not path.exists():
        return primary
    try:
        remove_tree_nofollow(path, ancestor)
    except Exception as exc:
        retained = retain_primary(primary, exc, decided=decided)
        if retained is not None:
            return retained
    return primary


def create_directory(path: Path, *, exist_ok: bool = False) -> None:
    """Create a publication directory. Allowlisted I/O errno values are storage_error."""

    try:
        path.mkdir(parents=True, exist_ok=exist_ok)
    except OSError as exc:
        _raise_storage_io(exc)


def is_reparse_point(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(flag and attributes & flag)


def assert_tree_has_no_reparse(root: Path, path: Path) -> None:
    """Reject symlinks and junctions on the lexical walk from root to path.

    ``Path.resolve`` follows reparse points, so it is not used to choose the
    components that are checked. A resolved path that leaves root is rejected.
    """

    if is_reparse_point(root):
        raise StorageFailure("reparse_point")
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise StorageFailure("path_escape") from exc
    current = root
    for part in relative.parts:
        if part in {"", ".", ".."}:
            raise StorageFailure("path_escape")
        current = current / part
        if is_reparse_point(current):
            raise StorageFailure("reparse_point")
    try:
        current.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise StorageFailure("path_escape") from exc


def is_regular_file(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if is_reparse_point(path):
        return False
    return stat.S_ISREG(info.st_mode)


def hash_file(path: Path, max_bytes: int) -> tuple[str, int]:
    """Hash a regular file. Raise StorageFailure when the stream exceeds max_bytes."""

    fd = os.open(path, _read_flags())
    hasher = hashlib.sha256()
    size = 0
    try:
        while True:
            chunk = os.read(fd, _CHUNK)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise StorageFailure("input_too_large")
            hasher.update(chunk)
    finally:
        os.close(fd)
    return hasher.hexdigest(), size


def write_bytes_fsync(path: Path, payload: bytes) -> None:
    """Create one new file and fsync it. Allowlisted write errno values are storage_error."""

    try:
        fd = os.open(path, _write_flags())
    except OSError as exc:
        _raise_storage_io(exc)
    pending: OSError | None = None
    try:
        view = memoryview(payload)
        while view:
            try:
                written = os.write(fd, view)
            except OSError as exc:
                pending = exc
                break
            view = view[written:]
        if pending is None:
            try:
                os.fsync(fd)
            except OSError as exc:
                pending = exc
    finally:
        _close_fd(fd, pending)
    if pending is not None:
        _raise_storage_io(pending)


def copy_hashed(source: Path, destination: Path, max_bytes: int) -> tuple[str, int]:
    """Copy bytes and fsync the destination. Read errors stay unclassified."""

    src_fd = os.open(source, _read_flags())
    pending: OSError | None = None
    read_error: OSError | None = None
    dst_fd: int | None = None
    hasher = hashlib.sha256()
    size = 0
    try:
        try:
            dst_fd = os.open(destination, _write_flags())
        except OSError as exc:
            pending = exc
        if dst_fd is not None and pending is None:
            while True:
                try:
                    chunk = os.read(src_fd, _CHUNK)
                except OSError as exc:
                    read_error = exc
                    break
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    pending = StorageFailure("input_too_large")
                    break
                hasher.update(chunk)
                view = memoryview(chunk)
                try:
                    while view:
                        written = os.write(dst_fd, view)
                        view = view[written:]
                except OSError as exc:
                    pending = exc
                    break
            if pending is None and read_error is None:
                try:
                    os.fsync(dst_fd)
                except OSError as exc:
                    pending = exc
    finally:
        if dst_fd is not None:
            _close_fd(dst_fd, pending if read_error is None else read_error)
        _close_fd(src_fd, pending if read_error is None else read_error)
    if read_error is not None:
        raise read_error
    if isinstance(pending, StorageFailure):
        raise pending
    if pending is not None:
        _raise_storage_io(pending)
    return hasher.hexdigest(), size


def fsync_file(path: Path) -> None:
    """Fsync one existing file. Allowlisted errno values are storage_error."""

    flags = os.O_RDWR
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        _raise_storage_io(exc)
    pending: OSError | None = None
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            pending = exc
    finally:
        _close_fd(fd, pending)
    if pending is not None:
        _raise_storage_io(pending)


def fsync_directory(path: Path) -> None:
    """Best-effort directory fsync. Failure is ignored and is not a durability claim."""

    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def rename_exclusive(source: Path, destination: Path) -> None:
    """Move source onto destination only when destination is absent.

    Windows os.rename already refuses an existing path, including an empty
    directory. POSIX os.rename can replace an empty directory, so it is not used.
    """

    if os.name == "nt":
        os.rename(source, destination)
        return
    _rename_noreplace(source, destination)


def _rename_noreplace(source: Path, destination: Path) -> None:
    """Publish with renameat2. There is no fallback to os.rename."""

    libc_name = ctypes.util.find_library("c")
    if libc_name is None:
        raise StorageFailure("rename_noreplace_unavailable")
    libc = ctypes.CDLL(libc_name, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise StorageFailure("rename_noreplace_unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    unsupported = {errno.ENOSYS}
    for name in ("ENOTSUP", "EOPNOTSUPP"):
        value = getattr(errno, name, None)
        if isinstance(value, int):
            unsupported.add(value)
    if error in unsupported:
        raise StorageFailure("rename_noreplace_unavailable")
    raise OSError(error, os.strerror(error), str(destination))


def replace_file(source: Path, destination: Path) -> None:
    """Publish one file by atomic replacement. Directory publication does not use this."""

    if is_reparse_point(source) or is_reparse_point(destination):
        raise StorageFailure("reparse_point")
    try:
        destination_info = destination.lstat()
    except FileNotFoundError:
        destination_info = None
    if destination_info is not None and stat.S_ISDIR(destination_info.st_mode):
        raise StorageFailure("storage_error")
    try:
        os.replace(source, destination)
    except OSError as exc:
        winerror = getattr(exc, "winerror", None)
        if exc.errno == errno.EXDEV or winerror == 17:
            raise StorageFailure("cross_device") from exc
        raise StorageFailure("storage_error") from exc


def remove_tree_nofollow(path: Path, ancestor: Path) -> None:
    """Delete one temporary tree under ancestor. Refuse reparse points instead of following them."""

    try:
        relative = path.relative_to(ancestor)
    except ValueError as exc:
        raise StorageFailure("path_escape") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise StorageFailure("path_escape")
    current = ancestor
    if is_reparse_point(current):
        raise StorageFailure("reparse_point")
    for part in relative.parts:
        current = current / part
        if is_reparse_point(current):
            raise StorageFailure("reparse_point")
    _reject_reparse_descendants(path)
    _remove(path)


def _reject_reparse_descendants(path: Path) -> None:
    if is_reparse_point(path):
        raise StorageFailure("reparse_point")
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        return
    for child in path.iterdir():
        _reject_reparse_descendants(child)


def _remove(path: Path) -> None:
    if is_reparse_point(path):
        raise StorageFailure("reparse_point")
    info = path.lstat()
    if stat.S_ISDIR(info.st_mode):
        for child in path.iterdir():
            _remove(child)
        path.rmdir()
        return
    if stat.S_ISREG(info.st_mode):
        path.unlink()
        return
    raise StorageFailure("unexpected_file_type")
