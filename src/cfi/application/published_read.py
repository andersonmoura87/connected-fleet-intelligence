"""Read one published Gold pointer and its immutable snapshot.

The reader opens current.json once, then the snapshot that pointer names.
It does not import DuckDB, does not read Bronze or Silver bytes, does not take
a publication lock, and does not create or replace files.
"""

import errno
import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from cfi.domain.gold import (
    GoldGeneration,
    GoldPointer,
    GoldSummary,
    format_decimal,
    pointer_key_for,
)
from cfi.infrastructure.storage import (
    StorageFailure,
    assert_tree_has_no_reparse,
    is_reparse_point,
)

_MAX_ABSOLUTE_PATH = 240
_MAX_BYTES = 1024 * 1024


class ReadFailure(Exception):
    """A read outcome with a stable code and HTTP status. The message is the code."""

    def __init__(self, code: str, status_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class PublishedSummary:
    source_id: str
    dataset_version: str
    input_sha256: str
    storage_key: str
    contract_version: str
    transform_version: str
    gold_contract_version: str
    aggregate_version: str
    trip_count: int
    total_distance_km: str
    average_duration_seconds: str | None
    rejected_count: int


def read_published(data_root: Path, source_id: str, dataset_version: str) -> PublishedSummary:
    """Return the summary named by the pointer that was read, after it checks out."""

    try:
        pointer_key = pointer_key_for(source_id, dataset_version)
    except ValueError as exc:
        raise ReadFailure("invalid_identifier", 422) from exc
    current = data_root / "gold" / "published" / pointer_key / "current.json"
    _guard(data_root, current)
    _require_regular(
        current,
        missing_code="not_published",
        missing_status=404,
        invalid_code="pointer_invalid",
        invalid_status=503,
    )
    raw_pointer = read_bounded(current, _MAX_BYTES)
    try:
        pointer = GoldPointer.model_validate_json(raw_pointer)
    except ValidationError as exc:
        raise ReadFailure("pointer_invalid", 503) from exc
    if pointer.source_id != source_id or pointer.dataset_version != dataset_version:
        raise ReadFailure("pointer_invalid", 503)
    summary_path = data_root / "gold" / "snapshots" / pointer.storage_key / "summary.json"
    manifest_path = summary_path.parent / "manifest.json"
    _guard(data_root, summary_path)
    _guard(data_root, manifest_path)
    for path in (summary_path, manifest_path):
        _require_regular(
            path,
            missing_code="snapshot_absent",
            missing_status=503,
            invalid_code="integrity_mismatch",
            invalid_status=503,
        )
    raw_summary = read_bounded(
        summary_path, _MAX_BYTES, missing_code="snapshot_absent", missing_status=503
    )
    raw_manifest = read_bounded(
        manifest_path, _MAX_BYTES, missing_code="snapshot_absent", missing_status=503
    )
    try:
        summary = GoldSummary.model_validate_json(raw_summary)
    except ValidationError as exc:
        raise ReadFailure("schema_mismatch", 503) from exc
    try:
        generation = GoldGeneration.model_validate_json(raw_manifest)
    except ValidationError as exc:
        raise ReadFailure("schema_mismatch", 503) from exc
    digest = hashlib.sha256(raw_summary).hexdigest()
    if (
        generation.storage_key != pointer.storage_key
        or generation.summary_sha256 != pointer.summary_sha256
        or digest != pointer.summary_sha256
        or generation.summary_size_bytes != len(raw_summary)
        or not _same_document(summary, generation)
        or summary.source_id != source_id
        or summary.dataset_version != dataset_version
    ):
        raise ReadFailure("integrity_mismatch", 503)
    average = summary.average_duration_seconds
    return PublishedSummary(
        source_id=summary.source_id,
        dataset_version=summary.dataset_version,
        input_sha256=summary.input_sha256,
        storage_key=pointer.storage_key,
        contract_version=summary.contract_version,
        transform_version=summary.transform_version,
        gold_contract_version=summary.gold_contract_version,
        aggregate_version=summary.aggregate_version,
        trip_count=summary.trip_count,
        total_distance_km=format_decimal(summary.total_distance_km),
        average_duration_seconds=None if average is None else format_decimal(average),
        rejected_count=summary.rejected_count,
    )


def read_bounded(
    path: Path,
    max_bytes: int,
    *,
    missing_code: str = "not_published",
    missing_status: int = 404,
) -> bytes:
    """Read one regular file without following a final symlink. Do not write."""

    flags = os.O_RDONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise _from_os(exc, missing_code=missing_code, missing_status=missing_status) from exc
    chunks: list[bytes] = []
    size = 0
    try:
        while True:
            block = os.read(fd, _MAX_BYTES)
            if not block:
                break
            size += len(block)
            if size > max_bytes:
                raise ReadFailure("storage_error", 500)
            chunks.append(block)
    except OSError as exc:
        raise ReadFailure("storage_error", 500) from exc
    finally:
        os.close(fd)
    return b"".join(chunks)


def _guard(root: Path, path: Path) -> None:
    absolute = path if path.is_absolute() else path.absolute()
    if len(str(absolute)) > _MAX_ABSOLUTE_PATH:
        raise ReadFailure("path_limit", 500)
    try:
        assert_tree_has_no_reparse(root, path)
    except StorageFailure as exc:
        code = exc.code if exc.code in {"reparse_point", "path_escape"} else "storage_error"
        raise ReadFailure(code, 500) from exc


def _require_regular(
    path: Path,
    *,
    missing_code: str,
    missing_status: int,
    invalid_code: str,
    invalid_status: int,
) -> None:
    if is_reparse_point(path):
        raise ReadFailure("reparse_point", 500)
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise ReadFailure(missing_code, missing_status) from exc
    except OSError as exc:
        raise ReadFailure("storage_error", 500) from exc
    if not stat.S_ISREG(info.st_mode):
        raise ReadFailure(invalid_code, invalid_status)


def _same_document(summary: GoldSummary, generation: GoldGeneration) -> bool:
    return (
        summary.trip_count == generation.trip_count
        and summary.total_distance_km == generation.total_distance_km
        and summary.average_duration_seconds == generation.average_duration_seconds
        and summary.rejected_count == generation.rejected_count
        and summary.source_id == generation.source_id
        and summary.dataset_version == generation.dataset_version
        and summary.input_sha256 == generation.input_sha256
        and summary.contract_version == generation.contract_version
        and summary.transform_version == generation.transform_version
        and summary.gold_contract_version == generation.gold_contract_version
        and summary.aggregate_version == generation.aggregate_version
        and summary.silver_storage_key == generation.silver_storage_key
    )


def _from_os(exc: OSError, *, missing_code: str, missing_status: int) -> ReadFailure:
    if exc.errno == errno.ENOENT:
        return ReadFailure(missing_code, missing_status)
    return ReadFailure("storage_error", 500)
