"""Read-only inspection of one publication identity.

The inspection does not create directories, locks, or pointers, and it does not
open DuckDB. A missing or invalid artifact is a reported state. A filesystem
failure stops the inspection and is not labeled as corruption.

Bronze, the pointer, Silver, and a requested snapshot are separate reads. The
pointer file is captured once. This is not a global transactional snapshot.
"""

import errno
import hashlib
import stat
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from cfi.application.published_read import ReadFailure, read_bounded
from cfi.domain.bronze import BronzeManifest
from cfi.domain.generation import GenerationManifest
from cfi.domain.gold import (
    GoldGeneration,
    GoldPointer,
    GoldSummary,
    gold_storage_key_for,
    pointer_key_for,
)
from cfi.domain.identifiers import validate_identifier
from cfi.domain.silver import storage_key_for
from cfi.infrastructure.storage import (
    StorageFailure,
    assert_tree_has_no_reparse,
    hash_file,
    is_reparse_point,
)

_JSON_MAX_BYTES = 1024 * 1024
_BLOB_MAX_BYTES = 64 * 1024 * 1024
_MAX_ABSOLUTE_PATH = 240
_HALT = frozenset({"path_escape", "path_limit", "reparse_point", "storage_error"})


class DiagnoseFailure(Exception):
    def __init__(self, code: str, exit_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


@dataclass(frozen=True, slots=True)
class _PointerView:
    state: str
    pointer_sha256: str | None = None
    published_input_sha256: str | None = None
    published_storage_key: str | None = None


def diagnose(
    data_root: Path,
    source_id: str,
    dataset_version: str,
    input_sha256: str | None,
) -> dict[str, object]:
    """Inspect one identity. Return the report or raise DiagnoseFailure."""

    source_id = _identifier(source_id)
    dataset_version = _identifier(dataset_version)
    checksum = None if input_sha256 is None else _checksum(input_sha256)
    _guard(data_root, data_root)
    bronze_state = _bronze(data_root, source_id, dataset_version)
    pointer = _pointer(data_root, source_id, dataset_version)
    report: dict[str, object] = {
        "bronze_state": bronze_state,
        "dataset_version": dataset_version,
        "pointer_state": pointer.state,
        "source_id": source_id,
        "status": "reported",
    }
    if pointer.pointer_sha256 is not None:
        report["pointer_sha256"] = pointer.pointer_sha256
    if pointer.state == "intact":
        report["published_input_sha256"] = pointer.published_input_sha256
        report["published_storage_key"] = pointer.published_storage_key
    if checksum is None:
        return report
    report["input_sha256"] = checksum
    silver_state, silver_key = _silver(data_root, source_id, dataset_version, checksum)
    snapshot_state, snapshot_key = _requested_snapshot(
        data_root, source_id, dataset_version, checksum
    )
    report["silver_state"] = silver_state
    report["snapshot_state"] = snapshot_state
    if silver_key is not None:
        report["silver_storage_key"] = silver_key
    if snapshot_key is not None:
        report["snapshot_storage_key"] = snapshot_key
    if pointer.state == "intact" and snapshot_state == "intact":
        report["same_generation"] = pointer.published_storage_key == snapshot_key
    return report


def _identifier(value: str) -> str:
    try:
        return validate_identifier(value)
    except ValueError as exc:
        raise DiagnoseFailure("invalid_identifier", 2) from exc


def _checksum(value: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise DiagnoseFailure("invalid_sha256", 2)
    return value


def _guard(root: Path, path: Path) -> None:
    absolute = path if path.is_absolute() else path.absolute()
    if len(str(absolute)) > _MAX_ABSOLUTE_PATH:
        raise DiagnoseFailure("path_limit", 4)
    try:
        assert_tree_has_no_reparse(root, path)
    except StorageFailure as exc:
        code = exc.code if exc.code in _HALT else "storage_error"
        raise DiagnoseFailure(code, 4) from exc
    except OSError as exc:
        raise DiagnoseFailure("storage_error", 4) from exc


def _kind(path: Path) -> str:
    if is_reparse_point(path):
        raise DiagnoseFailure("reparse_point", 4)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return "missing"
    except OSError as exc:
        raise DiagnoseFailure("storage_error", 4) from exc
    if stat.S_ISDIR(info.st_mode):
        return "dir"
    if stat.S_ISREG(info.st_mode):
        return "file"
    return "other"


def _read(path: Path) -> bytes | None:
    try:
        return read_bounded(path, _JSON_MAX_BYTES, missing_code="absent", missing_status=404)
    except ReadFailure as exc:
        if exc.code == "absent":
            return None
        code = exc.code if exc.code in _HALT else "storage_error"
        raise DiagnoseFailure(code, 4) from exc


def _hash(path: Path, max_bytes: int) -> tuple[str, int] | None:
    try:
        return hash_file(path, max_bytes)
    except StorageFailure as exc:
        raise DiagnoseFailure("storage_error", 4) from exc
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return None
        raise DiagnoseFailure("storage_error", 4) from exc


def _bronze(root: Path, source_id: str, dataset_version: str) -> str:
    final = root / "bronze" / "sources" / source_id / dataset_version
    _guard(root, final)
    if _kind(final) == "missing":
        return "absent"
    if _kind(final) != "dir":
        return "incomplete"
    original = final / "original"
    manifest_path = final / "manifest.json"
    _guard(root, original)
    _guard(root, manifest_path)
    if _kind(original) != "file" or _kind(manifest_path) != "file":
        return "incomplete"
    raw = _read(manifest_path)
    if raw is None:
        return "incomplete"
    try:
        manifest = BronzeManifest.model_validate_json(raw)
    except ValidationError:
        return "incomplete"
    hashed = _hash(original, _BLOB_MAX_BYTES)
    if hashed is None:
        return "incomplete"
    digest, size = hashed
    if (
        manifest.source_id != source_id
        or manifest.dataset_version != dataset_version
        or manifest.input_sha256 != digest
        or manifest.input_size_bytes != size
    ):
        return "mismatch"
    return "intact"


def _pointer(root: Path, source_id: str, dataset_version: str) -> _PointerView:
    current = (
        root / "gold" / "published" / pointer_key_for(source_id, dataset_version) / "current.json"
    )
    _guard(root, current)
    if _kind(current) == "missing":
        return _PointerView("absent")
    if _kind(current) != "file":
        return _PointerView("pointer_invalid")
    raw = _read(current)
    if raw is None:
        return _PointerView("absent")
    captured = hashlib.sha256(raw).hexdigest()
    try:
        pointer = GoldPointer.model_validate_json(raw)
    except ValidationError:
        return _PointerView("pointer_invalid", pointer_sha256=captured)
    if pointer.source_id != source_id or pointer.dataset_version != dataset_version:
        return _PointerView("pointer_invalid", pointer_sha256=captured)
    state, published_sha = _named_snapshot(root, pointer)
    if state == "intact":
        return _PointerView(
            "intact",
            pointer_sha256=captured,
            published_input_sha256=published_sha,
            published_storage_key=pointer.storage_key,
        )
    return _PointerView(state, pointer_sha256=captured)


def _named_snapshot(root: Path, pointer: GoldPointer) -> tuple[str, str | None]:
    """Validate the snapshot named by the already captured pointer."""

    final = root / "gold" / "snapshots" / pointer.storage_key
    _guard(root, final)
    summary_path = final / "summary.json"
    manifest_path = final / "manifest.json"
    if _kind(final) != "dir":
        return "snapshot_absent", None
    _guard(root, summary_path)
    _guard(root, manifest_path)
    if _kind(summary_path) != "file" or _kind(manifest_path) != "file":
        return "snapshot_absent", None
    summary_raw = _read(summary_path)
    manifest_raw = _read(manifest_path)
    if summary_raw is None or manifest_raw is None:
        return "snapshot_absent", None
    try:
        summary = GoldSummary.model_validate_json(summary_raw)
        generation = GoldGeneration.model_validate_json(manifest_raw)
    except ValidationError:
        return "schema_mismatch", None
    hashed = _hash(summary_path, _JSON_MAX_BYTES)
    if hashed is None:
        return "snapshot_absent", None
    digest, size = hashed
    if not _pointer_agrees(pointer, summary, generation, digest, size):
        return "integrity_mismatch", None
    return "intact", summary.input_sha256


def _pointer_agrees(
    pointer: GoldPointer,
    summary: GoldSummary,
    generation: GoldGeneration,
    digest: str,
    size: int,
) -> bool:
    return (
        generation.storage_key == pointer.storage_key
        and generation.summary_sha256 == pointer.summary_sha256
        and digest == pointer.summary_sha256
        and generation.summary_size_bytes == size
        and summary.source_id == pointer.source_id
        and summary.dataset_version == pointer.dataset_version
        and _documents_agree(summary, generation)
    )


def _silver(
    root: Path, source_id: str, dataset_version: str, checksum: str
) -> tuple[str, str | None]:
    key = storage_key_for(source_id, dataset_version, checksum)
    final = root / "silver" / "generations" / key
    _guard(root, final)
    if _kind(final) == "missing":
        return "absent", None
    if _kind(final) != "dir":
        return "incomplete", key
    manifest_path = final / "manifest.json"
    silver_path = final / "silver.parquet"
    quarantine_path = final / "quarantine.parquet"
    for path in (manifest_path, silver_path, quarantine_path):
        _guard(root, path)
    if any(_kind(path) != "file" for path in (manifest_path, silver_path, quarantine_path)):
        return "incomplete", key
    raw = _read(manifest_path)
    if raw is None:
        return "incomplete", key
    try:
        manifest = GenerationManifest.model_validate_json(raw)
    except ValidationError:
        return "incomplete", key
    silver_hash = _hash(silver_path, _BLOB_MAX_BYTES)
    quarantine_hash = _hash(quarantine_path, _BLOB_MAX_BYTES)
    if silver_hash is None or quarantine_hash is None:
        return "incomplete", key
    silver_digest, silver_size = silver_hash
    quarantine_digest, quarantine_size = quarantine_hash
    if (
        manifest.source_id != source_id
        or manifest.dataset_version != dataset_version
        or manifest.input_sha256 != checksum
        or manifest.storage_key != key
        or manifest.silver_sha256 != silver_digest
        or manifest.silver_size_bytes != silver_size
        or manifest.quarantine_sha256 != quarantine_digest
        or manifest.quarantine_size_bytes != quarantine_size
    ):
        return "mismatch", key
    return "intact", key


def _requested_snapshot(
    root: Path, source_id: str, dataset_version: str, checksum: str
) -> tuple[str, str | None]:
    key = gold_storage_key_for(source_id, dataset_version, checksum)
    final = root / "gold" / "snapshots" / key
    _guard(root, final)
    if _kind(final) == "missing":
        return "absent", None
    if _kind(final) != "dir":
        return "incomplete", key
    summary_path = final / "summary.json"
    manifest_path = final / "manifest.json"
    _guard(root, summary_path)
    _guard(root, manifest_path)
    if _kind(summary_path) != "file" or _kind(manifest_path) != "file":
        return "incomplete", key
    summary_raw = _read(summary_path)
    manifest_raw = _read(manifest_path)
    if summary_raw is None or manifest_raw is None:
        return "incomplete", key
    try:
        summary = GoldSummary.model_validate_json(summary_raw)
        generation = GoldGeneration.model_validate_json(manifest_raw)
    except ValidationError:
        return "incomplete", key
    hashed = _hash(summary_path, _JSON_MAX_BYTES)
    if hashed is None:
        return "incomplete", key
    digest, size = hashed
    if not _snapshot_agrees(
        source_id, dataset_version, checksum, key, summary, generation, digest, size
    ):
        return "mismatch", key
    return "intact", key


def _snapshot_agrees(
    source_id: str,
    dataset_version: str,
    checksum: str,
    key: str,
    summary: GoldSummary,
    generation: GoldGeneration,
    digest: str,
    size: int,
) -> bool:
    return (
        generation.storage_key == key
        and generation.summary_sha256 == digest
        and generation.summary_size_bytes == size
        and generation.source_id == source_id
        and generation.dataset_version == dataset_version
        and generation.input_sha256 == checksum
        and summary.source_id == source_id
        and summary.dataset_version == dataset_version
        and summary.input_sha256 == checksum
        and _documents_agree(summary, generation)
    )


def _documents_agree(summary: GoldSummary, generation: GoldGeneration) -> bool:
    return (
        summary.contract_version == generation.contract_version
        and summary.transform_version == generation.transform_version
        and summary.gold_contract_version == generation.gold_contract_version
        and summary.aggregate_version == generation.aggregate_version
        and summary.silver_storage_key == generation.silver_storage_key
        and summary.trip_count == generation.trip_count
        and summary.total_distance_km == generation.total_distance_km
        and summary.average_duration_seconds == generation.average_duration_seconds
        and summary.rejected_count == generation.rejected_count
    )
