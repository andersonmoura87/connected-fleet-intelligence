"""Bronze to Silver generation. Bronze bytes are not rewritten."""

import errno
import hashlib
import json
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from cfi.application.errors import integrity, lock_unavailable, storage, usage
from cfi.domain.bronze import BronzeManifest
from cfi.domain.generation import (
    QUARANTINE_COLUMNS,
    SILVER_COLUMNS,
    GenerationManifest,
    column_specs,
)
from cfi.domain.identifiers import validate_identifier
from cfi.domain.silver import TRANSFORM_VERSION, LogicalBatch, reduce_bronze, storage_key_for
from cfi.domain.trip import CONTRACT_VERSION
from cfi.infrastructure.lock import LockUnavailable, source_lock
from cfi.infrastructure.parquet import (
    ParquetRoundtripError,
    expected_quarantine_row,
    expected_silver_row,
    read_generation_parquet,
    write_generation_parquet,
    writer_version,
)
from cfi.infrastructure.storage import (
    StorageFailure,
    assert_tree_has_no_reparse,
    fsync_directory,
    hash_file,
    is_reparse_point,
    remove_tree_nofollow,
    rename_exclusive,
    write_bytes_fsync,
)

DEFAULT_MAX_INPUT_BYTES = 10 * 1024 * 1024
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
_MAX_ABSOLUTE_PATH = 240
_PARQUET_MAX_BYTES = 64 * 1024 * 1024
_MANIFEST_MAX_BYTES = 1024 * 1024
_TOKEN_CHARS = 32
_KEY_CHARS = 64


class Clock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class TransformSettings:
    data_root: Path
    max_input_bytes: int = DEFAULT_MAX_INPUT_BYTES
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS
    clock: Clock | None = None


@dataclass(frozen=True, slots=True)
class TransformRequest:
    source_id: str
    dataset_version: str
    run_id: str
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class TransformHooks:
    before_rename: Callable[[], None] | None = None
    after_rename: Callable[[], None] | None = None
    alter_batch: Callable[[LogicalBatch], LogicalBatch] | None = None


@dataclass(frozen=True, slots=True)
class TransformResult:
    status: str
    code: str
    source_id: str
    dataset_version: str
    input_sha256: str
    contract_version: str
    transform_version: str
    storage_key: str
    bronze_run_id: str
    run_id: str
    observed_run_id: str
    dry_run: bool
    input_line_count: int
    valid_record_count: int
    invalid_record_count: int
    silver_record_count: int
    identical_duplicate_extra_count: int
    conflict_rejection_count: int
    quarantine_row_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "bronze_run_id": self.bronze_run_id,
            "code": self.code,
            "conflict_rejection_count": self.conflict_rejection_count,
            "contract_version": self.contract_version,
            "dataset_version": self.dataset_version,
            "dry_run": self.dry_run,
            "identical_duplicate_extra_count": self.identical_duplicate_extra_count,
            "input_line_count": self.input_line_count,
            "input_sha256": self.input_sha256,
            "invalid_record_count": self.invalid_record_count,
            "observed_run_id": self.observed_run_id,
            "quarantine_row_count": self.quarantine_row_count,
            "run_id": self.run_id,
            "silver_record_count": self.silver_record_count,
            "source_id": self.source_id,
            "status": self.status,
            "storage_key": self.storage_key,
            "transform_version": self.transform_version,
            "valid_record_count": self.valid_record_count,
        }


def transform(
    settings: TransformSettings,
    request: TransformRequest,
    hooks: TransformHooks | None = None,
) -> TransformResult:
    source_id = _identifier(request.source_id)
    dataset_version = _identifier(request.dataset_version)
    run_id = _identifier(request.run_id)
    if settings.max_input_bytes < 0 or settings.lock_timeout_seconds < 0:
        raise usage("usage")
    root = settings.data_root
    _reject_long_paths(root)
    manifest, payload = _load_bronze(root, source_id, dataset_version, settings.max_input_bytes)
    active = hooks or TransformHooks()
    key = storage_key_for(source_id, dataset_version, manifest.input_sha256)
    final = _generation_dir(root, key)
    if request.dry_run:
        return _finish_existing_or_plan(
            root,
            final,
            manifest,
            _batch(payload, active),
            source_id,
            dataset_version,
            run_id,
            key,
            dry_run=True,
        )

    marker = root / "silver" / "locks" / f"{key}.lock"
    try:
        assert_tree_has_no_reparse(root, marker.parent)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    try:
        with source_lock(marker, settings.lock_timeout_seconds):
            payload = _read_original(root, source_id, dataset_version)
            _require_payload(payload, manifest)
            current = _batch(payload, active)
            return _finish_existing_or_plan(
                root,
                final,
                manifest,
                current,
                source_id,
                dataset_version,
                run_id,
                key,
                dry_run=False,
                settings=settings,
                hooks=active,
            )
    except LockUnavailable as exc:
        raise lock_unavailable() from exc


def _finish_existing_or_plan(
    root: Path,
    final: Path,
    bronze: BronzeManifest,
    batch: LogicalBatch,
    source_id: str,
    dataset_version: str,
    run_id: str,
    key: str,
    *,
    dry_run: bool,
    settings: TransformSettings | None = None,
    hooks: TransformHooks | None = None,
) -> TransformResult:
    kind, sealed = _inspect_generation(final, source_id, dataset_version, bronze.input_sha256, key)
    if kind == "absent":
        if dry_run:
            return _result(
                batch, bronze, source_id, dataset_version, run_id, run_id, key, dry_run=True
            )
        if settings is None or hooks is None:
            raise usage("usage")
        return _publish(
            root, final, bronze, batch, source_id, dataset_version, run_id, key, settings, hooks
        )
    if kind == "incomplete":
        raise integrity("integrity_incomplete")
    if kind == "mismatch":
        raise integrity("integrity_mismatch")
    if sealed is None:
        raise integrity("integrity_incomplete")
    if sealed.bronze_run_id != bronze.run_id:
        raise integrity("integrity_mismatch")
    try:
        actual_silver, actual_quarantine = read_generation_parquet(
            final / "silver.parquet",
            final / "quarantine.parquet",
        )
    except ParquetRoundtripError as exc:
        raise integrity("schema_mismatch") from exc
    if (
        len(actual_silver) != sealed.silver_record_count
        or len(actual_quarantine) != sealed.quarantine_row_count
    ):
        raise integrity("integrity_mismatch")
    if not _same_rows(batch, actual_silver, actual_quarantine, bronze):
        raise integrity("transform_conflict")
    return _result(
        batch,
        bronze,
        source_id,
        dataset_version,
        sealed.run_id,
        run_id,
        key,
        dry_run=dry_run,
        status="noop",
    )


def _publish(
    root: Path,
    final: Path,
    bronze: BronzeManifest,
    batch: LogicalBatch,
    source_id: str,
    dataset_version: str,
    run_id: str,
    key: str,
    settings: TransformSettings,
    hooks: TransformHooks,
) -> TransformResult:
    incoming = root / "silver" / ".incoming"
    temp = incoming / secrets.token_hex(16)
    published = False
    try:
        assert_tree_has_no_reparse(root, final.parent)
        temp.mkdir(parents=True)
        silver_path = temp / "silver.parquet"
        quarantine_path = temp / "quarantine.parquet"
        write_generation_parquet(
            silver_path,
            quarantine_path,
            batch.silver,
            batch.quarantine,
            source_id=source_id,
            dataset_version=dataset_version,
            input_sha256=bronze.input_sha256,
            contract_version=CONTRACT_VERSION,
            transform_version=TRANSFORM_VERSION,
            bronze_run_id=bronze.run_id,
        )
        _fsync_file(silver_path)
        _fsync_file(quarantine_path)
        try:
            read_silver, read_quarantine = read_generation_parquet(silver_path, quarantine_path)
        except ParquetRoundtripError as exc:
            raise storage("parquet_roundtrip") from exc
        if not _same_rows(batch, read_silver, read_quarantine, bronze):
            raise storage("parquet_roundtrip")
        _require_same_bronze(root, source_id, dataset_version, bronze, settings.max_input_bytes)
        silver_sha, silver_size = hash_file(silver_path, _PARQUET_MAX_BYTES)
        quarantine_sha, quarantine_size = hash_file(quarantine_path, _PARQUET_MAX_BYTES)
        manifest = _manifest(
            settings,
            bronze,
            batch,
            run_id,
            key,
            silver_sha,
            silver_size,
            quarantine_sha,
            quarantine_size,
        )
        write_bytes_fsync(temp / "manifest.json", _manifest_bytes(manifest))
        if hooks.before_rename is not None:
            hooks.before_rename()
        _require_same_bronze(root, source_id, dataset_version, bronze, settings.max_input_bytes)
        final.parent.mkdir(parents=True, exist_ok=True)
        assert_tree_has_no_reparse(root, final)
        try:
            rename_exclusive(temp, final)
        except FileExistsError:
            return _finish_existing_or_plan(
                root,
                final,
                bronze,
                batch,
                source_id,
                dataset_version,
                run_id,
                key,
                dry_run=False,
            )
        except StorageFailure:
            raise
        except OSError as exc:
            winerror = getattr(exc, "winerror", None)
            if exc.errno == errno.EXDEV or winerror == 17:
                raise storage("cross_device") from exc
            raise storage("storage_error") from exc
        published = True
        if hooks.after_rename is not None:
            hooks.after_rename()
        confirmed = _inspect_generation(final, source_id, dataset_version, bronze.input_sha256, key)
        if confirmed[0] != "sealed" or confirmed[1] is None:
            raise integrity("integrity_mismatch")
        if (
            confirmed[1].silver_sha256 != silver_sha
            or confirmed[1].quarantine_sha256 != quarantine_sha
        ):
            raise integrity("integrity_mismatch")
        fsync_directory(final.parent)
        return _result(
            batch, bronze, source_id, dataset_version, run_id, run_id, key, dry_run=False
        )
    except StorageFailure as exc:
        if exc.code in {"reparse_point", "path_escape", "rename_noreplace_unavailable"}:
            raise storage(exc.code) from exc
        raise storage("storage_error") from exc
    finally:
        if not published and temp.exists():
            remove_tree_nofollow(temp, incoming)


def _reject_long_paths(root: Path) -> None:
    key = "f" * _KEY_CHARS
    token = "f" * _TOKEN_CHARS
    candidates = (
        root / "silver" / "generations" / key / "quarantine.parquet",
        root / "silver" / "locks" / f"{key}.lock",
        root / "silver" / ".incoming" / token / "quarantine.parquet",
    )
    for candidate in candidates:
        absolute = candidate if candidate.is_absolute() else candidate.absolute()
        if len(str(absolute)) > _MAX_ABSOLUTE_PATH:
            raise storage("path_limit")


def _load_bronze(
    root: Path, source_id: str, dataset_version: str, max_bytes: int
) -> tuple[BronzeManifest, bytes]:
    final = root / "bronze" / "sources" / source_id / dataset_version
    try:
        assert_tree_has_no_reparse(root, final)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    if is_reparse_point(final):
        raise storage("reparse_point")
    if not final.exists():
        raise integrity("bronze_absent")
    original = final / "original"
    manifest_path = final / "manifest.json"
    if is_reparse_point(original) or is_reparse_point(manifest_path):
        raise storage("reparse_point")
    if not original.is_file() or not manifest_path.is_file():
        raise integrity("integrity_incomplete")
    raw_manifest = manifest_path.read_bytes()
    if len(raw_manifest) > _MANIFEST_MAX_BYTES:
        raise integrity("integrity_incomplete")
    try:
        manifest = BronzeManifest.model_validate_json(raw_manifest)
    except ValidationError as exc:
        raise integrity("integrity_incomplete") from exc
    if manifest.source_id != source_id or manifest.dataset_version != dataset_version:
        raise integrity("integrity_mismatch")
    try:
        digest, size = hash_file(original, max_bytes)
    except StorageFailure as exc:
        if exc.code == "input_too_large":
            raise integrity("integrity_mismatch") from exc
        raise storage("storage_error") from exc
    if digest != manifest.input_sha256 or size != manifest.input_size_bytes:
        raise integrity("integrity_mismatch")
    payload = original.read_bytes()
    if hashlib.sha256(payload).hexdigest() != manifest.input_sha256 or len(payload) != size:
        raise integrity("bronze_unstable")
    return manifest, payload


def _require_same_bronze(
    root: Path,
    source_id: str,
    dataset_version: str,
    manifest: BronzeManifest,
    max_bytes: int,
) -> None:
    original = root / "bronze" / "sources" / source_id / dataset_version / "original"
    if is_reparse_point(original) or not original.is_file():
        raise integrity("bronze_unstable")
    try:
        digest, size = hash_file(original, max_bytes)
    except StorageFailure as exc:
        raise integrity("bronze_unstable") from exc
    if digest != manifest.input_sha256 or size != manifest.input_size_bytes:
        raise integrity("bronze_unstable")
    _require_payload(original.read_bytes(), manifest)


def _require_payload(payload: bytes, manifest: BronzeManifest) -> None:
    digest = hashlib.sha256(payload).hexdigest()
    if digest != manifest.input_sha256 or len(payload) != manifest.input_size_bytes:
        raise integrity("bronze_unstable")


def _read_original(root: Path, source_id: str, dataset_version: str) -> bytes:
    return (root / "bronze" / "sources" / source_id / dataset_version / "original").read_bytes()


def _generation_dir(root: Path, key: str) -> Path:
    path = root / "silver" / "generations" / key
    try:
        assert_tree_has_no_reparse(root, path)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    return path


def _inspect_generation(
    final: Path,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    key: str,
) -> tuple[str, GenerationManifest | None]:
    if is_reparse_point(final):
        raise storage("reparse_point")
    if not final.exists():
        return "absent", None
    silver_path = final / "silver.parquet"
    quarantine_path = final / "quarantine.parquet"
    manifest_path = final / "manifest.json"
    paths = (silver_path, quarantine_path, manifest_path)
    if any(is_reparse_point(path) for path in paths):
        raise storage("reparse_point")
    if not all(path.is_file() for path in paths):
        return "incomplete", None
    raw = manifest_path.read_bytes()
    if len(raw) > _MANIFEST_MAX_BYTES:
        return "incomplete", None
    try:
        manifest = GenerationManifest.model_validate_json(raw)
    except ValidationError:
        return "incomplete", None
    if (
        manifest.source_id != source_id
        or manifest.dataset_version != dataset_version
        or manifest.input_sha256 != input_sha256
        or manifest.storage_key != key
        or manifest.contract_version != CONTRACT_VERSION
        or manifest.transform_version != TRANSFORM_VERSION
    ):
        return "mismatch", manifest
    try:
        silver_sha, silver_size = hash_file(silver_path, _PARQUET_MAX_BYTES)
        quarantine_sha, quarantine_size = hash_file(quarantine_path, _PARQUET_MAX_BYTES)
    except StorageFailure:
        return "incomplete", manifest
    if (
        silver_sha != manifest.silver_sha256
        or silver_size != manifest.silver_size_bytes
        or quarantine_sha != manifest.quarantine_sha256
        or quarantine_size != manifest.quarantine_size_bytes
    ):
        return "mismatch", manifest
    return "sealed", manifest


def _batch(payload: bytes, hooks: TransformHooks) -> LogicalBatch:
    batch = reduce_bronze(payload)
    if hooks.alter_batch is None:
        return batch
    altered = hooks.alter_batch(batch)
    if not altered.counts.reconciled():
        raise integrity("integrity_mismatch")
    return altered


def _same_rows(
    batch: LogicalBatch,
    silver_rows: tuple[tuple[object, ...], ...],
    quarantine_rows: tuple[tuple[object, ...], ...],
    bronze: BronzeManifest,
) -> bool:
    expected_silver = tuple(
        expected_silver_row(
            row,
            source_id=bronze.source_id,
            dataset_version=bronze.dataset_version,
            input_sha256=bronze.input_sha256,
            contract_version=CONTRACT_VERSION,
            transform_version=TRANSFORM_VERSION,
            bronze_run_id=bronze.run_id,
        )
        for row in batch.silver
    )
    expected_quarantine = tuple(
        expected_quarantine_row(
            row,
            source_id=bronze.source_id,
            dataset_version=bronze.dataset_version,
            input_sha256=bronze.input_sha256,
            contract_version=CONTRACT_VERSION,
            transform_version=TRANSFORM_VERSION,
            bronze_run_id=bronze.run_id,
        )
        for row in batch.quarantine
    )
    return silver_rows == expected_silver and quarantine_rows == expected_quarantine


def _manifest(
    settings: TransformSettings,
    bronze: BronzeManifest,
    batch: LogicalBatch,
    run_id: str,
    key: str,
    silver_sha: str,
    silver_size: int,
    quarantine_sha: str,
    quarantine_size: int,
) -> GenerationManifest:
    clock = settings.clock or Clock()
    created_at = clock.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    counts = batch.counts
    return GenerationManifest(
        manifest_version="1",
        source_id=bronze.source_id,
        dataset_version=bronze.dataset_version,
        input_sha256=bronze.input_sha256,
        contract_name="trip_record",
        contract_version=CONTRACT_VERSION,
        transform_version=TRANSFORM_VERSION,
        storage_key=key,
        bronze_run_id=bronze.run_id,
        run_id=run_id,
        created_at=created_at,
        status="published",
        silver_sha256=silver_sha,
        silver_size_bytes=silver_size,
        quarantine_sha256=quarantine_sha,
        quarantine_size_bytes=quarantine_size,
        silver_schema=column_specs(SILVER_COLUMNS),
        quarantine_schema=column_specs(QUARANTINE_COLUMNS),
        parquet_writer="duckdb",
        parquet_writer_version=writer_version(),
        parquet_compression="zstd",
        input_line_count=counts.input_line_count,
        valid_record_count=counts.valid_record_count,
        invalid_record_count=counts.invalid_record_count,
        silver_record_count=counts.silver_record_count,
        identical_duplicate_extra_count=counts.identical_duplicate_extra_count,
        conflict_rejection_count=counts.conflict_rejection_count,
        quarantine_row_count=counts.quarantine_row_count,
    )


def _manifest_bytes(manifest: GenerationManifest) -> bytes:
    payload = manifest.model_dump(mode="json")
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _result(
    batch: LogicalBatch,
    bronze: BronzeManifest,
    source_id: str,
    dataset_version: str,
    run_id: str,
    observed_run_id: str,
    key: str,
    *,
    dry_run: bool,
    status: str = "published",
) -> TransformResult:
    counts = batch.counts
    return TransformResult(
        status=status,
        code=status,
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=bronze.input_sha256,
        contract_version=CONTRACT_VERSION,
        transform_version=TRANSFORM_VERSION,
        storage_key=key,
        bronze_run_id=bronze.run_id,
        run_id=run_id,
        observed_run_id=observed_run_id,
        dry_run=dry_run,
        input_line_count=counts.input_line_count,
        valid_record_count=counts.valid_record_count,
        invalid_record_count=counts.invalid_record_count,
        silver_record_count=counts.silver_record_count,
        identical_duplicate_extra_count=counts.identical_duplicate_extra_count,
        conflict_rejection_count=counts.conflict_rejection_count,
        quarantine_row_count=counts.quarantine_row_count,
    )


def _fsync_file(path: Path) -> None:
    flags = os.O_RDWR
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _storage_code(code: str) -> str:
    if code in {"reparse_point", "path_escape"}:
        return code
    return "storage_error"


def _identifier(value: str) -> str:
    try:
        return validate_identifier(value)
    except ValueError as exc:
        raise usage("invalid_identifier") from exc
