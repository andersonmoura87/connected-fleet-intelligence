"""Bronze ingestion. Bytes are stored as read. Trip rows are not validated."""

import errno
import hashlib
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from cfi.application.errors import IngestFailure, integrity, lock_unavailable, storage, usage
from cfi.domain.bronze import (
    BronzeManifest,
    IngestAction,
    ObservationKind,
    decide,
)
from cfi.domain.identifiers import validate_identifier
from cfi.infrastructure.lock import LockPathDenied, LockUnavailable, lock_path_for, source_lock
from cfi.infrastructure.storage import (
    StorageFailure,
    assert_tree_has_no_reparse,
    copy_hashed,
    create_directory,
    fsync_directory,
    hash_file,
    is_regular_file,
    is_reparse_point,
    remove_unpublished,
    rename_exclusive,
    write_bytes_fsync,
)

DEFAULT_MAX_INPUT_BYTES = 10 * 1024 * 1024
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
_MANIFEST_MAX_BYTES = 64 * 1024


class Clock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class IngestSettings:
    data_root: Path
    max_input_bytes: int = DEFAULT_MAX_INPUT_BYTES
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS
    clock: Clock | None = None


@dataclass(frozen=True, slots=True)
class IngestRequest:
    input_path: Path
    source_id: str
    dataset_version: str
    run_id: str
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class IngestHooks:
    after_copy: Callable[[], None] | None = None
    before_rename: Callable[[], None] | None = None
    after_rename: Callable[[], None] | None = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: str
    code: str
    source_id: str
    dataset_version: str
    input_sha256: str
    input_size_bytes: int
    run_id: str
    observed_run_id: str
    dry_run: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "dataset_version": self.dataset_version,
            "dry_run": self.dry_run,
            "input_sha256": self.input_sha256,
            "input_size_bytes": self.input_size_bytes,
            "observed_run_id": self.observed_run_id,
            "run_id": self.run_id,
            "source_id": self.source_id,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class _Observation:
    kind: ObservationKind
    manifest: BronzeManifest | None
    sha256: str | None


def ingest(
    settings: IngestSettings,
    request: IngestRequest,
    hooks: IngestHooks | None = None,
) -> IngestResult:
    source_id = _identifier(request.source_id)
    dataset_version = _identifier(request.dataset_version)
    run_id = _identifier(request.run_id)
    if settings.max_input_bytes < 0 or settings.lock_timeout_seconds < 0:
        raise usage("usage")
    input_path = request.input_path
    if not input_path.exists() and not input_path.is_symlink():
        raise usage("input_not_found")
    if not is_regular_file(input_path):
        raise usage("input_not_regular")

    root = settings.data_root
    if is_reparse_point(root):
        raise storage("reparse_point")
    final = _final_dir(root, source_id, dataset_version)
    active_hooks = hooks or IngestHooks()
    if request.dry_run:
        observed_sha, observed_size = _read_input(input_path, settings.max_input_bytes)
        observation = _observe_if_present(
            root, final, source_id, dataset_version, settings.max_input_bytes
        )
        return _finish_decision(
            observation,
            source_id,
            dataset_version,
            run_id,
            observed_sha,
            observed_size,
            dry_run=True,
        )

    marker = lock_path_for(root, source_id, dataset_version)
    try:
        assert_tree_has_no_reparse(root, marker.parent)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    try:
        with source_lock(marker, settings.lock_timeout_seconds):
            observed_sha, observed_size = _read_input(input_path, settings.max_input_bytes)
            observation = _observe(final, source_id, dataset_version, settings.max_input_bytes)
            action = decide(observation.kind, observation.sha256, observed_sha)
            if action is not IngestAction.PUBLISH:
                return _from_action(
                    action,
                    observation,
                    source_id,
                    dataset_version,
                    run_id,
                    observed_sha,
                    observed_size,
                    dry_run=False,
                )
            return _publish(
                settings,
                root,
                final,
                input_path,
                source_id,
                dataset_version,
                run_id,
                observed_sha,
                observed_size,
                active_hooks,
            )
    except LockPathDenied as exc:
        raise storage("storage_error") from exc
    except LockUnavailable as exc:
        raise lock_unavailable() from exc


def _publish(
    settings: IngestSettings,
    root: Path,
    final: Path,
    input_path: Path,
    source_id: str,
    dataset_version: str,
    run_id: str,
    first_sha: str,
    first_size: int,
    hooks: IngestHooks,
) -> IngestResult:
    incoming_root = root / "bronze" / ".incoming"
    temp = incoming_root / f"{run_id}-{secrets.token_hex(8)}"
    published = False
    result: IngestResult | None = None
    primary: Exception | None = None
    try:
        assert_tree_has_no_reparse(root, final.parent)
        create_directory(temp)
        original = temp / "original"
        copied_sha, copied_size = copy_hashed(input_path, original, settings.max_input_bytes)
        if copied_sha != first_sha or copied_size != first_size:
            raise integrity("input_unstable")
        durable_sha, durable_size = hash_file(original, settings.max_input_bytes)
        if (durable_sha, durable_size) != (copied_sha, copied_size):
            raise storage("storage_error")
        if hooks.after_copy is not None:
            hooks.after_copy()
        second_sha, second_size = _read_input(input_path, settings.max_input_bytes)
        if (second_sha, second_size) != (copied_sha, copied_size):
            raise integrity("input_unstable")
        manifest = _manifest(
            settings,
            source_id,
            dataset_version,
            run_id,
            copied_sha,
            copied_size,
        )
        write_bytes_fsync(temp / "manifest.json", _manifest_bytes(manifest))
        if hooks.before_rename is not None:
            hooks.before_rename()
        create_directory(final.parent, exist_ok=True)
        assert_tree_has_no_reparse(root, final)
        try:
            rename_exclusive(temp, final)
        except StorageFailure:
            raise
        except FileExistsError:
            raced = _observe(final, source_id, dataset_version, settings.max_input_bytes)
            action = decide(raced.kind, raced.sha256, copied_sha)
            if action is IngestAction.PUBLISH:
                raise integrity("integrity_incomplete") from None
            result = _from_action(
                action,
                raced,
                source_id,
                dataset_version,
                run_id,
                copied_sha,
                copied_size,
                dry_run=False,
            )
        except OSError as exc:
            winerror = getattr(exc, "winerror", None)
            if exc.errno == errno.EXDEV or winerror == 17:
                raise storage("cross_device") from exc
            raise storage("storage_error") from exc
        else:
            published = True
            if hooks.after_rename is not None:
                hooks.after_rename()
            sealed = _observe(final, source_id, dataset_version, settings.max_input_bytes)
            if sealed.kind is not ObservationKind.SEALED or sealed.sha256 != copied_sha:
                raise integrity("integrity_mismatch")
            fsync_directory(final.parent)
            result = IngestResult(
                status="published",
                code="published",
                source_id=source_id,
                dataset_version=dataset_version,
                input_sha256=copied_sha,
                input_size_bytes=copied_size,
                run_id=run_id,
                observed_run_id=run_id,
                dry_run=False,
            )
    except StorageFailure as exc:
        primary = _failure_from_storage(exc)
    except Exception as exc:
        primary = exc
    if not published:
        primary = remove_unpublished(temp, incoming_root, primary, decided=result is not None)
    if isinstance(primary, StorageFailure):
        primary = _failure_from_storage(primary)
    if primary is not None:
        raise primary
    if result is None:
        raise RuntimeError("ingest publish produced no result")
    return result


def _finish_decision(
    observation: _Observation,
    source_id: str,
    dataset_version: str,
    run_id: str,
    incoming_sha: str,
    incoming_size: int,
    *,
    dry_run: bool,
) -> IngestResult:
    action = decide(observation.kind, observation.sha256, incoming_sha)
    if action is IngestAction.PUBLISH:
        return IngestResult(
            status="published",
            code="published",
            source_id=source_id,
            dataset_version=dataset_version,
            input_sha256=incoming_sha,
            input_size_bytes=incoming_size,
            run_id=run_id,
            observed_run_id=run_id,
            dry_run=dry_run,
        )
    return _from_action(
        action,
        observation,
        source_id,
        dataset_version,
        run_id,
        incoming_sha,
        incoming_size,
        dry_run=dry_run,
    )


def _from_action(
    action: IngestAction,
    observation: _Observation,
    source_id: str,
    dataset_version: str,
    run_id: str,
    incoming_sha: str,
    incoming_size: int,
    *,
    dry_run: bool,
) -> IngestResult:
    if action is IngestAction.NOOP:
        manifest = observation.manifest
        if manifest is None:
            raise integrity("integrity_incomplete")
        return IngestResult(
            status="noop",
            code="noop",
            source_id=source_id,
            dataset_version=dataset_version,
            input_sha256=manifest.input_sha256,
            input_size_bytes=manifest.input_size_bytes,
            run_id=manifest.run_id,
            observed_run_id=run_id,
            dry_run=dry_run,
        )
    if action is IngestAction.CONFLICT:
        raise integrity("identity_conflict")
    if action is IngestAction.INCOMPLETE:
        raise integrity("integrity_incomplete")
    raise integrity("integrity_mismatch")


def _observe_if_present(
    root: Path,
    final: Path,
    source_id: str,
    dataset_version: str,
    max_bytes: int,
) -> _Observation:
    if not root.exists():
        return _Observation(ObservationKind.ABSENT, None, None)
    if is_reparse_point(root):
        raise storage("reparse_point")
    try:
        assert_tree_has_no_reparse(root, final)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    except FileNotFoundError:
        return _Observation(ObservationKind.ABSENT, None, None)
    if not final.exists() and not final.is_symlink():
        return _Observation(ObservationKind.ABSENT, None, None)
    return _observe(final, source_id, dataset_version, max_bytes)


def _observe(final: Path, source_id: str, dataset_version: str, max_bytes: int) -> _Observation:
    if is_reparse_point(final):
        raise storage("reparse_point")
    if not final.exists():
        return _Observation(ObservationKind.ABSENT, None, None)
    original = final / "original"
    manifest_path = final / "manifest.json"
    if is_reparse_point(original) or is_reparse_point(manifest_path):
        raise storage("reparse_point")
    if not original.is_file() or not manifest_path.is_file():
        return _Observation(ObservationKind.INCOMPLETE, None, None)
    payload = manifest_path.read_bytes()
    if len(payload) > _MANIFEST_MAX_BYTES:
        return _Observation(ObservationKind.INCOMPLETE, None, None)
    try:
        manifest = BronzeManifest.model_validate_json(payload)
    except ValidationError:
        return _Observation(ObservationKind.INCOMPLETE, None, None)
    if manifest.source_id != source_id or manifest.dataset_version != dataset_version:
        return _Observation(ObservationKind.MISMATCH, manifest, None)
    try:
        actual_sha, actual_size = hash_file(original, max_bytes)
    except StorageFailure as exc:
        if exc.code == "input_too_large":
            return _Observation(ObservationKind.MISMATCH, manifest, None)
        raise
    except OSError:
        return _Observation(ObservationKind.INCOMPLETE, manifest, None)
    if actual_sha != manifest.input_sha256 or actual_size != manifest.input_size_bytes:
        return _Observation(ObservationKind.MISMATCH, manifest, actual_sha)
    return _Observation(ObservationKind.SEALED, manifest, actual_sha)


def _read_input(path: Path, max_bytes: int) -> tuple[str, int]:
    try:
        return hash_file(path, max_bytes)
    except StorageFailure as exc:
        if exc.code == "input_too_large":
            raise usage("input_too_large") from exc
        raise usage("input_unreadable") from exc
    except OSError as exc:
        raise usage("input_unreadable") from exc


def _manifest(
    settings: IngestSettings,
    source_id: str,
    dataset_version: str,
    run_id: str,
    digest: str,
    size: int,
) -> BronzeManifest:
    clock = settings.clock or Clock()
    created_at = _format_utc(clock.now())
    return BronzeManifest(
        manifest_version="1",
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=digest,
        input_size_bytes=size,
        run_id=run_id,
        created_at=created_at,
        status="published",
    )


def _manifest_bytes(manifest: BronzeManifest) -> bytes:
    payload = manifest.model_dump(mode="json")
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise usage("usage")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _final_dir(root: Path, source_id: str, dataset_version: str) -> Path:
    path = root / "bronze" / "sources" / source_id / dataset_version
    try:
        assert_tree_has_no_reparse(root, path)
    except StorageFailure as exc:
        raise storage(_storage_code(exc.code)) from exc
    return path


def _failure_from_storage(exc: StorageFailure) -> IngestFailure:
    try:
        if exc.code == "input_too_large":
            raise usage("input_too_large") from exc
        if exc.code in {"reparse_point", "path_escape"}:
            raise storage(exc.code) from exc
        raise storage("storage_error") from exc
    except IngestFailure as mapped:
        return mapped


def _storage_code(code: str) -> str:
    if code in {"reparse_point", "path_escape"}:
        return code
    return "storage_error"


def _identifier(value: str) -> str:
    try:
        return validate_identifier(value)
    except ValueError as exc:
        raise usage("invalid_identifier") from exc


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
