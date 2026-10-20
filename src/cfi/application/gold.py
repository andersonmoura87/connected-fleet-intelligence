"""Publish one Gold snapshot from an intact Silver generation.

Bronze paths are not opened. The Silver generation is selected by source id,
dataset version, and input checksum.
"""

import errno
import hashlib
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError

from cfi.application.errors import IngestFailure
from cfi.domain.generation import SILVER_COLUMNS, GenerationManifest
from cfi.domain.gold import (
    AGGREGATE_VERSION,
    GOLD_CONTRACT_VERSION,
    AggregationError,
    GoldGeneration,
    GoldMetrics,
    GoldPointer,
    GoldSummary,
    TripMetric,
    aggregate_trips,
    format_decimal,
    gold_storage_key_for,
    pointer_key_for,
)
from cfi.domain.identifiers import validate_identifier
from cfi.domain.silver import TRANSFORM_VERSION, storage_key_for
from cfi.domain.trip import CONTRACT_VERSION
from cfi.infrastructure.lock import LockPathDenied, LockUnavailable, source_lock
from cfi.infrastructure.parquet import (
    ParquetRoundtripError,
    read_generation_parquet,
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
    replace_file,
    write_bytes_fsync,
)

_MAX_ABSOLUTE_PATH = 240
_PARQUET_MAX_BYTES = 64 * 1024 * 1024
_MANIFEST_MAX_BYTES = 1024 * 1024
_COLUMN = {name: index for index, (name, _logical) in enumerate(SILVER_COLUMNS)}


class Clock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class GoldFailure(IngestFailure):
    """A Gold outcome that keeps snapshot and pointer state in the result."""

    def __init__(self, result: "GoldResult") -> None:
        super().__init__(result.code, result.exit_code)
        self.result = result


@dataclass(frozen=True, slots=True)
class GoldSettings:
    data_root: Path
    lock_timeout_seconds: float = 30.0
    clock: Clock | None = None


@dataclass(frozen=True, slots=True)
class GoldRequest:
    source_id: str
    dataset_version: str
    input_sha256: str
    run_id: str
    dry_run: bool = False
    promote: bool = False


@dataclass(frozen=True, slots=True)
class GoldHooks:
    before_snapshot_rename: Callable[[], None] | None = None
    after_snapshot_rename: Callable[[], None] | None = None
    before_pointer_replace: Callable[[], None] | None = None
    after_pointer_replace: Callable[[], None] | None = None


@dataclass(frozen=True, slots=True)
class GoldResult:
    status: str
    code: str
    exit_code: int
    dry_run: bool
    source_id: str | None
    dataset_version: str | None
    input_sha256: str | None
    contract_version: str
    transform_version: str
    gold_contract_version: str
    aggregate_version: str
    silver_storage_key: str | None
    storage_key: str | None
    pointer_key: str | None
    run_id: str | None
    observed_run_id: str | None
    snapshot_state: str
    pointer_state: str
    snapshot_durable: bool
    pointer_replaced: bool
    trip_count: int | None
    total_distance_km: Decimal | None
    average_duration_seconds: Decimal | None
    rejected_count: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "aggregate_version": self.aggregate_version,
            "average_duration_seconds": _json_decimal(self.average_duration_seconds),
            "code": self.code,
            "contract_version": self.contract_version,
            "dataset_version": self.dataset_version,
            "dry_run": self.dry_run,
            "exit_code": self.exit_code,
            "gold_contract_version": self.gold_contract_version,
            "input_sha256": self.input_sha256,
            "observed_run_id": self.observed_run_id,
            "pointer_key": self.pointer_key,
            "pointer_replaced": self.pointer_replaced,
            "pointer_state": self.pointer_state,
            "rejected_count": self.rejected_count,
            "run_id": self.run_id,
            "silver_storage_key": self.silver_storage_key,
            "snapshot_durable": self.snapshot_durable,
            "snapshot_state": self.snapshot_state,
            "source_id": self.source_id,
            "status": self.status,
            "storage_key": self.storage_key,
            "total_distance_km": _json_decimal(self.total_distance_km),
            "transform_version": self.transform_version,
            "trip_count": self.trip_count,
        }


@dataclass(frozen=True, slots=True)
class _Context:
    source_id: str
    dataset_version: str
    input_sha256: str
    run_id: str
    dry_run: bool
    promote: bool
    silver_key: str
    gold_key: str
    pointer_key: str
    summary: GoldSummary
    metrics: GoldMetrics
    bronze_run_id: str


class _Halt(Exception):
    def __init__(self, result: GoldResult) -> None:
        super().__init__(result.code)
        self.result = result


def gold(
    settings: GoldSettings,
    request: GoldRequest,
    hooks: GoldHooks | None = None,
) -> GoldResult:
    try:
        result = _execute(settings, request, hooks or GoldHooks())
    except _Halt as halt:
        result = halt.result
    if result.exit_code != 0:
        raise GoldFailure(result)
    return result


def _execute(settings: GoldSettings, request: GoldRequest, hooks: GoldHooks) -> GoldResult:
    if settings.lock_timeout_seconds < 0:
        return _bare(request, code="usage", exit_code=2, status="rejected")
    try:
        source_id = validate_identifier(request.source_id)
        dataset_version = validate_identifier(request.dataset_version)
        run_id = validate_identifier(request.run_id)
        silver_key = storage_key_for(source_id, dataset_version, request.input_sha256)
        gold_key = gold_storage_key_for(source_id, dataset_version, request.input_sha256)
        pointer_key = pointer_key_for(source_id, dataset_version)
    except ValueError as exc:
        code = "invalid_sha256" if str(exc) == "input_sha256" else "invalid_identifier"
        return _bare(request, code=code, exit_code=2, status="rejected")
    root = settings.data_root
    if _too_long(root, gold_key, pointer_key):
        return _identified(
            request,
            silver_key,
            gold_key,
            pointer_key,
            code="path_limit",
            exit_code=4,
            status="rejected",
            snapshot_state="snapshot_absent",
            pointer_state="pointer_unchanged",
        )
    try:
        view = _load_silver(
            root,
            source_id,
            dataset_version,
            request.input_sha256,
            silver_key,
            dry_run=request.dry_run,
        )
        metrics = aggregate_trips(view.rows, view.manifest.quarantine_row_count)
    except AggregationError as exc:
        return _identified(
            request,
            silver_key,
            gold_key,
            pointer_key,
            code="aggregation_invalid",
            exit_code=3,
            status="rejected",
            snapshot_state="snapshot_absent",
            pointer_state="pointer_unchanged",
            error=str(exc),
        )
    except _Halt as halt:
        return halt.result
    summary = _summary(view.manifest, metrics)
    context = _Context(
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=request.input_sha256,
        run_id=run_id,
        dry_run=request.dry_run,
        promote=request.promote,
        silver_key=silver_key,
        gold_key=gold_key,
        pointer_key=pointer_key,
        summary=summary,
        metrics=metrics,
        bronze_run_id=view.manifest.bronze_run_id,
    )
    if request.dry_run:
        return _finish(settings, context, hooks, publish=False)
    marker = root / "gold" / "locks" / f"{gold_key}.lock"
    try:
        with source_lock(marker, settings.lock_timeout_seconds):
            snapshot = _finish(settings, context, hooks, publish=True)
    except LockPathDenied as exc:
        raise _Halt(_storage_result(context, "storage_error")) from exc
    except LockUnavailable:
        return _snapshot_lock_unavailable(root, context)
    except AggregationError:
        return _identified(
            request,
            silver_key,
            gold_key,
            pointer_key,
            code="aggregation_invalid",
            exit_code=3,
            status="rejected",
            snapshot_state="snapshot_absent",
            pointer_state="pointer_unchanged",
        )
    except _Halt as halt:
        return halt.result
    if snapshot.exit_code != 0 or snapshot.snapshot_state == "snapshot_conflict":
        return snapshot
    pointer_marker = root / "gold" / "locks" / f"pointer-{pointer_key}.lock"
    try:
        with source_lock(pointer_marker, settings.lock_timeout_seconds):
            return _pointer(settings, context, hooks, snapshot)
    except LockPathDenied as exc:
        raise _Halt(
            _copy(
                snapshot,
                status="partial",
                code="storage_error",
                exit_code=4,
                pointer_state="pointer_unchanged",
                pointer_replaced=False,
            )
        ) from exc
    except LockUnavailable:
        return _copy(
            snapshot,
            status="partial",
            code="lock_unavailable",
            exit_code=5,
            pointer_state="pointer_unchanged",
            pointer_replaced=False,
        )
    except _Halt as halt:
        return halt.result


def _snapshot_lock_unavailable(root: Path, context: _Context) -> GoldResult:
    """Report a snapshot that already checks out. Do not claim it is absent."""

    final = root / "gold" / "snapshots" / context.gold_key
    kind, _generation = _observe_snapshot(final, context.summary, context.gold_key)
    if kind == "sealed":
        state = "snapshot_noop"
        durable = True
    elif kind == "conflict":
        state = "snapshot_conflict"
        durable = True
    elif kind in {"incomplete", "mismatch"}:
        state = "snapshot_incomplete"
        durable = False
    else:
        state = "snapshot_absent"
        durable = False
    return _success(
        context,
        status="rejected",
        code="lock_unavailable",
        exit_code=5,
        snapshot_state=state,
        pointer_state="pointer_unchanged",
        durable=durable,
        replaced=False,
    )


def _finish(
    settings: GoldSettings,
    context: _Context,
    hooks: GoldHooks,
    *,
    publish: bool,
) -> GoldResult:
    root = settings.data_root
    final = root / "gold" / "snapshots" / context.gold_key
    if publish:
        view = _load_silver(
            root,
            context.source_id,
            context.dataset_version,
            context.input_sha256,
            context.silver_key,
            dry_run=context.dry_run,
        )
        metrics = aggregate_trips(view.rows, view.manifest.quarantine_row_count)
        summary = _summary(view.manifest, metrics)
        context = _Context(
            source_id=context.source_id,
            dataset_version=context.dataset_version,
            input_sha256=context.input_sha256,
            run_id=context.run_id,
            dry_run=context.dry_run,
            promote=context.promote,
            silver_key=context.silver_key,
            gold_key=context.gold_key,
            pointer_key=context.pointer_key,
            summary=summary,
            metrics=metrics,
            bronze_run_id=view.manifest.bronze_run_id,
        )
    kind, generation = _observe_snapshot(final, context.summary, context.gold_key)
    if kind == "sealed" and generation is not None:
        pointer_state = "pointer_unchanged"
        code = "noop"
        exit_code = 0
        if context.dry_run:
            pointer_state, code, exit_code = _preview_pointer(root, context)
        return _success(
            context,
            status="dry_run" if context.dry_run else "complete",
            code=code,
            exit_code=exit_code,
            snapshot_state="snapshot_noop",
            pointer_state=pointer_state,
            durable=True,
            replaced=False,
            run_id=generation.run_id,
        )
    if kind == "conflict":
        return _success(
            context,
            status="dry_run" if context.dry_run else "conflict",
            code="snapshot_conflict",
            exit_code=3,
            snapshot_state="snapshot_conflict",
            pointer_state="pointer_unchanged",
            durable=True,
            replaced=False,
        )
    if kind in {"incomplete", "mismatch"}:
        return _success(
            context,
            status="dry_run" if context.dry_run else "rejected",
            code="integrity_incomplete" if kind == "incomplete" else "integrity_mismatch",
            exit_code=3,
            snapshot_state="snapshot_incomplete",
            pointer_state="pointer_unchanged",
            durable=False,
            replaced=False,
        )
    if context.dry_run or not publish:
        pointer_state, code, exit_code = _preview_pointer(root, context)
        status = "dry_run"
        if exit_code != 0:
            status = "dry_run"
        return _success(
            context,
            status=status,
            code=code,
            exit_code=exit_code,
            snapshot_state="snapshot_published",
            pointer_state=pointer_state,
            durable=False,
            replaced=False,
        )
    peer = _publish_snapshot(settings, context, hooks, final)
    if peer is not None:
        return _success(
            context,
            status="complete",
            code="noop",
            snapshot_state="snapshot_noop",
            pointer_state="pointer_unchanged",
            durable=True,
            replaced=False,
            run_id=peer.run_id,
        )
    return _success(
        context,
        status="complete",
        code="published",
        snapshot_state="snapshot_published",
        pointer_state="pointer_unchanged",
        durable=True,
        replaced=False,
    )


def _pointer(
    settings: GoldSettings,
    context: _Context,
    hooks: GoldHooks,
    snapshot: GoldResult,
) -> GoldResult:
    root = settings.data_root
    _require_intact_snapshot(root, context)
    kind = _classify_pointer(root, context)
    if kind == "current":
        return _copy(
            snapshot,
            status="complete",
            code="noop" if snapshot.snapshot_state == "snapshot_noop" else "published",
            exit_code=0,
            pointer_state="pointer_current",
            pointer_replaced=False,
        )
    if kind == "invalid":
        return _copy(
            snapshot,
            status="rejected",
            code="pointer_invalid",
            exit_code=3,
            pointer_state="pointer_invalid",
            pointer_replaced=False,
        )
    if kind == "other" and not context.promote:
        return _copy(
            snapshot,
            status="conflict",
            code="publication_conflict",
            exit_code=3,
            pointer_state="pointer_unchanged",
            pointer_replaced=False,
        )
    created = kind == "absent"
    _replace_pointer(settings, context, hooks, snapshot)
    return _copy(
        snapshot,
        status="complete",
        code="published" if created else "promoted",
        exit_code=0,
        pointer_state="pointer_created" if created else "pointer_updated",
        pointer_replaced=True,
    )


def _preview_pointer(root: Path, context: _Context) -> tuple[str, str, int]:
    kind = _classify_pointer(root, context)
    if kind == "absent":
        return "pointer_created", "published", 0
    if kind == "current":
        return "pointer_current", "noop", 0
    if kind == "invalid":
        return "pointer_invalid", "pointer_invalid", 3
    if context.promote:
        return "pointer_updated", "promoted", 0
    return "pointer_unchanged", "publication_conflict", 3


def _publish_snapshot(
    settings: GoldSettings,
    context: _Context,
    hooks: GoldHooks,
    final: Path,
) -> GoldGeneration | None:
    root = settings.data_root
    incoming = root / "gold" / ".incoming"
    temp = incoming / secrets.token_hex(16)
    published = False
    try:
        assert_tree_has_no_reparse(root, final.parent)
        temp.mkdir(parents=True)
        summary_bytes = _canonical(context.summary)
        summary_sha = hashlib.sha256(summary_bytes).hexdigest()
        manifest = _manifest(settings, context, summary_sha, len(summary_bytes))
        write_bytes_fsync(temp / "summary.json", summary_bytes)
        write_bytes_fsync(temp / "manifest.json", _canonical(manifest))
        if hooks.before_snapshot_rename is not None:
            hooks.before_snapshot_rename()
        final.parent.mkdir(parents=True, exist_ok=True)
        assert_tree_has_no_reparse(root, final)
        try:
            rename_exclusive(temp, final)
        except FileExistsError:
            kind, peer = _observe_snapshot(final, context.summary, context.gold_key)
            if kind == "sealed" and peer is not None:
                return peer
            if kind == "conflict":
                raise _Halt(
                    _success(
                        context,
                        status="conflict",
                        code="snapshot_conflict",
                        exit_code=3,
                        snapshot_state="snapshot_conflict",
                        pointer_state="pointer_unchanged",
                        durable=True,
                        replaced=False,
                    )
                ) from None
            raise _Halt(
                _success(
                    context,
                    status="rejected",
                    code="integrity_incomplete",
                    exit_code=3,
                    snapshot_state="snapshot_incomplete",
                    pointer_state="pointer_unchanged",
                    durable=False,
                    replaced=False,
                )
            ) from None
        except StorageFailure as exc:
            raise _Halt(_storage_result(context, exc.code)) from exc
        except OSError as exc:
            winerror = getattr(exc, "winerror", None)
            if exc.errno == errno.EXDEV or winerror == 17:
                raise _Halt(_storage_result(context, "cross_device")) from exc
            raise _Halt(_storage_result(context, "storage_error")) from exc
        published = True
        if hooks.after_snapshot_rename is not None:
            hooks.after_snapshot_rename()
        _require_intact_snapshot(root, context)
        fsync_directory(final.parent)
        return None
    except StorageFailure as exc:
        raise _Halt(_storage_result(context, exc.code)) from exc
    finally:
        if not published and temp.exists():
            remove_tree_nofollow(temp, incoming)


def _replace_pointer(
    settings: GoldSettings,
    context: _Context,
    hooks: GoldHooks,
    snapshot: GoldResult,
) -> None:
    root = settings.data_root
    pointer_dir = root / "gold" / "published" / context.pointer_key
    current = pointer_dir / "current.json"
    incoming = pointer_dir / ".incoming"
    temp = incoming / f"{secrets.token_hex(16)}.json"
    replaced = False
    try:
        assert_tree_has_no_reparse(root, pointer_dir)
        incoming.mkdir(parents=True, exist_ok=True)
        summary_sha = hashlib.sha256(_canonical(context.summary)).hexdigest()
        pointer = GoldPointer(
            pointer_version="1",
            source_id=context.source_id,
            dataset_version=context.dataset_version,
            storage_key=context.gold_key,
            summary_sha256=summary_sha,
            status="published",
        )
        write_bytes_fsync(temp, _canonical(pointer))
        if hooks.before_pointer_replace is not None:
            hooks.before_pointer_replace()
        replace_file(temp, current)
        replaced = True
        if hooks.after_pointer_replace is not None:
            hooks.after_pointer_replace()
        fsync_directory(pointer_dir)
    except StorageFailure as exc:
        mapped = (
            exc.code
            if exc.code in {"reparse_point", "path_escape", "cross_device"}
            else "storage_error"
        )
        raise _Halt(
            _copy(
                snapshot,
                status="partial",
                code=mapped,
                exit_code=4,
                pointer_state="pointer_unchanged",
                pointer_replaced=False,
            )
        ) from exc
    finally:
        if not replaced and temp.exists() and not is_reparse_point(temp):
            temp.unlink()


def _load_silver(
    root: Path,
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    silver_key: str,
    *,
    dry_run: bool,
) -> "_SilverView":
    final = root / "silver" / "generations" / silver_key
    try:
        assert_tree_has_no_reparse(root, final)
    except StorageFailure as exc:
        raise _Halt(
            _storage_result_for(
                source_id, dataset_version, input_sha256, silver_key, exc.code, dry_run=dry_run
            )
        ) from exc
    if is_reparse_point(final):
        raise _Halt(
            _storage_result_for(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                "reparse_point",
                dry_run=dry_run,
            )
        )
    if not final.exists():
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="silver_absent",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    manifest_path = final / "manifest.json"
    silver_path = final / "silver.parquet"
    quarantine_path = final / "quarantine.parquet"
    paths = (manifest_path, silver_path, quarantine_path)
    if any(is_reparse_point(path) for path in paths) or not all(path.is_file() for path in paths):
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_incomplete",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    raw = manifest_path.read_bytes()
    if len(raw) > _MANIFEST_MAX_BYTES:
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_incomplete",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    try:
        manifest = GenerationManifest.model_validate_json(raw)
    except ValidationError as exc:
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_incomplete",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        ) from exc
    if (
        manifest.source_id != source_id
        or manifest.dataset_version != dataset_version
        or manifest.input_sha256 != input_sha256
        or manifest.storage_key != silver_key
        or manifest.contract_version != CONTRACT_VERSION
        or manifest.transform_version != TRANSFORM_VERSION
    ):
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_mismatch",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    try:
        silver_sha, silver_size = hash_file(silver_path, _PARQUET_MAX_BYTES)
        quarantine_sha, quarantine_size = hash_file(quarantine_path, _PARQUET_MAX_BYTES)
    except StorageFailure as exc:
        raise _Halt(
            _storage_result_for(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                "storage_error",
                dry_run=dry_run,
            )
        ) from exc
    if (
        silver_sha != manifest.silver_sha256
        or silver_size != manifest.silver_size_bytes
        or quarantine_sha != manifest.quarantine_sha256
        or quarantine_size != manifest.quarantine_size_bytes
    ):
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_mismatch",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    try:
        silver_rows, quarantine_rows = read_generation_parquet(silver_path, quarantine_path)
    except ParquetRoundtripError as exc:
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="schema_mismatch",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        ) from exc
    if (
        len(silver_rows) != manifest.silver_record_count
        or len(quarantine_rows) != manifest.quarantine_row_count
    ):
        raise _Halt(
            _identified_raw(
                source_id,
                dataset_version,
                input_sha256,
                silver_key,
                dry_run=dry_run,
                code="integrity_mismatch",
                exit_code=3,
                status="rejected",
                snapshot_state="snapshot_absent",
                pointer_state="pointer_unchanged",
            )
        )
    metrics = tuple(_trip_metric(row, manifest) for row in silver_rows)
    return _SilverView(manifest, metrics)


def _observe_snapshot(
    final: Path,
    summary: GoldSummary,
    gold_key: str,
) -> tuple[str, GoldGeneration | None]:
    if is_reparse_point(final):
        raise _Halt(_storage_from_summary(summary, gold_key, "reparse_point"))
    if not final.exists():
        return "absent", None
    summary_path = final / "summary.json"
    manifest_path = final / "manifest.json"
    if any(is_reparse_point(path) for path in (summary_path, manifest_path)):
        raise _Halt(_storage_from_summary(summary, gold_key, "reparse_point"))
    if not summary_path.is_file() or not manifest_path.is_file():
        return "incomplete", None
    try:
        parsed_summary = GoldSummary.model_validate_json(summary_path.read_bytes())
        generation = GoldGeneration.model_validate_json(manifest_path.read_bytes())
    except ValidationError:
        return "incomplete", None
    try:
        digest, size = hash_file(summary_path, _MANIFEST_MAX_BYTES)
    except StorageFailure:
        return "incomplete", None
    if (
        generation.storage_key != gold_key
        or generation.summary_sha256 != digest
        or generation.summary_size_bytes != size
        or not _same_metrics(parsed_summary, generation)
    ):
        return "mismatch", generation
    if parsed_summary != summary:
        return "conflict", generation
    return "sealed", generation


def _classify_pointer(root: Path, context: _Context) -> str:
    current = root / "gold" / "published" / context.pointer_key / "current.json"
    if is_reparse_point(current) or is_reparse_point(current.parent):
        raise _Halt(_storage_from_summary(context.summary, context.gold_key, "reparse_point"))
    if not current.exists():
        return "absent"
    if not current.is_file():
        return "invalid"
    try:
        pointer = GoldPointer.model_validate_json(current.read_bytes())
    except ValidationError:
        return "invalid"
    if pointer.source_id != context.source_id or pointer.dataset_version != context.dataset_version:
        return "invalid"
    summary_sha = hashlib.sha256(_canonical(context.summary)).hexdigest()
    intact = _snapshot_matches(root, pointer.storage_key, pointer.summary_sha256)
    if pointer.storage_key == context.gold_key and pointer.summary_sha256 == summary_sha and intact:
        return "current"
    if pointer.storage_key != context.gold_key and intact:
        return "other"
    return "invalid"


def _snapshot_matches(root: Path, storage_key: str, summary_sha: str) -> bool:
    final = root / "gold" / "snapshots" / storage_key
    summary_path = final / "summary.json"
    manifest_path = final / "manifest.json"
    if (
        is_reparse_point(final)
        or is_reparse_point(summary_path)
        or is_reparse_point(manifest_path)
        or not summary_path.is_file()
        or not manifest_path.is_file()
    ):
        return False
    try:
        summary = GoldSummary.model_validate_json(summary_path.read_bytes())
        generation = GoldGeneration.model_validate_json(manifest_path.read_bytes())
        digest, size = hash_file(summary_path, _MANIFEST_MAX_BYTES)
    except (ValidationError, StorageFailure, OSError):
        return False
    return (
        generation.storage_key == storage_key
        and generation.summary_sha256 == summary_sha
        and digest == summary_sha
        and generation.summary_size_bytes == size
        and _same_metrics(summary, generation)
    )


def _require_intact_snapshot(root: Path, context: _Context) -> None:
    summary_sha = hashlib.sha256(_canonical(context.summary)).hexdigest()
    if not _snapshot_matches(root, context.gold_key, summary_sha):
        raise _Halt(
            _success(
                context,
                status="rejected",
                code="integrity_mismatch",
                exit_code=3,
                snapshot_state="snapshot_incomplete",
                pointer_state="pointer_unchanged",
                durable=False,
                replaced=False,
            )
        )


def _summary(manifest: GenerationManifest, metrics: GoldMetrics) -> GoldSummary:
    return GoldSummary(
        summary_version="1",
        source_id=manifest.source_id,
        dataset_version=manifest.dataset_version,
        input_sha256=manifest.input_sha256,
        contract_version=manifest.contract_version,
        transform_version=manifest.transform_version,
        silver_storage_key=manifest.storage_key,
        gold_contract_version=GOLD_CONTRACT_VERSION,
        aggregate_version=AGGREGATE_VERSION,
        trip_count=metrics.trip_count,
        total_distance_km=metrics.total_distance_km,
        average_duration_seconds=metrics.average_duration_seconds,
        rejected_count=metrics.rejected_count,
    )


def _manifest(
    settings: GoldSettings,
    context: _Context,
    summary_sha: str,
    summary_size: int,
) -> GoldGeneration:
    clock = settings.clock or Clock()
    created_at = clock.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    metrics = context.metrics
    return GoldGeneration(
        manifest_version="1",
        source_id=context.source_id,
        dataset_version=context.dataset_version,
        input_sha256=context.input_sha256,
        contract_version=CONTRACT_VERSION,
        transform_version=TRANSFORM_VERSION,
        silver_storage_key=context.silver_key,
        gold_contract_version=GOLD_CONTRACT_VERSION,
        aggregate_version=AGGREGATE_VERSION,
        storage_key=context.gold_key,
        pointer_key=context.pointer_key,
        bronze_run_id=context.bronze_run_id,
        run_id=context.run_id,
        created_at=created_at,
        status="published",
        summary_sha256=summary_sha,
        summary_size_bytes=summary_size,
        parquet_reader="duckdb",
        parquet_reader_version=writer_version(),
        trip_count=metrics.trip_count,
        total_distance_km=metrics.total_distance_km,
        average_duration_seconds=metrics.average_duration_seconds,
        rejected_count=metrics.rejected_count,
    )


def _trip_metric(row: tuple[object, ...], manifest: GenerationManifest) -> TripMetric:
    if len(row) != len(SILVER_COLUMNS):
        raise AggregationError("shape")
    identity = {
        "source_id": manifest.source_id,
        "dataset_version": manifest.dataset_version,
        "input_sha256": manifest.input_sha256,
        "contract_version": manifest.contract_version,
        "transform_version": manifest.transform_version,
        "bronze_run_id": manifest.bronze_run_id,
    }
    for name, expected in identity.items():
        if row[_COLUMN[name]] != expected:
            raise AggregationError("lineage")
    pickup = row[_COLUMN["pickup_at"]]
    dropoff = row[_COLUMN["dropoff_at"]]
    distance = row[_COLUMN["trip_distance"]]
    duplicate_count = row[_COLUMN["duplicate_count"]]
    trip_id = row[_COLUMN["trip_id"]]
    if not isinstance(pickup, datetime) or not isinstance(dropoff, datetime):
        raise AggregationError("timestamp")
    if not isinstance(distance, Decimal) or not isinstance(duplicate_count, int):
        raise AggregationError("distance")
    if not isinstance(trip_id, str):
        raise AggregationError("trip_id")
    return TripMetric(trip_id, pickup, dropoff, distance, duplicate_count)


def _same_metrics(summary: GoldSummary, generation: GoldGeneration) -> bool:
    return (
        summary.trip_count == generation.trip_count
        and summary.total_distance_km == generation.total_distance_km
        and summary.average_duration_seconds == generation.average_duration_seconds
        and summary.rejected_count == generation.rejected_count
        and summary.silver_storage_key == generation.silver_storage_key
    )


def _canonical(model: GoldSummary | GoldGeneration | GoldPointer) -> bytes:
    payload = model.model_dump(mode="json")
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _too_long(root: Path, gold_key: str, pointer_key: str) -> bool:
    token = "f" * 32
    candidates = (
        root / "gold" / "snapshots" / gold_key / "manifest.json",
        root / "gold" / "locks" / f"pointer-{pointer_key}.lock",
        root / "gold" / ".incoming" / token / "manifest.json",
        root / "gold" / "published" / pointer_key / ".incoming" / f"{token}.json",
    )
    for candidate in candidates:
        absolute = candidate if candidate.is_absolute() else candidate.absolute()
        if len(str(absolute)) > _MAX_ABSOLUTE_PATH:
            return True
    return False


def _success(
    context: _Context,
    *,
    status: str,
    code: str,
    snapshot_state: str,
    pointer_state: str,
    durable: bool,
    replaced: bool,
    exit_code: int = 0,
    run_id: str | None = None,
) -> GoldResult:
    metrics = context.metrics
    return GoldResult(
        status=status,
        code=code,
        exit_code=exit_code,
        dry_run=context.dry_run,
        source_id=context.source_id,
        dataset_version=context.dataset_version,
        input_sha256=context.input_sha256,
        contract_version=CONTRACT_VERSION,
        transform_version=TRANSFORM_VERSION,
        gold_contract_version=GOLD_CONTRACT_VERSION,
        aggregate_version=AGGREGATE_VERSION,
        silver_storage_key=context.silver_key,
        storage_key=context.gold_key,
        pointer_key=context.pointer_key,
        run_id=context.run_id if run_id is None else run_id,
        observed_run_id=context.run_id,
        snapshot_state=snapshot_state,
        pointer_state=pointer_state,
        snapshot_durable=durable,
        pointer_replaced=replaced,
        trip_count=metrics.trip_count,
        total_distance_km=metrics.total_distance_km,
        average_duration_seconds=metrics.average_duration_seconds,
        rejected_count=metrics.rejected_count,
    )


def _copy(
    snapshot: GoldResult,
    *,
    status: str,
    code: str,
    exit_code: int,
    pointer_state: str,
    pointer_replaced: bool,
) -> GoldResult:
    return GoldResult(
        status=status,
        code=code,
        exit_code=exit_code,
        dry_run=snapshot.dry_run,
        source_id=snapshot.source_id,
        dataset_version=snapshot.dataset_version,
        input_sha256=snapshot.input_sha256,
        contract_version=snapshot.contract_version,
        transform_version=snapshot.transform_version,
        gold_contract_version=snapshot.gold_contract_version,
        aggregate_version=snapshot.aggregate_version,
        silver_storage_key=snapshot.silver_storage_key,
        storage_key=snapshot.storage_key,
        pointer_key=snapshot.pointer_key,
        run_id=snapshot.run_id,
        observed_run_id=snapshot.observed_run_id,
        snapshot_state=snapshot.snapshot_state,
        pointer_state=pointer_state,
        snapshot_durable=snapshot.snapshot_durable,
        pointer_replaced=pointer_replaced,
        trip_count=snapshot.trip_count,
        total_distance_km=snapshot.total_distance_km,
        average_duration_seconds=snapshot.average_duration_seconds,
        rejected_count=snapshot.rejected_count,
    )


def _storage_result(context: _Context, code: str) -> GoldResult:
    mapped = (
        code
        if code in {"reparse_point", "path_escape", "cross_device", "rename_noreplace_unavailable"}
        else "storage_error"
    )
    return _success(
        context,
        status="rejected",
        code=mapped,
        exit_code=4,
        snapshot_state="snapshot_absent",
        pointer_state="pointer_unchanged",
        durable=False,
        replaced=False,
    )


def _bare(request: GoldRequest, *, code: str, exit_code: int, status: str) -> GoldResult:
    return _identified_raw(
        request.source_id,
        request.dataset_version,
        request.input_sha256,
        None,
        code=code,
        exit_code=exit_code,
        status=status,
        snapshot_state="snapshot_absent",
        pointer_state="pointer_unchanged",
        dry_run=request.dry_run,
        run_id=request.run_id,
    )


def _identified(
    request: GoldRequest,
    silver_key: str,
    gold_key: str,
    pointer_key: str,
    *,
    code: str,
    exit_code: int,
    status: str,
    snapshot_state: str,
    pointer_state: str,
    metrics: GoldMetrics | None = None,
    error: str | None = None,
) -> GoldResult:
    del error
    result = _identified_raw(
        request.source_id,
        request.dataset_version,
        request.input_sha256,
        silver_key,
        code=code,
        exit_code=exit_code,
        status=status,
        snapshot_state=snapshot_state,
        pointer_state=pointer_state,
        dry_run=request.dry_run,
        run_id=request.run_id,
        gold_key=gold_key,
        pointer_key=pointer_key,
    )
    if metrics is None:
        return result
    return GoldResult(
        status=result.status,
        code=result.code,
        exit_code=result.exit_code,
        dry_run=result.dry_run,
        source_id=result.source_id,
        dataset_version=result.dataset_version,
        input_sha256=result.input_sha256,
        contract_version=result.contract_version,
        transform_version=result.transform_version,
        gold_contract_version=result.gold_contract_version,
        aggregate_version=result.aggregate_version,
        silver_storage_key=result.silver_storage_key,
        storage_key=result.storage_key,
        pointer_key=result.pointer_key,
        run_id=result.run_id,
        observed_run_id=result.observed_run_id,
        snapshot_state=result.snapshot_state,
        pointer_state=result.pointer_state,
        snapshot_durable=result.snapshot_durable,
        pointer_replaced=result.pointer_replaced,
        trip_count=metrics.trip_count,
        total_distance_km=metrics.total_distance_km,
        average_duration_seconds=metrics.average_duration_seconds,
        rejected_count=metrics.rejected_count,
    )


def _identified_raw(
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    silver_key: str | None,
    *,
    code: str,
    exit_code: int,
    status: str,
    snapshot_state: str,
    pointer_state: str,
    dry_run: bool = False,
    run_id: str | None = None,
    gold_key: str | None = None,
    pointer_key: str | None = None,
) -> GoldResult:
    return GoldResult(
        status="dry_run" if dry_run else status,
        code=code,
        exit_code=exit_code,
        dry_run=dry_run,
        source_id=source_id,
        dataset_version=dataset_version,
        input_sha256=input_sha256,
        contract_version=CONTRACT_VERSION,
        transform_version=TRANSFORM_VERSION,
        gold_contract_version=GOLD_CONTRACT_VERSION,
        aggregate_version=AGGREGATE_VERSION,
        silver_storage_key=silver_key,
        storage_key=gold_key,
        pointer_key=pointer_key,
        run_id=run_id,
        observed_run_id=run_id,
        snapshot_state=snapshot_state,
        pointer_state=pointer_state,
        snapshot_durable=False,
        pointer_replaced=False,
        trip_count=None,
        total_distance_km=None,
        average_duration_seconds=None,
        rejected_count=None,
    )


def _storage_result_for(
    source_id: str,
    dataset_version: str,
    input_sha256: str,
    silver_key: str,
    code: str,
    *,
    dry_run: bool = False,
) -> GoldResult:
    mapped = code if code in {"reparse_point", "path_escape"} else "storage_error"
    return _identified_raw(
        source_id,
        dataset_version,
        input_sha256,
        silver_key,
        code=mapped,
        exit_code=4,
        status="rejected",
        snapshot_state="snapshot_absent",
        pointer_state="pointer_unchanged",
        dry_run=dry_run,
    )


def _storage_from_summary(summary: GoldSummary, gold_key: str, code: str) -> GoldResult:
    mapped = code if code in {"reparse_point", "path_escape"} else "storage_error"
    return _identified_raw(
        summary.source_id,
        summary.dataset_version,
        summary.input_sha256,
        summary.silver_storage_key,
        code=mapped,
        exit_code=4,
        status="rejected",
        snapshot_state="snapshot_absent",
        pointer_state="pointer_unchanged",
        gold_key=gold_key,
        pointer_key=pointer_key_for(summary.source_id, summary.dataset_version),
    )


def _json_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format_decimal(value)


@dataclass(frozen=True, slots=True)
class _SilverView:
    manifest: GenerationManifest
    rows: tuple[TripMetric, ...]
