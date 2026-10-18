"""Silver to Gold publication, promotion, replay, and failure behavior."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from cfi.application.gold import Clock, GoldFailure, GoldHooks, GoldRequest, GoldSettings, gold
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
from cfi.application.transform import TransformRequest, TransformSettings, transform
from cfi.domain.gold import (
    GoldGeneration,
    GoldPointer,
    GoldSummary,
    gold_storage_key_for,
    pointer_key_for,
)
from cfi.infrastructure.storage import StorageFailure, replace_file

FIXTURES = Path("tests/fixtures/synthetic")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"


class FixedClock(Clock):
    def now(self) -> datetime:
        return datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _prepare(root: Path, name: str, *, version: str = VERSION) -> str:
    source = root / f"{version}.bin"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes((FIXTURES / name).read_bytes())
    ingest(
        IngestSettings(data_root=root),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=version,
            run_id="bronze-001",
        ),
    )
    result = transform(
        TransformSettings(data_root=root, clock=FixedClock()),
        TransformRequest(source_id=SOURCE, dataset_version=version, run_id="silver-001"),
    )
    return result.input_sha256


def _request(sha: str, **kwargs: object) -> GoldRequest:
    values: dict[str, object] = {
        "source_id": SOURCE,
        "dataset_version": VERSION,
        "input_sha256": sha,
        "run_id": "gold-001",
    }
    values.update(kwargs)
    return GoldRequest(**values)  # type: ignore[arg-type]


def _run(root: Path, sha: str, hooks: GoldHooks | None = None, **kwargs: object):
    return gold(
        GoldSettings(data_root=root, clock=FixedClock()),
        _request(sha, **kwargs),
        hooks,
    )


def _snapshot(root: Path, sha: str) -> Path:
    return root / "gold" / "snapshots" / gold_storage_key_for(SOURCE, VERSION, sha)


def _pointer(root: Path, version: str = VERSION) -> Path:
    return root / "gold" / "published" / pointer_key_for(SOURCE, version) / "current.json"


def _canonical(model: GoldSummary | GoldGeneration | GoldPointer) -> bytes:
    payload = model.model_dump(mode="json")
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _seal(directory: Path) -> tuple[bytes, bytes]:
    return (
        (directory / "summary.json").read_bytes(),
        (directory / "manifest.json").read_bytes(),
    )


def test_valid_fixture_publishes_replays_and_ignores_bronze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    shutil.rmtree(tmp_path / "bronze")
    opened: list[str] = []
    real_open = os.open

    def guard(path: str | os.PathLike[str], flags: int, *args: object, **kwargs: object) -> int:
        text = os.fspath(path)
        opened.append(text)
        if f"{os.sep}bronze{os.sep}" in text:
            raise AssertionError(text)
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "open", guard)
    published = _run(tmp_path, sha)
    directory = _snapshot(tmp_path, sha)
    sealed = _seal(directory)
    pointer_bytes = _pointer(tmp_path).read_bytes()
    replay = _run(tmp_path, sha, run_id="gold-002")

    assert published.status == "complete"
    assert published.code == "published"
    assert published.snapshot_state == "snapshot_published"
    assert published.pointer_state == "pointer_created"
    assert published.snapshot_durable is True
    assert published.pointer_replaced is True
    assert published.trip_count == 2
    assert published.total_distance_km == Decimal("12.750")
    assert published.average_duration_seconds == Decimal("1500.000000")
    assert published.rejected_count == 0
    assert replay.status == "complete"
    assert replay.code == "noop"
    assert replay.snapshot_state == "snapshot_noop"
    assert replay.pointer_state == "pointer_current"
    assert replay.pointer_replaced is False
    assert replay.run_id == "gold-001"
    assert replay.observed_run_id == "gold-002"
    assert _seal(directory) == sealed
    assert _pointer(tmp_path).read_bytes() == pointer_bytes
    dry = _run(tmp_path, sha, dry_run=True, run_id="gold-003")
    assert dry.status == "dry_run"
    assert dry.pointer_replaced is False
    assert dry.snapshot_durable is True
    assert _seal(directory) == sealed
    assert _pointer(tmp_path).read_bytes() == pointer_bytes
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    manifest = GoldGeneration.model_validate_json((directory / "manifest.json").read_bytes())
    assert summary["total_distance_km"] == "12.750"
    assert summary["average_duration_seconds"] == "1500.000000"
    assert "created_at" not in summary
    assert manifest.created_at == "2024-06-01T12:00:00Z"
    assert manifest.bronze_run_id == "bronze-001"
    assert manifest.parquet_reader == "duckdb"
    assert manifest.summary_sha256 == hashlib.sha256(sealed[0]).hexdigest()
    pointer = GoldPointer.model_validate_json(_pointer(tmp_path).read_bytes())
    assert pointer.storage_key == manifest.storage_key
    assert pointer.summary_sha256 == manifest.summary_sha256
    assert not any(f"{os.sep}bronze{os.sep}" in path for path in opened)


def test_invalid_and_conflict_fixtures(tmp_path: Path) -> None:
    invalid = _prepare(tmp_path, "trips_invalid.jsonl")
    empty = _run(tmp_path, invalid)
    assert empty.trip_count == 0
    assert empty.total_distance_km == Decimal("0.000")
    assert empty.average_duration_seconds is None
    assert empty.rejected_count == 14
    summary = json.loads(
        (_snapshot(tmp_path, invalid) / "summary.json").read_text(encoding="utf-8")
    )
    assert summary["average_duration_seconds"] is None
    assert summary["total_distance_km"] == "0.000"

    other = tmp_path / "conflict"
    conflict_sha = _prepare(other, "trips_conflict.jsonl")
    conflict = _run(other, conflict_sha)
    assert conflict.trip_count == 1
    assert conflict.total_distance_km == Decimal("1.000")
    assert conflict.average_duration_seconds == Decimal("600.000000")
    assert conflict.rejected_count == 2


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    before = list(tmp_path.iterdir())

    result = _run(tmp_path, sha, dry_run=True)

    assert result.status == "dry_run"
    assert result.code == "published"
    assert result.pointer_replaced is False
    assert result.snapshot_durable is False
    assert result.trip_count == 2
    assert list(tmp_path.iterdir()) == before
    assert not (tmp_path / "gold").exists()


def test_missing_silver_is_rejected_without_gold_directories(tmp_path: Path) -> None:
    sha = "ab" * 32
    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha, dry_run=True)
    assert caught.value.result.code == "silver_absent"
    assert caught.value.result.exit_code == 3
    assert caught.value.result.dry_run is True
    assert not (tmp_path / "gold").exists()


def test_snapshot_conflict_does_not_replace(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, sha)
    directory = _snapshot(tmp_path, sha)
    summary = GoldSummary.model_validate_json((directory / "summary.json").read_bytes())
    generation = GoldGeneration.model_validate_json((directory / "manifest.json").read_bytes())
    tampered = summary.model_copy(update={"rejected_count": 9})
    raw = _canonical(tampered)
    sealed = generation.model_copy(
        update={
            "rejected_count": 9,
            "summary_sha256": hashlib.sha256(raw).hexdigest(),
            "summary_size_bytes": len(raw),
        }
    )
    (directory / "summary.json").write_bytes(raw)
    (directory / "manifest.json").write_bytes(_canonical(sealed))
    before = _seal(directory)

    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha)
    assert caught.value.result.code == "snapshot_conflict"
    assert caught.value.result.exit_code == 3
    assert caught.value.result.snapshot_durable is True
    assert caught.value.result.pointer_replaced is False
    assert _seal(directory) == before


def test_incomplete_snapshot_is_preserved(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    directory = _snapshot(tmp_path, sha)
    directory.mkdir(parents=True)
    (directory / "summary.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha)
    assert caught.value.result.code == "integrity_incomplete"
    assert (directory / "summary.json").read_text(encoding="utf-8") == "{}\n"
    assert not (directory / "manifest.json").exists()


def test_second_checksum_conflicts_until_promote(tmp_path: Path) -> None:
    first = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, first)
    shutil.rmtree(tmp_path / "bronze")
    second = _prepare(tmp_path, "trips_invalid.jsonl")
    pointer_before = _pointer(tmp_path).read_bytes()

    with pytest.raises(GoldFailure) as dry:
        _run(tmp_path, second, dry_run=True)
    assert dry.value.result.code == "publication_conflict"
    assert dry.value.result.dry_run is True
    assert not _snapshot(tmp_path, second).exists()

    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, second)
    assert caught.value.result.code == "publication_conflict"
    assert caught.value.result.snapshot_state == "snapshot_published"
    assert caught.value.result.snapshot_durable is True
    assert caught.value.result.pointer_replaced is False
    assert _pointer(tmp_path).read_bytes() == pointer_before
    assert _snapshot(tmp_path, first).exists()
    assert _snapshot(tmp_path, second).exists()

    promoted = _run(tmp_path, second, promote=True)
    assert promoted.code == "promoted"
    assert promoted.pointer_state == "pointer_updated"
    assert promoted.pointer_replaced is True
    assert GoldPointer.model_validate_json(_pointer(tmp_path).read_bytes()).storage_key == (
        gold_storage_key_for(SOURCE, VERSION, second)
    )


def test_promote_does_not_repair_a_bad_pointer(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    published = _run(tmp_path, sha)
    current = _pointer(tmp_path)
    original = current.read_bytes()
    current.write_bytes(b"{}\n")

    with pytest.raises(GoldFailure) as corrupt:
        _run(tmp_path, sha, promote=True)
    assert corrupt.value.result.code == "pointer_invalid"
    assert corrupt.value.result.pointer_replaced is False
    assert current.read_bytes() == b"{}\n"

    current.write_bytes(original)
    missing = GoldPointer(
        pointer_version="1",
        source_id=SOURCE,
        dataset_version=VERSION,
        storage_key="c" * 64,
        summary_sha256="d" * 64,
        status="published",
    )
    current.write_bytes(_canonical(missing))
    saved = current.read_bytes()
    with pytest.raises(GoldFailure) as absent:
        _run(tmp_path, sha, promote=True)
    assert absent.value.result.code == "pointer_invalid"
    assert current.read_bytes() == saved

    mismatched = GoldPointer(
        pointer_version="1",
        source_id=SOURCE,
        dataset_version=VERSION,
        storage_key=published.storage_key or "",
        summary_sha256="e" * 64,
        status="published",
    )
    current.write_bytes(_canonical(mismatched))
    saved = current.read_bytes()
    with pytest.raises(GoldFailure) as mismatch:
        _run(tmp_path, sha, promote=True)
    assert mismatch.value.result.code == "pointer_invalid"
    assert current.read_bytes() == saved


def test_distinct_dataset_versions_have_distinct_pointers(tmp_path: Path) -> None:
    first = _prepare(tmp_path, "trips_valid.jsonl", version="v1")
    second = _prepare(tmp_path, "trips_valid.jsonl", version="v2")
    left = _run(tmp_path, first)
    right = gold(
        GoldSettings(data_root=tmp_path, clock=FixedClock()),
        GoldRequest(
            source_id=SOURCE,
            dataset_version="v2",
            input_sha256=second,
            run_id="gold-002",
        ),
    )

    assert left.pointer_key != right.pointer_key
    assert _pointer(tmp_path, "v1").is_file()
    assert _pointer(tmp_path, "v2").is_file()


def test_pointer_lock_after_a_durable_snapshot_is_partial(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, sha)
    sealed = _seal(_snapshot(tmp_path, sha))
    pointer = _pointer(tmp_path).read_bytes()
    marker = tmp_path / "gold" / "locks" / f"pointer-{pointer_key_for(SOURCE, VERSION)}.lock"
    ready = tmp_path / "ready"
    holder = """
import sys
import time
from pathlib import Path
from cfi.infrastructure.lock import source_lock
path = Path(sys.argv[1])
ready = Path(sys.argv[2])
with source_lock(path, timeout_seconds=5):
    ready.write_text("held", encoding="ascii")
    time.sleep(2)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", holder, str(marker), str(ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            if proc.poll() is not None:
                raise AssertionError(proc.stderr.read() if proc.stderr else "holder exited")
            time.sleep(0.05)
        else:
            raise AssertionError("holder did not acquire the lock")
        with pytest.raises(GoldFailure) as caught:
            gold(
                GoldSettings(data_root=tmp_path, clock=FixedClock(), lock_timeout_seconds=0.2),
                _request(sha, run_id="gold-002"),
            )
        assert caught.value.result.code == "lock_unavailable"
        assert caught.value.result.exit_code == 5
        assert caught.value.result.status == "partial"
        assert caught.value.result.snapshot_state == "snapshot_noop"
        assert caught.value.result.snapshot_durable is True
        assert caught.value.result.pointer_replaced is False
        assert _seal(_snapshot(tmp_path, sha)) == sealed
        assert _pointer(tmp_path).read_bytes() == pointer
    finally:
        proc.wait(timeout=10)


def test_snapshot_lock_timeout_publishes_nothing(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    marker = tmp_path / "gold" / "locks" / f"{gold_storage_key_for(SOURCE, VERSION, sha)}.lock"
    ready = tmp_path / "ready"
    holder = """
import sys
import time
from pathlib import Path
from cfi.infrastructure.lock import source_lock
path = Path(sys.argv[1])
ready = Path(sys.argv[2])
with source_lock(path, timeout_seconds=5):
    ready.write_text("held", encoding="ascii")
    time.sleep(2)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", holder, str(marker), str(ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            if proc.poll() is not None:
                raise AssertionError(proc.stderr.read() if proc.stderr else "holder exited")
            time.sleep(0.05)
        else:
            raise AssertionError("holder did not acquire the lock")
        with pytest.raises(GoldFailure) as caught:
            gold(
                GoldSettings(data_root=tmp_path, clock=FixedClock(), lock_timeout_seconds=0.2),
                _request(sha),
            )
        assert caught.value.result.exit_code == 5
        assert caught.value.result.snapshot_state == "snapshot_absent"
        assert caught.value.result.snapshot_durable is False
        assert not (tmp_path / "gold" / "snapshots").exists()
    finally:
        proc.wait(timeout=10)


def test_snapshot_lock_timeout_reports_an_existing_snapshot(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, sha)
    sealed = _seal(_snapshot(tmp_path, sha))
    pointer = _pointer(tmp_path).read_bytes()
    marker = tmp_path / "gold" / "locks" / f"{gold_storage_key_for(SOURCE, VERSION, sha)}.lock"
    ready = tmp_path / "ready"
    holder = """
import sys
import time
from pathlib import Path
from cfi.infrastructure.lock import source_lock
path = Path(sys.argv[1])
ready = Path(sys.argv[2])
with source_lock(path, timeout_seconds=5):
    ready.write_text("held", encoding="ascii")
    time.sleep(2)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", holder, str(marker), str(ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            if proc.poll() is not None:
                raise AssertionError(proc.stderr.read() if proc.stderr else "holder exited")
            time.sleep(0.05)
        else:
            raise AssertionError("holder did not acquire the lock")
        with pytest.raises(GoldFailure) as caught:
            gold(
                GoldSettings(data_root=tmp_path, clock=FixedClock(), lock_timeout_seconds=0.2),
                _request(sha, run_id="gold-002"),
            )
        result = caught.value.result
        assert result.code == "lock_unavailable"
        assert result.exit_code == 5
        assert result.snapshot_state == "snapshot_noop"
        assert result.snapshot_durable is True
        assert result.pointer_state == "pointer_unchanged"
        assert result.pointer_replaced is False
        assert _seal(_snapshot(tmp_path, sha)) == sealed
        assert _pointer(tmp_path).read_bytes() == pointer
    finally:
        proc.wait(timeout=10)


def test_crash_hooks_leave_the_named_artifact(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")

    def boom() -> None:
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError):
        _run(tmp_path, sha, GoldHooks(before_snapshot_rename=boom))
    assert not (tmp_path / "gold" / "snapshots").exists()

    with pytest.raises(RuntimeError):
        _run(tmp_path, sha, GoldHooks(after_snapshot_rename=boom))
    assert _snapshot(tmp_path, sha).is_dir()
    assert not _pointer(tmp_path).exists()

    with pytest.raises(RuntimeError):
        _run(tmp_path, sha, GoldHooks(before_pointer_replace=boom))
    assert not _pointer(tmp_path).exists()

    with pytest.raises(RuntimeError):
        _run(tmp_path, sha, GoldHooks(after_pointer_replace=boom))
    assert _pointer(tmp_path).is_file()
    follow = _run(tmp_path, sha, run_id="gold-002")
    assert follow.code == "noop"
    assert follow.pointer_state == "pointer_current"


def test_concurrent_processes_publish_once(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    code = """
import json, sys
from pathlib import Path
from cfi.application.gold import GoldFailure, GoldRequest, GoldSettings, gold
settings = GoldSettings(data_root=Path(sys.argv[1]), lock_timeout_seconds=10)
request = GoldRequest(
    source_id="synthetic.trips.v1",
    dataset_version="v1",
    input_sha256=sys.argv[2],
    run_id=sys.argv[3],
)
try:
    result = gold(settings, request)
    print(json.dumps({"code": result.code, "pointer": result.pointer_state}))
except GoldFailure as exc:
    print(json.dumps({"code": exc.result.code, "pointer": exc.result.pointer_state}))
    sys.exit(exc.exit_code)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(tmp_path), sha, run_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for run_id in ("gold-a", "gold-b")
    ]
    outputs = []
    for proc in processes:
        stdout, stderr = proc.communicate(timeout=30)
        assert proc.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    assert "published" in {item["code"] for item in outputs}
    assert {item["code"] for item in outputs} <= {"noop", "published"}
    assert len(list((tmp_path / "gold" / "snapshots").iterdir())) == 1
    assert _pointer(tmp_path).is_file()


def test_concurrent_promotions_stay_on_intact_snapshots(tmp_path: Path) -> None:
    first = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, first)
    shutil.rmtree(tmp_path / "bronze")
    second = _prepare(tmp_path, "trips_invalid.jsonl")
    code = """
import json, sys
from pathlib import Path
from cfi.application.gold import GoldFailure, GoldRequest, GoldSettings, gold
settings = GoldSettings(data_root=Path(sys.argv[1]), lock_timeout_seconds=10)
request = GoldRequest(
    source_id="synthetic.trips.v1",
    dataset_version="v1",
    input_sha256=sys.argv[2],
    run_id=sys.argv[3],
    promote=True,
)
try:
    result = gold(settings, request)
    payload = {"code": result.code, "pointer": result.pointer_state}
except GoldFailure as exc:
    payload = {"code": exc.result.code, "pointer": exc.result.pointer_state}
    print(json.dumps(payload))
    sys.exit(exc.exit_code)
print(json.dumps(payload))
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(tmp_path), sha, run_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for sha, run_id in ((first, "gold-a"), (second, "gold-b"))
    ]
    outputs = []
    for proc in processes:
        stdout, stderr = proc.communicate(timeout=30)
        assert proc.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    assert "pointer_invalid" not in {item["pointer"] for item in outputs}
    assert _snapshot(tmp_path, first).is_dir()
    assert _snapshot(tmp_path, second).is_dir()
    pointer = GoldPointer.model_validate_json(_pointer(tmp_path).read_bytes())
    assert pointer.storage_key in {
        gold_storage_key_for(SOURCE, VERSION, first),
        gold_storage_key_for(SOURCE, VERSION, second),
    }


def test_symlink_and_junction_on_snapshots_are_rejected(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "gold" / "snapshots"
    parent.parent.mkdir(parents=True)
    try:
        parent.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is not permitted on this system")
    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha)
    assert caught.value.result.code == "reparse_point"
    assert list(outside.iterdir()) == []


def test_junction_on_snapshots_is_rejected(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("directory junctions are a Windows reparse point")
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "gold" / "snapshots"
    parent.parent.mkdir(parents=True)
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(parent), str(outside)],
        check=False,
        capture_output=True,
    )
    if created.returncode != 0:
        pytest.skip("junction creation is not permitted on this system")
    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha)
    assert caught.value.result.code == "reparse_point"
    assert list(outside.iterdir()) == []


def test_path_limit_creates_nothing(tmp_path: Path) -> None:
    root = tmp_path / ("p" * 300)
    with pytest.raises(GoldFailure) as caught:
        gold(
            GoldSettings(data_root=root, clock=FixedClock()),
            _request("ab" * 32),
        )
    assert caught.value.result.code == "path_limit"
    assert caught.value.result.exit_code == 4
    assert ("p" * 300) not in {child.name for child in tmp_path.iterdir()}


def test_manifest_hash_lie_is_not_repaired(tmp_path: Path) -> None:
    sha = _prepare(tmp_path, "trips_valid.jsonl")
    _run(tmp_path, sha)
    directory = _snapshot(tmp_path, sha)
    summary = (directory / "summary.json").read_bytes()
    manifest = GoldGeneration.model_validate_json((directory / "manifest.json").read_bytes())
    lied = manifest.model_copy(update={"summary_sha256": "a" * 64})
    (directory / "manifest.json").write_bytes(_canonical(lied))
    before = _seal(directory)

    with pytest.raises(GoldFailure) as caught:
        _run(tmp_path, sha)

    assert caught.value.result.code == "integrity_mismatch"
    assert caught.value.result.pointer_replaced is False
    assert _seal(directory) == before
    assert (directory / "summary.json").read_bytes() == summary


def test_replace_file_overwrites_one_file(tmp_path: Path) -> None:
    destination = tmp_path / "current.json"
    destination.write_text("old", encoding="utf-8")
    source = tmp_path / "next.json"
    source.write_text("new", encoding="utf-8")

    replace_file(source, destination)

    assert destination.read_text(encoding="utf-8") == "new"
    assert not source.exists()
    directory = tmp_path / "dir"
    directory.mkdir()
    again = tmp_path / "again.json"
    again.write_text("x", encoding="utf-8")
    with pytest.raises(StorageFailure):
        replace_file(again, directory)
