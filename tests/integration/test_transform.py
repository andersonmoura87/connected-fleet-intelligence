"""Bronze to Silver publication, replay, and failure behavior."""

import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from cfi.application.errors import IngestFailure
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
from cfi.application.transform import (
    Clock,
    TransformHooks,
    TransformRequest,
    TransformResult,
    TransformSettings,
    transform,
)
from cfi.domain.generation import GenerationManifest
from cfi.domain.silver import LogicalBatch, reduce_bronze, storage_key_for
from cfi.infrastructure.parquet import read_generation_parquet

FIXTURES = Path("tests/fixtures/synthetic")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"


class FixedClock(Clock):
    def now(self) -> datetime:
        return datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _ingest(root: Path, payload: bytes, *, run_id: str = "bronze-001") -> None:
    source = root / "input.bin"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(payload)
    ingest(
        IngestSettings(data_root=root),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=VERSION,
            run_id=run_id,
        ),
    )


def _request(**kwargs: object) -> TransformRequest:
    values: dict[str, object] = {
        "source_id": SOURCE,
        "dataset_version": VERSION,
        "run_id": "silver-001",
    }
    values.update(kwargs)
    return TransformRequest(**values)  # type: ignore[arg-type]


def _run(root: Path, hooks: TransformHooks | None = None, **kwargs: object) -> TransformResult:
    return transform(
        TransformSettings(data_root=root, clock=FixedClock()),
        _request(**kwargs),
        hooks,
    )


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _generation(root: Path) -> Path:
    found = list((root / "silver" / "generations").iterdir())
    assert len(found) == 1
    return found[0]


def _manifest(root: Path) -> GenerationManifest:
    return GenerationManifest.model_validate_json(
        (_generation(root) / "manifest.json").read_bytes()
    )


def _counts(result: TransformResult) -> tuple[int, int, int, int, int, int, int]:
    return (
        result.input_line_count,
        result.valid_record_count,
        result.invalid_record_count,
        result.silver_record_count,
        result.identical_duplicate_extra_count,
        result.conflict_rejection_count,
        result.quarantine_row_count,
    )


def _seal(root: Path) -> tuple[bytes, bytes, bytes]:
    directory = _generation(root)
    return (
        (directory / "manifest.json").read_bytes(),
        (directory / "silver.parquet").read_bytes(),
        (directory / "quarantine.parquet").read_bytes(),
    )


def test_valid_fixture_publishes_and_replays_without_rewriting(tmp_path: Path) -> None:
    payload = _fixture("trips_valid.jsonl")
    _ingest(tmp_path, payload)
    bronze = (tmp_path / "bronze" / "sources" / SOURCE / VERSION / "original").read_bytes()

    published = _run(tmp_path)
    directory = _generation(tmp_path)
    sealed = _seal(tmp_path)
    replay = _run(tmp_path, run_id="silver-002")

    assert published.status == "published"
    assert published.contract_version == "1.1.0"
    assert published.transform_version == "1"
    assert _counts(published) == (2, 2, 0, 2, 0, 0, 0)
    assert replay.status == "noop"
    assert replay.run_id == "silver-001"
    assert replay.observed_run_id == "silver-002"
    assert _seal(tmp_path) == sealed
    assert bronze == payload
    assert sorted(path.name for path in directory.iterdir()) == [
        "manifest.json",
        "quarantine.parquet",
        "silver.parquet",
    ]
    manifest = _manifest(tmp_path)
    assert manifest.storage_key == storage_key_for(SOURCE, VERSION, manifest.input_sha256)
    assert manifest.bronze_run_id == "bronze-001"
    assert manifest.created_at == "2024-06-01T12:00:00Z"
    assert manifest.parquet_writer == "duckdb"
    assert manifest.parquet_compression == "zstd"
    assert manifest.input_line_count == 2
    silver, quarantine = read_generation_parquet(
        directory / "silver.parquet",
        directory / "quarantine.parquet",
    )
    assert quarantine == ()
    assert [row[0] for row in silver] == ["trip-001", "trip-002"]
    assert silver[0][3] == Decimal("4.500")
    assert silver[0][3].as_tuple().exponent == -3
    assert silver[1][3] == Decimal("8.250")
    assert silver[1][1] == datetime(2024, 6, 1, 13, 0, tzinfo=UTC)
    assert silver[1][4] is None
    assert silver[0][13] == 1


def test_invalid_and_conflict_fixtures(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_invalid.jsonl"))
    rejected = _run(tmp_path)
    empty_silver, full_quarantine = read_generation_parquet(
        _generation(tmp_path) / "silver.parquet",
        _generation(tmp_path) / "quarantine.parquet",
    )

    assert _counts(rejected) == (14, 0, 14, 0, 0, 0, 14)
    assert empty_silver == ()
    assert len(full_quarantine) == 14
    assert full_quarantine[0][10] == ["blank_trip_id"]

    other = tmp_path / "other"
    _ingest(other, _fixture("trips_conflict.jsonl"))
    conflict = _run(other)
    silver, quarantine = read_generation_parquet(
        _generation(other) / "silver.parquet",
        _generation(other) / "quarantine.parquet",
    )
    assert _counts(conflict) == (4, 4, 0, 1, 1, 2, 2)
    assert silver[0][0] == "trip-100"
    assert silver[0][3] == Decimal("1.000")
    assert silver[0][13] == 2
    assert [row[6] for row in quarantine] == [3, 4]
    assert [row[10] for row in quarantine] == [["duplicate_conflict"], ["duplicate_conflict"]]
    assert all("2.500" not in str(item) for row in quarantine for item in row)


def test_duckdb_roundtrip_keeps_microseconds_and_decimal_scale(tmp_path: Path) -> None:
    payload = (
        b'{"trip_id":"trip-micro","pickup_at":"2024-06-01T07:00:00.123456-05:00",'
        b'"dropoff_at":"2024-06-01T12:10:00.123456Z","trip_distance":"1.230"}\n'
        b'{"trip_id":"trip-scale","pickup_at":"2024-06-01T12:00:00Z",'
        b'"dropoff_at":"2024-06-01T12:05:00Z","trip_distance":"1.5"}\n'
    )
    _ingest(tmp_path, payload)
    _run(tmp_path)
    silver, quarantine = read_generation_parquet(
        _generation(tmp_path) / "silver.parquet",
        _generation(tmp_path) / "quarantine.parquet",
    )

    assert quarantine == ()
    micro = silver[0]
    assert micro[1] == datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=UTC)
    assert micro[1].microsecond == 123456
    assert micro[2].microsecond == 123456
    assert micro[3] == Decimal("1.230")
    assert micro[3].as_tuple().exponent == -3
    assert silver[1][3] == Decimal("1.500")
    assert silver[1][3] == Decimal("1.5")
    connection = duckdb.connect(":memory:")
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(_generation(tmp_path) / "silver.parquet")],
        ).fetchall()
    finally:
        connection.close()
    types = {str(row[0]): str(row[1]) for row in described}
    assert types["trip_distance"] == "DECIMAL(18,3)"
    assert types["pickup_at"] == "TIMESTAMP"
    assert types["source_zone"] == "VARCHAR"


def test_empty_jsonl_and_parse_failures(tmp_path: Path) -> None:
    _ingest(tmp_path, b"")
    empty = _run(tmp_path)
    silver, quarantine = read_generation_parquet(
        _generation(tmp_path) / "silver.parquet",
        _generation(tmp_path) / "quarantine.parquet",
    )
    assert _counts(empty) == (0, 0, 0, 0, 0, 0, 0)
    assert silver == ()
    assert quarantine == ()

    broken = tmp_path / "broken"
    _ingest(broken, b"\n\xff\n{\n[]\n")
    parsed = _run(broken)
    _, rows = read_generation_parquet(
        _generation(broken) / "silver.parquet",
        _generation(broken) / "quarantine.parquet",
    )
    assert _counts(parsed) == (4, 0, 4, 0, 0, 0, 4)
    assert [row[10][0] for row in rows] == [
        "line_empty",
        "invalid_utf8",
        "invalid_json",
        "json_not_object",
    ]
    assert rows[1][7] == hashlib.sha256(b"\xff").hexdigest()


def test_dry_run_creates_nothing(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    before = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))

    result = _run(tmp_path, dry_run=True)

    after = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*"))
    assert result.dry_run is True
    assert result.status == "published"
    assert _counts(result) == (2, 2, 0, 2, 0, 0, 0)
    assert after == before
    assert not (tmp_path / "silver").exists()


def test_bronze_absent_and_corrupt_do_not_publish(tmp_path: Path) -> None:
    with pytest.raises(IngestFailure) as absent:
        _run(tmp_path)
    assert absent.value.code == "bronze_absent"
    assert absent.value.exit_code == 3
    assert not (tmp_path / "silver").exists()

    _ingest(tmp_path, b'{"trip_id":"trip-a"}\n')
    original = tmp_path / "bronze" / "sources" / SOURCE / VERSION / "original"
    original.write_bytes(b"mutated")
    with pytest.raises(IngestFailure) as mismatch:
        _run(tmp_path)
    assert mismatch.value.code == "integrity_mismatch"
    assert original.read_bytes() == b"mutated"
    assert not (tmp_path / "silver" / "generations").exists()

    original.write_bytes(b'{"trip_id":"trip-a"}\n')
    manifest = tmp_path / "bronze" / "sources" / SOURCE / VERSION / "manifest.json"
    manifest.write_bytes(b"{")
    with pytest.raises(IngestFailure) as incomplete:
        _run(tmp_path)
    assert incomplete.value.code == "integrity_incomplete"


def test_bronze_change_before_publish_keeps_the_generation_absent(tmp_path: Path) -> None:
    payload = _fixture("trips_valid.jsonl")
    _ingest(tmp_path, payload)
    original = tmp_path / "bronze" / "sources" / SOURCE / VERSION / "original"

    def mutate() -> None:
        original.write_bytes(payload + b"\n")

    with pytest.raises(IngestFailure) as caught:
        _run(tmp_path, TransformHooks(before_rename=mutate))

    assert caught.value.code == "bronze_unstable"
    assert not (tmp_path / "silver" / "generations").exists()
    assert list((tmp_path / "silver" / ".incoming").glob("*")) == []


def test_failure_before_and_after_rename(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))

    def explode() -> None:
        raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, TransformHooks(before_rename=explode))
    assert not (tmp_path / "silver" / "generations").exists()
    assert list((tmp_path / "silver" / ".incoming").glob("*")) == []

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, TransformHooks(after_rename=explode))
    sealed = _seal(tmp_path)
    replay = _run(tmp_path, run_id="silver-002")

    assert replay.status == "noop"
    assert _seal(tmp_path) == sealed


def test_semantic_conflict_does_not_replace_the_generation(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    _run(tmp_path)
    sealed = _seal(tmp_path)

    def alter(batch: LogicalBatch) -> LogicalBatch:
        changed = replace(batch.silver[0], trip_distance=Decimal("9.999"))
        return replace(batch, silver=(changed, *batch.silver[1:]))

    with pytest.raises(IngestFailure) as caught:
        _run(tmp_path, TransformHooks(alter_batch=alter), run_id="silver-002")

    assert caught.value.code == "transform_conflict"
    assert caught.value.exit_code == 3
    assert _seal(tmp_path) == sealed


def test_hash_and_schema_mismatch_keep_the_directory(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    _run(tmp_path)
    directory = _generation(tmp_path)
    silver = directory / "silver.parquet"
    original = silver.read_bytes()
    silver.write_bytes(original + b"x")
    with pytest.raises(IngestFailure) as mismatch:
        _run(tmp_path, run_id="silver-002")
    assert mismatch.value.code == "integrity_mismatch"
    assert silver.read_bytes() == original + b"x"

    silver.write_bytes(original)
    wrong = tmp_path / "wrong.parquet"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE wrong_shape (trip_id VARCHAR)")
        destination = wrong.as_posix().replace("'", "''")
        connection.execute(
            f"COPY wrong_shape TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.close()
    replacement = wrong.read_bytes()
    silver.write_bytes(replacement)
    manifest_path = directory / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["silver_sha256"] = hashlib.sha256(replacement).hexdigest()
    payload["silver_size_bytes"] = len(replacement)
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(IngestFailure) as schema:
        _run(tmp_path, run_id="silver-003")
    assert schema.value.code == "schema_mismatch"
    assert silver.read_bytes() == replacement


def test_path_limit_rejects_before_writing(tmp_path: Path) -> None:
    root = tmp_path / ("p" * 300)
    with pytest.raises(IngestFailure) as caught:
        transform(TransformSettings(data_root=root, clock=FixedClock()), _request())
    assert caught.value.code == "path_limit"
    assert caught.value.exit_code == 4
    assert ("p" * 300) not in {child.name for child in tmp_path.iterdir()}
    assert not (tmp_path / "silver").exists()


def test_symlink_on_the_generation_parent_is_rejected(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "silver" / "generations"
    parent.parent.mkdir(parents=True)
    try:
        parent.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is not permitted on this system")

    with pytest.raises(IngestFailure) as caught:
        _run(tmp_path)
    assert caught.value.code == "reparse_point"
    assert list(outside.iterdir()) == []


def test_junction_on_the_generation_parent_is_rejected(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("directory junctions are a Windows reparse point")
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "silver" / "generations"
    parent.parent.mkdir(parents=True)
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(parent), str(outside)],
        check=False,
        capture_output=True,
    )
    if created.returncode != 0:
        pytest.skip("junction creation is not permitted on this system")

    with pytest.raises(IngestFailure) as caught:
        _run(tmp_path)
    assert caught.value.code == "reparse_point"
    assert list(outside.iterdir()) == []


def test_lock_timeout_does_not_publish(tmp_path: Path) -> None:
    payload = _fixture("trips_valid.jsonl")
    _ingest(tmp_path, payload)
    digest = hashlib.sha256(payload).hexdigest()
    marker = tmp_path / "silver" / "locks" / f"{storage_key_for(SOURCE, VERSION, digest)}.lock"
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
        with pytest.raises(IngestFailure) as caught:
            transform(
                TransformSettings(data_root=tmp_path, clock=FixedClock(), lock_timeout_seconds=0.2),
                _request(),
            )
        assert caught.value.code == "lock_unavailable"
        assert caught.value.exit_code == 5
        assert not (tmp_path / "silver" / "generations").exists()
    finally:
        proc.wait(timeout=10)


def test_concurrent_processes_publish_once(tmp_path: Path) -> None:
    _ingest(tmp_path, _fixture("trips_valid.jsonl"))
    code = """
import json, sys
from pathlib import Path
from cfi.application.errors import IngestFailure
from cfi.application.transform import TransformRequest, TransformSettings, transform
settings = TransformSettings(data_root=Path(sys.argv[1]), lock_timeout_seconds=10)
request = TransformRequest(
    source_id="synthetic.trips.v1",
    dataset_version="v1",
    run_id=sys.argv[2],
)
try:
    result = transform(settings, request)
    print(json.dumps({"status": result.status}))
except IngestFailure as exc:
    print(json.dumps({"status": exc.code}))
    sys.exit(exc.exit_code)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(tmp_path), run_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for run_id in ("silver-a", "silver-b")
    ]
    outputs = []
    for proc in processes:
        stdout, stderr = proc.communicate(timeout=30)
        assert proc.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    assert sorted(item["status"] for item in outputs) == ["noop", "published"]
    assert len(list((tmp_path / "silver" / "generations").iterdir())) == 1


def test_reducer_counts_match_the_published_manifest(tmp_path: Path) -> None:
    payload = _fixture("trips_conflict.jsonl")
    _ingest(tmp_path, payload)
    result = _run(tmp_path)
    batch = reduce_bronze(payload)
    manifest = _manifest(tmp_path)

    assert result.input_line_count == batch.counts.input_line_count == manifest.input_line_count
    assert (
        manifest.silver_record_count
        + manifest.identical_duplicate_extra_count
        + (manifest.conflict_rejection_count)
        == manifest.valid_record_count
    )
    assert manifest.invalid_record_count + manifest.conflict_rejection_count == (
        manifest.quarantine_row_count
    )
