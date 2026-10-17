"""Bronze publication, replay, and failure behavior on the local filesystem."""

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cfi.application.errors import IngestFailure
from cfi.application.ingest import (
    Clock,
    IngestHooks,
    IngestRequest,
    IngestSettings,
    ingest,
)
from cfi.infrastructure.storage import StorageFailure, remove_tree_nofollow, rename_exclusive

FIXTURE = Path("tests/fixtures/synthetic/trips_invalid.jsonl")


class FixedClock(Clock):
    def now(self) -> datetime:
        return datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _settings(root: Path, **kwargs: object) -> IngestSettings:
    return IngestSettings(data_root=root, clock=FixedClock(), **kwargs)  # type: ignore[arg-type]


def _request(path: Path, **kwargs: object) -> IngestRequest:
    values: dict[str, object] = {
        "input_path": path,
        "source_id": "synthetic.trips.v1",
        "dataset_version": "v1",
        "run_id": "run-001",
    }
    values.update(kwargs)
    return IngestRequest(**values)  # type: ignore[arg-type]


def _artifact(root: Path, source: str = "synthetic.trips.v1", version: str = "v1") -> Path:
    return root / "bronze" / "sources" / source / version


def test_publish_preserves_crlf_and_bare_cr(tmp_path: Path) -> None:
    payload = b"line\nline\r\nline\r"
    source = tmp_path / "input.bin"
    source.write_bytes(payload)
    result = ingest(_settings(tmp_path), _request(source))
    stored = (_artifact(tmp_path) / "original").read_bytes()

    assert result.status == "published"
    assert stored == payload


def test_publish_preserves_invalid_fixture_bytes(tmp_path: Path) -> None:
    payload = FIXTURE.read_bytes()
    source = tmp_path / "input.jsonl"
    source.write_bytes(payload)
    result = ingest(_settings(tmp_path), _request(source))
    stored = (_artifact(tmp_path) / "original").read_bytes()
    manifest = json.loads((_artifact(tmp_path) / "manifest.json").read_text(encoding="utf-8"))

    assert result.status == "published"
    assert stored == payload
    assert manifest["input_sha256"] == result.input_sha256
    assert manifest["input_size_bytes"] == len(payload)
    assert manifest["run_id"] == "run-001"
    assert manifest["created_at"] == "2024-06-01T12:00:00Z"
    assert manifest["status"] == "published"
    assert set(manifest) == {
        "created_at",
        "dataset_version",
        "input_sha256",
        "input_size_bytes",
        "manifest_version",
        "run_id",
        "source_id",
        "status",
    }


def test_replay_does_not_change_manifest_or_bytes(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"same-bytes")
    first = ingest(_settings(tmp_path), _request(source))
    before = (_artifact(tmp_path) / "manifest.json").read_bytes()
    original = (_artifact(tmp_path) / "original").read_bytes()
    second = ingest(_settings(tmp_path), _request(source, run_id="run-002"))

    assert first.status == "published"
    assert second.status == "noop"
    assert second.run_id == "run-001"
    assert second.observed_run_id == "run-002"
    assert (_artifact(tmp_path) / "manifest.json").read_bytes() == before
    assert (_artifact(tmp_path) / "original").read_bytes() == original


def test_changed_bytes_conflict_and_keep_the_original(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"first")
    ingest(_settings(tmp_path), _request(source))
    source.write_bytes(b"second")
    with pytest.raises(IngestFailure) as caught:
        ingest(_settings(tmp_path), _request(source, run_id="run-002"))

    assert caught.value.code == "identity_conflict"
    assert caught.value.exit_code == 3
    assert (_artifact(tmp_path) / "original").read_bytes() == b"first"


def test_new_version_and_new_source_are_independent(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"shared")
    ingest(_settings(tmp_path), _request(source))
    other_version = ingest(
        _settings(tmp_path), _request(source, dataset_version="v2", run_id="run-002")
    )
    other_source = ingest(
        _settings(tmp_path),
        _request(source, source_id="other.source", run_id="run-003"),
    )

    assert other_version.status == "published"
    assert other_source.status == "published"
    assert (_artifact(tmp_path, version="v2") / "original").read_bytes() == b"shared"
    assert (_artifact(tmp_path, source="other.source") / "original").read_bytes() == b"shared"


def test_empty_input_is_published(tmp_path: Path) -> None:
    source = tmp_path / "empty.bin"
    source.write_bytes(b"")
    result = ingest(_settings(tmp_path), _request(source))

    assert result.status == "published"
    assert result.input_size_bytes == 0
    assert (_artifact(tmp_path) / "original").read_bytes() == b""


def test_dry_run_creates_nothing(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    data = tmp_path / "data"
    before = list(data.rglob("*")) if data.exists() else []
    result = ingest(_settings(data), _request(source, dry_run=True))

    assert result.status == "published"
    assert result.dry_run is True
    assert list(data.rglob("*")) == before
    assert not data.exists()


def test_existing_empty_directory_is_not_replaced(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    final = _artifact(tmp_path)
    final.mkdir(parents=True)
    with pytest.raises(IngestFailure) as caught:
        ingest(_settings(tmp_path), _request(source))

    assert caught.value.code == "integrity_incomplete"
    assert list(final.iterdir()) == []


def test_rename_exclusive_refuses_empty_destination(tmp_path: Path) -> None:
    source = tmp_path / "incoming"
    destination = tmp_path / "final"
    source.mkdir()
    (source / "original").write_bytes(b"abc")
    destination.mkdir()

    with pytest.raises(FileExistsError):
        rename_exclusive(source, destination)

    assert (source / "original").read_bytes() == b"abc"
    assert list(destination.iterdir()) == []


def test_posix_rename_replaces_an_empty_directory(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Windows os.rename already refuses an empty destination")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    (source / "marker").write_bytes(b"new")
    destination.mkdir()

    os.rename(source, destination)

    assert (destination / "marker").read_bytes() == b"new"


def test_cleanup_refuses_to_follow_a_symlink(tmp_path: Path) -> None:
    ancestor = tmp_path / "incoming"
    temp = ancestor / "run-temp"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_bytes(b"keep")
    temp.mkdir(parents=True)
    (temp / "local").write_bytes(b"local")
    try:
        (temp / "link").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is not permitted on this system")

    with pytest.raises(StorageFailure) as rejected:
        remove_tree_nofollow(temp, ancestor)

    assert rejected.value.code == "reparse_point"
    assert (outside / "keep").read_bytes() == b"keep"
    assert (temp / "local").read_bytes() == b"local"


def test_failure_before_rename_leaves_no_final(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")

    def fail() -> None:
        raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        ingest(_settings(tmp_path), _request(source), IngestHooks(before_rename=fail))

    assert not _artifact(tmp_path).exists()
    assert list((tmp_path / "bronze" / ".incoming").glob("*")) == []


def test_failure_after_rename_keeps_a_verifiable_artifact(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")

    def fail() -> None:
        raise RuntimeError("after")

    with pytest.raises(RuntimeError, match="after"):
        ingest(_settings(tmp_path), _request(source), IngestHooks(after_rename=fail))
    replay = ingest(_settings(tmp_path), _request(source, run_id="run-002"))

    assert (_artifact(tmp_path) / "original").read_bytes() == b"payload"
    assert replay.status == "noop"


def test_corrupt_original_and_manifest_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    ingest(_settings(tmp_path), _request(source))
    original = _artifact(tmp_path) / "original"
    original.write_bytes(b"mutated")
    with pytest.raises(IngestFailure) as mismatch:
        ingest(_settings(tmp_path), _request(source, run_id="run-002"))
    assert mismatch.value.code == "integrity_mismatch"
    assert original.read_bytes() == b"mutated"

    original.write_bytes(b"payload")
    manifest = _artifact(tmp_path) / "manifest.json"
    manifest.write_bytes(b"{")
    with pytest.raises(IngestFailure) as incomplete:
        ingest(_settings(tmp_path), _request(source, run_id="run-003"))
    assert incomplete.value.code == "integrity_incomplete"
    assert manifest.read_bytes() == b"{"


def test_unstable_input_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"stable")

    def mutate() -> None:
        source.write_bytes(b"changed")

    with pytest.raises(IngestFailure) as caught:
        ingest(_settings(tmp_path), _request(source), IngestHooks(after_copy=mutate))

    assert caught.value.code == "input_unstable"
    assert not _artifact(tmp_path).exists()


def test_oversized_input_and_bad_identifier(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"abcdef")
    with pytest.raises(IngestFailure) as large:
        ingest(_settings(tmp_path, max_input_bytes=3), _request(source))
    assert large.value.exit_code == 2
    assert large.value.code == "input_too_large"

    with pytest.raises(IngestFailure) as escaped:
        ingest(_settings(tmp_path), _request(source, source_id="../outside"))
    assert escaped.value.code == "invalid_identifier"
    assert not (tmp_path / "outside").exists()


def test_missing_and_symlink_input(tmp_path: Path) -> None:
    missing = tmp_path / "missing.bin"
    with pytest.raises(IngestFailure) as not_found:
        ingest(_settings(tmp_path), _request(missing))
    assert not_found.value.code == "input_not_found"

    target = tmp_path / "target.bin"
    target.write_bytes(b"x")
    link = tmp_path / "link.bin"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is not permitted on this system")
    with pytest.raises(IngestFailure) as linked:
        ingest(_settings(tmp_path), _request(link))
    assert linked.value.code == "input_not_regular"


def test_junction_on_the_write_path_is_rejected(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("directory junctions are a Windows reparse point")
    root = tmp_path / "data"
    target = root / "bronze" / "aliased"
    target.mkdir(parents=True)
    link = root / "bronze" / "sources"
    try:
        created = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
        )
    except FileNotFoundError:
        pytest.skip("junction creation is not permitted on this system")
    if created.returncode != 0:
        pytest.skip("junction creation is not permitted on this system")
    source = tmp_path / "input.bin"
    source.write_bytes(b"abc")
    with pytest.raises(IngestFailure) as rejected:
        ingest(_settings(root), _request(source))
    assert rejected.value.code == "reparse_point"
    assert list(target.iterdir()) == []
