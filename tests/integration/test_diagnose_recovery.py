"""Partial Gold publication, read-only diagnosis, and pointer recovery."""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient
from pytest import CaptureFixture, MonkeyPatch

from cfi.domain.gold import gold_storage_key_for, pointer_key_for
from cfi.interfaces.cli import main
from cfi.interfaces.http import create_app

FIXTURES = Path("tests/fixtures/synthetic")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"
OTHER = "ab" * 32


def _cli(
    root: Path,
    args: list[str],
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> tuple[int, dict[str, object], str]:
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    code = main(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert isinstance(payload, dict)
    return code, payload, captured.err


def _events(stderr: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in stderr.splitlines() if line.startswith("{")]


def _tree(root: Path) -> dict[str, bytes]:
    found: dict[str, bytes] = {}
    for path in root.rglob("*"):
        if path.is_file():
            found[path.relative_to(root).as_posix()] = path.read_bytes()
    return found


def _published(tree: dict[str, bytes]) -> dict[str, bytes]:
    return {key: value for key, value in tree.items() if "/locks/" not in f"/{key}"}


def _ingest(
    root: Path, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> tuple[int, dict[str, object]]:
    source = root / "input.bin"
    source.write_bytes((FIXTURES / "trips_valid.jsonl").read_bytes())
    code, payload, _stderr = _cli(
        root,
        [
            "ingest",
            "--input",
            str(source),
            "--source-id",
            SOURCE,
            "--dataset-version",
            VERSION,
            "--run-id",
            "bronze-001",
        ],
        monkeypatch,
        capsys,
    )
    return code, payload


def _transform(
    root: Path, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> tuple[int, dict[str, object]]:
    code, payload, _stderr = _cli(
        root,
        [
            "transform",
            "--source-id",
            SOURCE,
            "--dataset-version",
            VERSION,
            "--run-id",
            "silver-001",
        ],
        monkeypatch,
        capsys,
    )
    return code, payload


def _gold(
    root: Path,
    sha: str,
    run_id: str,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> tuple[int, dict[str, object]]:
    code, payload, _stderr = _cli(
        root,
        [
            "gold",
            "--source-id",
            SOURCE,
            "--dataset-version",
            VERSION,
            "--input-sha256",
            sha,
            "--run-id",
            run_id,
        ],
        monkeypatch,
        capsys,
    )
    return code, payload


def _diagnose(
    root: Path,
    sha: str | None,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> tuple[int, dict[str, object], str]:
    args = ["diagnose", "--source-id", SOURCE, "--dataset-version", VERSION]
    if sha is not None:
        args.extend(["--input-sha256", sha])
    return _cli(root, args, monkeypatch, capsys)


def _summary(root: Path) -> tuple[int, dict[str, object]]:
    with TestClient(create_app(root)) as client:
        response = client.get(
            "/v1/trips/summary",
            params={"source_id": SOURCE, "dataset_version": VERSION},
        )
    body = response.json()
    assert isinstance(body, dict)
    return response.status_code, body


def _hold_pointer_lock(root: Path) -> subprocess.Popen[str]:
    marker = root / "gold" / "locks" / f"pointer-{pointer_key_for(SOURCE, VERSION)}.lock"
    ready = root / "ready"
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
    for _ in range(100):
        if ready.exists():
            return proc
        if proc.poll() is not None:
            detail = proc.stderr.read() if proc.stderr is not None else ""
            raise AssertionError(detail or "holder exited")
        time.sleep(0.05)
    proc.kill()
    raise AssertionError("holder did not acquire the lock")


def test_partial_snapshot_is_diagnosed_and_recovered_without_a_rewrite(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    code, ingested = _ingest(tmp_path, monkeypatch, capsys)
    assert code == 0
    sha = ingested["input_sha256"]
    assert isinstance(sha, str)
    code, _transformed = _transform(tmp_path, monkeypatch, capsys)
    assert code == 0

    holder = _hold_pointer_lock(tmp_path)
    try:
        monkeypatch.setenv("CFI_LOCK_TIMEOUT_SECONDS", "0.2")
        code, partial = _gold(tmp_path, sha, "gold-001", monkeypatch, capsys)
    finally:
        holder.wait(timeout=10)
    assert code == 5
    assert partial["status"] == "partial"
    assert partial["code"] == "lock_unavailable"
    assert partial["snapshot_durable"] is True
    assert partial["pointer_replaced"] is False
    storage_key = partial["storage_key"]
    assert isinstance(storage_key, str)
    snapshot = tmp_path / "gold" / "snapshots" / storage_key
    summary = (snapshot / "summary.json").read_bytes()
    manifest = (snapshot / "manifest.json").read_bytes()
    pointer = tmp_path / "gold" / "published" / pointer_key_for(SOURCE, VERSION) / "current.json"
    assert not pointer.exists()

    before = _tree(tmp_path)
    monkeypatch.setenv("CFI_LOCK_TIMEOUT_SECONDS", "30")
    code, report, stderr = _diagnose(tmp_path, sha, monkeypatch, capsys)
    assert code == 0
    assert report["status"] == "reported"
    assert report["bronze_state"] == "intact"
    assert report["silver_state"] == "intact"
    assert report["snapshot_state"] == "intact"
    assert report["pointer_state"] == "absent"
    assert report["snapshot_storage_key"] == storage_key
    assert "same_generation" not in report
    assert _tree(tmp_path) == before
    assert sha not in stderr
    assert str(tmp_path) not in stderr
    events = _events(stderr)
    assert [event["event_name"] for event in events] == ["command_started", "command_finished"]
    assert events[1]["stage"] == "diagnose"
    assert events[1]["status"] == "succeeded"

    code, recovered = _gold(tmp_path, sha, "gold-002", monkeypatch, capsys)
    assert code == 0
    assert recovered["code"] == "published"
    assert recovered["pointer_state"] == "pointer_created"
    assert recovered["pointer_replaced"] is True
    after = _published(_tree(tmp_path))
    created = set(after) - set(_published(before))
    assert created == {pointer.relative_to(tmp_path).as_posix()}
    assert (snapshot / "summary.json").read_bytes() == summary
    assert (snapshot / "manifest.json").read_bytes() == manifest
    pointer_bytes = pointer.read_bytes()

    status, body = _summary(tmp_path)
    assert status == 200
    assert body["trip_count"] == 2
    assert body["total_distance_km"] == "12.750"
    assert body["average_duration_seconds"] == "1500.000000"
    assert body["rejected_count"] == 0

    code, replay = _gold(tmp_path, sha, "gold-003", monkeypatch, capsys)
    assert code == 0
    assert replay["code"] == "noop"
    assert replay["pointer_replaced"] is False
    assert pointer.read_bytes() == pointer_bytes
    assert (snapshot / "summary.json").read_bytes() == summary
    assert (snapshot / "manifest.json").read_bytes() == manifest


def test_an_intact_pointer_and_another_intact_snapshot_are_different_generations(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    """Generation A stays published. Generation B is a second intact snapshot.

    The CLI rejects a second checksum while Bronze remains. This test deletes
    that directory inside tmp_path before publishing again. Deleting Bronze is
    test setup, not a supported update or promotion.
    """

    code, ingested = _ingest(tmp_path, monkeypatch, capsys)
    assert code == 0
    sha_a = ingested["input_sha256"]
    assert isinstance(sha_a, str)
    code, _transformed = _transform(tmp_path, monkeypatch, capsys)
    assert code == 0
    code, published = _gold(tmp_path, sha_a, "gold-001", monkeypatch, capsys)
    assert code == 0
    key_a = published["storage_key"]
    assert isinstance(key_a, str)
    pointer = tmp_path / "gold" / "published" / pointer_key_for(SOURCE, VERSION) / "current.json"
    pointer_bytes = pointer.read_bytes()
    snapshot_a = tmp_path / "gold" / "snapshots" / key_a
    summary_a = (snapshot_a / "summary.json").read_bytes()
    manifest_a = (snapshot_a / "manifest.json").read_bytes()

    shutil.rmtree(tmp_path / "bronze")
    source = tmp_path / "second.bin"
    source.write_bytes((FIXTURES / "trips_invalid.jsonl").read_bytes())
    code, second, _stderr = _cli(
        tmp_path,
        [
            "ingest",
            "--input",
            str(source),
            "--source-id",
            SOURCE,
            "--dataset-version",
            VERSION,
            "--run-id",
            "bronze-010",
        ],
        monkeypatch,
        capsys,
    )
    assert code == 0
    sha_b = second["input_sha256"]
    assert isinstance(sha_b, str)
    assert sha_b != sha_a
    code, _second_transform, _stderr = _cli(
        tmp_path,
        [
            "transform",
            "--source-id",
            SOURCE,
            "--dataset-version",
            VERSION,
            "--run-id",
            "silver-010",
        ],
        monkeypatch,
        capsys,
    )
    assert code == 0
    code, conflict = _gold(tmp_path, sha_b, "gold-010", monkeypatch, capsys)
    assert code == 3
    assert conflict["code"] == "publication_conflict"
    assert conflict["pointer_replaced"] is False
    key_b = conflict["storage_key"]
    assert isinstance(key_b, str)
    assert key_b != key_a
    assert key_b == gold_storage_key_for(SOURCE, VERSION, sha_b)
    assert pointer.read_bytes() == pointer_bytes

    before = _tree(tmp_path)
    code, report, stderr = _diagnose(tmp_path, sha_b, monkeypatch, capsys)
    assert code == 0
    assert report["pointer_state"] == "intact"
    assert report["snapshot_state"] == "intact"
    assert report["same_generation"] is False
    assert report["published_storage_key"] == key_a
    assert report["snapshot_storage_key"] == key_b
    assert _tree(tmp_path) == before
    assert pointer.read_bytes() == pointer_bytes
    assert (snapshot_a / "summary.json").read_bytes() == summary_a
    assert (snapshot_a / "manifest.json").read_bytes() == manifest_a
    assert sha_b not in stderr
    assert str(tmp_path) not in stderr


def test_another_generation_and_a_divergent_snapshot_are_reported(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    code, ingested = _ingest(tmp_path, monkeypatch, capsys)
    assert code == 0
    sha = ingested["input_sha256"]
    assert isinstance(sha, str)
    code, transformed = _transform(tmp_path, monkeypatch, capsys)
    assert code == 0
    code, missing, _stderr = _diagnose(tmp_path, sha, monkeypatch, capsys)
    assert code == 0
    assert missing["silver_state"] == "intact"
    assert missing["snapshot_state"] == "absent"
    assert missing["pointer_state"] == "absent"

    code, published = _gold(tmp_path, sha, "gold-001", monkeypatch, capsys)
    assert code == 0
    code, other, _stderr = _diagnose(tmp_path, OTHER, monkeypatch, capsys)
    assert code == 0
    assert other["pointer_state"] == "intact"
    assert other["snapshot_state"] == "absent"
    assert other["published_storage_key"] == published["storage_key"]
    assert other["published_storage_key"] != gold_storage_key_for(SOURCE, VERSION, OTHER)
    assert "same_generation" not in other
    assert "snapshot_storage_key" not in other

    code, same, _stderr = _diagnose(tmp_path, sha, monkeypatch, capsys)
    assert code == 0
    assert same["pointer_state"] == "intact"
    assert same["snapshot_state"] == "intact"
    assert same["silver_state"] == "intact"
    assert same["same_generation"] is True

    silver_key = transformed["storage_key"]
    assert isinstance(silver_key, str)
    manifest_path = tmp_path / "silver" / "generations" / silver_key / "manifest.json"
    silver_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    silver_manifest["silver_sha256"] = "ab" * 32
    manifest_path.write_text(json.dumps(silver_manifest), encoding="utf-8")
    code, mismatched, _stderr = _diagnose(tmp_path, sha, monkeypatch, capsys)
    assert code == 0
    assert mismatched["silver_state"] == "mismatch"
    assert mismatched["snapshot_state"] == "intact"
    assert mismatched["pointer_state"] == "intact"

    storage_key = published["storage_key"]
    assert isinstance(storage_key, str)
    summary_path = tmp_path / "gold" / "snapshots" / storage_key / "summary.json"
    summary_document = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_document["rejected_count"] = int(summary_document["rejected_count"]) + 1
    summary_path.write_bytes(
        (json.dumps(summary_document, indent=2, sort_keys=True) + "\n").encode()
    )
    code, divergent, stderr = _diagnose(tmp_path, sha, monkeypatch, capsys)
    assert code == 0
    assert divergent["status"] == "reported"
    assert divergent["snapshot_state"] == "mismatch"
    assert divergent["pointer_state"] == "integrity_mismatch"
    assert "Traceback" not in stderr
    assert str(tmp_path) not in stderr
