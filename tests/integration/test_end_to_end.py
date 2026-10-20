"""Public CLI commands, then the read-only HTTP summary."""

import hashlib
import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient
from pytest import CaptureFixture, MonkeyPatch

from cfi.domain.gold import GoldGeneration, pointer_key_for
from cfi.interfaces.cli import main
from cfi.interfaces.http import create_app

FIXTURES = Path("tests/fixtures/synthetic")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"


def _cli(
    root: Path,
    args: list[str],
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> tuple[int, dict[str, object]]:
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, dict)
    return code, payload


def _ingest(
    root: Path, name: str, run_id: str, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> tuple[int, dict[str, object]]:
    source = root / f"{run_id}.bin"
    source.write_bytes((FIXTURES / name).read_bytes())
    return _cli(
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
            run_id,
        ],
        monkeypatch,
        capsys,
    )


def _transform(
    root: Path, run_id: str, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> tuple[int, dict[str, object]]:
    return _cli(
        root,
        ["transform", "--source-id", SOURCE, "--dataset-version", VERSION, "--run-id", run_id],
        monkeypatch,
        capsys,
    )


def _gold(
    root: Path,
    sha: str,
    run_id: str,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
    *,
    promote: bool = False,
) -> tuple[int, dict[str, object]]:
    args = [
        "gold",
        "--source-id",
        SOURCE,
        "--dataset-version",
        VERSION,
        "--input-sha256",
        sha,
        "--run-id",
        run_id,
    ]
    if promote:
        args.append("--promote")
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


def _names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {child.name for child in path.iterdir()}


def _forbid_network(monkeypatch: MonkeyPatch) -> None:
    real_connect = socket.socket.connect

    def connect(self: socket.socket, address: object) -> object:
        host = address[0] if isinstance(address, tuple) and address else address
        if str(host) not in {"127.0.0.1", "::1", "localhost"}:
            raise AssertionError(f"network access: {address}")
        return real_connect(self, address)  # type: ignore[arg-type]

    monkeypatch.setattr(socket.socket, "connect", connect)


def test_cli_publication_replay_conflict_and_promotion(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    _forbid_network(monkeypatch)
    repo_data = Path("data")
    data_before = repo_data.exists()

    code, ingested = _ingest(tmp_path, "trips_valid.jsonl", "bronze-001", monkeypatch, capsys)
    assert code == 0
    assert ingested["status"] == "published"
    sha = ingested["input_sha256"]
    assert isinstance(sha, str)
    code, transformed = _transform(tmp_path, "silver-001", monkeypatch, capsys)
    assert code == 0
    assert transformed["status"] == "published"
    assert transformed["input_sha256"] == sha
    code, published = _gold(tmp_path, sha, "gold-001", monkeypatch, capsys)
    assert code == 0
    assert published["code"] == "published"
    storage_key = published["storage_key"]
    assert isinstance(storage_key, str)

    status, body = _summary(tmp_path)
    assert status == 200
    assert body["trip_count"] == 2
    assert body["total_distance_km"] == "12.750"
    assert body["average_duration_seconds"] == "1500.000000"
    assert body["rejected_count"] == 0
    assert body["input_sha256"] == sha
    assert body["storage_key"] == storage_key

    original = tmp_path / "bronze" / "sources" / SOURCE / VERSION / "original"
    summary = tmp_path / "gold" / "snapshots" / storage_key / "summary.json"
    manifest = summary.parent / "manifest.json"
    bronze_manifest = original.parent / "manifest.json"
    silver_parquet = (
        tmp_path / "silver" / "generations" / str(transformed["storage_key"]) / "silver.parquet"
    )
    sealed = {
        "original": original.read_bytes(),
        "bronze_manifest": bronze_manifest.read_bytes(),
        "summary": summary.read_bytes(),
        "manifest": manifest.read_bytes(),
        "silver": silver_parquet.read_bytes(),
    }
    generations = _names(tmp_path / "silver" / "generations")
    snapshots = _names(tmp_path / "gold" / "snapshots")
    GoldGeneration.model_validate_json(sealed["manifest"])

    code, replay_ingest = _ingest(tmp_path, "trips_valid.jsonl", "bronze-002", monkeypatch, capsys)
    assert code == 0
    assert replay_ingest["status"] == "noop"
    assert replay_ingest["input_sha256"] == sha
    code, replay_transform = _transform(tmp_path, "silver-002", monkeypatch, capsys)
    assert code == 0
    assert replay_transform["status"] == "noop"
    assert replay_transform["storage_key"] == transformed["storage_key"]
    code, replay_gold = _gold(tmp_path, sha, "gold-002", monkeypatch, capsys)
    assert code == 0
    assert replay_gold["code"] == "noop"
    assert replay_gold["pointer_state"] == "pointer_current"
    assert replay_gold["storage_key"] == storage_key
    assert _summary(tmp_path) == (200, body)
    assert original.read_bytes() == sealed["original"]
    assert bronze_manifest.read_bytes() == sealed["bronze_manifest"]
    assert summary.read_bytes() == sealed["summary"]
    assert manifest.read_bytes() == sealed["manifest"]
    assert silver_parquet.read_bytes() == sealed["silver"]
    assert _names(tmp_path / "silver" / "generations") == generations
    assert _names(tmp_path / "gold" / "snapshots") == snapshots

    code, rejected = _ingest(tmp_path, "trips_invalid.jsonl", "bronze-009", monkeypatch, capsys)
    assert code == 3
    assert rejected["code"] == "identity_conflict"
    assert original.read_bytes() == sealed["original"]

    # Artificial setup inside tmp_path only. The CLI rejects a second checksum
    # while Bronze remains, so this test deletes that temporary directory before
    # publishing again. Deleting Bronze is not a supported update or promotion.
    shutil.rmtree(tmp_path / "bronze")
    code, second = _ingest(tmp_path, "trips_invalid.jsonl", "bronze-010", monkeypatch, capsys)
    assert code == 0
    assert second["status"] == "published"
    second_sha = second["input_sha256"]
    assert isinstance(second_sha, str)
    assert second_sha != sha
    code, second_transform = _transform(tmp_path, "silver-010", monkeypatch, capsys)
    assert code == 0
    assert second_transform["input_sha256"] == second_sha
    pointer = tmp_path / "gold" / "published" / pointer_key_for(SOURCE, VERSION) / "current.json"
    pointer_bytes = pointer.read_bytes()
    code, conflict = _gold(tmp_path, second_sha, "gold-010", monkeypatch, capsys)
    assert code == 3
    assert conflict["code"] == "publication_conflict"
    assert conflict["pointer_replaced"] is False
    assert conflict["snapshot_durable"] is True
    assert pointer.read_bytes() == pointer_bytes
    assert summary.read_bytes() == sealed["summary"]
    assert manifest.read_bytes() == sealed["manifest"]
    assert _summary(tmp_path) == (200, body)

    code, promoted = _gold(tmp_path, second_sha, "gold-011", monkeypatch, capsys, promote=True)
    assert code == 0
    assert promoted["code"] == "promoted"
    assert promoted["pointer_replaced"] is True
    assert promoted["storage_key"] != storage_key
    status, moved = _summary(tmp_path)
    assert status == 200
    assert moved["trip_count"] == 0
    assert moved["total_distance_km"] == "0.000"
    assert moved["average_duration_seconds"] is None
    assert moved["rejected_count"] == 14
    assert moved["input_sha256"] == second_sha
    assert moved["storage_key"] == promoted["storage_key"]
    assert summary.read_bytes() == sealed["summary"]
    assert manifest.read_bytes() == sealed["manifest"]
    assert repo_data.exists() is data_before


def test_snapshot_without_pointer_is_not_published(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    code, ingested = _ingest(tmp_path, "trips_valid.jsonl", "bronze-001", monkeypatch, capsys)
    assert code == 0
    sha = ingested["input_sha256"]
    assert isinstance(sha, str)
    code, _transformed = _transform(tmp_path, "silver-001", monkeypatch, capsys)
    assert code == 0

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
                detail = proc.stderr.read() if proc.stderr is not None else ""
                raise AssertionError(detail or "holder exited")
            time.sleep(0.05)
        else:
            raise AssertionError("holder did not acquire the lock")
        monkeypatch.setenv("CFI_LOCK_TIMEOUT_SECONDS", "0.2")
        code, partial = _gold(tmp_path, sha, "gold-001", monkeypatch, capsys)
    finally:
        proc.wait(timeout=10)

    assert code == 5
    assert partial["status"] == "partial"
    assert partial["code"] == "lock_unavailable"
    assert partial["snapshot_state"] == "snapshot_published"
    assert partial["snapshot_durable"] is True
    assert partial["pointer_replaced"] is False
    storage_key = partial["storage_key"]
    assert isinstance(storage_key, str)
    snapshot = tmp_path / "gold" / "snapshots" / storage_key
    summary = (snapshot / "summary.json").read_bytes()
    manifest_bytes = (snapshot / "manifest.json").read_bytes()
    manifest = GoldGeneration.model_validate_json(manifest_bytes)
    assert manifest.summary_sha256 == hashlib.sha256(summary).hexdigest()
    assert manifest.summary_size_bytes == len(summary)
    assert manifest.storage_key == storage_key
    pointer = tmp_path / "gold" / "published" / pointer_key_for(SOURCE, VERSION) / "current.json"
    assert not pointer.exists()

    with TestClient(create_app(tmp_path)) as client:
        response = client.get(
            "/v1/trips/summary",
            params={"source_id": SOURCE, "dataset_version": VERSION},
        )
    assert response.status_code == 404
    assert response.json() == {"code": "not_published"}
    assert str(tmp_path) not in response.text
    assert "Traceback" not in response.text
    assert (snapshot / "summary.json").read_bytes() == summary
    assert (snapshot / "manifest.json").read_bytes() == manifest_bytes
