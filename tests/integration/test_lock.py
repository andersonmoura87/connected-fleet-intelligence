"""Process-level Bronze lock behavior on this operating system."""

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from cfi.application.errors import IngestFailure
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
from cfi.infrastructure.lock import lock_path_for, source_lock

_HOLD = """
import sys
import time
from pathlib import Path

from cfi.infrastructure.lock import source_lock

path = Path(sys.argv[1])
ready = Path(sys.argv[2])
seconds = float(sys.argv[3])
with source_lock(path, timeout_seconds=5):
    ready.write_text("held", encoding="ascii")
    time.sleep(seconds)
"""


def _hold(path: Path, ready: Path, seconds: float) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", _HOLD, str(path), str(ready), str(seconds)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_ready(ready: Path, proc: subprocess.Popen[str]) -> None:
    for _ in range(100):
        if ready.exists():
            return
        if proc.poll() is not None:
            raise AssertionError(proc.stderr.read() if proc.stderr else "holder exited")
        time.sleep(0.05)
    raise AssertionError("holder did not acquire the lock")


def test_abrupt_exit_releases_the_os_lock(tmp_path: Path) -> None:
    marker = lock_path_for(tmp_path, "synthetic.trips.v1", "v1")
    ready = tmp_path / "ready"
    proc = _hold(marker, ready, 30)
    _wait_ready(ready, proc)
    proc.kill()
    proc.wait(timeout=10)

    with source_lock(marker, timeout_seconds=5):
        held = True
    assert held
    assert marker.exists()


def test_timeout_does_not_steal_a_live_lock(tmp_path: Path) -> None:
    marker = lock_path_for(tmp_path, "synthetic.trips.v1", "v1")
    ready = tmp_path / "ready"
    proc = _hold(marker, ready, 2)
    _wait_ready(ready, proc)
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    try:
        with pytest.raises(IngestFailure) as caught:
            ingest(
                IngestSettings(data_root=tmp_path, lock_timeout_seconds=0.4),
                IngestRequest(
                    input_path=source,
                    source_id="synthetic.trips.v1",
                    dataset_version="v1",
                    run_id="run-001",
                ),
            )
        assert caught.value.code == "lock_unavailable"
        assert caught.value.exit_code == 5
        assert not (tmp_path / "bronze" / "sources").exists()
    finally:
        proc.wait(timeout=10)
    assert marker.exists()
    published = ingest(
        IngestSettings(data_root=tmp_path, lock_timeout_seconds=5),
        IngestRequest(
            input_path=source,
            source_id="synthetic.trips.v1",
            dataset_version="v1",
            run_id="run-001",
        ),
    )
    assert published.status == "published"


def test_distinct_sources_are_not_blocked(tmp_path: Path) -> None:
    marker = lock_path_for(tmp_path, "held.source", "v1")
    ready = tmp_path / "ready"
    proc = _hold(marker, ready, 2)
    _wait_ready(ready, proc)
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    try:
        result = ingest(
            IngestSettings(data_root=tmp_path, lock_timeout_seconds=2),
            IngestRequest(
                input_path=source,
                source_id="other.source",
                dataset_version="v1",
                run_id="run-001",
            ),
        )
    finally:
        proc.wait(timeout=10)
    assert result.status == "published"


def test_concurrent_processes_publish_once(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"same-bytes")
    code = """
import json, sys
from pathlib import Path
from cfi.application.errors import IngestFailure
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
settings = IngestSettings(data_root=Path(sys.argv[1]), lock_timeout_seconds=10)
request = IngestRequest(
    input_path=Path(sys.argv[2]),
    source_id="synthetic.trips.v1",
    dataset_version="v1",
    run_id=sys.argv[3],
)
try:
    result = ingest(settings, request)
    print(json.dumps({"status": result.status}))
except IngestFailure as exc:
    print(json.dumps({"status": exc.code}))
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(tmp_path), str(source), run_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for run_id in ("run-a", "run-b")
    ]
    outputs = []
    for proc in processes:
        stdout, stderr = proc.communicate(timeout=30)
        assert proc.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    assert sorted(item["status"] for item in outputs) == ["noop", "published"]
    assert (
        tmp_path / "bronze" / "sources" / "synthetic.trips.v1" / "v1" / "original"
    ).read_bytes() == (b"same-bytes")
