"""Operational events around the existing CLI and HTTP results."""

import json
import subprocess
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient
from pytest import CaptureFixture, MonkeyPatch

from cfi.domain.gold import pointer_key_for
from cfi.interfaces.cli import main
from cfi.interfaces.http import create_app

_SOURCE = "synthetic.trips.v1"
_VERSION = "v1"
_FIXTURE = Path("tests/fixtures/synthetic/trips_valid.jsonl")


def test_ingest_preserves_stdout_and_correlates_one_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))
    source = tmp_path / "input.bin"
    source.write_bytes(b"observed")
    code = main(_ingest(source, "run-001"))
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    events = _events(captured.err)

    assert code == 0
    assert payload["status"] == "published"
    assert payload["run_id"] == "run-001"
    assert "event_name" not in payload
    assert [event["event_name"] for event in events] == ["command_started", "command_finished"]
    assert events[0]["run_id"] == events[1]["run_id"] == "run-001"
    assert events[1]["status"] == "succeeded"
    assert events[1]["duration_ms"] >= 0
    assert payload["input_sha256"] not in captured.err
    assert str(tmp_path) not in captured.err
    assert source.name not in captured.err


def test_replay_is_noop_and_dry_run_writes_nothing(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    source = tmp_path / "input.bin"
    source.write_bytes(b"observed")

    assert main(_ingest(source, "run-001", dry_run=True)) == 0
    dry_out, dry_err = _split(capsys)
    dry_payload = json.loads(dry_out)
    dry_events = _events(dry_err)
    assert dry_payload["dry_run"] is True
    assert dry_payload["status"] == "published"
    assert dry_events[1]["status"] == "succeeded"
    assert dry_events[1]["dry_run"] is True
    assert not root.exists()

    assert main(_ingest(source, "run-001")) == 0
    capsys.readouterr()
    before = _files(root)
    assert main(_ingest(source, "run-002")) == 0
    replay_out, replay_err = _split(capsys)
    replay = json.loads(replay_out)
    replay_events = _events(replay_err)
    assert replay["status"] == "noop"
    assert replay_events[0]["run_id"] == replay_events[1]["run_id"] == "run-002"
    assert replay_events[1]["status"] == "noop"
    assert _files(root) == before


def test_classified_failure_keeps_the_stdout_code(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))
    code = main(
        [
            "transform",
            "--source-id",
            _SOURCE,
            "--dataset-version",
            _VERSION,
            "--run-id",
            "silver-missing",
        ]
    )
    captured = capsys.readouterr()
    assert code == 3
    assert json.loads(captured.out) == {
        "code": "bronze_absent",
        "dry_run": False,
        "exit_code": 3,
        "status": "rejected",
    }
    finished = _events(captured.err)[1]
    assert finished["status"] == "rejected"
    assert finished["error_code"] == "bronze_absent"
    assert finished["exit_code"] == 3
    assert not (tmp_path / "data").exists()


def test_unexpected_error_emits_a_terminal_event_and_propagates(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))

    def explode(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(r"C:\secret\trips.jsonl")

    monkeypatch.setattr("cfi.application.ingest.ingest", explode)
    try:
        main(_ingest(tmp_path / "input.bin", "run-001"))
    except RuntimeError as exc:
        assert "secret" in str(exc)
    else:
        raise AssertionError("unexpected error was swallowed")
    captured = capsys.readouterr()
    assert captured.out == ""
    events = _events(captured.err)
    assert [event["event_name"] for event in events] == ["command_started", "command_finished"]
    assert events[1]["status"] == "rejected"
    assert "error_code" not in events[1]
    assert "secret" not in captured.err
    assert "Traceback" not in captured.err
    assert not (tmp_path / "data").exists()


def test_gold_partial_publication_is_visible_on_stderr(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    source = tmp_path / "trips_valid.jsonl"
    source.write_bytes(_FIXTURE.read_bytes())
    assert main(_ingest(source, "bronze-001")) == 0
    sha = json.loads(capsys.readouterr().out)["input_sha256"]
    assert isinstance(sha, str)
    assert main(_transform("silver-001")) == 0
    capsys.readouterr()

    marker = root / "gold" / "locks" / f"pointer-{pointer_key_for(_SOURCE, _VERSION)}.lock"
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
        code = main(_gold(sha, "gold-001"))
    finally:
        proc.wait(timeout=10)

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    finished = _events(captured.err)[1]
    assert code == 5
    assert payload["status"] == "partial"
    assert payload["code"] == "lock_unavailable"
    assert payload["snapshot_durable"] is True
    assert payload["pointer_replaced"] is False
    assert finished["status"] == "partial"
    assert finished["error_code"] == "lock_unavailable"
    assert finished["exit_code"] == 5
    assert finished["snapshot_state"] == "snapshot_published"
    assert finished["snapshot_durable"] is True
    assert finished["pointer_replaced"] is False
    assert finished["run_id"] == "gold-001"
    assert sha not in captured.err
    assert not (root / "gold" / "published").exists()


def test_http_events_cover_success_and_classified_failures(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    root = tmp_path / "data"
    monkeypatch.setenv("CFI_DATA_ROOT", str(root))
    source = tmp_path / "trips_valid.jsonl"
    source.write_bytes(_FIXTURE.read_bytes())
    assert main(_ingest(source, "bronze-001")) == 0
    sha = json.loads(capsys.readouterr().out)["input_sha256"]
    assert main(_transform("silver-001")) == 0
    capsys.readouterr()
    assert isinstance(sha, str)
    assert main(_gold(sha, "gold-001")) == 0
    capsys.readouterr()
    before = _files(root)

    with TestClient(create_app(root)) as client:
        live = client.get("/health/live")
        missing = client.get("/v1/trips/summary")
        invalid = client.get(
            "/v1/trips/summary",
            params={"source_id": "../secret", "dataset_version": _VERSION, "token": "secret"},
        )
        absent = client.get(
            "/v1/trips/summary",
            params={"source_id": "other.source", "dataset_version": _VERSION},
        )
        summary = client.get(
            "/v1/trips/summary",
            params={"source_id": _SOURCE, "dataset_version": _VERSION, "token": "secret"},
        )
        unknown = client.get("/private/secret-token?token=secret")
    captured = capsys.readouterr()

    assert live.status_code == 200
    assert live.json() == {"status": "live"}
    assert missing.status_code == 422
    assert missing.json() == {"code": "missing_parameter"}
    assert invalid.status_code == 422
    assert invalid.json() == {"code": "invalid_identifier"}
    assert absent.status_code == 404
    assert absent.json() == {"code": "not_published"}
    assert summary.status_code == 200
    assert summary.json()["trip_count"] == 2
    assert unknown.status_code == 404
    assert _files(root) == before
    assert not (tmp_path / "data-other").exists()

    events = _events(captured.err)
    assert [event["event_name"] for event in events] == ["http_request"] * len(events)
    assert len(events) == 6
    by_route = [(event["route"], event["http_status"], event.get("error_code")) for event in events]
    assert by_route == [
        ("/health/live", 200, None),
        ("/v1/trips/summary", 422, "missing_parameter"),
        ("/v1/trips/summary", 422, "invalid_identifier"),
        ("/v1/trips/summary", 404, "not_published"),
        ("/v1/trips/summary", 200, None),
        ("unmatched", 404, None),
    ]
    rendered = captured.err
    assert "token" not in rendered
    assert "secret" not in rendered
    assert "query" not in rendered
    assert sha not in rendered
    assert "12.750" not in rendered
    assert str(root) not in rendered
    assert events[4]["source_id"] == _SOURCE
    assert events[4]["dataset_version"] == _VERSION
    assert "source_id" not in events[2]
    assert all(event["duration_ms"] >= 0 for event in events)
    assert all(event["stage"] == "serve" for event in events)


def test_invalid_pointer_is_http_503_without_a_query_echo(
    tmp_path: Path,
    capsys: CaptureFixture[str],
) -> None:
    pointer = tmp_path / "gold" / "published" / pointer_key_for(_SOURCE, _VERSION) / "current.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_text("{", encoding="utf-8")
    with TestClient(create_app(tmp_path)) as client:
        response = client.get(
            "/v1/trips/summary",
            params={"source_id": _SOURCE, "dataset_version": _VERSION, "token": "secret"},
        )
    captured = capsys.readouterr()
    event = _events(captured.err)[0]
    assert response.status_code == 503
    assert response.json() == {"code": "pointer_invalid"}
    assert event["route"] == "/v1/trips/summary"
    assert event["http_status"] == 503
    assert event["error_code"] == "pointer_invalid"
    assert event["status"] == "rejected"
    assert "token" not in captured.err
    assert "secret" not in captured.err
    assert pointer.read_text(encoding="utf-8") == "{"


def _ingest(source: Path, run_id: str, *, dry_run: bool = False) -> list[str]:
    args = [
        "ingest",
        "--input",
        str(source),
        "--source-id",
        _SOURCE,
        "--dataset-version",
        _VERSION,
        "--run-id",
        run_id,
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def _transform(run_id: str) -> list[str]:
    return [
        "transform",
        "--source-id",
        _SOURCE,
        "--dataset-version",
        _VERSION,
        "--run-id",
        run_id,
    ]


def _gold(sha: str, run_id: str) -> list[str]:
    return [
        "gold",
        "--source-id",
        _SOURCE,
        "--dataset-version",
        _VERSION,
        "--input-sha256",
        sha,
        "--run-id",
        run_id,
    ]


def _events(stderr: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for line in stderr.splitlines():
        if not line:
            continue
        if line.startswith("{"):
            parsed = json.loads(line)
            assert isinstance(parsed, dict)
            events.append(parsed)
            continue
        assert "Using `httpx` with `starlette.testclient` is deprecated" in line
    return events


def _split(capsys: CaptureFixture[str]) -> tuple[str, str]:
    captured = capsys.readouterr()
    return captured.out, captured.err


def _files(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())
