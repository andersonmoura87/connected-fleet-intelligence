"""A real local listener for liveness, absence, and shutdown."""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import uvicorn

from cfi.interfaces.http import create_app
from cfi.interfaces.serve import server_log_config


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    assert isinstance(port, int)
    return port


def _child_env(root: Path, port: int) -> dict[str, str]:
    env = os.environ.copy()
    src = str(Path("src").resolve())
    current = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src if not current else src + os.pathsep + current
    env["CFI_DATA_ROOT"] = str(root)
    env["CFI_HTTP_HOST"] = "127.0.0.1"
    env["CFI_HTTP_PORT"] = str(port)
    return env


def test_serve_answers_without_gold_and_stops_on_sigterm(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from cfi.interfaces.cli import main; raise SystemExit(main(['serve']))",
        ],
        env=_child_env(root, port),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    live = f"http://127.0.0.1:{port}/health/live"
    summary = (
        "http://127.0.0.1:"
        f"{port}/v1/trips/summary?source_id=synthetic.trips.v1&dataset_version=v1&token=secret"
    )
    stderr = ""
    stdout = ""
    code: int | None = None
    try:
        body = b""
        for _ in range(50):
            if proc.poll() is not None:
                detail = proc.stderr.read() if proc.stderr is not None else ""
                raise AssertionError(detail or "server exited")
            try:
                with urllib.request.urlopen(live, timeout=0.2) as response:
                    body = response.read()
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.1)
        else:
            raise AssertionError("server did not become live")
        assert json.loads(body) == {"status": "live"}
        try:
            urllib.request.urlopen(summary, timeout=1)
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
            assert json.loads(exc.read()) == {"code": "not_published"}
        else:
            raise AssertionError("missing publication returned success")
        assert not root.exists()
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=10)
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        stdout = proc.stdout.read() if proc.stdout is not None else ""
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            stdout = proc.stdout.read() if proc.stdout is not None else ""
    # Uvicorn handles SIGTERM, finishes the server, then re-raises the signal.
    # POSIX therefore reports -SIGTERM. Windows maps SIGTERM to TerminateProcess.
    if os.name == "posix":
        assert code == -signal.SIGTERM
    else:
        assert code is not None
    assert not root.exists()
    assert stdout == ""
    assert "token" not in stderr
    assert "secret" not in stderr
    assert str(root) not in stderr
    assert f":{port}" not in stderr
    lines = [json.loads(line) for line in stderr.splitlines() if line]
    assert any(line.get("event_name") == "http_request" for line in lines)
    assert any(line.get("event") == "server_listening" for line in lines)
    assert all("event_name" in line or line.get("logger") == "uvicorn" for line in lines)


def test_server_shutdown_writes_lifecycle_json_without_a_query(
    tmp_path: Path,
    capsys: object,
) -> None:
    root = tmp_path / "absent"
    port = _free_port()
    config = uvicorn.Config(
        create_app(root),
        host="127.0.0.1",
        port=port,
        access_log=False,
        log_config=server_log_config(),
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run)
    thread.start()
    try:
        for _ in range(50):
            if server.started:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("server did not start")
        summary = (
            "http://127.0.0.1:"
            f"{port}/v1/trips/summary?source_id=synthetic.trips.v1&dataset_version=v1&token=secret"
        )
        try:
            urllib.request.urlopen(summary, timeout=2)
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
            assert json.loads(exc.read()) == {"code": "not_published"}
        else:
            raise AssertionError("missing publication returned success")
        assert not root.exists()
        server.should_exit = True
        thread.join(timeout=10)
    finally:
        server.should_exit = True
        if thread.is_alive():
            thread.join(timeout=10)
    captured = capsys.readouterr()  # type: ignore[attr-defined]
    assert captured.out == ""
    assert not thread.is_alive()
    assert not root.exists()
    rendered = captured.err
    assert "token" not in rendered
    assert "secret" not in rendered
    assert "127.0.0.1" not in rendered
    assert str(root) not in rendered
    assert f":{port}" not in rendered
    lines = [json.loads(line) for line in rendered.splitlines() if line]
    lifecycle = [line["event"] for line in lines if line.get("logger") == "uvicorn"]
    assert lifecycle[:4] == [
        "process_started",
        "application_startup",
        "application_ready",
        "server_listening",
    ]
    assert "shutting_down" in lifecycle
    assert "application_shutdown" in lifecycle
    assert "application_shutdown_complete" in lifecycle
    assert lifecycle[-1] == "process_finished"
    requests = [line for line in lines if line.get("event_name") == "http_request"]
    assert len(requests) == 1
    assert requests[0]["route"] == "/v1/trips/summary"
    assert requests[0]["error_code"] == "not_published"
    assert "query" not in requests[0]


def test_serve_process_does_not_import_duckdb() -> None:
    env = os.environ.copy()
    src = str(Path("src").resolve())
    current = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src if not current else src + os.pathsep + current
    env["CFI_HTTP_PORT"] = "nope"
    code = (
        "import sys\n"
        "from cfi.interfaces.cli import main\n"
        "status = main(['serve'])\n"
        "print(status)\n"
        "raise SystemExit(0 if status == 2 and 'duckdb' not in sys.modules else 1)\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.splitlines()[0])["code"] == "usage"
