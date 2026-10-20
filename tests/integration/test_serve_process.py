"""A real local listener for liveness, absence, and shutdown."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


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
        f"http://127.0.0.1:{port}/v1/trips/summary?source_id=synthetic.trips.v1&dataset_version=v1"
    )
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
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    # Uvicorn handles SIGTERM, finishes the server, then re-raises the signal.
    # POSIX therefore reports -SIGTERM. Windows maps SIGTERM to TerminateProcess.
    if os.name == "posix":
        assert code == -signal.SIGTERM
    else:
        assert code is not None
    assert not root.exists()


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
