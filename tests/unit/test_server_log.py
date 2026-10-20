"""Uvicorn lifecycle lines stay JSON and omit the bind address."""

import json
import logging

from cfi.interfaces.serve import ServerLogFilter, ServerLogFormatter


def _record(
    msg: object,
    args: tuple[object, ...] = (),
    *,
    level: int = logging.INFO,
    exc_info: tuple[type[BaseException], BaseException, None] | None = None,
) -> logging.LogRecord:
    return logging.LogRecord(
        name="uvicorn.error",
        level=level,
        pathname=r"C:\secret\uvicorn\server.py",
        lineno=190,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


def test_listening_line_omits_host_and_port() -> None:
    record = _record(
        "Uvicorn running on %s://%s:%d (Press CTRL+C to quit)",
        ("http", "0.0.0.0", 8000),
    )
    rendered = ServerLogFormatter().format(record)
    event = json.loads(rendered)
    assert event["event"] == "server_listening"
    assert event["logger"] == "uvicorn"
    assert event["level"] == "info"
    assert "0.0.0.0" not in rendered
    assert "8000" not in rendered
    assert "secret" not in rendered
    assert set(event) == {"event", "level", "logger", "timestamp"}


def test_process_line_keeps_the_pid_only() -> None:
    event = json.loads(ServerLogFormatter().format(_record("Started server process [%d]", (7,))))
    assert event["event"] == "process_started"
    assert event["pid"] == 7


def test_bind_error_keeps_the_type_and_drops_the_address() -> None:
    error = OSError(98, "Address already in use")
    error.filename = ("0.0.0.0", 8000)
    rendered = ServerLogFormatter().format(_record(error, level=logging.ERROR))
    event = json.loads(rendered)
    assert event["event"] == "server_error"
    assert event["level"] == "error"
    assert event["error_type"] == "OSError"
    assert event["errno"] == 98
    assert "0.0.0.0" not in rendered
    assert "8000" not in rendered
    assert "Address" not in rendered


def test_unknown_text_is_not_copied() -> None:
    leaked = _record("Loading environment from '/tmp/secret.env'")
    assert ServerLogFilter().filter(leaked) is False
    failure = _record("Error loading ASGI app. /tmp/secret", level=logging.ERROR)
    assert ServerLogFilter().filter(failure) is True
    rendered = ServerLogFormatter().format(failure)
    assert json.loads(rendered)["event"] == "server_error"
    assert "secret" not in rendered
    assert "/tmp" not in rendered
