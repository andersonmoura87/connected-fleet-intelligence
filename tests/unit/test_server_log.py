"""Uvicorn lifecycle lines stay JSON and omit the bind address."""

import json
import logging
import re
from datetime import UTC, datetime

from pytest import MonkeyPatch

from cfi.interfaces.serve import ServerLogFilter, ServerLogFormatter

_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_LISTENING_KEYS = frozenset({"event", "level", "logger", "timestamp"})
_FORBIDDEN_FIELDS = frozenset(
    {
        "bind",
        "host",
        "message",
        "msg",
        "pathname",
        "port",
        "query",
        "query_string",
        "uri",
        "url",
    }
)


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
    _assert_listening(event)
    assert "secret" not in rendered
    _assert_bind_absent(event, "0.0.0.0", "8000")


def test_port_digits_inside_the_timestamp_do_not_expose_the_port(monkeypatch: MonkeyPatch) -> None:
    class _Clock:
        @staticmethod
        def now(tz: object) -> datetime:
            return datetime(2026, 10, 20, 17, 23, 45, 800000, tzinfo=UTC)

    monkeypatch.setattr("cfi.interfaces.serve.datetime", _Clock)
    rendered = ServerLogFormatter().format(
        _record(
            "Uvicorn running on %s://%s:%d (Press CTRL+C to quit)",
            ("http", "0.0.0.0", 8000),
        )
    )
    event = json.loads(rendered)
    _assert_listening(event)
    assert event["timestamp"] == "2026-10-20T17:23:45.800000Z"
    assert "8000" in event["timestamp"]
    assert "8000" in rendered
    _assert_bind_absent(event, "0.0.0.0", "8000")


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
    assert "Address" not in rendered
    _assert_bind_absent(event, "0.0.0.0", "8000")


def test_unknown_text_is_not_copied() -> None:
    leaked = _record("Loading environment from '/tmp/secret.env'")
    assert ServerLogFilter().filter(leaked) is False
    failure = _record("Error loading ASGI app. /tmp/secret", level=logging.ERROR)
    assert ServerLogFilter().filter(failure) is True
    rendered = ServerLogFormatter().format(failure)
    assert json.loads(rendered)["event"] == "server_error"
    assert "secret" not in rendered
    assert "/tmp" not in rendered


def _assert_listening(event: dict[str, object]) -> None:
    assert event["event"] == "server_listening"
    assert event["logger"] == "uvicorn"
    assert event["level"] == "info"
    timestamp = event["timestamp"]
    assert isinstance(timestamp, str)
    assert _TIMESTAMP.fullmatch(timestamp)
    assert set(event) == _LISTENING_KEYS


def _assert_bind_absent(event: dict[str, object], host: str, port: str) -> None:
    assert _FORBIDDEN_FIELDS.isdisjoint(event)
    content = json.dumps(
        {key: value for key, value in event.items() if key != "timestamp"},
        sort_keys=True,
    )
    assert host not in content
    assert port not in content
    assert "http://" not in content
    assert "Press CTRL+C" not in content
