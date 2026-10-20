"""Start the read-only HTTP application.

This module does not ingest, transform, or publish Gold, and it does not create
the data root.
"""

import ipaddress
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn

from cfi.application.errors import usage
from cfi.interfaces.http import create_app

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8000
_HOST_NAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,253}[A-Za-z0-9])?$")


@dataclass(frozen=True, slots=True)
class ServeSettings:
    data_root: Path
    host: str
    port: int


def serve_from_env() -> ServeSettings:
    """Read the data root, bind host, and port. Do not create directories."""

    root = Path(os.environ.get("CFI_DATA_ROOT", "data"))
    host = os.environ.get("CFI_HTTP_HOST", DEFAULT_HTTP_HOST)
    port = _port(os.environ.get("CFI_HTTP_PORT", str(DEFAULT_HTTP_PORT)))
    if not _valid_host(host):
        raise usage("invalid_host")
    return ServeSettings(data_root=root, host=host, port=port)


def serve(data_root: Path, host: str, port: int) -> None:
    """Listen until the process receives a shutdown signal."""

    if not _valid_host(host) or port < 1 or port > 65535:
        raise usage("invalid_host")
    uvicorn.run(
        create_app(data_root),
        host=host,
        port=port,
        access_log=False,
        log_config=server_log_config(),
    )


def server_log_config() -> dict[str, Any]:
    """Format Uvicorn lifecycle logs as JSON Lines on stderr.

    The application event contract is unchanged. Access logs stay off.
    """

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"()": "cfi.interfaces.serve.ServerLogFormatter"},
            "access": {"()": "cfi.interfaces.serve.ServerLogFormatter"},
        },
        "filters": {"server": {"()": "cfi.interfaces.serve.ServerLogFilter"}},
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "filters": ["server"],
            }
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"level": "INFO"},
            "uvicorn.access": {"handlers": [], "level": "INFO", "propagate": False},
        },
    }


class ServerLogFilter(logging.Filter):
    """Keep lifecycle lines and real warnings. Drop other text."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return True
        return _known_event(record) is not None


class ServerLogFormatter(logging.Formatter):
    """One JSON object, without the bind address, a path, or exception text."""

    def format(self, record: logging.LogRecord) -> str:
        known = _known_event(record)
        if known is not None:
            event = known
        elif record.levelno >= logging.ERROR:
            event = "server_error"
        else:
            event = "server_warning"
        payload: dict[str, object] = {
            "event": event,
            "level": _level(record.levelno),
            "logger": "uvicorn",
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        payload.update(_details(record, event))
        return json.dumps(payload, sort_keys=True)


_INFO_EVENTS = {
    "Started server process [%d]": "process_started",
    "Waiting for application startup.": "application_startup",
    "Application startup complete.": "application_ready",
    "Uvicorn running on socket %s (Press CTRL+C to quit)": "server_listening",
    "Uvicorn running on unix socket %s (Press CTRL+C to quit)": "server_listening",
    "Uvicorn running on %s://%s:%d (Press CTRL+C to quit)": "server_listening",
    "Uvicorn running on %s://[%s]:%d (Press CTRL+C to quit)": "server_listening",
    "Shutting down": "shutting_down",
    "Waiting for application shutdown.": "application_shutdown",
    "Application shutdown complete.": "application_shutdown_complete",
    "Finished server process [%d]": "process_finished",
    "Waiting for connections to close. (CTRL+C to force quit)": "connections_closing",
    "Waiting for background tasks to complete. (CTRL+C to force quit)": "tasks_draining",
    "Maximum request limit of %d exceeded. Terminating process.": "request_limit",
}
_ERROR_EVENTS = {
    "Application startup failed. Exiting.": "startup_failed",
    "Application shutdown failed. Exiting.": "shutdown_failed",
    "Cancel %s running task(s), timeout graceful shutdown exceeded": "shutdown_timeout",
}
_TYPE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,63}$")


def _known_event(record: logging.LogRecord) -> str | None:
    if not isinstance(record.msg, str):
        return None
    if record.levelno >= logging.ERROR:
        return _ERROR_EVENTS.get(record.msg)
    if record.levelno >= logging.WARNING:
        return None
    return _INFO_EVENTS.get(record.msg)


def _level(level: int) -> str:
    if level >= logging.ERROR:
        return "error"
    if level >= logging.WARNING:
        return "warning"
    return "info"


def _details(record: logging.LogRecord, event: str) -> dict[str, object]:
    fields: dict[str, object] = {}
    if (
        event in {"process_finished", "process_started"}
        and isinstance(record.args, tuple)
        and record.args
    ):
        pid = record.args[0]
        if isinstance(pid, int) and not isinstance(pid, bool) and pid >= 0:
            fields["pid"] = pid
    exc = _exception(record)
    if exc is None:
        return fields
    name = type(exc).__name__
    if _TYPE_NAME.fullmatch(name):
        fields["error_type"] = name
    errno = getattr(exc, "errno", None)
    if isinstance(errno, int) and not isinstance(errno, bool):
        fields["errno"] = errno
    return fields


def _exception(record: logging.LogRecord) -> BaseException | None:
    if isinstance(record.msg, BaseException):
        return record.msg
    info = record.exc_info
    if info is not None and isinstance(info[1], BaseException):
        return info[1]
    return None


def _port(value: str) -> int:
    if not value.isdigit():
        raise usage("usage")
    port = int(value)
    if port < 1 or port > 65535:
        raise usage("usage")
    return port


def _valid_host(value: str) -> bool:
    if value == "":
        return False
    try:
        ipaddress.ip_address(value)
    except ValueError:
        if len(value) > 255 or ".." in value or _HOST_NAME.fullmatch(value) is None:
            return False
    return True
