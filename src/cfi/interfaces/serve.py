"""Start the read-only HTTP application.

This module does not ingest, transform, or publish Gold, and it does not create
the data root.
"""

import ipaddress
import os
import re
from dataclasses import dataclass
from pathlib import Path

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
    uvicorn.run(create_app(data_root), host=host, port=port, access_log=False)


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
