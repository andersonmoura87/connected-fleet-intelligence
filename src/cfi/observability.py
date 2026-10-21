"""One JSON object per line on stderr.

The logger does not open a file. Stdout stays the machine result of the command
that called this module.
"""

import json
import logging
import re
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TextIO

from cfi.domain.identifiers import validate_identifier

EVENT_NAMES = frozenset({"command_started", "command_finished", "http_request"})
STAGES = frozenset({"diagnose", "gold", "ingest", "serve", "transform"})
STATUSES = frozenset({"started", "succeeded", "noop", "rejected", "partial"})
_IDENTIFIER_FIELDS = frozenset({"run_id", "source_id", "dataset_version"})
_COUNT_FIELDS = frozenset(
    {
        "conflict_rejection_count",
        "identical_duplicate_extra_count",
        "input_line_count",
        "invalid_record_count",
        "quarantine_row_count",
        "rejected_count",
        "silver_record_count",
        "trip_count",
        "valid_record_count",
    }
)
_BOOL_FIELDS = frozenset({"dry_run", "pointer_replaced", "snapshot_durable"})
_SNAPSHOT_STATES = frozenset(
    {
        "snapshot_absent",
        "snapshot_conflict",
        "snapshot_incomplete",
        "snapshot_noop",
        "snapshot_published",
    }
)
_POINTER_STATES = frozenset(
    {
        "pointer_created",
        "pointer_current",
        "pointer_invalid",
        "pointer_unchanged",
        "pointer_updated",
    }
)
_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ROUTE = re.compile(r"^/[A-Za-z0-9._~/-]{1,80}$")
_EXIT_CODES = frozenset({0, 2, 3, 4, 5})
_LOGGER_NAME = "cfi.events"


class _StderrHandler(logging.StreamHandler[TextIO]):
    """Write the message only. A failed write does not append a traceback."""

    def handleError(self, record: logging.LogRecord) -> None:
        return


def elapsed_ms(started_ns: int) -> int:
    """Return whole milliseconds since a perf_counter_ns reading."""

    elapsed = time.perf_counter_ns() - started_ns
    if elapsed < 0:
        return 0
    return elapsed // 1_000_000


def emit(event: Mapping[str, object]) -> None:
    """Write one operational event. Drop unknown, forbidden, and invalid fields."""

    payload = _payload(event)
    if payload is None:
        return
    _logger().info(json.dumps(payload, sort_keys=True))


def _payload(event: Mapping[str, object]) -> dict[str, object] | None:
    event_name = event.get("event_name")
    stage = event.get("stage")
    status = event.get("status")
    if event_name not in EVENT_NAMES or stage not in STAGES or status not in STATUSES:
        return None
    payload: dict[str, object] = {
        "event_name": event_name,
        "stage": stage,
        "status": status,
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    for key, value in event.items():
        accepted = _optional(key, value)
        if accepted is not None:
            payload[key] = accepted
    return payload


def _optional(key: str, value: object) -> object | None:
    if key in _IDENTIFIER_FIELDS and isinstance(value, str):
        try:
            return validate_identifier(value)
        except ValueError:
            return None
    if key in _COUNT_FIELDS and _non_negative(value):
        return value
    if key in _BOOL_FIELDS and isinstance(value, bool):
        return value
    if key == "duration_ms" and _non_negative(value):
        return value
    if key == "exit_code" and isinstance(value, int) and value in _EXIT_CODES:
        return value
    if key == "error_code" and isinstance(value, str) and _CODE.fullmatch(value):
        return value
    if key == "http_status" and isinstance(value, int) and not isinstance(value, bool):
        if 100 <= value <= 599:
            return value
        return None
    if key == "snapshot_state" and isinstance(value, str) and value in _SNAPSHOT_STATES:
        return value
    if key == "pointer_state" and isinstance(value, str) and value in _POINTER_STATES:
        return value
    if (
        key == "route"
        and isinstance(value, str)
        and (value == "unmatched" or _ROUTE.fullmatch(value))
    ):
        return value
    return None


def _non_negative(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = next(
        (item for item in logger.handlers if isinstance(item, _StderrHandler)),
        None,
    )
    if handler is None:
        handler = _StderrHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    handler.stream = sys.stderr
    return logger
