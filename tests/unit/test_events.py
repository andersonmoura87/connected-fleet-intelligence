"""Closed schema for one operational event."""

import json
import re
import time

from pytest import CaptureFixture

from cfi.observability import elapsed_ms, emit

_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_ALLOWED = frozenset(
    {
        "conflict_rejection_count",
        "dataset_version",
        "dry_run",
        "duration_ms",
        "error_code",
        "event_name",
        "exit_code",
        "http_status",
        "identical_duplicate_extra_count",
        "input_line_count",
        "invalid_record_count",
        "pointer_replaced",
        "pointer_state",
        "quarantine_row_count",
        "rejected_count",
        "route",
        "run_id",
        "silver_record_count",
        "snapshot_durable",
        "snapshot_state",
        "source_id",
        "stage",
        "status",
        "timestamp",
        "trip_count",
        "valid_record_count",
    }
)


def test_emit_writes_one_json_line_with_a_utc_timestamp(capsys: CaptureFixture[str]) -> None:
    emit(
        {
            "dataset_version": "v1",
            "duration_ms": 0,
            "event_name": "command_finished",
            "exit_code": 0,
            "run_id": "run-001",
            "source_id": "synthetic.trips.v1",
            "stage": "ingest",
            "status": "succeeded",
            "trip_count": 2,
        }
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    line = captured.err.strip()
    event = json.loads(line)
    assert isinstance(event, dict)
    assert set(event) <= _ALLOWED
    assert event["event_name"] == "command_finished"
    assert event["stage"] == "ingest"
    assert event["status"] == "succeeded"
    assert _TIMESTAMP.fullmatch(event["timestamp"])
    assert event["duration_ms"] == 0
    assert event["trip_count"] == 2


def test_emit_drops_unknown_forbidden_and_invalid_fields(capsys: CaptureFixture[str]) -> None:
    emit(
        {
            "data_root": "C:/secret/data",
            "error_code": "Not a code",
            "event_name": "command_finished",
            "exception": "C:/secret/trips.jsonl",
            "input_sha256": "a" * 64,
            "query_string": "token=secret",
            "route": "/v1/trips/summary?token=secret",
            "run_id": "../secret",
            "source_id": "synthetic.trips.v1",
            "stack": "Traceback",
            "stage": "gold",
            "status": "rejected",
        }
    )
    event = json.loads(capsys.readouterr().err)
    assert event["source_id"] == "synthetic.trips.v1"
    assert "run_id" not in event
    assert "input_sha256" not in event
    assert "route" not in event
    assert "error_code" not in event
    rendered = json.dumps(event)
    assert "secret" not in rendered
    assert "Traceback" not in rendered
    assert "token" not in rendered


def test_emit_ignores_an_unknown_event_name(capsys: CaptureFixture[str]) -> None:
    emit({"event_name": "span_started", "stage": "ingest", "status": "started"})
    assert capsys.readouterr().err == ""


def test_elapsed_ms_is_never_negative() -> None:
    assert elapsed_ms(time.perf_counter_ns()) >= 0
    assert elapsed_ms(time.perf_counter_ns() + 10**18) == 0
