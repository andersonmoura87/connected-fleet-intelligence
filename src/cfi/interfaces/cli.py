"""Command line for batch publication and the read-only HTTP server.

Batch imports stay inside the ingest, transform, and gold branches so `cfi serve`
does not load DuckDB.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Never

from cfi.application.errors import IngestFailure, usage
from cfi.domain.identifiers import validate_identifier
from cfi.interfaces.serve import serve, serve_from_env
from cfi.observability import elapsed_ms, emit

if TYPE_CHECKING:
    from cfi.application.ingest import IngestSettings


_PARSE_ARGV: list[str] = []


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        del message
        if _first_command(_PARSE_ARGV) == "diagnose":
            _write_diagnose_rejection("usage", 2)
        else:
            _emit_error("usage", 2, dry_run=False)
        raise SystemExit(2)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    started = time.perf_counter_ns()
    context = _preview(arguments)
    stage = context.get("stage")
    if isinstance(stage, str):
        emit({**context, "event_name": "command_started", "status": "started"})
    try:
        code, terminal = _dispatch(arguments)
    except Exception:
        if isinstance(stage, str):
            emit(
                {
                    **context,
                    "duration_ms": elapsed_ms(started),
                    "event_name": "command_finished",
                    "status": "rejected",
                }
            )
        raise
    if isinstance(stage, str):
        emit(
            {
                **context,
                **terminal,
                "duration_ms": elapsed_ms(started),
                "event_name": "command_finished",
            }
        )
    return code


def _dispatch(argv: list[str]) -> tuple[int, dict[str, object]]:
    global _PARSE_ARGV
    _PARSE_ARGV = argv
    parser = _Parser(prog="cfi")
    subcommands = parser.add_subparsers(dest="command", required=True)
    ingest_parser = subcommands.add_parser("ingest")
    ingest_parser.add_argument("--input", required=True)
    ingest_parser.add_argument("--source-id", required=True)
    ingest_parser.add_argument("--dataset-version", required=True)
    ingest_parser.add_argument("--run-id", required=True)
    ingest_parser.add_argument("--dry-run", action="store_true")
    transform_parser = subcommands.add_parser("transform")
    transform_parser.add_argument("--source-id", required=True)
    transform_parser.add_argument("--dataset-version", required=True)
    transform_parser.add_argument("--run-id", required=True)
    transform_parser.add_argument("--dry-run", action="store_true")
    gold_parser = subcommands.add_parser("gold")
    gold_parser.add_argument("--source-id", required=True)
    gold_parser.add_argument("--dataset-version", required=True)
    gold_parser.add_argument("--input-sha256", required=True)
    gold_parser.add_argument("--run-id", required=True)
    gold_parser.add_argument("--dry-run", action="store_true")
    gold_parser.add_argument("--promote", action="store_true")
    diagnose_parser = subcommands.add_parser("diagnose")
    diagnose_parser.add_argument("--source-id", required=True)
    diagnose_parser.add_argument("--dataset-version", required=True)
    diagnose_parser.add_argument("--input-sha256")
    subcommands.add_parser("serve")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = 2 if exc.code is None else int(exc.code)
        if _first_command(argv) == "diagnose":
            return code, {"error_code": "usage", "exit_code": code, "status": "rejected"}
        return code, _rejected("usage", code, dry_run=False)

    if args.command == "serve":
        return _serve()
    if args.command == "diagnose":
        return _diagnose(args)
    if args.command not in {"ingest", "transform", "gold"}:
        _emit_error("usage", 2, dry_run=False)
        return 2, _rejected("usage", 2, dry_run=False)
    try:
        settings = _settings_from_env()
        if args.command == "gold":
            from cfi.application.gold import GoldFailure, GoldRequest, GoldSettings, gold

            try:
                result = gold(
                    GoldSettings(
                        data_root=settings.data_root,
                        lock_timeout_seconds=settings.lock_timeout_seconds,
                    ),
                    GoldRequest(
                        source_id=args.source_id,
                        dataset_version=args.dataset_version,
                        input_sha256=args.input_sha256,
                        run_id=args.run_id,
                        dry_run=bool(args.dry_run),
                        promote=bool(args.promote),
                    ),
                )
            except GoldFailure as exc:
                payload = exc.result.as_dict()
                sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
                return exc.exit_code, _terminal(payload, exc.exit_code)
            payload = result.as_dict()
            sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
            return result.exit_code, _terminal(payload, result.exit_code)
        if args.command == "transform":
            from cfi.application.transform import TransformRequest, TransformSettings, transform

            payload = transform(
                TransformSettings(
                    data_root=settings.data_root,
                    max_input_bytes=settings.max_input_bytes,
                    lock_timeout_seconds=settings.lock_timeout_seconds,
                ),
                TransformRequest(
                    source_id=args.source_id,
                    dataset_version=args.dataset_version,
                    run_id=args.run_id,
                    dry_run=bool(args.dry_run),
                ),
            ).as_dict()
        else:
            from cfi.application.ingest import IngestRequest, ingest

            payload = ingest(
                settings,
                IngestRequest(
                    input_path=Path(args.input),
                    source_id=args.source_id,
                    dataset_version=args.dataset_version,
                    run_id=args.run_id,
                    dry_run=bool(args.dry_run),
                ),
            ).as_dict()
    except IngestFailure as exc:
        _emit_error(exc.code, exc.exit_code, dry_run=bool(args.dry_run))
        return exc.exit_code, _rejected(exc.code, exc.exit_code, dry_run=bool(args.dry_run))
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
    return 0, _terminal(payload, 0)


def _diagnose(args: argparse.Namespace) -> tuple[int, dict[str, object]]:
    from cfi.application.diagnose import DiagnoseFailure, diagnose

    root = Path(os.environ.get("CFI_DATA_ROOT", "data"))
    try:
        payload = diagnose(
            root,
            args.source_id,
            args.dataset_version,
            args.input_sha256,
        )
    except DiagnoseFailure as exc:
        _write_diagnose_rejection(exc.code, exc.exit_code)
        return exc.exit_code, {
            "error_code": exc.code,
            "exit_code": exc.exit_code,
            "status": "rejected",
        }
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
    return 0, {"exit_code": 0, "status": "succeeded"}


def _serve() -> tuple[int, dict[str, object]]:
    try:
        settings = serve_from_env()
        serve(settings.data_root, settings.host, settings.port)
    except IngestFailure as exc:
        _emit_error(exc.code, exc.exit_code, dry_run=False)
        return exc.exit_code, _rejected(exc.code, exc.exit_code, dry_run=False)
    return 0, {"dry_run": False, "exit_code": 0, "status": "succeeded"}


def _preview(argv: list[str]) -> dict[str, object]:
    stage: str | None = None
    flags: dict[str, str] = {}
    dry_run = False
    index = 0
    while index < len(argv):
        token = argv[index]
        if stage is None and token in {"diagnose", "gold", "ingest", "serve", "transform"}:
            stage = token
            index += 1
            continue
        if token == "--dry-run":
            dry_run = True
            index += 1
            continue
        name, value, consumed = _flag(argv, index)
        if name is not None and value is not None:
            flags[name] = value
            index += consumed
            continue
        index += 1
    context: dict[str, object] = {}
    if stage != "diagnose":
        context["dry_run"] = dry_run
    if stage is not None:
        context["stage"] = stage
    for key in ("dataset_version", "run_id", "source_id"):
        raw = flags.get(key)
        if raw is None:
            continue
        try:
            context[key] = validate_identifier(raw)
        except ValueError:
            continue
    return context


def _flag(argv: list[str], index: int) -> tuple[str | None, str | None, int]:
    token = argv[index]
    names = {
        "--dataset-version": "dataset_version",
        "--run-id": "run_id",
        "--source-id": "source_id",
    }
    for flag, key in names.items():
        if token == flag and index + 1 < len(argv):
            return key, argv[index + 1], 2
        prefix = flag + "="
        if token.startswith(prefix):
            return key, token[len(prefix) :], 1
    return None, None, 1


def _terminal(payload: dict[str, object], exit_code: int) -> dict[str, object]:
    code = payload.get("code")
    status = _event_status(payload.get("status"), code, exit_code)
    fields: dict[str, object] = {"exit_code": exit_code, "status": status}
    if status in {"partial", "rejected"} and isinstance(code, str):
        fields["error_code"] = code
    if isinstance(payload.get("dry_run"), bool):
        fields["dry_run"] = payload["dry_run"]
    for key in (
        "conflict_rejection_count",
        "identical_duplicate_extra_count",
        "input_line_count",
        "invalid_record_count",
        "quarantine_row_count",
        "rejected_count",
        "silver_record_count",
        "trip_count",
        "valid_record_count",
        "pointer_replaced",
        "pointer_state",
        "snapshot_durable",
        "snapshot_state",
    ):
        if payload.get(key) is not None:
            fields[key] = payload[key]
    return fields


def _event_status(status: object, code: object, exit_code: int) -> str:
    if status == "partial":
        return "partial"
    if status == "noop" or code == "noop":
        return "noop"
    if status in {"conflict", "rejected"} or exit_code != 0:
        return "rejected"
    if status in {"complete", "dry_run", "published"}:
        return "succeeded"
    return "rejected"


def _rejected(code: str, exit_code: int, *, dry_run: bool) -> dict[str, object]:
    return {"dry_run": dry_run, "error_code": code, "exit_code": exit_code, "status": "rejected"}


def _settings_from_env() -> "IngestSettings":
    from cfi.application.ingest import (
        DEFAULT_LOCK_TIMEOUT_SECONDS,
        DEFAULT_MAX_INPUT_BYTES,
        IngestSettings,
    )

    root = Path(os.environ.get("CFI_DATA_ROOT", "data"))
    try:
        max_bytes = int(os.environ.get("CFI_MAX_INPUT_BYTES", str(DEFAULT_MAX_INPUT_BYTES)))
        timeout = float(
            os.environ.get("CFI_LOCK_TIMEOUT_SECONDS", str(DEFAULT_LOCK_TIMEOUT_SECONDS))
        )
    except ValueError as exc:
        raise usage("usage") from exc
    if max_bytes < 0 or timeout < 0:
        raise usage("usage")
    return IngestSettings(
        data_root=root,
        max_input_bytes=max_bytes,
        lock_timeout_seconds=timeout,
    )


def _first_command(argv: list[str]) -> str | None:
    for token in argv:
        if token == "--":
            return None
        if not token.startswith("-"):
            return token
    return None


def _write_diagnose_rejection(code: str, exit_code: int) -> None:
    payload = {"code": code, "exit_code": exit_code, "status": "rejected"}
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")


def _emit_error(code: str, exit_code: int, *, dry_run: bool) -> None:
    payload = {"code": code, "dry_run": dry_run, "exit_code": exit_code, "status": "rejected"}
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
