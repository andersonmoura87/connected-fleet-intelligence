"""Command line for Bronze ingestion."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Never

from cfi.application.errors import IngestFailure, usage
from cfi.application.ingest import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    DEFAULT_MAX_INPUT_BYTES,
    IngestRequest,
    IngestSettings,
    ingest,
)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        del message
        _emit_error("usage", 2, dry_run=False)
        raise SystemExit(2)


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="cfi")
    subcommands = parser.add_subparsers(dest="command", required=True)
    ingest_parser = subcommands.add_parser("ingest")
    ingest_parser.add_argument("--input", required=True)
    ingest_parser.add_argument("--source-id", required=True)
    ingest_parser.add_argument("--dataset-version", required=True)
    ingest_parser.add_argument("--run-id", required=True)
    ingest_parser.add_argument("--dry-run", action="store_true")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return 2 if code is None else int(code)

    if args.command != "ingest":
        _emit_error("usage", 2, dry_run=False)
        return 2
    try:
        settings = _settings_from_env()
        result = ingest(
            settings,
            IngestRequest(
                input_path=Path(args.input),
                source_id=args.source_id,
                dataset_version=args.dataset_version,
                run_id=args.run_id,
                dry_run=bool(args.dry_run),
            ),
        )
    except IngestFailure as exc:
        _emit_error(exc.code, exc.exit_code, dry_run=bool(args.dry_run))
        return exc.exit_code
    sys.stdout.write(json.dumps(result.as_dict(), sort_keys=True) + "\n")
    return 0


def _settings_from_env() -> IngestSettings:
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


def _emit_error(code: str, exit_code: int, *, dry_run: bool) -> None:
    payload = {"code": code, "dry_run": dry_run, "exit_code": exit_code, "status": "rejected"}
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
