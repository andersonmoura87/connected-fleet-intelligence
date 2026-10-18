"""Command line for Bronze ingestion, Silver transformation, and Gold publication."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Never

from cfi.application.errors import IngestFailure, usage
from cfi.application.gold import GoldFailure, GoldRequest, GoldSettings, gold
from cfi.application.ingest import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    DEFAULT_MAX_INPUT_BYTES,
    IngestRequest,
    IngestSettings,
    ingest,
)
from cfi.application.transform import TransformRequest, TransformSettings, transform


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
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return 2 if code is None else int(code)

    if args.command not in {"ingest", "transform", "gold"}:
        _emit_error("usage", 2, dry_run=False)
        return 2
    try:
        settings = _settings_from_env()
        if args.command == "gold":
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
                sys.stdout.write(json.dumps(exc.result.as_dict(), sort_keys=True) + "\n")
                return exc.exit_code
            sys.stdout.write(json.dumps(result.as_dict(), sort_keys=True) + "\n")
            return result.exit_code
        if args.command == "transform":
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
        return exc.exit_code
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
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
