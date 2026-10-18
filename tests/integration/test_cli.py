"""CLI exit codes and dry-run output."""

import json
from pathlib import Path

from pytest import CaptureFixture, MonkeyPatch

from cfi.interfaces.cli import main


def _args(source: Path, run_id: str, *, dry_run: bool = False) -> list[str]:
    args = [
        "ingest",
        "--input",
        str(source),
        "--source-id",
        "synthetic.trips.v1",
        "--dataset-version",
        "v1",
        "--run-id",
        run_id,
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def test_cli_publish_replay_and_conflict(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))
    source = tmp_path / "input.bin"
    source.write_bytes(b"cli-bytes")

    assert main(_args(source, "run-001")) == 0
    assert main(_args(source, "run-002")) == 0
    source.write_bytes(b"other")
    assert main(_args(source, "run-003")) == 3
    assert (
        tmp_path / "data" / "bronze" / "sources" / "synthetic.trips.v1" / "v1" / "original"
    ).read_bytes() == (b"cli-bytes")


def test_cli_dry_run_prints_json_without_directories(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))
    source = tmp_path / "input.bin"
    source.write_bytes(b"dry")

    assert main(_args(source, "run-001", dry_run=True)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["status"] == "published"
    assert not (tmp_path / "data").exists()


def test_cli_transform_publishes_and_replays(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CFI_DATA_ROOT", str(tmp_path / "data"))
    source = tmp_path / "input.bin"
    source.write_bytes(b"")
    ingest_args = [
        "ingest",
        "--input",
        str(source),
        "--source-id",
        "synthetic.trips.v1",
        "--dataset-version",
        "v1",
        "--run-id",
        "bronze-001",
    ]
    transform_args = [
        "transform",
        "--source-id",
        "synthetic.trips.v1",
        "--dataset-version",
        "v1",
        "--run-id",
        "silver-001",
    ]

    assert main(ingest_args) == 0
    capsys.readouterr()
    assert main([*transform_args, "--dry-run"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["dry_run"] is True
    assert dry["status"] == "published"
    assert dry["contract_version"] == "1.1.0"
    assert not (tmp_path / "data" / "silver").exists()
    assert main(transform_args) == 0
    published = json.loads(capsys.readouterr().out)
    assert published["status"] == "published"
    assert published["input_line_count"] == 0
    assert main(transform_args[:-1] + ["silver-002"]) == 0
    replay = json.loads(capsys.readouterr().out)
    assert replay["status"] == "noop"
    assert (
        main(["transform", "--source-id", "../bad", "--dataset-version", "v1", "--run-id", "x"])
        == 2
    )
