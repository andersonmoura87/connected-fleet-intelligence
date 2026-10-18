"""Read-only HTTP access to a published Gold pointer."""

import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from cfi.application.gold import Clock, GoldFailure, GoldRequest, GoldSettings, gold
from cfi.application.ingest import IngestRequest, IngestSettings, ingest
from cfi.application.published_read import read_bounded
from cfi.application.transform import TransformRequest, TransformSettings, transform
from cfi.domain.gold import GoldPointer, GoldSummary, gold_storage_key_for, pointer_key_for
from cfi.infrastructure.storage import is_reparse_point
from cfi.interfaces.http import create_app

FIXTURES = Path("tests/fixtures/synthetic")
SOURCE = "synthetic.trips.v1"
VERSION = "v1"


class FixedClock(Clock):
    def now(self) -> datetime:
        return datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def _publish(root: Path, name: str, *, version: str = VERSION, promote: bool = False) -> str:
    source = root / f"{version}-{name}.bin"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes((FIXTURES / name).read_bytes())
    ingest(
        IngestSettings(data_root=root),
        IngestRequest(
            input_path=source,
            source_id=SOURCE,
            dataset_version=version,
            run_id="bronze-001",
        ),
    )
    transformed = transform(
        TransformSettings(data_root=root, clock=FixedClock()),
        TransformRequest(source_id=SOURCE, dataset_version=version, run_id="silver-001"),
    )
    gold(
        GoldSettings(data_root=root, clock=FixedClock()),
        GoldRequest(
            source_id=SOURCE,
            dataset_version=version,
            input_sha256=transformed.input_sha256,
            run_id="gold-001",
            promote=promote,
        ),
    )
    return transformed.input_sha256


def _params(version: str = VERSION) -> dict[str, str]:
    return {"source_id": SOURCE, "dataset_version": version}


def _pointer(root: Path, version: str = VERSION) -> Path:
    return root / "gold" / "published" / pointer_key_for(SOURCE, version) / "current.json"


def _snapshot(root: Path, sha: str, version: str = VERSION) -> Path:
    return root / "gold" / "snapshots" / gold_storage_key_for(SOURCE, version, sha)


def _canonical(model: GoldPointer | GoldSummary) -> bytes:
    payload = model.model_dump(mode="json")
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and "gold" in path.parts
    }


def _inventory(root: Path) -> tuple[tuple[str, str], tuple[tuple[str, bytes], ...]]:
    """List directories and regular files without following a reparse point."""

    if not root.exists():
        return (), ()
    nodes: list[tuple[str, str]] = []
    files: list[tuple[str, bytes]] = []
    for current, dirnames, filenames in os.walk(root):
        current_path = Path(current)
        kept: list[str] = []
        for name in sorted(dirnames):
            child = current_path / name
            relative = child.relative_to(root).as_posix()
            if is_reparse_point(child):
                nodes.append((relative, "reparse"))
                continue
            nodes.append((relative, "dir"))
            kept.append(name)
        dirnames[:] = kept
        for name in sorted(filenames):
            child = current_path / name
            relative = child.relative_to(root).as_posix()
            if is_reparse_point(child):
                nodes.append((relative, "reparse"))
                continue
            nodes.append((relative, "file"))
            files.append((relative, child.read_bytes()))
    return tuple(nodes), tuple(files)


def _read_without_writing(client: TestClient, root: Path) -> Response:
    before = _inventory(root)
    response = client.get("/v1/trips/summary", params=_params())
    assert _inventory(root) == before
    assert str(root) not in response.text
    assert "Traceback" not in response.text
    return response


def test_live_does_not_require_gold(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    with TestClient(create_app(root)) as client:
        response = client.get("/health/live")
        missing = client.get("/v1/trips/summary", params=_params())
        document = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json() == {"status": "live"}
    assert missing.status_code == 404
    assert missing.json() == {"code": "not_published"}
    assert not root.exists()
    paths = document.json()["paths"]
    assert "/health/live" in paths
    assert "/v1/trips/summary" in paths
    assert "/health" not in paths
    assert "/metrics" not in paths
    assert "/health/ready" not in paths
    parameters = paths["/v1/trips/summary"]["get"]["parameters"]
    assert {item["name"] for item in parameters} == {"source_id", "dataset_version"}
    assert all(item["required"] is True for item in parameters)


def test_valid_summary_survives_without_bronze_or_silver(tmp_path: Path) -> None:
    sha = _publish(tmp_path, "trips_valid.jsonl")
    sealed = _files(tmp_path)
    shutil.rmtree(tmp_path / "bronze")
    shutil.rmtree(tmp_path / "silver")
    opened: list[str] = []
    real_open = os.open

    def guard(path: str | os.PathLike[str], flags: int, *args: object, **kwargs: object) -> int:
        text = os.fspath(path)
        opened.append(text)
        if f"{os.sep}bronze{os.sep}" in text or f"{os.sep}silver{os.sep}" in text:
            raise AssertionError(text)
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    with TestClient(create_app(tmp_path)) as client:
        before = _files(tmp_path)
        import os as os_module

        os_module.open = guard  # type: ignore[method-assign]
        try:
            response = client.get("/v1/trips/summary", params=_params())
            rejected = client.post("/v1/trips/summary", params=_params())
        finally:
            os_module.open = real_open  # type: ignore[method-assign]
    body = response.json()
    assert response.status_code == 200
    assert body["trip_count"] == 2
    assert body["total_distance_km"] == "12.750"
    assert body["average_duration_seconds"] == "1500.000000"
    assert body["rejected_count"] == 0
    assert body["source_id"] == SOURCE
    assert body["dataset_version"] == VERSION
    assert body["input_sha256"] == sha
    assert body["storage_key"] == gold_storage_key_for(SOURCE, VERSION, sha)
    assert isinstance(body["total_distance_km"], str)
    assert isinstance(body["average_duration_seconds"], str)
    assert "generated_at" not in body
    assert "total_distance" not in body
    assert rejected.status_code == 405
    assert _files(tmp_path) == before
    assert not (tmp_path / "bronze").exists()
    assert not (tmp_path / "silver").exists()
    assert sealed != {}


def test_empty_gold_average_is_null(tmp_path: Path) -> None:
    _publish(tmp_path, "trips_invalid.jsonl")
    with TestClient(create_app(tmp_path)) as client:
        response = client.get("/v1/trips/summary", params=_params())
    body = response.json()
    assert response.status_code == 200
    assert body["trip_count"] == 0
    assert body["total_distance_km"] == "0.000"
    assert body["average_duration_seconds"] is None
    assert body["rejected_count"] == 14


def test_query_validation(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path)) as client:
        missing = client.get("/v1/trips/summary")
        partial = client.get("/v1/trips/summary", params={"source_id": SOURCE})
        invalid = client.get(
            "/v1/trips/summary",
            params={"source_id": "bad id", "dataset_version": VERSION},
        )
        blank = client.get(
            "/v1/trips/summary",
            params={"source_id": "", "dataset_version": VERSION},
        )
    assert missing.status_code == 422
    assert missing.json() == {"code": "missing_parameter"}
    assert partial.status_code == 422
    assert partial.json() == {"code": "missing_parameter"}
    assert invalid.status_code == 422
    assert invalid.json() == {"code": "invalid_identifier"}
    assert blank.status_code == 422
    assert blank.json() == {"code": "invalid_identifier"}
    assert "path" not in missing.text
    assert str(tmp_path) not in invalid.text
    assert "Traceback" not in invalid.text
    assert _inventory(tmp_path) == ((), ())


def test_corrupt_pointer_and_snapshot_are_not_not_found(tmp_path: Path) -> None:
    sha = _publish(tmp_path, "trips_valid.jsonl")
    pointer = _pointer(tmp_path)
    directory = _snapshot(tmp_path, sha)
    original_pointer = pointer.read_bytes()
    original_summary = (directory / "summary.json").read_bytes()
    original_manifest = (directory / "manifest.json").read_bytes()
    with TestClient(create_app(tmp_path)) as client:
        pointer.write_bytes(b"{")
        corrupt = _read_without_writing(client, tmp_path)
        pointer.write_bytes(original_pointer)
        shifted = GoldPointer.model_validate_json(original_pointer).model_copy(
            update={"source_id": "other.source"}
        )
        pointer.write_bytes(_canonical(shifted))
        identity = _read_without_writing(client, tmp_path)
        pointer.write_bytes(original_pointer)
        shutil.rmtree(directory)
        absent = _read_without_writing(client, tmp_path)
        directory.mkdir()
        (directory / "summary.json").write_bytes(original_summary)
        (directory / "manifest.json").write_bytes(original_manifest)
        summary = GoldSummary.model_validate_json(original_summary)
        (directory / "summary.json").write_bytes(
            _canonical(summary.model_copy(update={"rejected_count": 9}))
        )
        hashed = _read_without_writing(client, tmp_path)
        (directory / "summary.json").write_bytes(original_summary)
        (directory / "manifest.json").write_bytes(b"[]\n")
        manifest = _read_without_writing(client, tmp_path)
        (directory / "manifest.json").write_bytes(original_manifest)
        payload = json.loads(original_summary)
        payload["unexpected"] = True
        (directory / "summary.json").write_bytes(
            (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        )
        schema = _read_without_writing(client, tmp_path)
        (directory / "summary.json").write_bytes(original_summary)
        restored = _read_without_writing(client, tmp_path)
    assert corrupt.status_code == 503
    assert corrupt.json() == {"code": "pointer_invalid"}
    assert identity.status_code == 503
    assert identity.json() == {"code": "pointer_invalid"}
    assert absent.status_code == 503
    assert absent.json() == {"code": "snapshot_absent"}
    assert hashed.status_code == 503
    assert hashed.json() == {"code": "integrity_mismatch"}
    assert manifest.status_code == 503
    assert manifest.json() == {"code": "schema_mismatch"}
    assert schema.status_code == 503
    assert schema.json() == {"code": "schema_mismatch"}
    assert restored.status_code == 200
    assert restored.json()["total_distance_km"] == "12.750"
    assert pointer.read_bytes() == original_pointer


def test_captured_pointer_stays_valid_through_a_later_promotion(tmp_path: Path) -> None:
    first = _publish(tmp_path, "trips_valid.jsonl")
    shutil.rmtree(tmp_path / "bronze")
    with pytest.raises(GoldFailure) as conflict:
        _publish(tmp_path, "trips_invalid.jsonl")
    assert conflict.value.result.code == "publication_conflict"
    assert conflict.value.result.snapshot_state == "snapshot_published"
    second = conflict.value.result.input_sha256
    assert second is not None
    first_key = gold_storage_key_for(SOURCE, VERSION, first)
    second_key = gold_storage_key_for(SOURCE, VERSION, second)
    first_summary = (_snapshot(tmp_path, first) / "summary.json").read_bytes()
    first_manifest = (_snapshot(tmp_path, first) / "manifest.json").read_bytes()
    second_summary = (_snapshot(tmp_path, second) / "summary.json").read_bytes()
    second_manifest = (_snapshot(tmp_path, second) / "manifest.json").read_bytes()
    replacement = _canonical(
        GoldPointer(
            pointer_version="1",
            source_id=SOURCE,
            dataset_version=VERSION,
            storage_key=second_key,
            summary_sha256=hashlib.sha256(second_summary).hexdigest(),
            status="published",
        )
    )
    seen: list[str] = []

    def wrapped(
        path: Path,
        max_bytes: int,
        *,
        missing_code: str = "not_published",
        missing_status: int = 404,
    ) -> bytes:
        data = read_bounded(
            path,
            max_bytes,
            missing_code=missing_code,
            missing_status=missing_status,
        )
        if path.name == "current.json" and not seen:
            seen.append(path.name)
            path.write_bytes(replacement)
        return data

    import cfi.application.published_read as reader

    with TestClient(create_app(tmp_path)) as client:
        original = reader.read_bounded
        reader.read_bounded = wrapped  # type: ignore[method-assign]
        try:
            during = client.get("/v1/trips/summary", params=_params())
        finally:
            reader.read_bounded = original  # type: ignore[method-assign]
        after = client.get("/v1/trips/summary", params=_params())
    assert during.status_code == 200
    assert during.json()["storage_key"] == first_key
    assert during.json()["trip_count"] == 2
    assert after.status_code == 200
    assert after.json()["storage_key"] == second_key
    assert after.json()["trip_count"] == 0
    assert after.json()["average_duration_seconds"] is None
    assert (_snapshot(tmp_path, first) / "summary.json").read_bytes() == first_summary
    assert (_snapshot(tmp_path, first) / "manifest.json").read_bytes() == first_manifest
    assert (_snapshot(tmp_path, second) / "summary.json").read_bytes() == second_summary
    assert (_snapshot(tmp_path, second) / "manifest.json").read_bytes() == second_manifest


def test_importing_the_http_package_does_not_load_duckdb() -> None:
    code = (
        "import sys\n"
        "import cfi.interfaces.http\n"
        "import cfi.application.published_read\n"
        "raise SystemExit(0 if 'duckdb' not in sys.modules else 1)\n"
    )
    env = os.environ.copy()
    src = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = src if not env.get("PYTHONPATH") else src + os.pathsep + env["PYTHONPATH"]
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 0, completed.stderr
    tree = ast.parse(Path("src/cfi/interfaces/http.py").read_text(encoding="utf-8"))
    modules = [
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    ]
    assert "cfi.application.gold" not in modules


def test_symlink_on_the_pointer_parent_is_not_followed(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "gold" / "published"
    parent.parent.mkdir()
    try:
        parent.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is not permitted on this system")
    with TestClient(create_app(tmp_path)) as client:
        response = _read_without_writing(client, tmp_path)
    assert response.status_code == 500
    assert response.json() == {"code": "reparse_point"}
    assert list(outside.iterdir()) == []


def test_junction_on_the_pointer_parent_is_not_followed(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("directory junctions are a Windows reparse point")
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = tmp_path / "gold" / "published"
    parent.parent.mkdir()
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(parent), str(outside)],
        check=False,
        capture_output=True,
    )
    if created.returncode != 0:
        pytest.skip("junction creation is not permitted on this system")
    with TestClient(create_app(tmp_path)) as client:
        response = _read_without_writing(client, tmp_path)
    assert response.status_code == 500
    assert response.json() == {"code": "reparse_point"}
    assert list(outside.iterdir()) == []
