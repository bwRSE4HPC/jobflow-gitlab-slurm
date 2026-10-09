"""persistence / runs / test_integrity contracts."""

import hashlib
import json
import os

import pytest

from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunCreationError,
    open_run,
)
from tests.persistence.runs import _run_support as run_setup


@pytest.mark.parametrize("source_index", [0, 1, 2])
def test_bad_artifact_bytes_preserve_failed_staging(run_inputs, source_index):
    run_inputs.sources[source_index].write_bytes(b"wrong bytes")
    with pytest.raises(RunCreationError) as caught:
        run_setup.create(run_inputs)
    error = caught.value
    assert isinstance(error.__cause__, ValueError)
    assert error.run_id == run_setup.RUN_ID
    assert error.path == run_inputs.root / run_setup.RUN_ID
    assert error.staging_path.is_dir()
    assert not error.publication_uncertain
    assert not error.path.exists()
    assert "Inspect/reopen this same run ID" in str(error)


def test_missing_artifact_preserves_inspection_path(run_inputs):
    run_inputs.sources[1].unlink()
    with pytest.raises(RunCreationError) as caught:
        run_setup.create(run_inputs)
    assert isinstance(caught.value.__cause__, FileNotFoundError)
    assert caught.value.staging_path.is_dir()


@pytest.mark.parametrize("relative", [".", "flow", "artifacts"])
def test_symlinked_run_directories_are_rejected(run_inputs, relative):
    handle = run_setup.create(run_inputs)
    path = handle.path if relative == "." else handle.path / relative
    target = path.with_name(path.name + "-preserved")
    path.rename(target)
    path.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="real directory"):
        open_run(run_inputs.root, run_setup.RUN_ID)


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "missing"])
def test_unsafe_record_files_are_rejected(run_inputs, kind):
    handle = run_setup.create(run_inputs)
    path = handle.path / "run.json"
    original = path.read_bytes()
    path.unlink()
    if kind == "symlink":
        target = handle.path / "preserved.json"
        target.write_bytes(original)
        path.symlink_to(target)
        error = OSError
    elif kind == "fifo":
        os.mkfifo(path)
        error = ValueError
    elif kind == "directory":
        path.mkdir()
        error = ValueError
    else:
        error = FileNotFoundError
    with pytest.raises(error):
        open_run(run_inputs.root, run_setup.RUN_ID)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":1,"schema_version":1}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
        b"{",
        b"\xff",
    ],
)
def test_invalid_json_records_are_rejected(run_inputs, payload):
    handle = run_setup.create(run_inputs)
    (handle.path / "run.json").write_bytes(payload)
    with pytest.raises(ValueError):
        open_run(run_inputs.root, run_setup.RUN_ID)


@pytest.mark.parametrize(
    ("relative", "field", "value"),
    [
        ("run.json", "schema_version", 2),
        ("run.json", "schema_version", True),
        ("run.json", "kind", "wrong"),
        ("run.json", "unexpected", "value"),
        ("run.json", "jobflow_version", "99.0"),
        ("run.json", "backend_version", "99.0"),
        ("flow/original.json", "schema_version", 2),
        ("flow/original.json", "run_id", run_setup.OTHER_ID),
        ("flow/original.json", "created_at", "2026-01-01T00:00:00.000000Z"),
        ("artifacts/references.json", "schema_version", 2),
        ("artifacts/references.json", "run_id", run_setup.OTHER_ID),
        ("artifacts/references.json", "created_at", "2026-01-01T00:00:00.000000Z"),
    ],
)
def test_incompatible_or_inconsistent_records_fail(run_inputs, relative, field, value):
    handle = run_setup.create(run_inputs)
    run_setup.rewrite(handle.path / relative, lambda data: data.update({field: value}))
    with pytest.raises(ValueError):
        open_run(run_inputs.root, run_setup.RUN_ID)


@pytest.mark.parametrize("artifact", ["consumer_code", "worker_runtime"])
def test_external_digest_must_match_request(run_inputs, artifact):
    handle = run_setup.create(run_inputs)
    run_setup.rewrite(
        handle.path / "artifacts/references.json",
        lambda data: data[artifact].update({"sha256": "0" * 64}),
    )
    with pytest.raises(ValueError, match="SHA-256 does not match request"):
        open_run(run_inputs.root, run_setup.RUN_ID)


def test_directory_identity_must_match_manifest(run_inputs):
    handle = run_setup.create(run_inputs)
    for relative in (
        "run.json",
        "flow/original.json",
        "artifacts/references.json",
    ):
        run_setup.rewrite(
            handle.path / relative,
            lambda data: data.update({"run_id": run_setup.OTHER_ID}),
        )
    with pytest.raises(ValueError, match="directory name"):
        open_run(run_inputs.root, run_setup.RUN_ID)


def test_root_must_match_pinned_site(run_inputs):
    handle = run_setup.create(run_inputs)

    def alter_root(data):
        snapshot = data["site_snapshot"]
        site = json.loads(snapshot["content"])
        site["storage"]["runs_root"] += "-other"
        content = json.dumps(
            site, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        snapshot.update(
            content=content,
            sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
        )

    run_setup.rewrite(handle.path / "run.json", alter_root)
    with pytest.raises(ValueError, match="supplied root"):
        open_run(run_inputs.root, run_setup.RUN_ID)


def test_corrupted_flow_bytes_are_rejected(run_inputs):
    handle = run_setup.create(run_inputs)
    (handle.path / "flow/payload.json").write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        open_run(run_inputs.root, run_setup.RUN_ID)


def test_flow_digest_claim_must_match_request(run_inputs):
    handle = run_setup.create(run_inputs)
    run_setup.rewrite(
        handle.path / "flow/original.json",
        lambda data: data["payload"].update({"sha256": "0" * 64}),
    )
    with pytest.raises(ValueError, match="SHA-256"):
        open_run(run_inputs.root, run_setup.RUN_ID)


@pytest.mark.parametrize("external", [False, True])
def test_artifact_size_claims_are_checked(run_inputs, external):
    handle = run_setup.create(run_inputs)
    if external:
        relative = "artifacts/references.json"
        field = "consumer_code"
    else:
        relative = "flow/original.json"
        field = "payload"
    run_setup.rewrite(
        handle.path / relative,
        lambda data: data[field].update({"size_bytes": 999999}),
    )
    with pytest.raises(ValueError, match="size mismatch"):
        open_run(run_inputs.root, run_setup.RUN_ID, verify_external=external)
