"""persistence / runs / test_creation contracts."""

import stat
import sys

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.runs.storage import (
    create_run,
    open_run,
)
from tests.persistence.runs import _run_support as run_setup


@pytest.mark.parametrize("kind", ["directory", "file", "symlink", "dangling"])
def test_existing_run_is_never_overwritten(run_inputs, kind):
    path = run_inputs.root / run_setup.RUN_ID
    target = run_inputs.root / "preserved"
    if kind == "directory":
        path.mkdir()
        (path / "keep").write_bytes(b"keep")
    elif kind == "file":
        path.write_bytes(b"keep")
    else:
        if kind == "symlink":
            target.write_bytes(b"keep")
        path.symlink_to(target)
    with pytest.raises(FileExistsError, match="inspect/reopen"):
        run_setup.create(run_inputs)
    if kind == "directory":
        assert (path / "keep").read_bytes() == b"keep"
    elif kind == "file":
        assert path.read_bytes() == b"keep"
    elif kind == "symlink":
        assert target.read_bytes() == b"keep"
    else:
        assert path.is_symlink()
    assert not list(run_inputs.root.glob(".staging-*"))


@pytest.mark.parametrize(
    "run_id",
    [
        "../escape",
        "not-a-uuid",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        None,
    ],
)
def test_invalid_run_id_fails_before_filesystem_changes(run_inputs, run_id):
    with pytest.raises(ValidationError):
        create_run(
            run_inputs.site,
            run_inputs.request,
            *run_inputs.sources,
            run_id=run_id,
        )
    assert not list(run_inputs.root.iterdir())
    with pytest.raises(ValidationError):
        open_run(run_inputs.root, run_id)


@pytest.mark.parametrize("kind", ["missing", "file", "symlink"])
def test_invalid_root_is_rejected(run_inputs, tmp_path, kind):
    root = tmp_path / "invalid-root"
    if kind == "file":
        root.write_bytes(b"not a directory")
    elif kind == "symlink":
        root.symlink_to(run_inputs.root, target_is_directory=True)
    run_inputs.site.storage.runs_root = str(root)
    error = FileNotFoundError if kind == "missing" else ValueError
    with pytest.raises(error):
        run_setup.create(run_inputs)
    assert not list(run_inputs.root.iterdir())


def test_site_changes_do_not_modify_created_run(run_inputs):
    handle = run_setup.create(run_inputs)
    run_inputs.site.slurm.account = "changed"
    run_inputs.site.slurm.partitions.reverse()
    reopened = open_run(run_inputs.root, run_setup.RUN_ID)
    assert reopened == handle
    assert reopened.manifest.site_snapshot.to_site().slurm.account == "example-account"


def test_creation_preserves_bytes_and_records_external_references(run_inputs):
    handle = run_setup.create(
        run_inputs, workspace_expires_at="2100-01-01T00:00:00.000000Z"
    )
    assert handle.path == run_inputs.root / run_setup.RUN_ID
    assert handle.manifest.request == run_inputs.request
    assert handle.manifest.workspace_expires_at == "2100-01-01T00:00:00.000000Z"
    assert handle.flow.payload.size_bytes == len(run_setup.FLOW_BYTES)
    assert (handle.path / "flow/payload.json").read_bytes() == run_setup.FLOW_BYTES
    assert handle.artifacts.consumer_code.path == str(run_inputs.sources[1])
    assert handle.artifacts.worker_runtime.path == str(run_inputs.sources[2])
    assert "never_import_me" not in sys.modules
    assert not list(run_inputs.root.glob(".staging-*"))
    assert {
        str(path.relative_to(handle.path))
        for path in handle.path.rglob("*")
        if path.is_file()
    } == {
        "run.json",
        "flow/original.json",
        "flow/payload.json",
        "artifacts/references.json",
    }
    assert open_run(run_inputs.root, run_setup.RUN_ID, verify_external=True) == handle
    assert stat.S_IMODE(handle.path.stat().st_mode) == 0o700
    for path in handle.path.rglob("*"):
        expected = 0o700 if path.is_dir() else 0o600
        assert stat.S_IMODE(path.stat().st_mode) == expected
    assert (
        stat.S_IMODE(
            (run_inputs.root / ".locks" / f"{run_setup.RUN_ID}.lock").stat().st_mode
        )
        == 0o600
    )
