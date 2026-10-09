"""persistence / runs / test_failures contracts."""

import os

import pytest

from jobflow_gitlab_slurm.persistence.runs import storage
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunCreationError,
    open_run,
)
from tests.persistence.runs import _run_support as run_setup


@pytest.mark.parametrize(
    "boundary",
    [
        "mkdtemp",
        "original.json",
        "references.json",
        "run.json",
        "flow-sync",
        "artifacts-sync",
        "staging-sync",
        "rename",
        "published-root-sync",
    ],
)
def test_injected_creation_failures_retain_evidence(run_inputs, monkeypatch, boundary):
    original_write = storage._write_record
    original_sync = storage._sync_directory

    def fail():
        raise OSError("injected publication failure")

    def write(path, record):
        if path.name == boundary:
            fail()
        original_write(path, record)

    def sync(path):
        if (
            (boundary == "flow-sync" and path.name == "flow")
            or (boundary == "artifacts-sync" and path.name == "artifacts")
            or (boundary == "staging-sync" and path.name.startswith(".staging-"))
            or (
                boundary == "published-root-sync"
                and path == run_inputs.root
                and (run_inputs.root / run_setup.RUN_ID).exists()
            )
        ):
            fail()
        original_sync(path)

    monkeypatch.setattr(storage, "_write_record", write)
    monkeypatch.setattr(storage, "_sync_directory", sync)
    if boundary == "mkdtemp":
        monkeypatch.setattr(storage.tempfile, "mkdtemp", lambda **kwargs: fail())
    elif boundary == "rename":
        monkeypatch.setattr(storage.os, "rename", lambda source, destination: fail())

    with pytest.raises(RunCreationError) as caught:
        run_setup.create(run_inputs)
    error = caught.value
    assert isinstance(error.__cause__, OSError)
    assert error.publication_uncertain is (
        boundary in {"rename", "published-root-sync"}
    )
    if boundary == "mkdtemp":
        assert error.staging_path is None
    elif boundary == "published-root-sync":
        assert error.path.is_dir()
        assert not error.staging_path.exists()
        assert (
            open_run(run_inputs.root, run_setup.RUN_ID).manifest.run_id
            == run_setup.RUN_ID
        )
    else:
        assert error.staging_path.is_dir()
        assert not error.path.exists()


def test_directory_sync_closes_descriptor_after_failure(run_inputs, monkeypatch):
    descriptors = []

    def fail(descriptor):
        descriptors.append(descriptor)
        raise OSError("injected fsync failure")

    monkeypatch.setattr(storage.os, "fsync", fail)
    with pytest.raises(OSError, match="injected"):
        storage._sync_directory(run_inputs.root)
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_record_write_never_overwrites_and_retains_partial_failure(
    run_inputs, monkeypatch
):
    handle = run_setup.create(run_inputs)
    path = run_inputs.root / "record.json"
    path.write_bytes(b"preserved")
    with pytest.raises(FileExistsError):
        storage._write_record(path, handle.manifest)
    assert path.read_bytes() == b"preserved"
    partial = run_inputs.root / "partial.json"

    def fail(descriptor):
        raise OSError("injected fsync failure")

    monkeypatch.setattr(storage.os, "fsync", fail)
    with pytest.raises(OSError, match="injected"):
        storage._write_record(partial, handle.manifest)
    assert partial.is_file()
    assert partial.read_bytes() == handle.manifest.model_dump_json().encode("utf-8")
