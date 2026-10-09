"""persistence / attempts / test_definitions_failures contracts."""

import os
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.attempts import definitions
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    DefinitionIntegrityError,
    DefinitionPublicationError,
)
from tests.persistence.attempts import _definition_support as definition_setup


@pytest.mark.parametrize("kind", ["missing", "directory", "symlink", "fifo"])
def test_unsafe_source_fails_before_container_creation(definition_run, kind):
    original = definition_run.payload.read_bytes()
    definition_run.payload.unlink()
    if kind == "directory":
        definition_run.payload.mkdir()
    elif kind == "symlink":
        target = definition_run.payload.with_name("preserved-source")
        target.write_bytes(original)
        definition_run.payload.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(definition_run.payload)

    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises((OSError, ValueError)):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("level", [0, 1, 2, 3])
@pytest.mark.parametrize("kind", ["file", "symlink", "fifo"])
def test_unsafe_containers_block_reads_and_publication(definition_run, level, kind):
    definition_setup.publish(definition_run)
    parents, _ = definitions._paths(
        definition_run.handle, definition_setup.record_for(definition_run)
    )
    path = parents[level + 1]
    preserved = definition_run.path.parent / f"preserved-container-{level}"
    path.rename(preserved)
    if kind == "file":
        path.write_bytes(b"not a directory")
    elif kind == "symlink":
        path.symlink_to(preserved, target_is_directory=True)
    else:
        os.mkfifo(path)

    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "fifo"])
def test_unsafe_published_unit_is_never_overwritten(definition_run, kind):
    handle = definition_setup.publish(definition_run)
    preserved = definition_run.path.parent / "preserved-definition"
    handle.path.rename(preserved)
    if kind == "file":
        handle.path.write_bytes(b"keep")
    elif kind == "directory":
        handle.path.mkdir()
    elif kind == "symlink":
        handle.path.symlink_to(preserved, target_is_directory=True)
    else:
        os.mkfifo(handle.path)

    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before
    assert (preserved / "job.json").read_bytes() == definition_setup.JOB_BYTES


@pytest.mark.parametrize("filename", ["definition.json", "job.json"])
@pytest.mark.parametrize("kind", ["missing", "directory", "symlink", "fifo"])
def test_missing_or_unsafe_published_file_is_held(definition_run, filename, kind):
    handle = definition_setup.publish(definition_run)
    path = handle.path / filename
    original = path.read_bytes()
    path.unlink()
    if kind == "directory":
        path.mkdir()
    elif kind == "symlink":
        preserved = definition_run.path.parent / f"preserved-{filename}"
        preserved.write_bytes(original)
        path.symlink_to(preserved)
    elif kind == "fifo":
        os.mkfifo(path)

    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize(
    "phase",
    ["containers", "staging", "metadata", "payload", "staging-flush", "parent-flush"],
)
def test_publication_phase_failures_retain_evidence(definition_run, monkeypatch, phase):
    original_mkdir = Path.mkdir
    original_sync = definitions._sync_directory
    target = definition_setup.destination(definition_run)

    def fail(*arguments, **keywords):
        raise OSError("injected publication failure")

    def mkdir(path, *arguments, **keywords):
        if path == definition_run.path / "jobs":
            fail()
        return original_mkdir(path, *arguments, **keywords)

    def sync(path):
        failing = (
            path.name.startswith(".staging-")
            if phase == "staging-flush"
            else path == target.parent
        )
        if failing:
            fail()
        original_sync(path)

    with monkeypatch.context() as patch:
        if phase == "containers":
            patch.setattr(Path, "mkdir", mkdir)
        elif phase == "staging":
            patch.setattr(definitions.tempfile, "mkdtemp", fail)
        elif phase == "metadata":
            patch.setattr(definitions, "_write_metadata", fail)
        elif phase == "payload":
            patch.setattr(definitions, "stage_verified_artifact", fail)
        else:
            patch.setattr(definitions, "_sync_directory", sync)

        with pytest.raises(DefinitionPublicationError) as caught:
            definition_setup.publish(definition_run)

    error = caught.value
    definition_setup.assert_publication_error(
        error,
        definition_run,
        phase=phase,
        uncertain=phase == "parent-flush",
    )
    assert isinstance(error.__cause__, OSError)
    if phase in {"containers", "staging"}:
        assert error.staging_path is None
    elif phase == "parent-flush":
        assert target.is_dir()
        assert not error.staging_path.exists()
        assert definition_setup.read(
            definition_run
        ).record == definition_setup.record_for(definition_run)
    else:
        assert error.staging_path.is_dir()
        assert not target.exists()

    assert definition_setup.publish(
        definition_run
    ).record == definition_setup.record_for(definition_run)


@pytest.mark.parametrize("helper", ["_sync_file", "_sync_directory"])
def test_acknowledgment_flush_failure_is_not_success(
    definition_run, monkeypatch, helper
):
    handle = definition_setup.publish(definition_run)
    before = definition_setup.snapshot(definition_run.root)

    def fail(path):
        raise OSError("acknowledgment flush failed")

    with monkeypatch.context() as patch:
        patch.setattr(definitions, helper, fail)
        with pytest.raises(DefinitionPublicationError) as caught:
            definition_setup.publish(definition_run)

    definition_setup.assert_publication_error(
        caught.value,
        definition_run,
        phase="acknowledge-flush",
        operation="acknowledge",
        uncertain=True,
    )
    assert caught.value.staging_path is None
    assert definition_setup.snapshot(definition_run.root) == before
    assert definition_setup.publish(definition_run) == handle


def test_destination_appearing_before_rename_is_preserved(definition_run, monkeypatch):
    target = definition_setup.destination(definition_run)
    original = definitions._sync_directory

    def sync(path):
        original(path)
        if path.name.startswith(".staging-"):
            target.mkdir()

    with monkeypatch.context() as patch:
        patch.setattr(definitions, "_sync_directory", sync)
        with pytest.raises(DefinitionPublicationError) as caught:
            definition_setup.publish(definition_run)

    definition_setup.assert_publication_error(
        caught.value, definition_run, phase="rename"
    )
    assert isinstance(caught.value.__cause__, FileExistsError)
    assert target.is_dir()
    assert not list(target.iterdir())
    assert caught.value.staging_path.is_dir()
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)


@pytest.mark.parametrize("writing", [False, True])
def test_fdopen_failure_closes_descriptor(definition_run, monkeypatch, writing):
    handle = definition_setup.publish(definition_run)
    descriptors = []
    original = definitions.os.fdopen

    def fail(descriptor, mode, **keywords):
        descriptors.append(descriptor)
        raise OSError("fdopen failure")

    with monkeypatch.context() as patch:
        patch.setattr(definitions.os, "fdopen", fail)
        with pytest.raises(OSError):
            if writing:
                definitions._write_metadata(
                    definition_run.payload.with_name("private-metadata.json"),
                    definition_setup.record_for(definition_run),
                )
            else:
                definitions._read_metadata(handle.path / "definition.json")

    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert original is definitions.os.fdopen
    assert definition_setup.read(definition_run) == handle


@pytest.mark.parametrize("directory", [False, True])
def test_sync_failure_closes_descriptor(definition_run, monkeypatch, directory):
    handle = definition_setup.publish(definition_run)
    descriptors = []

    def fail(descriptor):
        descriptors.append(descriptor)
        raise OSError("fsync failure")

    with monkeypatch.context() as patch:
        patch.setattr(definitions.os, "fsync", fail)
        with pytest.raises(OSError):
            if directory:
                definitions._sync_directory(handle.path)
            else:
                definitions._sync_file(handle.path / "job.json")

    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert definition_setup.read(definition_run) == handle


def test_parser_recursion_failure_becomes_integrity_hold(definition_run, monkeypatch):
    handle = definition_setup.publish(definition_run)

    def fail(*arguments, **keywords):
        raise RecursionError("injected parser limit")

    with monkeypatch.context() as patch:
        patch.setattr(definitions.json, "loads", fail)
        with pytest.raises(DefinitionIntegrityError) as caught:
            definitions._read_locked(
                definition_run.handle, definition_setup.record_for(definition_run)
            )

    assert isinstance(caught.value.__cause__, RecursionError)
    assert definition_setup.read(definition_run) == handle
