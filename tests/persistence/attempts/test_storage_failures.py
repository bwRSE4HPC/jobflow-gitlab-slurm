"""persistence / attempts / test_storage_failures contracts."""

import os

import pytest

from jobflow_gitlab_slurm.persistence.attempts import storage as attempts
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    AttemptIntegrityError,
    AttemptPublicationError,
)
from tests.persistence.attempts import _attempt_support as attempt_setup


@pytest.mark.parametrize("kind", ["file", "directory", "symlink", "fifo"])
def test_unsafe_existing_publication_unit_is_held(
    attempt_or_invocation, tmp_path, kind
):
    handle = attempt_setup.publish(attempt_or_invocation)
    preserved = tmp_path / "preserved-unit"
    handle.path.rename(preserved)
    if kind == "file":
        handle.path.write_bytes(b"preserve")
    elif kind == "directory":
        handle.path.mkdir()
    elif kind == "symlink":
        handle.path.symlink_to(preserved, target_is_directory=True)
    else:
        os.mkfifo(handle.path)

    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before
    assert (preserved / attempt_or_invocation.filename).is_file()


@pytest.mark.parametrize("kind", ["file", "symlink", "fifo"])
def test_unsafe_direct_parent_is_rejected(attempt_or_invocation, tmp_path, kind):
    attempt_setup.publish(attempt_or_invocation)
    parent = attempt_setup.paths(attempt_or_invocation)[0][-1]
    preserved = tmp_path / "preserved-parent"
    parent.rename(preserved)
    if kind == "file":
        parent.write_bytes(b"preserve")
    elif kind == "symlink":
        parent.symlink_to(preserved, target_is_directory=True)
    else:
        os.mkfifo(parent)

    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


@pytest.mark.parametrize(
    "data",
    [
        b"{",
        b"\xff",
        b"[]",
        b'{"schema_version":1,"schema_version":1}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
    ],
)
def test_malformed_metadata_is_held(attempt_or_invocation, data):
    handle = attempt_setup.publish(attempt_or_invocation)
    (handle.path / attempt_or_invocation.filename).write_bytes(data)
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


@pytest.mark.parametrize(
    "phase",
    ["containers", "staging", "metadata", "staging-flush", "parent-flush"],
)
def test_publication_failure_phases_retain_evidence(
    attempt_or_invocation, monkeypatch, phase
):
    parents, target = attempt_setup.paths(attempt_or_invocation)
    original_ensure = attempts.definitions._ensure_directory
    original_sync = attempts.definitions._sync_directory

    def fail(*arguments, **keywords):
        raise OSError("injected publication failure")

    def ensure(path):
        if path == parents[-1]:
            fail()
        original_ensure(path)

    def sync(path):
        failing = (
            path.name.startswith(".staging-")
            if phase == "staging-flush"
            else path == parents[-1]
        )
        if failing:
            fail()
        original_sync(path)

    with monkeypatch.context() as patch:
        if phase == "containers":
            patch.setattr(attempts.definitions, "_ensure_directory", ensure)
        elif phase == "staging":
            patch.setattr(attempts.tempfile, "mkdtemp", fail)
        elif phase == "metadata":
            patch.setattr(attempts, "_write_record", fail)
        else:
            patch.setattr(attempts.definitions, "_sync_directory", sync)

        with pytest.raises(AttemptPublicationError) as caught:
            attempt_setup.publish(attempt_or_invocation)

    error = caught.value
    attempt_setup.assert_publication_error(
        error,
        attempt_or_invocation,
        phase=phase,
        uncertain=phase == "parent-flush",
    )
    assert isinstance(error.__cause__, OSError)
    if phase in {"containers", "staging"}:
        assert error.staging_path is None
    elif phase == "parent-flush":
        assert target.is_dir()
        assert not error.staging_path.exists()
        assert (
            attempt_setup.read(attempt_or_invocation).record
            == attempt_or_invocation.record
        )
    else:
        assert error.staging_path.is_dir()
        assert not target.exists()

    assert (
        attempt_setup.publish(attempt_or_invocation).record
        == attempt_or_invocation.record
    )


@pytest.mark.parametrize("helper", ["_sync_file", "_sync_directory"])
def test_acknowledgment_failure_does_not_claim_success(
    attempt_or_invocation, monkeypatch, helper
):
    handle = attempt_setup.publish(attempt_or_invocation)
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)

    def fail(path):
        raise OSError("acknowledgment flush failed")

    with monkeypatch.context() as patch:
        patch.setattr(attempts.definitions, helper, fail)
        with pytest.raises(AttemptPublicationError) as caught:
            attempt_setup.publish(attempt_or_invocation)

    attempt_setup.assert_publication_error(
        caught.value,
        attempt_or_invocation,
        phase="acknowledge-flush",
        operation="acknowledge",
        uncertain=True,
    )
    assert caught.value.staging_path is None
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before
    assert attempt_setup.publish(attempt_or_invocation) == handle


def test_destination_appearing_before_rename_is_not_overwritten(
    attempt_or_invocation, monkeypatch
):
    target = attempt_setup.paths(attempt_or_invocation)[1]
    original = attempts.definitions._sync_directory

    def sync(path):
        original(path)
        if path.name.startswith(".staging-"):
            target.mkdir()

    with monkeypatch.context() as patch:
        patch.setattr(attempts.definitions, "_sync_directory", sync)
        with pytest.raises(AttemptPublicationError) as caught:
            attempt_setup.publish(attempt_or_invocation)

    attempt_setup.assert_publication_error(
        caught.value, attempt_or_invocation, phase="rename"
    )
    assert isinstance(caught.value.__cause__, FileExistsError)
    assert target.is_dir()
    assert not list(target.iterdir())
    assert caught.value.staging_path.is_dir()
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)


@pytest.mark.parametrize("writing", [False, True])
def test_fdopen_failure_closes_descriptor(attempt_or_invocation, monkeypatch, writing):
    handle = attempt_setup.publish(attempt_or_invocation)
    descriptors = []

    def fail(descriptor, mode, **keywords):
        descriptors.append(descriptor)
        raise OSError("fdopen failure")

    with monkeypatch.context() as patch:
        patch.setattr(attempts.os, "fdopen", fail)
        with pytest.raises(OSError):
            if writing:
                attempts._write_record(
                    attempt_or_invocation.run.payload.with_name("private-record.json"),
                    attempt_or_invocation.record,
                )
            else:
                attempts._read_record(
                    handle.path / attempt_or_invocation.filename,
                    type(attempt_or_invocation.record),
                )

    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert attempt_setup.read(attempt_or_invocation) == handle


def test_metadata_fsync_failure_closes_descriptor(attempt_or_invocation, monkeypatch):
    descriptors = []

    def fail(descriptor):
        descriptors.append(descriptor)
        raise OSError("metadata fsync failure")

    with monkeypatch.context() as patch:
        patch.setattr(attempts.os, "fsync", fail)
        with pytest.raises(OSError):
            attempts._write_record(
                attempt_or_invocation.run.payload.with_name("private-record.json"),
                attempt_or_invocation.record,
            )

    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_parser_recursion_failure_is_an_integrity_hold(
    attempt_or_invocation, monkeypatch
):
    handle = attempt_setup.publish(attempt_or_invocation)
    reader = (
        attempts._read_attempt_locked
        if attempt_or_invocation.mode == "attempt"
        else attempts._read_invocation_locked
    )

    def fail(*arguments, **keywords):
        raise RecursionError("injected parser limit")

    with monkeypatch.context() as patch:
        patch.setattr(attempts.json, "loads", fail)
        with pytest.raises(AttemptIntegrityError) as caught:
            reader(attempt_or_invocation.run.handle, attempt_or_invocation.record)

    assert isinstance(caught.value.__cause__, RecursionError)
    assert attempt_setup.read(attempt_or_invocation) == handle
