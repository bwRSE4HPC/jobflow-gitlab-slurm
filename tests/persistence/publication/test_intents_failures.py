"""persistence / publication / test_intents_failures contracts."""

import errno
import json
import os
import stat
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.publication import storage
from jobflow_gitlab_slurm.persistence.publication.storage import (
    IntentInspectionError,
    IntentIntegrityError,
    IntentPublicationError,
    read_owned_publication_intent,
)
from tests.persistence.publication import _intent_support as intent_setup


@pytest.mark.parametrize("operation", ["read", "publish"])
@pytest.mark.parametrize("entry", ["directory", "symlink", "fifo", "invalid"])
def test_unsafe_or_invalid_intent_is_held(
    intent_staging, operation, entry, monkeypatch
):
    if entry == "directory":
        intent_staging.path.mkdir()
    elif entry == "symlink":
        intent_staging.path.symlink_to(intent_staging.invocation.path / "missing")
    elif entry == "fifo":
        os.mkfifo(intent_staging.path)
    else:
        intent_staging.path.write_bytes(intent_setup.SECRET.encode())

    before = intent_setup.snapshot(intent_staging.root)
    if entry != "invalid":

        def forbidden(*args, **kwargs):
            raise AssertionError("unsafe entry must not be opened")

        monkeypatch.setattr(storage, "_read_bytes", forbidden)

    with pytest.raises(IntentIntegrityError) as caught:
        if operation == "read":
            intent_setup.read(intent_staging)
        else:
            intent_setup.publish(intent_staging)
    assert intent_setup.snapshot(intent_staging.root) == before
    assert intent_setup.SECRET not in str(caught.value)
    assert intent_setup.SECRET not in json.dumps(caught.value.to_report())


def test_unsafe_invocation_directory_is_rejected(intent_staging):
    with owned_invocation(*intent_staging.parameters) as ownership:
        moved = intent_staging.invocation.path.with_name("retained-invocation")
        intent_staging.invocation.path.rename(moved)
        intent_staging.invocation.path.symlink_to(moved, target_is_directory=True)
        with pytest.raises(IntentIntegrityError, match="unsafe_invocation_directory"):
            read_owned_publication_intent(ownership)


def test_preflight_io_failure_is_distinct(intent_staging, monkeypatch):
    intent_setup.publish(intent_staging)

    def fail(*args, **kwargs):
        raise OSError(errno.EACCES, intent_setup.SECRET)

    monkeypatch.setattr(storage, "_read_bytes", fail)
    with pytest.raises(IntentInspectionError) as caught:
        intent_setup.read(intent_staging)
    assert caught.value.errno == errno.EACCES
    assert intent_setup.SECRET not in str(caught.value)
    with pytest.raises(IntentInspectionError):
        intent_setup.publish(intent_staging)


def test_published_path_io_failure_is_distinct(intent_staging, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError(errno.EACCES, intent_setup.SECRET)

    monkeypatch.setattr(storage, "_require_absent", fail)
    with pytest.raises(IntentInspectionError, match="published_path_unavailable"):
        intent_setup.publish(intent_staging)
    assert not intent_staging.path.exists()


@pytest.mark.parametrize("phase", intent_setup.PHASES)
def test_publication_failure_reports_phase_and_preserves_evidence(
    intent_staging, monkeypatch, phase
):
    intent_setup.install_failure(intent_staging, monkeypatch, phase)
    with pytest.raises(IntentPublicationError) as caught:
        intent_setup.publish(intent_staging)
    error = caught.value
    assert error.operation == "publish"
    assert error.phase == phase
    assert error.intent_publication_uncertain is (
        intent_setup.PHASES.index(phase) >= intent_setup.PHASES.index("intent-rename")
    )
    assert intent_setup.SECRET not in str(error)
    report = error.to_report()
    assert intent_setup.SECRET not in json.dumps(report)
    assert json.loads(json.dumps(report)) == report
    assert report["identity"]["invocation_id"] == intent_setup.INVOCATION_ID
    assert error.errno == errno.EIO
    assert error.__suppress_context__ is True
    if phase == "temporary-create":
        assert error.temporary_path is None
    else:
        assert error.temporary_path is not None
        assert error.temporary_path.parent == intent_staging.invocation.path
        if intent_setup.PHASES.index(phase) <= intent_setup.PHASES.index(
            "intent-rename"
        ):
            assert error.temporary_path.exists()


@pytest.mark.parametrize("phase", ["file", "parent", "verification"])
def test_acknowledgment_failures_remain_uncertain(intent_staging, monkeypatch, phase):
    intent_setup.publish(intent_staging)
    before = intent_setup.snapshot(intent_staging.root)

    def fail(*args, **kwargs):
        raise OSError(errno.EIO, intent_setup.SECRET)

    if phase == "file":
        monkeypatch.setattr(storage, "_sync_file", fail)
    elif phase == "parent":
        monkeypatch.setattr(storage, "_sync_directory", fail)
    else:
        original = storage._read
        calls = 0

        def read_existing(ownership, path):
            nonlocal calls
            calls += 1
            if calls == 2:
                fail()
            return original(ownership, path)

        monkeypatch.setattr(storage, "_read", read_existing)

    with pytest.raises(IntentPublicationError) as caught:
        intent_setup.publish(intent_staging)
    assert caught.value.operation == "acknowledge"
    assert caught.value.intent_publication_uncertain is True
    assert caught.value.temporary_path is None
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("target", ["intent", "published"])
def test_destination_appearing_before_rename_is_preserved(
    intent_staging, monkeypatch, target
):
    original = storage._sync_directory

    def sync(path):
        original(path)
        if not intent_staging.path.exists():
            if target == "intent":
                intent_staging.path.write_bytes(b"retained competing evidence")
            else:
                intent_staging.published.mkdir()

    monkeypatch.setattr(storage, "_sync_directory", sync)
    with pytest.raises(IntentPublicationError) as caught:
        intent_setup.publish(intent_staging)
    assert caught.value.intent_publication_uncertain is False
    assert list(intent_staging.invocation.path.glob(".publication-intent-*"))
    if target == "intent":
        assert intent_staging.path.read_bytes() == b"retained competing evidence"
    else:
        assert intent_staging.published.is_dir()


@pytest.mark.parametrize("damage", ["nonregular", "cross-device"])
def test_temporary_descriptor_guard(intent_staging, monkeypatch, damage):
    original_create = storage.tempfile.mkstemp
    original_stat = os.fstat
    descriptors = []

    def create(*args, **kwargs):
        descriptor, name = original_create(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor, name

    def fstat(descriptor):
        metadata = original_stat(descriptor)
        if descriptor in descriptors:
            return SimpleNamespace(
                st_mode=(stat.S_IFIFO if damage == "nonregular" else metadata.st_mode),
                st_dev=(
                    metadata.st_dev + 1 if damage == "cross-device" else metadata.st_dev
                ),
            )
        return metadata

    monkeypatch.setattr(storage.tempfile, "mkstemp", create)
    monkeypatch.setattr(storage.os, "fstat", fstat)
    with pytest.raises(IntentPublicationError) as caught:
        intent_setup.publish(intent_staging)
    assert caught.value.phase == "temporary-check"
    assert caught.value.reason == (
        "unsafe_temporary_file" if damage == "nonregular" else "cross_filesystem"
    )
    for descriptor in descriptors:
        with pytest.raises(OSError):
            original_stat(descriptor)


def test_stream_failure_closes_temporary_descriptor(intent_staging, monkeypatch):
    original_create = storage.tempfile.mkstemp
    original_fdopen = os.fdopen
    descriptors = []

    def create(*args, **kwargs):
        descriptor, name = original_create(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor, name

    def fdopen(descriptor, mode, *args, **kwargs):
        if mode == "wb":
            raise OSError(errno.EIO, intent_setup.SECRET)
        return original_fdopen(descriptor, mode, *args, **kwargs)

    monkeypatch.setattr(storage.tempfile, "mkstemp", create)
    monkeypatch.setattr(storage.os, "fdopen", fdopen)
    with pytest.raises(IntentPublicationError):
        intent_setup.publish(intent_staging)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
