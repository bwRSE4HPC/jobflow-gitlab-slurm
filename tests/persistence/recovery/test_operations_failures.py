"""persistence / recovery / test_operations_failures contracts."""

import errno
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
)
from jobflow_gitlab_slurm.persistence.recovery import operations as recovery
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.operations import (
    BundleRecoveryCompletionError,
    BundleRecoveryIntegrityError,
)
from tests.persistence.recovery import _operation_support as operation_setup
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(("original", "current"), operation_setup.REGRESSIONS)
def test_regressions_are_held_before_changes(recoverable_bundle, original, current):
    context = recoverable_bundle
    operation_setup.original_state(context, original)
    operation_setup.current_state(context, current)
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context)

    assert caught.value.code == "bundle_not_valid_forward_progress"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt", "intent"])
def test_malformed_audit_evidence_is_held(recoverable_bundle, record):
    context = recoverable_bundle
    operation_setup.retain_request(context)
    path = {
        "request": context.path,
        "receipt": context.audit,
        "intent": context.intent_path,
    }[record]
    path.write_bytes(b"{")
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_partial_temporary_audit_evidence_is_held(recoverable_bundle, record):
    context = recoverable_bundle
    context.unit.mkdir(parents=True)
    if record == "receipt":
        operation_setup.retain_request(context)
    path = context.unit / f".recovery-{record}-retained"
    path.write_bytes(b"{")
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("status", ["published_uncommitted", "committed"])
@pytest.mark.parametrize("damage", ["symlink", "fifo", "directory", "empty-suffix"])
def test_unsafe_temporary_markers_are_never_opened(
    recoverable_bundle, monkeypatch, status, damage
):
    context = recoverable_bundle
    operation_setup.original_state(context, status)
    target = context.invocation.path / ".bundle-commit-retained"
    if damage == "symlink":
        target.symlink_to(context.intent_path)
    elif damage == "fifo":
        os.mkfifo(target)
    elif damage == "directory":
        target.mkdir()
    else:
        (context.invocation.path / ".bundle-commit-").write_bytes(b"retained")
    before = request_setup.snapshot(context.root)
    original = recovery._read_bytes

    def guarded_read(path):
        if path.name.startswith(".bundle-commit-"):
            pytest.fail("unsafe marker must not be opened")
        return original(path)

    monkeypatch.setattr(recovery, "_read_bytes", guarded_read)
    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context)
    assert caught.value.code == "unsafe_temporary_marker"
    assert request_setup.snapshot(context.root) == before


def test_cross_filesystem_temporary_marker_is_held(recoverable_bundle, monkeypatch):
    context = recoverable_bundle
    target = operation_setup.marker_temporary(
        context, encode_bundle_commit(context.intent.expected_commit)
    )
    before = request_setup.snapshot(context.root)
    original = Path.lstat

    def fake_device(path, *args, **kwargs):
        metadata = original(path, *args, **kwargs)
        if path == target:
            return SimpleNamespace(
                st_mode=metadata.st_mode,
                st_dev=metadata.st_dev + 1,
            )
        return metadata

    with monkeypatch.context() as patch:
        patch.setattr(Path, "lstat", fake_device)
        with pytest.raises(BundleRecoveryIntegrityError) as caught:
            operation_setup.recover(context)
        assert caught.value.code == "unsafe_temporary_marker"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("phase", operation_setup.PHASES)
def test_phase_failures_preserve_payloads_and_report_uncertainty(
    recoverable_bundle, monkeypatch, phase
):
    context = recoverable_bundle
    before_payloads = operation_setup.payload_snapshot(context)
    original = recovery._step

    def fail_at(progress, current_phase, operation, *args):
        if current_phase == phase:
            progress.phase = current_phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, current_phase, operation, *args)

    monkeypatch.setattr(recovery, "_step", fail_at)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)

    error = caught.value
    assert error.phase == phase
    assert error.errno == errno.EIO
    assert error.request_acknowledged == (
        operation_setup.PHASES.index(phase)
        > operation_setup.PHASES.index("request-acknowledgment")
    )
    assert error.bundle_publication_uncertain == (
        operation_setup.PHASES.index(phase)
        >= operation_setup.PHASES.index("directory-rename")
    )
    assert error.audit_completion_uncertain == (phase == "receipt-completion")
    assert request_setup.SECRET not in str(error)
    assert request_setup.SECRET not in json.dumps(error.to_report())
    assert operation_setup.payload_snapshot(context) == before_payloads
    assert not context.audit.exists()

    if operation_setup.PHASES.index(phase) > operation_setup.PHASES.index(
        "marker-rename"
    ):
        assert (context.published / "COMMIT.json").read_bytes() == (
            encode_bundle_commit(context.intent.expected_commit)
        )
        assert not error.temporary_path.exists()
    elif error.temporary_path is not None:
        assert error.temporary_path.exists()


def test_request_acknowledgment_failure_never_mutates_bundle(
    recoverable_bundle, monkeypatch
):
    context = recoverable_bundle
    before = request_setup.snapshot(context.staging)
    original = requests._step

    def fail_at(progress, phase, operation, *args):
        if phase == "request-rename":
            progress.phase = phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(requests, "_step", fail_at)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)
    assert caught.value.phase == "request-acknowledgment"
    assert caught.value.request_publication_uncertain
    assert not caught.value.request_acknowledged
    assert request_setup.snapshot(context.staging) == before
    assert not context.published.exists()


def test_partial_marker_write_closes_descriptor_and_retains_evidence(
    recoverable_bundle, monkeypatch
):
    context = recoverable_bundle
    operation_setup.retain_request(context)
    descriptor_used = None

    def fail(descriptor, data):
        nonlocal descriptor_used
        descriptor_used = descriptor
        os.write(descriptor, data[:10])
        raise OSError(errno.EIO, request_setup.SECRET)

    monkeypatch.setattr(requests, "_write_bytes", fail)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)
    assert caught.value.phase == "marker-write"
    assert (
        caught.value.temporary_path.read_bytes()
        == (encode_bundle_commit(context.intent.expected_commit)[:10])
    )
    with pytest.raises(OSError) as closed:
        os.fstat(descriptor_used)
    assert closed.value.errno == errno.EBADF


@pytest.mark.parametrize("destination", ["published", "marker"])
def test_appearing_destinations_are_not_replaced(
    recoverable_bundle, monkeypatch, destination
):
    context = recoverable_bundle
    original = recovery._step
    phase_to_change = (
        "published-destination-check"
        if destination == "published"
        else "marker-destination-check"
    )

    def appeared(progress, phase, operation, *args):
        if phase == phase_to_change:
            if destination == "published":
                context.published.mkdir()
                (context.published / "foreign").write_bytes(b"retained")
            else:
                (context.published / "COMMIT.json").write_bytes(b"retained")
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(recovery, "_step", appeared)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)
    assert caught.value.phase == phase_to_change
    target = context.published / (
        "foreign" if destination == "published" else "COMMIT.json"
    )
    assert target.read_bytes() == b"retained"


def test_reinspection_rejects_regression_before_bundle_mutation(
    recoverable_bundle, monkeypatch
):
    context = recoverable_bundle
    operation_setup.original_state(context, "committed")
    original = recovery._step

    def regressed(progress, phase, operation, *args):
        if phase == "recovery-reinspection":
            operation_setup.current_state(context, "published_uncommitted")
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(recovery, "_step", regressed)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)
    assert caught.value.phase == "recovery-reinspection"
    assert caught.value.reason == "bundle_not_valid_forward_progress"
    assert not context.audit.exists()
