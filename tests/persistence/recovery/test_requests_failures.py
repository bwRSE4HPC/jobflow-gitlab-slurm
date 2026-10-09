"""persistence / recovery / test_requests_failures contracts."""

import errno
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.bundles.inspection import BundleInspectionError
from jobflow_gitlab_slurm.persistence.publication.storage import (
    IntentInspectionError,
)
from jobflow_gitlab_slurm.persistence.recovery import requests as storage
from jobflow_gitlab_slurm.persistence.recovery.records import (
    encode_recovery_request,
)
from jobflow_gitlab_slurm.persistence.recovery.requests import (
    RecoveryRequestInspectionError,
    RecoveryRequestIntegrityError,
    RecoveryRequestPublicationError,
)
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    "damage",
    [
        "root-file",
        "root-symlink",
        "invalid-name",
        "unit-file",
        "unit-symlink",
        "request-directory",
        "request-symlink",
        "request-fifo",
        "temporary-symlink",
        "unknown-file",
        "empty-temporary-suffix",
    ],
)
def test_unsafe_namespace_is_held_without_opening(
    recovery_staging, damage, monkeypatch
):
    if damage == "root-file":
        recovery_staging.recovery_root.write_bytes(b"retained")
    elif damage == "root-symlink":
        recovery_staging.recovery_root.symlink_to(recovery_staging.root / "missing")
    else:
        recovery_staging.recovery_root.mkdir()
        if damage == "invalid-name":
            (recovery_staging.recovery_root / "not-a-uuid").mkdir()
        elif damage == "unit-file":
            recovery_staging.unit.write_bytes(b"retained")
        elif damage == "unit-symlink":
            recovery_staging.unit.symlink_to(recovery_staging.root / "missing")
        else:
            recovery_staging.unit.mkdir()
            if damage == "request-directory":
                recovery_staging.path.mkdir()
            elif damage == "request-symlink":
                recovery_staging.path.symlink_to(recovery_staging.root / "missing")
            elif damage == "request-fifo":
                os.mkfifo(recovery_staging.path)
            elif damage == "temporary-symlink":
                (recovery_staging.unit / ".recovery-request-retained").symlink_to(
                    recovery_staging.root / "missing"
                )
            elif damage == "unknown-file":
                (recovery_staging.unit / "unexpected.json").write_bytes(b"retained")
            else:
                (recovery_staging.unit / ".recovery-request-").write_bytes(b"retained")

    before = request_setup.snapshot(recovery_staging.root)

    def forbidden(*args, **kwargs):
        pytest.fail("unsafe audit entries must not be opened")

    monkeypatch.setattr(storage, "_read_bytes", forbidden)
    with pytest.raises(RecoveryRequestIntegrityError, match="invalid_audit_namespace"):
        request_setup.read(recovery_staging)
    assert request_setup.snapshot(recovery_staging.root) == before


def test_cross_filesystem_namespace_is_held(recovery_staging, monkeypatch):
    recovery_staging.recovery_root.mkdir()
    original = Path.lstat

    def changed(path, *args, **kwargs):
        metadata = original(path, *args, **kwargs)
        if path == recovery_staging.recovery_root:
            return SimpleNamespace(
                st_mode=metadata.st_mode,
                st_dev=metadata.st_dev + 1,
            )
        return metadata

    monkeypatch.setattr(Path, "lstat", changed)
    with pytest.raises(RecoveryRequestIntegrityError):
        request_setup.read(recovery_staging)


@pytest.mark.parametrize(
    "target", ["namespace", "request", "intent", "bundle", "manifest"]
)
def test_preflight_io_failures_are_sanitized(recovery_staging, monkeypatch, target):
    if target == "request":
        request_setup.persist(recovery_staging)

    def fail(*args, **kwargs):
        raise OSError(errno.EACCES, request_setup.SECRET)

    if target == "namespace":
        monkeypatch.setattr(storage, "_scan", fail)
    elif target == "request":
        monkeypatch.setattr(storage, "_read_bytes", fail)
    elif target == "intent":

        def unavailable(ownership):
            raise IntentInspectionError(
                ownership.invocation.record,
                recovery_staging.intent_path,
                "unavailable",
                errno.EACCES,
            )

        monkeypatch.setattr(storage, "read_owned_publication_intent", unavailable)
    elif target == "bundle":

        def unavailable(ownership):
            raise BundleInspectionError(
                ownership.invocation.record,
                ownership.invocation.path,
                errno.EACCES,
            )

        monkeypatch.setattr(storage, "inspect_owned_bundle", unavailable)
    else:
        monkeypatch.setattr(storage, "_read_bytes", fail)

    with pytest.raises(RecoveryRequestInspectionError) as caught:
        if target in {"namespace", "request"}:
            request_setup.read(recovery_staging)
        else:
            request_setup.persist(recovery_staging)
    assert caught.value.errno == errno.EACCES
    assert request_setup.SECRET not in str(caught.value)
    assert request_setup.SECRET not in json.dumps(caught.value.to_report())


@pytest.mark.parametrize("phase", request_setup.PHASES)
def test_publication_failures_report_phase_and_preserve_bundle(
    recovery_staging, monkeypatch, phase
):
    before = request_setup.snapshot(recovery_staging.staging)
    original = storage._step

    def fail_at(progress, current_phase, operation, *args):
        if current_phase == phase:
            progress.phase = current_phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, current_phase, operation, *args)

    monkeypatch.setattr(storage, "_step", fail_at)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)

    error = caught.value
    assert error.phase == phase
    assert error.operation == "publish"
    assert error.preparation_uncertain
    assert error.request_publication_uncertain == (
        request_setup.PHASES.index(phase)
        >= request_setup.PHASES.index("request-rename")
    )
    assert error.errno == errno.EIO
    assert request_setup.SECRET not in str(error)
    assert request_setup.SECRET not in json.dumps(error.to_report())
    assert request_setup.snapshot(recovery_staging.staging) == before
    assert not recovery_staging.published.exists()
    rename_completed = request_setup.PHASES.index(phase) > request_setup.PHASES.index(
        "request-rename"
    )
    if rename_completed:
        assert error.temporary_path is not None
        assert not error.temporary_path.exists()
        assert recovery_staging.path.read_bytes() == encode_recovery_request(
            recovery_staging.request
        )
    else:
        assert not recovery_staging.path.exists()
        if error.temporary_path is not None:
            assert error.temporary_path.exists()


@pytest.mark.parametrize("phase", request_setup.ACK_PHASES)
def test_acknowledgment_failures_remain_uncertain(recovery_staging, monkeypatch, phase):
    request_setup.persist(recovery_staging)
    before = request_setup.snapshot(recovery_staging.root)
    original = storage._step

    def fail_at(progress, current_phase, operation, *args):
        if current_phase == phase:
            progress.phase = current_phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, current_phase, operation, *args)

    monkeypatch.setattr(storage, "_step", fail_at)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.operation == "acknowledge"
    assert caught.value.request_publication_uncertain
    assert not caught.value.preparation_uncertain
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("damage", ["nonregular", "cross-device"])
def test_temporary_descriptor_guard(recovery_staging, monkeypatch, damage):
    original = storage._check_temporary
    real_fstat = os.fstat

    def changed(ownership, descriptor):
        metadata = real_fstat(descriptor)
        fake = SimpleNamespace(
            st_mode=stat.S_IFIFO if damage == "nonregular" else metadata.st_mode,
            st_dev=metadata.st_dev + (damage == "cross-device"),
        )
        with monkeypatch.context() as patch:
            patch.setattr(storage.os, "fstat", lambda _: fake)
            original(ownership, descriptor)

    monkeypatch.setattr(storage, "_check_temporary", changed)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.phase == "temporary-check"
    assert caught.value.temporary_path.exists()


def test_write_failure_closes_descriptor_and_retains_partial_file(
    recovery_staging, monkeypatch
):
    descriptor_used = None

    def fail(descriptor, data):
        nonlocal descriptor_used
        descriptor_used = descriptor
        os.write(descriptor, data[:10])
        raise OSError(errno.EIO, request_setup.SECRET)

    monkeypatch.setattr(storage, "_write_bytes", fail)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.phase == "temporary-write"
    assert (
        caught.value.temporary_path.read_bytes()
        == (encode_recovery_request(recovery_staging.request)[:10])
    )
    with pytest.raises(OSError) as closed:
        os.fstat(descriptor_used)
    assert closed.value.errno == errno.EBADF


def test_destination_appearing_before_rename_is_not_replaced(
    recovery_staging, monkeypatch
):
    original = storage._step

    def appeared(progress, phase, operation, *args):
        if phase == "request-destination-check":
            recovery_staging.path.write_bytes(b"retained foreign final evidence")
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(storage, "_step", appeared)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.phase == "request-destination-check"
    assert recovery_staging.path.read_bytes() == b"retained foreign final evidence"
    assert caught.value.temporary_path.exists()


@pytest.mark.parametrize(
    "damage",
    ["directory", "symlink", "fifo", "empty-suffix"],
)
def test_unsafe_receipt_namespace_is_held(recovery_staging, damage):
    request_setup.persist(recovery_staging)
    target = recovery_staging.unit / "receipt.json"
    if damage == "directory":
        target.mkdir()
    elif damage == "symlink":
        target.symlink_to(recovery_staging.path)
    elif damage == "fifo":
        os.mkfifo(target)
    else:
        (recovery_staging.unit / ".recovery-receipt-").write_bytes(b"retained")

    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError):
        request_setup.read(recovery_staging)
    assert request_setup.snapshot(recovery_staging.root) == before
