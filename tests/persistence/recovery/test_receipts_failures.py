"""persistence / recovery / test_receipts_failures contracts."""

import errno
import os

import pytest

from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.inspection import BundleInspectionError
from jobflow_gitlab_slurm.persistence.recovery import receipts as storage
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.receipts import (
    RecoveryReceiptInspectionError,
    RecoveryReceiptIntegrityError,
    RecoveryReceiptPublicationError,
)
from tests.persistence.recovery import _receipt_support as receipt_setup
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    "damage", ["symlink", "fifo", "directory", "unknown", "empty-suffix"]
)
def test_unsafe_namespace_is_rejected_before_opening(
    committed_recovery_bundle, monkeypatch, damage
):
    context = committed_recovery_bundle
    if damage == "symlink":
        context.audit.symlink_to(context.path)
    elif damage == "fifo":
        os.mkfifo(context.audit)
    elif damage == "directory":
        context.audit.mkdir()
    elif damage == "unknown":
        (context.unit / "unknown").write_bytes(b"retained")
    else:
        (context.unit / ".recovery-receipt-").write_bytes(b"retained")
    before = request_setup.snapshot(context.root)

    def forbidden(*args, **kwargs):
        pytest.fail("unsafe entries must be rejected before receipt reads")

    monkeypatch.setattr(storage, "_read_bytes", forbidden)
    with pytest.raises(RecoveryReceiptIntegrityError):
        receipt_setup.read(context)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("phase", receipt_setup.ACK_PHASES)
def test_existing_receipt_acknowledgment_failures_remain_uncertain(
    committed_recovery_bundle, monkeypatch, phase
):
    context = committed_recovery_bundle
    receipt_setup.persist(context)
    before = request_setup.snapshot(context.root)
    original = storage._step

    def fail_at(progress, current_phase, operation, *args):
        if current_phase == phase:
            progress.phase = current_phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, current_phase, operation, *args)

    monkeypatch.setattr(storage, "_step", fail_at)
    with pytest.raises(RecoveryReceiptPublicationError) as caught:
        receipt_setup.persist(context)
    assert caught.value.operation == "acknowledge"
    assert caught.value.receipt_publication_uncertain
    assert request_setup.snapshot(context.root) == before


def test_destination_appearing_is_not_replaced(committed_recovery_bundle, monkeypatch):
    context = committed_recovery_bundle
    original = storage._step

    def appeared(progress, phase, operation, *args):
        if phase == "receipt-destination-check":
            context.audit.write_bytes(b"foreign retained evidence")
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(storage, "_step", appeared)
    with pytest.raises(RecoveryReceiptPublicationError) as caught:
        receipt_setup.persist(context)
    assert caught.value.phase == "receipt-destination-check"
    assert not caught.value.receipt_publication_uncertain
    assert context.audit.read_bytes() == b"foreign retained evidence"
    assert caught.value.temporary_path.exists()


@pytest.mark.parametrize("failure", ["inspection", "read", "value", "filesystem"])
def test_bundle_verification_failures_are_classified(
    committed_recovery_bundle, monkeypatch, failure
):
    context = committed_recovery_bundle
    if failure == "inspection":

        def inspect(*args):
            raise BundleInspectionError(
                context.invocation.record, context.published, errno.EIO
            )

        monkeypatch.setattr(storage, "inspect_owned_bundle", inspect)
        expected_error = RecoveryReceiptInspectionError
    elif failure in ("read", "value"):

        def unavailable(path):
            if failure == "read":
                raise OSError(errno.EIO, request_setup.SECRET)
            raise ValueError(request_setup.SECRET)

        monkeypatch.setattr(storage, "_read_bytes", unavailable)
        expected_error = (
            RecoveryReceiptInspectionError
            if failure == "read"
            else RecoveryReceiptIntegrityError
        )
    else:

        def unsafe(*args):
            raise bundles.BundleIntegrityError(
                context.invocation.record,
                context.published,
                "cross_filesystem_bundle",
            )

        monkeypatch.setattr(bundles, "require_same_filesystem", unsafe)
        expected_error = RecoveryReceiptIntegrityError

    before = request_setup.snapshot(context.root)
    with pytest.raises(expected_error) as caught:
        receipt_setup.persist(context)
    assert request_setup.SECRET not in str(caught.value)
    assert request_setup.snapshot(context.root) == before


def test_request_acknowledgment_failure_cannot_publish_receipt(
    committed_recovery_bundle, monkeypatch
):
    context = committed_recovery_bundle

    def fail(*args):
        raise requests.RecoveryRequestInspectionError(
            context.invocation.record,
            request_setup.RECOVERY_ID,
            context.path,
            "request_unavailable",
            errno=errno.EIO,
        )

    monkeypatch.setattr(requests, "persist_owned_recovery_request", fail)
    with pytest.raises(RecoveryReceiptPublicationError) as caught:
        receipt_setup.persist(context)
    assert caught.value.phase == "request-acknowledgment"
    assert caught.value.reason == "request_unavailable"
    assert not caught.value.receipt_publication_uncertain
    assert not caught.value.bundle_acknowledged
    assert not context.audit.exists()
