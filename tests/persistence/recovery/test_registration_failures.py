"""persistence / recovery / test_registration_failures contracts."""

import errno
import os

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
    JournalHead,
    decode_event,
)
from jobflow_gitlab_slurm.persistence.recovery import receipts
from jobflow_gitlab_slurm.persistence.recovery.registration import (
    RecoveryJournalIntegrityError,
    RecoveryJournalRegistrationError,
)
from tests.persistence.recovery import _registration_support as registration_setup
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize("special", ["symlink", "fifo", "directory"])
def test_unsafe_receipt_is_held_without_opening(
    recovered_bundle_for_registration, monkeypatch, special
):
    context = recovered_bundle_for_registration
    context.audit.unlink()
    if special == "symlink":
        context.audit.symlink_to(context.path)
    elif special == "fifo":
        os.mkfifo(context.audit)
    else:
        context.audit.mkdir()
    before = request_setup.snapshot(context.root)
    original = receipts._read_bytes

    def guarded(path):
        assert path != context.audit
        return original(path)

    monkeypatch.setattr(receipts, "_read_bytes", guarded)
    with pytest.raises(RecoveryJournalIntegrityError):
        registration_setup.register(context)
    assert request_setup.snapshot(context.root) == before


def test_receipt_acknowledgment_failure_never_attempts_journal(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration

    def fail(ownership, receipt):
        progress = receipts._Progress(
            operation="acknowledge",
            phase="bundle-payload-flush",
            receipt_publication_uncertain=True,
        )
        raise receipts.RecoveryReceiptPublicationError(
            ownership,
            receipt.recovery_id,
            context.audit,
            progress,
            OSError(errno.EIO, request_setup.SECRET),
        )

    def forbidden(*args, **kwargs):
        pytest.fail("journal must not be attempted after failed acknowledgment")

    monkeypatch.setattr(receipts, "persist_owned_recovery_receipt", fail)
    monkeypatch.setattr(journal, "append_event", forbidden)
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalRegistrationError) as caught:
        registration_setup.register(context)

    report = registration_setup.assert_private(caught.value)
    assert report["phase"] == "filesystem-acknowledgment"
    assert report["inner_phase"] == "bundle-payload-flush"
    assert report["filesystem_acknowledged_in_this_call"] is False
    assert report["filesystem_acknowledgment_uncertain"] is True
    assert report["journal_registration_uncertain"] is False
    assert report["errno"] == errno.EIO
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("target", ["event", "head"])
def test_journal_publication_failure_preserves_evidence_and_retries(
    recovered_bundle_for_registration, monkeypatch, target
):
    context = recovered_bundle_for_registration
    original = journal._write_staged

    def fail(path, record):
        selected = (
            isinstance(record, EventRecord)
            if target == "event"
            else isinstance(record, JournalHead) and record.last_sequence == 1
        )
        if selected:
            raise OSError(errno.EIO, request_setup.SECRET)
        original(path, record)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_write_staged", fail)
        with pytest.raises(RecoveryJournalRegistrationError) as caught:
            registration_setup.register(context)

    report = registration_setup.assert_private(caught.value)
    assert report["phase"] == "journal-registration"
    assert report["filesystem_acknowledged_in_this_call"] is True
    assert report["filesystem_acknowledgment_uncertain"] is False
    assert report["journal_registration_uncertain"] is True
    assert report["errno"] == errno.EIO
    assert report["event_path"] == str(registration_setup.event_path(context))
    assert report["event_staging_path"] is not None

    if target == "head":
        assert report["head_staging_path"] is not None
        original_event = decode_event(
            registration_setup.event_path(context).read_bytes()
        )
        assert registration_setup.register(context) == original_event
    else:
        assert registration_setup.register(context).sequence == 1
    assert len(journal.read_events(context.root, context.request.run_id).events) == 1
