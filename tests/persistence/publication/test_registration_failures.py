"""persistence / publication / test_registration_failures contracts."""

import errno
import os

import pytest

from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
    JournalHead,
    decode_event,
)
from jobflow_gitlab_slurm.persistence.publication import storage as intents
from jobflow_gitlab_slurm.persistence.publication.registration import (
    PublicationJournalIntegrityError,
    PublicationJournalRegistrationError,
)
from tests.persistence.publication import (
    _registration_support as publication_registration_setup,
)
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize("special", ["symlink", "fifo", "directory"])
def test_unsafe_intent_is_not_opened(
    published_bundle_for_registration, monkeypatch, special
):
    context = published_bundle_for_registration
    context.intent_path.unlink()
    if special == "symlink":
        context.intent_path.symlink_to(context.published / "COMMIT.json")
    elif special == "fifo":
        os.mkfifo(context.intent_path)
    else:
        context.intent_path.mkdir()

    def forbidden(*args):
        pytest.fail("unsafe intent must not be opened")

    monkeypatch.setattr(intents, "_read_bytes", forbidden)
    before = request_setup.snapshot(context.root)
    with pytest.raises(PublicationJournalIntegrityError):
        publication_registration_setup.register(context)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("origin", ["intent-file", "bundle-file", "bundle-directory"])
def test_acknowledgment_failure_prevents_journal(
    published_bundle_for_registration, monkeypatch, origin
):
    context = published_bundle_for_registration

    def fail(*args):
        raise OSError(errno.EIO, request_setup.SECRET)

    def forbidden(*args, **kwargs):
        pytest.fail("journal must not follow failed acknowledgment")

    target = intents if origin == "intent-file" else bundles
    attribute = "_sync_directory" if origin == "bundle-directory" else "_sync_file"
    monkeypatch.setattr(target, attribute, fail)
    monkeypatch.setattr(journal, "append_event", forbidden)
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalRegistrationError) as caught:
        publication_registration_setup.register(context)

    report = publication_registration_setup.assert_private(caught.value)
    intent_failed = origin == "intent-file"
    assert report["intent_acknowledged_in_this_call"] is not intent_failed
    assert report["bundle_acknowledged_in_this_call"] is False
    assert report["intent_acknowledgment_uncertain"] is intent_failed
    assert report["bundle_acknowledgment_uncertain"] is not intent_failed
    assert report["journal_registration_uncertain"] is False
    assert report["errno"] == errno.EIO
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("target", ["event", "head"])
def test_journal_failure_preserves_evidence_and_retries(
    published_bundle_for_registration, monkeypatch, target
):
    context = published_bundle_for_registration
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
        with pytest.raises(PublicationJournalRegistrationError) as caught:
            publication_registration_setup.register(context)

    report = publication_registration_setup.assert_private(caught.value)
    assert report["phase"] == "journal-registration"
    assert report["intent_acknowledged_in_this_call"] is True
    assert report["bundle_acknowledged_in_this_call"] is True
    assert report["intent_acknowledgment_uncertain"] is False
    assert report["bundle_acknowledgment_uncertain"] is False
    assert report["errno"] == errno.EIO
    assert report["event_path"] == str(
        publication_registration_setup.event_path(context)
    )
    assert report["event_staging_path"] is not None

    if target == "head":
        assert report["head_staging_path"] is not None
        retained = decode_event(
            publication_registration_setup.event_path(context).read_bytes()
        )
        assert publication_registration_setup.register(context) == retained
    else:
        assert publication_registration_setup.register(context).sequence == 1
    assert (
        len(journal.read_events(context.root, context.run.manifest.run_id).events) == 1
    )
