"""Explicit scenario preparation and observations; no collected tests."""

import errno
import json

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    decode_event,
)
from jobflow_gitlab_slurm.persistence.recovery import receipts
from jobflow_gitlab_slurm.persistence.recovery import registration as association
from jobflow_gitlab_slurm.persistence.recovery.registration import (
    RecoveryJournalRegistrationError,
    register_owned_recovery_event,
)
from tests.persistence.recovery._request_support import (
    OTHER_ID,
    SECRET,
)

REFERENCES = (
    "intent",
    "request",
    "receipt",
    "bundle_manifest",
    "bundle_commit",
)


def recovered_bundle_for_registration(committed_recovery_bundle):
    with owned_invocation(*committed_recovery_bundle.parameters) as ownership:
        receipts.persist_owned_recovery_receipt(
            ownership, committed_recovery_bundle.receipt
        )
    return committed_recovery_bundle


def register(context, request=None, receipt=None):
    with owned_invocation(*context.parameters) as ownership:
        return register_owned_recovery_event(
            ownership,
            context.request if request is None else request,
            receipt=context.receipt if receipt is None else receipt,
        )


def payload_for(context):
    with owned_invocation(*context.parameters) as ownership:
        handle = receipts.read_owned_recovery_receipt(
            ownership, context.request.recovery_id
        )
        return association._payload(ownership, context.request, context.receipt, handle)


def event_path(context, sequence=1):
    return context.run.path / "events" / f"{sequence:012d}.json"


def assert_private(error):
    report = error.to_report()
    assert report["kind"] == "recovery-journal-error"
    assert SECRET not in str(error)
    assert SECRET not in json.dumps(report)
    assert error.__cause__ is None
    assert "Do not manufacture a receipt" in report["hint"]
    return report


def make_pending(context, monkeypatch, *, unrelated=False):
    original = journal._rename_new

    def interrupt(source, destination):
        original(source, destination)
        if destination.parent.name == "events":
            raise OSError(errno.EIO, SECRET)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_rename_new", interrupt)
        if unrelated:
            with pytest.raises(journal.JournalPublicationError):
                journal.append_event(
                    context.root,
                    context.request.run_id,
                    event_id=OTHER_ID,
                    event_type="example.note",
                    payload={"value": 1},
                )
        else:
            with pytest.raises(RecoveryJournalRegistrationError):
                register(context)
    return decode_event(event_path(context).read_bytes())
