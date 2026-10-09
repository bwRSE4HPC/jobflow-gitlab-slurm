"""Explicit scenario preparation and observations; no collected tests."""

import errno
import json

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    decode_event,
)
from jobflow_gitlab_slurm.persistence.publication import registration as association
from jobflow_gitlab_slurm.persistence.publication import storage as intents
from jobflow_gitlab_slurm.persistence.publication.registration import (
    PublicationJournalRegistrationError,
    register_owned_publication_event,
)
from tests.persistence.recovery._request_support import (
    OTHER_ID,
    SECRET,
)

REFERENCES = ("intent", "bundle_manifest", "bundle_commit")


def published_bundle_for_registration(recovery_staging):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        bundles.publish_owned_bundle(
            ownership,
            recovery_staging.intent.expected_manifest,
            recovery_staging.intent.expected_commit,
        )
    return recovery_staging


def register(context, intent=None):
    with owned_invocation(*context.parameters) as ownership:
        return register_owned_publication_event(
            ownership,
            context.intent if intent is None else intent,
        )


def payload_for(context):
    with owned_invocation(*context.parameters) as ownership:
        handle = intents.read_owned_publication_intent(ownership)
        return association._payload(ownership, context.intent, handle)


def event_path(context, sequence=1):
    return context.run.path / "events" / f"{sequence:012d}.json"


def assert_private(error):
    report = error.to_report()
    assert report["kind"] == "publication-journal-error"
    assert SECRET not in str(error)
    assert SECRET not in json.dumps(report)
    assert error.__cause__ is None
    assert "Do not replace" in report["hint"]
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
                    context.run.manifest.run_id,
                    event_id=OTHER_ID,
                    event_type="example.note",
                    payload={"value": 1},
                )
        else:
            with pytest.raises(PublicationJournalRegistrationError):
                register(context)
    return decode_event(event_path(context).read_bytes())
