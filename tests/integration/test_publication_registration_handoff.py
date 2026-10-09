"""integration / test_publication_registration_handoff contracts."""

import subprocess
import sys

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    decode_event,
)
from tests.persistence.publication import (
    _registration_support as publication_registration_setup,
)
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    "boundary",
    [
        "intent-acknowledged",
        "bundle-acknowledged",
        "event-staged",
        "event-published",
        "head-staged",
        "head-published",
        "registration-returned",
    ],
)
def test_separate_process_restart_preserves_identity(
    published_bundle_for_registration, boundary
):
    context = published_bundle_for_registration
    before = request_setup.snapshot(context.invocation.path)
    script = """
import os
import sys
from pathlib import Path

from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.publication import registration as association
from jobflow_gitlab_slurm.persistence.publication import storage as intents
from jobflow_gitlab_slurm.persistence.journal.records import EventRecord, JournalHead
from jobflow_gitlab_slurm.persistence.attempts.ownership import owned_invocation
from jobflow_gitlab_slurm.persistence.publication.records import decode_publication_intent

root, run, job, index, attempt, invocation, intent_path, boundary = sys.argv[1:]
intent = decode_publication_intent(Path(intent_path).read_bytes())
intent_original = intents.publish_owned_publication_intent
bundle_original = bundles.publish_owned_bundle
write_original = journal._write_staged
rename_original = journal._rename_new
replace_original = journal.os.replace

def acknowledge_intent(*args):
    result = intent_original(*args)
    if boundary == "intent-acknowledged":
        os._exit(73)
    return result

def acknowledge_bundle(*args):
    result = bundle_original(*args)
    if boundary == "bundle-acknowledged":
        os._exit(73)
    return result

def write(path, record):
    write_original(path, record)
    if boundary == "event-staged" and isinstance(record, EventRecord):
        os._exit(73)
    if (
        boundary == "head-staged"
        and isinstance(record, JournalHead)
        and record.last_sequence == 1
    ):
        os._exit(73)

def rename(source, destination):
    rename_original(source, destination)
    if boundary == "event-published" and destination.parent.name == "events":
        os._exit(73)

def replace(source, destination):
    replace_original(source, destination)
    if boundary == "head-published":
        os._exit(73)

intents.publish_owned_publication_intent = acknowledge_intent
bundles.publish_owned_bundle = acknowledge_bundle
journal._write_staged = write
journal._rename_new = rename
journal.os.replace = replace

with owned_invocation(root, run, job, int(index), attempt, invocation) as ownership:
    association.register_owned_publication_event(ownership, intent)
    if boundary == "registration-returned":
        os._exit(73)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            *(str(value) for value in context.parameters),
            str(context.intent_path),
            boundary,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 73, result.stderr

    retained_event = (
        decode_event(publication_registration_setup.event_path(context).read_bytes())
        if publication_registration_setup.event_path(context).exists()
        else None
    )
    retained_staging = {
        path: path.read_bytes()
        for path in context.run.path.rglob(".staging-*")
        if path.is_file()
    }
    first = publication_registration_setup.register(context)
    assert publication_registration_setup.register(context) == first
    if retained_event is not None:
        assert first == retained_event

    assert first.event_id == context.intent.intent_id
    assert first.sequence == 1
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        first,
    )
    assert request_setup.snapshot(context.invocation.path) == before
    assert {path: path.read_bytes() for path in retained_staging} == retained_staging
