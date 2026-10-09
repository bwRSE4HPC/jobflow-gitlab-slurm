"""integration / test_recovery_registration_handoff contracts."""

import subprocess
import sys

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    decode_event,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    encode_recovery_receipt,
    encode_recovery_request,
)
from tests.persistence.recovery import _registration_support as registration_setup
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    "boundary",
    [
        "filesystem-acknowledged",
        "event-staged",
        "event-published",
        "head-staged",
        "head-published",
        "registration-returned",
    ],
)
def test_separate_process_restart_preserves_same_operation(
    recovered_bundle_for_registration, boundary
):
    context = recovered_bundle_for_registration
    before = request_setup.snapshot(context.invocation.path)
    request_file = context.root.parent / "expected-request.json"
    receipt_file = context.root.parent / "expected-receipt.json"
    request_file.write_bytes(encode_recovery_request(context.request))
    receipt_file.write_bytes(encode_recovery_receipt(context.receipt))
    script = """
import os
import sys
from pathlib import Path

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.recovery import registration as association
from jobflow_gitlab_slurm.persistence.recovery import receipts
from jobflow_gitlab_slurm.persistence.journal.records import EventRecord, JournalHead
from jobflow_gitlab_slurm.persistence.attempts.ownership import owned_invocation
from jobflow_gitlab_slurm.persistence.recovery.records import (
    decode_recovery_request,
    decode_recovery_receipt,
)

root, run, job, index, attempt, invocation, request_path, receipt_path, boundary = sys.argv[1:]
request = decode_recovery_request(Path(request_path).read_bytes())
receipt = decode_recovery_receipt(Path(receipt_path).read_bytes())

ack_original = receipts.persist_owned_recovery_receipt
write_original = journal._write_staged
rename_original = journal._rename_new
replace_original = journal.os.replace

def acknowledge(*args):
    result = ack_original(*args)
    if boundary == "filesystem-acknowledged":
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

receipts.persist_owned_recovery_receipt = acknowledge
journal._write_staged = write
journal._rename_new = rename
journal.os.replace = replace

with owned_invocation(root, run, job, int(index), attempt, invocation) as ownership:
    association.register_owned_recovery_event(ownership, request, receipt=receipt)
    if boundary == "registration-returned":
        os._exit(73)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            *(str(value) for value in context.parameters),
            str(request_file),
            str(receipt_file),
            boundary,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr

    retained_event = (
        decode_event(registration_setup.event_path(context).read_bytes())
        if registration_setup.event_path(context).exists()
        else None
    )
    retained_staging = {
        path: path.read_bytes()
        for path in context.run.path.rglob(".staging-*")
        if path.is_file()
    }
    first = registration_setup.register(context)
    assert registration_setup.register(context) == first
    if retained_event is not None:
        assert first == retained_event

    assert first.event_id == context.request.journal_event_id
    assert first.sequence == 1
    assert journal.read_events(context.root, context.request.run_id).events == (first,)
    assert request_setup.snapshot(context.invocation.path) == before
    assert {path: path.read_bytes() for path in retained_staging} == retained_staging
