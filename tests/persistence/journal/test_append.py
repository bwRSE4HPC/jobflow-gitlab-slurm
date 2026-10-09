"""persistence / journal / test_append contracts."""

import stat
import sys
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
    JournalHead,
    decode_event,
    encode_record,
)
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalIncompleteError,
    JournalPublicationError,
    read_events,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    locked_run,
)
from tests.persistence.journal import _append_support as append_setup


def test_append_creates_private_canonical_chain_and_preserves_run_inputs(append_run):
    originals = {
        path: path.read_bytes()
        for path in (
            append_run.path / "run.json",
            append_run.path / "flow/original.json",
            append_run.path / "flow/payload.json",
            append_run.path / "artifacts/references.json",
        )
    }
    first = append_setup.append(append_run)
    second = append_setup.append(
        append_run, append_setup.SECOND_ID, payload={"value": 2}
    )

    replay = read_events(append_run.root, append_setup.RUN_ID, verify_external=True)
    assert replay.events == (first, second)
    assert first.sequence == 1
    assert first.previous_sha256 is None
    assert second.sequence == 2
    assert second.previous_sha256 == first.sha256
    assert replay.head.last_sequence == 2
    assert replay.head.last_sha256 == second.sha256
    assert decode_event(append_setup.event_path(append_run).read_bytes()) == first
    assert append_setup.event_path(append_run).read_bytes() == encode_record(first)
    assert append_run.head_path.read_bytes() == encode_record(replay.head)
    assert stat.S_IMODE(append_run.events_path.stat().st_mode) == 0o700
    for path in (
        append_run.head_path,
        append_setup.event_path(append_run),
        append_setup.event_path(append_run, 2),
    ):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not list(append_run.path.glob(".staging-journal-head-*"))
    assert not list(append_run.events_path.glob(".staging-*"))
    assert {path: path.read_bytes() for path in originals} == originals
    assert "do_not_import_this_append_consumer" not in sys.modules


def test_initialization_and_publication_flush_order(append_run, monkeypatch):
    trace = []
    original_write = journal._write_staged
    original_rename = journal._rename_new
    original_sync = journal._sync_directory
    original_replace = journal.os.replace
    original_mkdir = Path.mkdir

    def write(path, record):
        original_write(path, record)
        if isinstance(record, EventRecord):
            trace.append("write-event")
        else:
            trace.append(f"write-head-{record.last_sequence}")

    def rename(source, destination):
        original_rename(source, destination)
        trace.append(
            "publish-event"
            if destination.parent == append_run.events_path
            else "publish-zero"
        )

    def sync(path):
        original_sync(path)
        trace.append("sync-events" if path == append_run.events_path else "sync-run")

    def replace(source, destination):
        original_replace(source, destination)
        trace.append("replace-head")

    def mkdir(path, *args, **kwargs):
        result = original_mkdir(path, *args, **kwargs)
        if path == append_run.events_path:
            trace.append("mkdir-events")
        return result

    monkeypatch.setattr(journal, "_write_staged", write)
    monkeypatch.setattr(journal, "_rename_new", rename)
    monkeypatch.setattr(journal, "_sync_directory", sync)
    monkeypatch.setattr(journal.os, "replace", replace)
    monkeypatch.setattr(Path, "mkdir", mkdir)

    append_setup.append(append_run)
    zero = trace.index("publish-zero")
    mkdir_events = trace.index("mkdir-events")
    published = trace.index("publish-event")
    advance_write = trace.index("write-head-1")
    replaced = trace.index("replace-head")

    assert trace.index("write-head-0") < zero < mkdir_events
    assert "sync-run" in trace[zero + 1 : mkdir_events]
    assert trace.index("write-event") < published < advance_write < replaced
    assert "sync-events" in trace[published + 1 : advance_write]
    assert trace[replaced + 1 :] == ["sync-run"]


def test_existing_zero_head_is_reacknowledged_before_directory_creation(
    append_run, monkeypatch
):
    append_setup.zero_head(append_run)
    observed = []
    original_sync = journal._sync_record
    original_mkdir = Path.mkdir

    def sync(path):
        original_sync(path)
        observed.append("head-sync")

    def mkdir(path, *args, **kwargs):
        if path == append_run.events_path:
            observed.append("mkdir-events")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(journal, "_sync_record", sync)
    monkeypatch.setattr(Path, "mkdir", mkdir)
    assert append_setup.append(append_run).sequence == 1
    assert observed.index("head-sync") < observed.index("mkdir-events")


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": ""},
        {"event_id": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
        {"event_type": ""},
        {"event_type": "Bad"},
        {"event_type": "a;command"},
        {"event_type": None},
        {"payload": []},
        {"payload": {"value": float("nan")}},
        {"payload": {1: "bad key"}},
    ],
)
def test_invalid_input_fails_without_journal_changes(append_run, changes):
    before = append_setup.snapshot(append_run.root)
    with pytest.raises(ValueError):
        append_setup.append(append_run, **changes)
    assert append_setup.snapshot(append_run.root) == before


def test_sequence_exhaustion_fails_without_writes(append_run, monkeypatch):
    append_setup.append(append_run)
    before = append_setup.snapshot(append_run.root)
    monkeypatch.setattr(journal, "MAX_SEQUENCE", 1)
    with pytest.raises(ValueError, match="sequence space exhausted"):
        append_setup.append(append_run, append_setup.SECOND_ID)
    assert append_setup.snapshot(append_run.root) == before


@pytest.mark.parametrize("target", ["zero", "event"])
def test_existing_publication_destination_is_never_overwritten(
    append_run, monkeypatch, target
):
    original = journal._write_staged

    def collide(path, record):
        original(path, record)
        selected = (
            isinstance(record, JournalHead) and record.last_sequence == 0
            if target == "zero"
            else isinstance(record, EventRecord)
        )
        if selected:
            destination = (
                append_run.head_path
                if target == "zero"
                else append_setup.event_path(append_run)
            )
            destination.write_bytes(b"foreign evidence")

    monkeypatch.setattr(journal, "_write_staged", collide)
    with pytest.raises(JournalPublicationError) as caught:
        append_setup.append(append_run)
    destination = (
        append_run.head_path
        if target == "zero"
        else append_setup.event_path(append_run)
    )
    assert destination.read_bytes() == b"foreign evidence"
    assert isinstance(caught.value.__cause__, FileExistsError)
    staging = (
        caught.value.head_staging_path
        if target == "zero"
        else caught.value.event_staging_path
    )
    assert staging.exists()


def test_busy_appender_does_not_initialize_journal(append_run):
    before = append_setup.snapshot(append_run.root)
    with locked_run(append_run.root, append_setup.RUN_ID), pytest.raises(RunBusyError):
        append_setup.append(append_run)
    assert append_setup.snapshot(append_run.root) == before


def test_competing_process_cannot_append_while_first_appender_holds_lock(
    append_run, monkeypatch
):
    original = journal._write_staged
    competing = []
    code = """
import sys
from jobflow_gitlab_slurm.persistence.journal.storage import append_event
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError
try:
    event = append_event(
        sys.argv[1], sys.argv[2],
        event_id=sys.argv[3],
        event_type="example.note",
        payload={"value": 1},
    )
except RunBusyError:
    sys.exit(3)
print(event.sequence)
"""

    def contend(path, record):
        if isinstance(record, EventRecord):
            competing.append(
                append_setup.process(
                    code, append_run.root, append_setup.RUN_ID, append_setup.EVENT_ID
                )
            )
        original(path, record)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_write_staged", contend)
        first = append_setup.append(append_run)

    assert len(competing) == 1
    assert competing[0].returncode == 3, competing[0].stderr
    retry = append_setup.process(
        code, append_run.root, append_setup.RUN_ID, append_setup.EVENT_ID
    )
    assert retry.returncode == 0, retry.stderr
    assert retry.stdout.strip() == "1"
    assert read_events(append_run.root, append_setup.RUN_ID).events == (first,)


@pytest.mark.parametrize(
    "boundary",
    [
        "zero-staged",
        "zero-published",
        "event-staged",
        "event-published",
        "advanced-head-staged",
        "head-committed",
    ],
)
def test_abrupt_process_exit_recovers_same_event_without_duplicate_calculation(
    append_run, boundary
):
    result = append_setup.process(
        """
import os
import sys
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import EventRecord, JournalHead

boundary = sys.argv[4]
write_original = journal._write_staged
rename_original = journal._rename_new
replace_original = journal.os.replace

def write(path, record):
    write_original(path, record)
    if (
        boundary == "zero-staged"
        and isinstance(record, JournalHead)
        and record.last_sequence == 0
    ):
        os._exit(73)
    if boundary == "event-staged" and isinstance(record, EventRecord):
        os._exit(73)
    if (
        boundary == "advanced-head-staged"
        and isinstance(record, JournalHead)
        and record.last_sequence == 1
    ):
        os._exit(73)

def rename(source, destination):
    rename_original(source, destination)
    if boundary == "zero-published" and destination.name == "journal-head.json":
        os._exit(73)
    if boundary == "event-published" and destination.parent.name == "events":
        os._exit(73)

def replace(source, destination):
    replace_original(source, destination)
    if boundary == "head-committed":
        os._exit(73)

journal._write_staged = write
journal._rename_new = rename
journal.os.replace = replace
journal.append_event(
    sys.argv[1], sys.argv[2],
    event_id=sys.argv[3],
    event_type="example.note",
    payload={"value": 1},
)
""",
        append_run.root,
        append_setup.RUN_ID,
        append_setup.EVENT_ID,
        boundary,
    )
    assert result.returncode == 73, result.stderr
    original_event = (
        decode_event(append_setup.event_path(append_run).read_bytes())
        if append_setup.event_path(append_run).exists()
        else None
    )
    staging = {
        path: path.read_bytes()
        for path in append_run.path.rglob(".staging-*")
        if path.is_file()
    }

    if boundary in {"event-published", "advanced-head-staged"}:
        with pytest.raises(JournalIncompleteError):
            read_events(append_run.root, append_setup.RUN_ID)
    elif boundary == "head-committed":
        assert read_events(append_run.root, append_setup.RUN_ID).events == (
            original_event,
        )
    else:
        assert read_events(append_run.root, append_setup.RUN_ID).events == ()

    recovered = append_setup.append(append_run)
    assert recovered.event_id == append_setup.EVENT_ID
    assert recovered.sequence == 1
    if original_event is not None:
        assert recovered == original_event
    assert read_events(append_run.root, append_setup.RUN_ID).events == (recovered,)
    assert {path: path.read_bytes() for path in staging} == staging
    assert (
        append_run.path / "flow/payload.json"
    ).read_bytes() == append_setup.FLOW_BYTES
