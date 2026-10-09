"""persistence / journal / test_append_failures contracts."""

import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
    JournalHead,
    decode_event,
    decode_head,
)
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalIncompleteError,
    JournalIntegrityError,
    JournalPublicationError,
    read_events,
)
from tests.persistence.journal import _append_support as append_setup


@pytest.mark.parametrize("stage", ["zero", "event", "advanced"])
@pytest.mark.parametrize("after_write", [False, True])
def test_staged_write_failures_preserve_evidence_and_allow_same_id_retry(
    append_run, monkeypatch, stage, after_write
):
    original = journal._write_staged

    def selected(record):
        if stage == "event":
            return isinstance(record, EventRecord)
        return isinstance(record, JournalHead) and record.last_sequence == (
            0 if stage == "zero" else 1
        )

    def fail(path, record):
        if selected(record):
            if after_write:
                original(path, record)
            raise OSError("injected staged write failure")
        original(path, record)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_write_staged", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    error = caught.value
    append_setup.assert_publication_error(
        error, append_run, phase="event-write" if stage == "event" else "head-write"
    )
    staging = error.event_staging_path if stage == "event" else error.head_staging_path
    assert staging is not None
    assert staging.exists() == after_write
    saved = staging.read_bytes() if after_write else None

    if stage == "advanced":
        with pytest.raises(JournalIncompleteError):
            read_events(append_run.root, append_setup.RUN_ID)
    else:
        assert read_events(append_run.root, append_setup.RUN_ID).events == ()

    recovered = append_setup.append(append_run)
    assert read_events(append_run.root, append_setup.RUN_ID).events == (recovered,)
    if after_write:
        assert staging.read_bytes() == saved


@pytest.mark.parametrize("target", ["zero", "event"])
@pytest.mark.parametrize("after_rename", [False, True])
def test_rename_failures_are_uncertain_and_reconcile_without_duplicate_events(
    append_run, monkeypatch, target, after_rename
):
    original = journal._rename_new

    def fail(source, destination):
        selected = (
            destination == append_run.head_path
            if target == "zero"
            else destination == append_setup.event_path(append_run)
        )
        if selected:
            if after_rename:
                original(source, destination)
            raise OSError("injected rename failure")
        original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_rename_new", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    error = caught.value
    append_setup.assert_publication_error(
        error, append_run, phase="head-publish" if target == "zero" else "event-publish"
    )
    assert error.publication_uncertain is True
    previous = (
        decode_event(append_setup.event_path(append_run).read_bytes())
        if target == "event" and after_rename
        else None
    )
    recovered = append_setup.append(append_run)
    assert recovered.event_id == append_setup.EVENT_ID
    assert read_events(append_run.root, append_setup.RUN_ID).events == (recovered,)
    if previous is not None:
        assert recovered == previous


@pytest.mark.parametrize("after_replace", [False, True])
def test_head_replacement_failure_before_or_after_commit_is_recoverable(
    append_run, monkeypatch, after_replace
):
    original = journal.os.replace

    def fail(source, destination):
        if after_replace:
            original(source, destination)
        raise OSError("injected head replacement failure")

    with monkeypatch.context() as patch:
        patch.setattr(journal.os, "replace", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    error = caught.value
    append_setup.assert_publication_error(error, append_run, phase="head-publish")
    assert error.publication_uncertain is True
    original_event = decode_event(append_setup.event_path(append_run).read_bytes())

    if after_replace:
        assert read_events(append_run.root, append_setup.RUN_ID).events == (
            original_event,
        )
    else:
        with pytest.raises(JournalIncompleteError):
            read_events(append_run.root, append_setup.RUN_ID)
        assert error.head_staging_path.exists()

    recovered = append_setup.append(append_run)
    assert recovered == original_event
    assert read_events(append_run.root, append_setup.RUN_ID).events == (original_event,)


@pytest.mark.parametrize("stage", ["zero", "directory", "event", "final"])
def test_directory_flush_failures_do_not_acknowledge_success(
    append_run, monkeypatch, stage
):
    original = journal._sync_directory

    def fail(path):
        head_sequence = (
            decode_head(append_run.head_path.read_bytes()).last_sequence
            if append_run.head_path.exists()
            else None
        )
        selected = {
            "zero": (
                path == append_run.path
                and head_sequence == 0
                and not append_run.events_path.exists()
            ),
            "directory": path == append_run.events_path
            and not append_setup.event_path(append_run).exists(),
            "event": path == append_run.events_path
            and append_setup.event_path(append_run).exists(),
            "final": path == append_run.path and head_sequence == 1,
        }[stage]
        if selected:
            raise OSError("injected directory flush failure")
        original(path)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_sync_directory", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(
        caught.value,
        append_run,
        phase=(
            "head-directory-sync"
            if stage in {"zero", "final"}
            else "events-directory-sync"
        ),
    )
    original_event = (
        decode_event(append_setup.event_path(append_run).read_bytes())
        if append_setup.event_path(append_run).exists()
        else None
    )
    recovered = append_setup.append(append_run)
    assert read_events(append_run.root, append_setup.RUN_ID).events == (recovered,)
    if original_event is not None:
        assert recovered == original_event


def test_directory_creation_failure_preserves_zero_head(append_run, monkeypatch):
    original = Path.mkdir

    def fail(path, *args, **kwargs):
        if path == append_run.events_path:
            raise OSError("injected directory creation failure")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "mkdir", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(
        caught.value, append_run, phase="events-directory-create"
    )
    assert read_events(append_run.root, append_setup.RUN_ID).head.last_sequence == 0
    assert append_setup.append(append_run).sequence == 1


def test_existing_zero_head_flush_failure_does_not_create_events(
    append_run, monkeypatch
):
    append_setup.zero_head(append_run)
    original = journal._sync_record

    def fail(path):
        if path == append_run.head_path:
            raise OSError("injected existing-head flush failure")
        original(path)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_sync_record", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(caught.value, append_run, phase="head-sync")
    assert not append_run.events_path.exists()
    assert append_setup.append(append_run).sequence == 1


def test_actual_staging_file_fsync_failure_closes_descriptor_and_retains_file(
    append_run, monkeypatch
):
    descriptors = []
    original_open = journal.os.open
    original_fsync = journal.os.fsync

    def capture(path, *args, **kwargs):
        descriptor = original_open(path, *args, **kwargs)
        if Path(path).name.startswith(f".staging-{append_setup.EVENT_ID}-"):
            descriptors.append(descriptor)
        return descriptor

    def fail(descriptor):
        if descriptor in descriptors:
            raise OSError("injected staged file fsync failure")
        original_fsync(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(journal.os, "open", capture)
        patch.setattr(journal.os, "fsync", fail)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(caught.value, append_run, phase="event-write")
    assert caught.value.event_staging_path.exists()
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert append_setup.append(append_run).sequence == 1


@pytest.mark.parametrize("target", ["zero", "event"])
def test_exclusive_staging_collision_preserves_existing_file(
    append_run, monkeypatch, target
):
    suffix = "a" * 32
    monkeypatch.setattr(journal, "uuid4", lambda: SimpleNamespace(hex=suffix))
    if target == "zero":
        staging = append_run.path / f".staging-journal-head-{suffix}"
    else:
        append_setup.zero_head(append_run)
        append_run.events_path.mkdir()
        staging = (
            append_run.events_path / f".staging-{append_setup.EVENT_ID}-{suffix}.json"
        )
    staging.write_bytes(b"existing staged evidence")

    with pytest.raises(JournalPublicationError) as caught:
        append_setup.append(append_run)
    assert isinstance(caught.value.__cause__, FileExistsError)
    assert staging.read_bytes() == b"existing staged evidence"
    assert not append_setup.event_path(append_run).exists()


@pytest.mark.parametrize(
    ("target", "phase"),
    [
        ("event", "retry-event-sync"),
        ("events", "retry-events-directory-sync"),
        ("head", "retry-head-sync"),
        ("run", "retry-run-directory-sync"),
    ],
)
def test_committed_retry_flush_failure_is_uncertain_and_does_not_rewrite_history(
    append_run, monkeypatch, target, phase
):
    first = append_setup.append(append_run)
    before = append_setup.snapshot(append_run.root)
    original_record = journal._sync_record
    original_directory = journal._sync_directory

    def record(path):
        if (target == "event" and path == append_setup.event_path(append_run)) or (
            target == "head" and path == append_run.head_path
        ):
            raise OSError("injected retry file flush failure")
        original_record(path)

    def directory(path):
        if (target == "events" and path == append_run.events_path) or (
            target == "run" and path == append_run.path
        ):
            raise OSError("injected retry directory flush failure")
        original_directory(path)

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_sync_record", record)
        patch.setattr(journal, "_sync_directory", directory)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(
        caught.value, append_run, phase=phase, operation="retry"
    )
    assert caught.value.publication_uncertain is True
    assert append_setup.snapshot(append_run.root) == before
    assert append_setup.append(append_run) == first


@pytest.mark.parametrize("target", ["event", "directory"])
def test_tail_recovery_flush_failure_keeps_original_identity(
    append_run, monkeypatch, target
):
    pending = append_setup.make_pending(append_run, monkeypatch)
    original_record = journal._sync_record
    original_directory = journal._sync_directory

    def record(path):
        raise OSError("injected recovery file flush failure")

    def directory(path):
        raise OSError("injected recovery directory flush failure")

    with monkeypatch.context() as patch:
        if target == "event":
            patch.setattr(journal, "_sync_record", record)
        else:
            patch.setattr(journal, "_sync_directory", directory)
        with pytest.raises(JournalPublicationError) as caught:
            append_setup.append(append_run)

    append_setup.assert_publication_error(
        caught.value,
        append_run,
        phase=(
            "recovery-event-sync"
            if target == "event"
            else "recovery-events-directory-sync"
        ),
        operation="recover",
    )
    assert caught.value.publication_uncertain is True
    assert append_setup.append(append_run) == pending
    assert journal._sync_record is original_record
    assert journal._sync_directory is original_directory


def test_unsafe_file_during_acknowledgment_is_not_flushed_or_repaired(
    append_run, monkeypatch
):
    append_setup.append(append_run)
    original = journal._sync_record

    def replace_with_fifo(path):
        if path == append_setup.event_path(append_run):
            path.unlink()
            os.mkfifo(path)
        original(path)

    monkeypatch.setattr(journal, "_sync_record", replace_with_fifo)
    with pytest.raises(JournalPublicationError) as caught:
        append_setup.append(append_run)
    assert isinstance(caught.value.__cause__, ValueError)
    assert stat.S_ISFIFO(append_setup.event_path(append_run).lstat().st_mode)
    with pytest.raises(JournalIntegrityError):
        read_events(append_run.root, append_setup.RUN_ID)


@pytest.mark.parametrize("damage", ["head", "event", "gap"])
def test_integrity_failures_block_append_without_mutating_evidence(append_run, damage):
    append_setup.append(append_run)
    append_setup.append(append_run, append_setup.SECOND_ID)
    if damage == "head":
        append_run.head_path.write_bytes(b"corrupt head")
    elif damage == "event":
        append_setup.event_path(append_run).write_bytes(b"corrupt event")
    else:
        append_setup.event_path(append_run).unlink()
    before = append_setup.snapshot(append_run.root)

    with pytest.raises(JournalIntegrityError):
        append_setup.append(append_run, append_setup.THIRD_ID)
    assert append_setup.snapshot(append_run.root) == before


def test_external_artifact_failure_blocks_append_when_requested(append_run):
    append_run.sources[1].write_bytes(b"changed consumer")
    before = append_setup.snapshot(append_run.root)
    with pytest.raises(ValueError, match="SHA-256"):
        append_setup.append(append_run, verify_external=True)
    assert append_setup.snapshot(append_run.root) == before
