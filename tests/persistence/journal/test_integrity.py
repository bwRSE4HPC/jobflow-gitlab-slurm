"""persistence / journal / test_integrity contracts."""

import json
import os
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalIncompleteError,
    JournalIntegrityError,
    read_events,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    locked_run,
)
from tests.persistence.journal import _replay_support as replay_setup


@pytest.mark.parametrize("kind", ["file", "symlink", "dangling", "fifo"])
def test_events_path_must_be_a_real_directory(replay_run, kind):
    replay_setup.write_head(replay_run)
    if kind == "file":
        replay_run.events_path.write_bytes(b"not a directory")
    elif kind == "symlink":
        replay_run.events_path.symlink_to(replay_run.root, target_is_directory=True)
    elif kind == "dangling":
        replay_run.events_path.symlink_to(replay_run.path / "absent")
    else:
        os.mkfifo(replay_run.events_path)

    with pytest.raises(JournalIntegrityError, match="real directory"):
        read_events(replay_run.root, replay_setup.RUN_ID)


@pytest.mark.parametrize("location", ["head", "event"])
@pytest.mark.parametrize("kind", ["directory", "symlink", "dangling", "fifo"])
def test_record_paths_must_be_regular_files_without_symlinks(
    replay_run, location, kind
):
    records = replay_setup.chain(1)
    replay_setup.install(replay_run, records)
    path = (
        replay_run.head_path
        if location == "head"
        else replay_run.events_path / "000000000001.json"
    )
    original = path.read_bytes()
    path.unlink()

    if kind == "directory":
        path.mkdir()
    elif kind == "symlink":
        target = replay_run.path / "record-target"
        target.write_bytes(original)
        path.symlink_to(target)
    elif kind == "dangling":
        path.symlink_to(replay_run.path / "absent")
    else:
        os.mkfifo(path)

    with pytest.raises(JournalIntegrityError) as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.path == path
    assert caught.value.__cause__ is not None


@pytest.mark.parametrize("location", ["head", "event"])
@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"{",
        b"\xff",
        b"[]",
        b'{"schema_version":1,"schema_version":1}',
        b'{"value":NaN}',
    ],
)
def test_invalid_record_bytes_fail_closed(replay_run, location, data):
    replay_setup.install(replay_run, replay_setup.chain(1))
    path = (
        replay_run.head_path
        if location == "head"
        else replay_run.events_path / "000000000001.json"
    )
    path.write_bytes(data)
    before = replay_setup.snapshot(replay_run.root)

    with pytest.raises(JournalIntegrityError) as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.path == path
    assert caught.value.__cause__ is not None
    assert replay_setup.snapshot(replay_run.root) == before


@pytest.mark.parametrize("location", ["head", "event"])
@pytest.mark.parametrize("change", ["checksum", "schema", "noncanonical"])
def test_bad_checksum_schema_and_noncanonical_records_are_rejected(
    replay_run, location, change
):
    replay_setup.install(replay_run, replay_setup.chain(1))
    path = (
        replay_run.head_path
        if location == "head"
        else replay_run.events_path / "000000000001.json"
    )
    document = json.loads(path.read_bytes())
    if change == "checksum":
        document["sha256"] = "0" * 64
    elif change == "schema":
        document["schema_version"] = 2

    if change == "noncanonical":
        data = json.dumps(document, indent=2).encode("utf-8")
    else:
        data = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    path.write_bytes(data)

    with pytest.raises(JournalIntegrityError):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_foreign_head_identity_is_rejected(replay_run):
    replay_setup.write_head(replay_run, run_id=replay_setup.OTHER_ID)
    with pytest.raises(JournalIntegrityError, match="head run_id"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_foreign_event_identity_is_rejected(replay_run):
    records = (replay_setup.event(1, run_id=replay_setup.OTHER_ID),)
    replay_setup.install(replay_run, records)
    with pytest.raises(JournalIntegrityError, match="event run_id"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_filename_sequence_mismatch_is_rejected(replay_run):
    record = replay_setup.event(1)
    replay_setup.write_event(replay_run, record, filename="000000000002.json")
    replay_setup.write_head(replay_run, (record,))
    with pytest.raises(JournalIntegrityError, match="filename"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_gap_is_rejected_even_when_each_record_has_a_valid_checksum(replay_run):
    records = replay_setup.chain(3)
    replay_setup.install(replay_run, records)
    (replay_run.events_path / "000000000002.json").unlink()
    with pytest.raises(JournalIntegrityError, match="contiguous"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_history_must_begin_at_one(replay_run):
    record = replay_setup.event(2, "0" * 64)
    replay_setup.write_event(replay_run, record)
    replay_setup.write_head(replay_run)
    with pytest.raises(JournalIntegrityError, match="contiguous"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_duplicate_event_id_is_rejected(replay_run):
    first = replay_setup.event(1)
    second = replay_setup.event(2, first.sha256, event_id=first.event_id)
    replay_setup.install(replay_run, (first, second))
    with pytest.raises(JournalIntegrityError, match="duplicate event_id"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_predecessor_must_match_actual_history(replay_run):
    first = replay_setup.event(1)
    second = replay_setup.event(2, "0" * 64)
    replay_setup.install(replay_run, (first, second))
    with pytest.raises(JournalIntegrityError, match="predecessor checksum"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_anchor_checksum_must_match_its_event(replay_run):
    records = replay_setup.chain(1)
    replay_setup.install(replay_run, records)
    replay_setup.write_head(replay_run, records, last_sha256="0" * 64)
    with pytest.raises(JournalIntegrityError, match="anchored event"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_anchor_ahead_of_history_is_rejected(replay_run):
    records = replay_setup.chain(2)
    replay_setup.install(replay_run, records)
    (replay_run.events_path / "000000000002.json").unlink()
    with pytest.raises(JournalIntegrityError, match="missing events"):
        read_events(replay_run.root, replay_setup.RUN_ID)


@pytest.mark.parametrize("directory_present", [False, True])
def test_positive_anchor_without_events_is_not_empty(replay_run, directory_present):
    records = replay_setup.chain(1)
    replay_setup.write_head(replay_run, records)
    if directory_present:
        replay_run.events_path.mkdir()
    with pytest.raises(JournalIntegrityError, match="missing events"):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_deleting_a_valid_suffix_is_detected_while_anchor_survives(replay_run):
    records = replay_setup.chain(3)
    replay_setup.install(replay_run, records)
    (replay_run.events_path / "000000000002.json").unlink()
    (replay_run.events_path / "000000000003.json").unlink()
    with pytest.raises(JournalIntegrityError, match="missing events"):
        read_events(replay_run.root, replay_setup.RUN_ID)


@pytest.mark.parametrize("anchor", [0, 1])
def test_one_unanchored_event_holds_replay_and_preserves_evidence(replay_run, anchor):
    records = replay_setup.chain(anchor + 1)
    head = replay_setup.install(replay_run, records, anchor=anchor)
    before = replay_setup.snapshot(replay_run.root)

    with pytest.raises(JournalIncompleteError) as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)

    error = caught.value
    assert not isinstance(error, JournalIntegrityError)
    assert error.run_id == replay_setup.RUN_ID
    assert error.head_path == replay_run.head_path
    assert error.head == head
    assert error.committed_count == anchor
    assert error.pending_event == records[-1]
    assert error.pending_path == replay_run.events_path / f"{anchor + 1:012d}.json"
    assert records[-1].event_id in str(error)
    assert "same event ID" in str(error)
    assert "explicit matching append_event retry" in str(error)
    assert replay_setup.snapshot(replay_run.root) == before


def test_multiple_unanchored_events_are_not_adopted(replay_run):
    replay_setup.install(replay_run, replay_setup.chain(3), anchor=1)
    before = replay_setup.snapshot(replay_run.root)
    with pytest.raises(JournalIntegrityError, match="multiple unanchored"):
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert replay_setup.snapshot(replay_run.root) == before


def test_corrupt_unanchored_tail_is_integrity_failure_not_recoverable_tail(replay_run):
    records = replay_setup.chain(2)
    replay_setup.install(replay_run, records, anchor=1)
    (replay_run.events_path / "000000000002.json").write_bytes(b"corrupt")
    with pytest.raises(JournalIntegrityError):
        read_events(replay_run.root, replay_setup.RUN_ID)


def test_directory_enumeration_failure_is_not_an_empty_history(replay_run, monkeypatch):
    replay_setup.install(replay_run, replay_setup.chain(1))
    original = Path.iterdir

    def fail(path):
        if path == replay_run.events_path:
            raise PermissionError("injected enumeration failure")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", fail)
    with pytest.raises(JournalIntegrityError, match="cannot enumerate") as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.path == replay_run.events_path


def test_disappearing_event_during_read_is_not_skipped(replay_run, monkeypatch):
    replay_setup.install(replay_run, replay_setup.chain(1))
    target = replay_run.events_path / "000000000001.json"
    original = journal.os.open

    def fail(path, *args, **kwargs):
        if Path(path) == target:
            raise FileNotFoundError("injected disappearance")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(journal.os, "open", fail)
    with pytest.raises(JournalIntegrityError) as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.path == target
    assert isinstance(caught.value.__cause__, FileNotFoundError)
    assert target.exists()


def test_read_failure_closes_the_record_descriptor(replay_run, monkeypatch):
    replay_setup.write_head(replay_run)
    descriptors = []
    original_open = journal.os.open
    original_fdopen = journal.os.fdopen

    def capture(path, *args, **kwargs):
        descriptor = original_open(path, *args, **kwargs)
        if Path(path) == replay_run.head_path:
            descriptors.append(descriptor)
        return descriptor

    def fail(descriptor, *args, **kwargs):
        if descriptor in descriptors:
            raise OSError("injected read setup failure")
        return original_fdopen(descriptor, *args, **kwargs)

    monkeypatch.setattr(journal.os, "open", capture)
    monkeypatch.setattr(journal.os, "fdopen", fail)

    with pytest.raises(JournalIntegrityError):
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])

    # Remove failure injection before reopening: descriptor numbers are reused.
    monkeypatch.setattr(journal.os, "open", original_open)
    monkeypatch.setattr(journal.os, "fdopen", original_fdopen)

    # The run lock is also released after the failed journal read.
    with locked_run(replay_run.root, replay_setup.RUN_ID):
        pass


def test_external_verification_is_explicit_and_failure_does_not_repair(replay_run):
    records = replay_setup.chain(1)
    replay_setup.install(replay_run, records)
    replay_run.sources[1].write_bytes(b"changed consumer")
    assert read_events(replay_run.root, replay_setup.RUN_ID).events == records
    before = replay_setup.snapshot(replay_run.root)

    with pytest.raises(ValueError, match="SHA-256"):
        read_events(replay_run.root, replay_setup.RUN_ID, verify_external=True)
    assert replay_setup.snapshot(replay_run.root) == before


def test_original_flow_validation_failure_propagates_before_replay(replay_run):
    replay_setup.install(replay_run, replay_setup.chain(1))
    (replay_run.path / "flow/payload.json").write_bytes(b"changed flow")
    before = replay_setup.snapshot(replay_run.root)

    with pytest.raises(ValueError, match="SHA-256"):
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert replay_setup.snapshot(replay_run.root) == before
