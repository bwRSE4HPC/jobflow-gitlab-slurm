"""persistence / journal / test_replay contracts."""

import json
import os
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalIntegrityError,
    read_events,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    locked_run,
)
from tests.persistence.journal import _replay_support as replay_setup


def test_uninitialized_journal_is_empty_and_creates_nothing(replay_run):
    before = replay_setup.snapshot(replay_run.root)
    replay = read_events(replay_run.root, replay_setup.RUN_ID)
    assert replay.run_id == replay_setup.RUN_ID
    assert replay.head is None
    assert replay.events == ()
    assert replay_setup.snapshot(replay_run.root) == before
    assert not replay_run.events_path.exists()
    assert not replay_run.head_path.exists()


@pytest.mark.parametrize("with_directory", [False, True])
def test_zero_head_is_empty_without_initializing_or_flushing(
    replay_run, monkeypatch, with_directory
):
    head = replay_setup.write_head(replay_run)
    if with_directory:
        replay_run.events_path.mkdir()
    before = replay_setup.snapshot(replay_run.root)

    def forbidden(*args, **kwargs):
        raise AssertionError("read-only replay attempted a filesystem mutation")

    monkeypatch.setattr(journal.os, "fsync", forbidden)
    monkeypatch.setattr(journal.os, "rename", forbidden)
    monkeypatch.setattr(journal.os, "replace", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    monkeypatch.setattr(Path, "write_bytes", forbidden)
    monkeypatch.setattr(Path, "write_text", forbidden)
    monkeypatch.setattr(Path, "unlink", forbidden)

    replay = read_events(replay_run.root, replay_setup.RUN_ID)
    assert replay.head == head
    assert replay.events == ()
    assert replay_setup.snapshot(replay_run.root) == before


def test_complete_history_is_immutable_detached_and_read_only(replay_run):
    records = replay_setup.chain(3)
    head = replay_setup.install(replay_run, records)
    before = replay_setup.snapshot(replay_run.root)

    replay = read_events(replay_run.root, replay_setup.RUN_ID, verify_external=True)
    assert replay.head == head
    assert replay.events == records
    assert isinstance(replay.events, tuple)
    with pytest.raises(FrozenInstanceError):
        replay.run_id = replay_setup.OTHER_ID

    payload = replay.events[0].payload()
    payload["value"] = "changed"
    assert replay.events[0].payload() == {"value": 1}
    assert replay_setup.snapshot(replay_run.root) == before
    assert "do_not_import_this_journal_consumer" not in sys.modules


def test_replay_in_another_process_preserves_ids_and_original_flow(replay_run):
    records = replay_setup.chain(3)
    replay_setup.install(replay_run, records)
    before = replay_setup.snapshot(replay_run.root)

    result = replay_setup.process(
        """
import json
import sys
from jobflow_gitlab_slurm.persistence.journal.storage import read_events
replay = read_events(sys.argv[1], sys.argv[2], verify_external=True)
print(json.dumps({
    "run_id": replay.run_id,
    "sequences": [event.sequence for event in replay.events],
    "event_ids": [event.event_id for event in replay.events],
    "checksums": [event.sha256 for event in replay.events],
    "head": replay.head.last_sha256,
}))
""",
        replay_run.root,
        replay_setup.RUN_ID,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "run_id": replay_setup.RUN_ID,
        "sequences": [1, 2, 3],
        "event_ids": [record.event_id for record in records],
        "checksums": [record.sha256 for record in records],
        "head": records[-1].sha256,
    }
    assert (
        replay_run.path / "flow/payload.json"
    ).read_bytes() == replay_setup.FLOW_BYTES
    assert replay_setup.snapshot(replay_run.root) == before


@pytest.mark.parametrize("with_events", [False, True])
def test_missing_head_with_events_directory_is_not_an_empty_journal(
    replay_run, with_events
):
    replay_run.events_path.mkdir()
    if with_events:
        replay_setup.write_event(replay_run, replay_setup.event(1))
    before = replay_setup.snapshot(replay_run.root)

    with pytest.raises(JournalIntegrityError, match="without a valid head") as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.run_id == replay_setup.RUN_ID
    assert caught.value.path == replay_run.head_path
    assert "Preserve evidence" in str(caught.value)
    assert replay_setup.snapshot(replay_run.root) == before


@pytest.mark.parametrize("kind", ["file", "directory", "symlink"])
def test_unexpected_entries_are_not_silently_ignored(replay_run, kind):
    replay_setup.install(replay_run, replay_setup.chain(1))
    path = replay_run.events_path / "unexpected"
    if kind == "file":
        path.write_bytes(b"preserve")
    elif kind == "directory":
        path.mkdir()
    else:
        path.symlink_to(replay_run.path / "absent")

    with pytest.raises(JournalIntegrityError, match="unexpected entry"):
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert os.path.lexists(path)


def test_reserved_staging_evidence_is_ignored_and_preserved(replay_run):
    records = replay_setup.chain(1)
    replay_setup.install(replay_run, records)
    (replay_run.events_path / ".staging-interrupted").write_bytes(b"partial bytes")
    (replay_run.events_path / ".staging-directory").mkdir()
    (replay_run.events_path / ".staging-link").symlink_to(replay_run.path / "absent")
    (replay_run.path / ".staging-journal-head-interrupted").write_bytes(b"partial head")
    before = replay_setup.snapshot(replay_run.root)

    assert read_events(replay_run.root, replay_setup.RUN_ID).events == records
    assert replay_setup.snapshot(replay_run.root) == before


@pytest.mark.parametrize("location", ["head", "events"])
def test_path_inspection_errors_are_not_treated_as_absence(
    replay_run, monkeypatch, location
):
    target = replay_run.head_path if location == "head" else replay_run.events_path
    original = Path.lstat

    def fail(path, *args, **kwargs):
        if path == target:
            raise PermissionError("injected inspection failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", fail)
    with pytest.raises(JournalIntegrityError, match="cannot inspect") as caught:
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert caught.value.path == target
    assert isinstance(caught.value.__cause__, PermissionError)


def test_busy_run_propagates_without_initializing_journal(replay_run):
    before = replay_setup.snapshot(replay_run.root)
    with locked_run(replay_run.root, replay_setup.RUN_ID), pytest.raises(RunBusyError):
        read_events(replay_run.root, replay_setup.RUN_ID)
    assert replay_setup.snapshot(replay_run.root) == before
    assert read_events(replay_run.root, replay_setup.RUN_ID).events == ()


def test_separate_process_observes_lock_contention_then_replays(replay_run):
    records = replay_setup.chain(1)
    replay_setup.install(replay_run, records)
    code = """
import sys
from jobflow_gitlab_slurm.persistence.journal.storage import read_events
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError
try:
    replay = read_events(sys.argv[1], sys.argv[2])
except RunBusyError:
    print("busy")
    sys.exit(3)
print(len(replay.events))
"""
    with locked_run(replay_run.root, replay_setup.RUN_ID):
        busy = replay_setup.process(code, replay_run.root, replay_setup.RUN_ID)
    assert busy.returncode == 3, busy.stderr
    assert busy.stdout.strip() == "busy"

    completed = replay_setup.process(code, replay_run.root, replay_setup.RUN_ID)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "1"
