"""persistence / queries / test_inspection contracts."""

import json
from contextlib import contextmanager
from dataclasses import FrozenInstanceError

import pytest

from jobflow_gitlab_slurm.persistence.queries import inspection
from jobflow_gitlab_slurm.persistence.queries.discovery import discover_runs
from jobflow_gitlab_slurm.persistence.queries.inspection import (
    JOURNAL_NOT_CHECKED,
    inspect_run,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    locked_run,
)
from tests.support import inspection as inspection_setup


@pytest.mark.parametrize(
    "name",
    ["README.md", ".unknown", "not-a-uuid", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"],
)
def test_unexpected_names_are_reported(discovery_inputs, name):
    path = discovery_inputs.root / name
    path.write_bytes(b"preserve")
    entries = discover_runs(discovery_inputs.root)
    assert len(entries) == 1
    assert entries[0].path == path
    assert entries[0].run_id is None
    assert entries[0].status == "invalid"
    assert entries[0].handle is None
    assert entries[0].error
    assert path.read_bytes() == b"preserve"


def test_incomplete_published_candidate_is_reported(discovery_inputs):
    (discovery_inputs.root / inspection_setup.RUN_ID).mkdir()
    entries = discover_runs(discovery_inputs.root)
    assert len(entries) == 1
    assert entries[0].status == "invalid"
    assert entries[0].run_id == inspection_setup.RUN_ID
    assert entries[0].error
    assert not (discovery_inputs.root / ".locks").exists()


@pytest.mark.parametrize(
    ("state", "status", "count"),
    [
        ("uninitialized", "uninitialized", 0),
        ("empty", "valid", 0),
        ("valid", "valid", 1),
        ("incomplete", "incomplete", 0),
        ("invalid", "invalid", None),
    ],
)
def test_inspection_separates_metadata_and_journal_findings(
    inspection_runs, state, status, count
):
    handle = inspection_setup.prepare(inspection_runs, state)
    before = inspection_setup.snapshot(inspection_runs.root)

    result = inspect_run(
        inspection_runs.root, inspection_setup.RUN_ID, verify_external=True
    )
    assert result.handle == handle
    assert result.journal.status == status
    assert result.journal.committed_count == count
    report = result.journal.to_report()
    assert report["schema_version"] == 1
    assert report["kind"] == "journal-inspection"
    assert report["checked"] is True
    assert report["head_path"] == str(handle.path / "journal-head.json")
    assert report["events_path"] == str(handle.path / "events")
    assert report["hint"]
    assert inspection_setup.PRIVATE_PAYLOAD not in json.dumps(report)
    assert inspection_setup.snapshot(inspection_runs.root) == before

    if state == "incomplete":
        assert report["pending_event_id"] == inspection_setup.EVENT_ID
        assert report["pending_sequence"] == 1
        assert report["pending_event_sha256"]
        assert report["pending_path"] == str(handle.path / "events/000000000001.json")
        assert report["problem_path"] == report["pending_path"]
        assert report["error_type"] == "JournalIncompleteError"
        assert "same pending event ID" in report["hint"]
        assert report["head_sha256"]
        assert report["last_event_sha256"] is None
    elif state == "invalid":
        assert report["problem_path"] == str(handle.path / "journal-head.json")
        assert report["error_type"] == "JournalIntegrityError"
        assert report["head_sha256"] is None
        assert report["last_event_sha256"] is None
        assert report["pending_event_id"] is None
        assert report["error"]
    else:
        assert report["error"] is None
        assert report["pending_event_id"] is None


def test_reports_are_detached_and_inspection_objects_are_frozen(inspection_runs):
    inspection_setup.prepare(inspection_runs, "valid")
    result = inspect_run(inspection_runs.root, inspection_setup.RUN_ID)
    first = result.journal.to_report()
    first["hint"] = "changed"
    assert result.journal.to_report()["hint"] != "changed"

    with pytest.raises(FrozenInstanceError):
        result.journal.status = "invalid"
    with pytest.raises(FrozenInstanceError):
        result.handle = None

    unchecked = JOURNAL_NOT_CHECKED.to_report()
    assert unchecked["status"] == "not_checked"
    assert unchecked["checked"] is False
    assert unchecked["committed_count"] is None
    assert unchecked["head_path"] is None
    assert unchecked["events_path"] is None
    assert unchecked["pending_path"] is None
    assert unchecked["problem_path"] is None


def test_metadata_and_journal_share_one_lock_and_return_without_retaining_it(
    inspection_runs, monkeypatch
):
    inspection_setup.prepare(inspection_runs, "valid")
    calls = []
    original_lock = inspection.locked_run
    original_read = inspection.journal._read_events_locked

    @contextmanager
    def counted(root, run_id, **kwargs):
        calls.append((root, run_id, kwargs))
        with original_lock(root, run_id, **kwargs) as handle:
            yield handle

    def verify_held(path, run_id):
        with pytest.raises(RunBusyError), locked_run(inspection_runs.root, run_id):
            pass
        return original_read(path, run_id)

    monkeypatch.setattr(inspection, "locked_run", counted)
    monkeypatch.setattr(inspection.journal, "_read_events_locked", verify_held)
    result = inspect_run(
        inspection_runs.root, inspection_setup.RUN_ID, verify_external=True
    )

    assert result.journal.status == "valid"
    assert calls == [
        (inspection_runs.root, inspection_setup.RUN_ID, {"verify_external": True})
    ]
    with locked_run(inspection_runs.root, inspection_setup.RUN_ID):
        pass
