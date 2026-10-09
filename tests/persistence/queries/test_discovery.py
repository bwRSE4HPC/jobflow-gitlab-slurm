"""persistence / queries / test_discovery contracts."""

from contextlib import contextmanager
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.queries import inspection
from jobflow_gitlab_slurm.persistence.queries.discovery import discover_runs
from jobflow_gitlab_slurm.persistence.runs.storage import (
    locked_run,
)
from tests.support import inspection as inspection_setup
from tests.support import runs as runs_setup


def test_discovery_is_sorted_and_does_not_create_state(discovery_inputs):
    second = runs_setup.create(discovery_inputs, runs_setup.OTHER_ID)
    first = runs_setup.create(discovery_inputs)
    results = discover_runs(discovery_inputs.root, verify_external=True)
    assert [entry.run_id for entry in results] == [
        inspection_setup.RUN_ID,
        runs_setup.OTHER_ID,
    ]
    assert [entry.status for entry in results] == ["valid", "valid"]
    assert [entry.handle for entry in results] == [first, second]
    assert all(entry.error is None for entry in results)


def test_empty_root_and_private_names_are_ignored(discovery_inputs):
    assert discover_runs(discovery_inputs.root) == ()
    (discovery_inputs.root / ".locks").mkdir()
    staging = discovery_inputs.root / ".staging-interrupted-creation"
    staging.mkdir()
    (staging / "partial").write_bytes(b"evidence")
    before = sorted(path.name for path in discovery_inputs.root.iterdir())
    assert discover_runs(discovery_inputs.root) == ()
    assert sorted(path.name for path in discovery_inputs.root.iterdir()) == before
    assert (staging / "partial").read_bytes() == b"evidence"


@pytest.mark.parametrize("kind", ["file", "symlink", "dangling"])
def test_uuid_entries_must_be_real_directories(discovery_inputs, kind):
    path = discovery_inputs.root / inspection_setup.RUN_ID
    if kind == "file":
        path.write_bytes(b"not a run directory")
    else:
        target = (
            discovery_inputs.root
            if kind == "symlink"
            else discovery_inputs.root / "absent"
        )
        path.symlink_to(target)
    entries = discover_runs(discovery_inputs.root)
    assert len(entries) == 1
    assert entries[0].run_id == inspection_setup.RUN_ID
    assert entries[0].status == "invalid"
    assert "real directory" in entries[0].error


def test_discovery_reports_lock_contention(discovery_inputs):
    runs_setup.create(discovery_inputs)
    with locked_run(discovery_inputs.root, inspection_setup.RUN_ID):
        entries = discover_runs(discovery_inputs.root)
    assert len(entries) == 1
    assert entries[0].status == "busy"
    assert entries[0].run_id == inspection_setup.RUN_ID
    assert entries[0].handle is None
    assert "busy" in entries[0].error
    assert discover_runs(discovery_inputs.root)[0].status == "valid"


def test_entry_disappearing_during_discovery_is_reported(discovery_inputs, monkeypatch):
    path = discovery_inputs.root / inspection_setup.RUN_ID
    path.mkdir()

    def disappear(self):
        if self == path:
            raise FileNotFoundError("entry disappeared")
        return original(self)

    original = Path.lstat
    monkeypatch.setattr(Path, "lstat", disappear)
    entries = discover_runs(discovery_inputs.root)
    assert entries[0].status == "invalid"
    assert "entry disappeared" in entries[0].error


@pytest.mark.parametrize("kind", ["missing", "file", "symlink", "relative"])
def test_invalid_discovery_root_fails(discovery_inputs, tmp_path, kind):
    root = tmp_path / "invalid-root"
    if kind == "file":
        root.write_bytes(b"file")
    elif kind == "symlink":
        root.symlink_to(discovery_inputs.root, target_is_directory=True)
    elif kind == "relative":
        root = "relative/root"
    error = FileNotFoundError if kind == "missing" else ValueError
    with pytest.raises(error):
        discover_runs(root)


def test_discovery_can_optionally_verify_external_bytes(discovery_inputs):
    runs_setup.create(discovery_inputs)
    discovery_inputs.sources[2].write_bytes(b"changed runtime")
    assert discover_runs(discovery_inputs.root)[0].status == "valid"
    entry = discover_runs(discovery_inputs.root, verify_external=True)[0]
    assert entry.status == "invalid"
    assert "SHA-256 mismatch" in entry.error


def test_discovery_uses_one_inspection_lock_per_run(inspection_runs, monkeypatch):
    inspection_setup.prepare(inspection_runs, "valid")
    inspection_setup.prepare(inspection_runs, "incomplete", inspection_setup.SECOND_ID)
    calls = []
    original = inspection.locked_run

    @contextmanager
    def counted(root, run_id, **kwargs):
        calls.append(run_id)
        with original(root, run_id, **kwargs) as handle:
            yield handle

    monkeypatch.setattr(inspection, "locked_run", counted)
    entries = discover_runs(inspection_runs.root)
    assert calls == [inspection_setup.RUN_ID, inspection_setup.SECOND_ID]
    assert [entry.status for entry in entries] == ["valid", "valid"]
    assert [entry.journal.status for entry in entries] == ["valid", "incomplete"]
    assert all(entry.error is None for entry in entries)
