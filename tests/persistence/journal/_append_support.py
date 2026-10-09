"""Explicit scenario preparation and observations; no collected tests."""

from tests.support.filesystem import descendant_metadata_and_bytes as snapshot
from tests.support.processes import python_child as process

__all__ = ["process", "snapshot"]

import hashlib
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    decode_event,
    encode_record,
    make_head,
)
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalPublicationError,
    append_event,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    create_run,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

EVENT_ID = "00000000-0000-4000-8000-000000000101"

SECOND_ID = "00000000-0000-4000-8000-000000000102"

THIRD_ID = "00000000-0000-4000-8000-000000000103"

CREATED = "2026-10-07T12:00:00.000000Z"

FLOW_BYTES = b'{ "@module": "do_not_import_this_append_consumer" }\r\n'


def append_run(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    for path, data in zip(sources, (FLOW_BYTES, b"consumer", b"\x00runtime")):
        path.write_bytes(data)
    digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in sources]

    site = SiteConfig.model_validate(
        {
            "schema_version": 1,
            "site_id": "example-cluster",
            "slurm": {
                "cluster_name": "example-slurm",
                "account": "example-account",
                "partitions": ["short"],
                "default_partition": "short",
            },
            "storage": {"provider": "posix", "runs_root": str(root)},
            "worker": {
                "launch_mode": "apptainer",
                "apptainer_command": "apptainer",
            },
        }
    )
    request = RunRequest.model_validate(
        {
            "schema_version": 1,
            "site_id": site.site_id,
            "workflow": {
                "name": "append-example",
                "serialized_flow_sha256": digests[0],
                "consumer_code_sha256": digests[1],
            },
            "runtime": {"worker_runtime_sha256": digests[2]},
            "resources": {
                "partition": "short",
                "nodes": 1,
                "tasks_per_node": 2,
                "cpus_per_task": 1,
                "memory_mb_per_node": 4096,
                "walltime_seconds": 1800,
            },
        }
    )
    handle = create_run(site, request, *sources, run_id=RUN_ID)
    return SimpleNamespace(
        root=root,
        path=handle.path,
        sources=sources,
        head_path=handle.path / "journal-head.json",
        events_path=handle.path / "events",
    )


def append(append_run, event_id=EVENT_ID, **changes):
    values = {
        "event_id": event_id,
        "event_type": "example.note",
        "payload": {"value": 1},
    }
    values.update(changes)
    return append_event(append_run.root, RUN_ID, **values)


def event_path(append_run, sequence=1):
    return append_run.events_path / f"{sequence:012d}.json"


def zero_head(append_run):
    head = make_head(
        run_id=RUN_ID,
        updated_at=CREATED,
        last_sequence=0,
        last_sha256=None,
    )
    append_run.head_path.write_bytes(encode_record(head))
    return head


def assert_publication_error(error, append_run, *, phase, operation="append"):
    assert error.run_id == RUN_ID
    assert error.event_id == EVENT_ID
    assert error.run_path == append_run.path
    assert error.head_path == append_run.head_path
    assert error.event_path == event_path(append_run)
    assert error.sequence == 1
    assert error.operation == operation
    assert error.phase == phase
    assert isinstance(error.__cause__, OSError)
    assert "Preserve" not in str(error) or "evidence" in str(error)
    assert "preserve evidence" in str(error)
    assert "same run/event ID" in str(error)


def make_pending(append_run, monkeypatch):
    original = journal._rename_new

    def interrupt(source, destination):
        original(source, destination)
        if destination == event_path(append_run):
            raise OSError("interrupted after event publication")

    with monkeypatch.context() as patch:
        patch.setattr(journal, "_rename_new", interrupt)
        with pytest.raises(JournalPublicationError):
            append(append_run)
    return decode_event(event_path(append_run).read_bytes())
