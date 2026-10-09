"""Explicit scenario preparation and observations; no collected tests."""

import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.journal.records import (
    encode_record,
    make_event,
    make_head,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    create_run,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

OTHER_ID = "00000000-0000-4000-8000-000000000002"

CREATED = "2026-10-07T12:00:00.000000Z"

FLOW_BYTES = b'{ "@module": "do_not_import_this_journal_consumer" }\r\n'


def replay_run(tmp_path):
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
                "name": "journal-example",
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


def event(sequence, previous_sha256=None, **changes):
    values = {
        "run_id": RUN_ID,
        "event_id": f"00000000-0000-4000-8000-{sequence + 100:012d}",
        "sequence": sequence,
        "created_at": CREATED,
        "event_type": "example.note",
        "payload": {"value": sequence},
        "previous_sha256": previous_sha256,
    }
    values.update(changes)
    return make_event(**values)


def chain(count):
    records = []
    previous = None
    for sequence in range(1, count + 1):
        record = event(sequence, previous)
        records.append(record)
        previous = record.sha256
    return tuple(records)


def write_event(replay_run, record, *, filename=None):
    replay_run.events_path.mkdir(exist_ok=True)
    path = replay_run.events_path / (filename or f"{record.sequence:012d}.json")
    path.write_bytes(encode_record(record))
    return path


def write_head(replay_run, records=(), *, anchor=None, **changes):
    sequence = len(records) if anchor is None else anchor
    digest = records[sequence - 1].sha256 if sequence else None
    values = {
        "run_id": RUN_ID,
        "updated_at": CREATED,
        "last_sequence": sequence,
        "last_sha256": digest,
    }
    values.update(changes)
    head = make_head(**values)
    replay_run.head_path.write_bytes(encode_record(head))
    return head


def install(replay_run, records, *, anchor=None):
    replay_run.events_path.mkdir(exist_ok=True)
    for record in records:
        write_event(replay_run, record)
    return write_head(replay_run, records, anchor=anchor)


def snapshot(root):
    result = {}
    for path in root.rglob("*"):
        metadata = path.lstat()
        result[str(path.relative_to(root))] = (
            metadata.st_mode,
            metadata.st_mtime_ns,
            path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None,
        )
    return result


def process(code, *arguments):
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(
        Path(__import__("jobflow_gitlab_slurm").__file__).resolve().parent.parent
    )
    return subprocess.run(
        [sys.executable, "-c", code, *(str(value) for value in arguments)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
