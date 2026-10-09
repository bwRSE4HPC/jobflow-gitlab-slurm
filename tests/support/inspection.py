"""Explicit scenario preparation and observations; no collected tests."""

import hashlib
import json
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

SECOND_ID = "00000000-0000-4000-8000-000000000002"

EVENT_ID = "00000000-0000-4000-8000-000000000101"

CREATED = "2026-10-07T12:00:00.000000Z"

PRIVATE_PAYLOAD = "private-payload-sentinel"

FLOW_BYTES = b'{ "@module": "never_import_inspection_consumer" }\r\n'


def inspection_runs(tmp_path):
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
                "name": "inspection-example",
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
    return SimpleNamespace(root=root, sources=sources, site=site, request=request)


def create(inspection_runs, run_id=RUN_ID):
    return create_run(
        inspection_runs.site,
        inspection_runs.request,
        *inspection_runs.sources,
        run_id=run_id,
    )


def prepare(inspection_runs, state, run_id=RUN_ID):
    handle = create(inspection_runs, run_id)
    if state == "uninitialized":
        return handle

    head_path = handle.path / "journal-head.json"
    if state == "invalid":
        head_path.write_text(
            json.dumps({"private": PRIVATE_PAYLOAD}),
            encoding="utf-8",
        )
        return handle

    event = make_event(
        run_id=run_id,
        event_id=EVENT_ID,
        sequence=1,
        created_at=CREATED,
        event_type="example.note",
        payload={"private": PRIVATE_PAYLOAD},
        previous_sha256=None,
    )
    if state in {"valid", "incomplete"}:
        directory = handle.path / "events"
        directory.mkdir()
        (directory / "000000000001.json").write_bytes(encode_record(event))

    sequence = 1 if state == "valid" else 0
    head = make_head(
        run_id=run_id,
        updated_at=CREATED,
        last_sequence=sequence,
        last_sha256=event.sha256 if sequence else None,
    )
    head_path.write_bytes(encode_record(head))
    return handle


def inspect_arguments(inspection_runs, run_id=RUN_ID):
    return ["inspect-run", run_id, "--runs-root", str(inspection_runs.root)]


def list_arguments(inspection_runs):
    return ["list-runs", "--runs-root", str(inspection_runs.root)]


def snapshot(root):
    return {
        str(path.relative_to(root)): (
            path.stat().st_mtime_ns,
            path.read_bytes() if path.is_file() else None,
        )
        for path in root.rglob("*")
    }
