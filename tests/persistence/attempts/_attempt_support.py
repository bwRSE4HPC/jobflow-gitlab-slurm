"""Explicit scenario preparation and observations; no collected tests."""

import hashlib
import json
import os
import stat
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.attempts import storage as attempts
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    publish_job_definition,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    read_attempt,
    read_invocation,
    register_invocation,
    reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

OTHER_RUN_ID = "00000000-0000-4000-8000-000000000099"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

SECOND_DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000009"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

SECOND_ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000009"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

SECOND_INVOCATION_ID = "cccccccc-0000-4000-8000-000000000009"

JOB_UUID = "../../opaque-attempt-job-é"

MODES = ("attempt", "invocation")

FLOW_BYTES = b'{ "@module": "do_not_import_attempt_flow" }\r\n'

JOB_BYTES = b'{ "@module": "do_not_import_attempt_job", "value": 42 }\r\n'


def attempt_run(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    for path, data in zip(sources, (FLOW_BYTES, b"consumer", b"\0runtime")):
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
                "name": "attempt-example",
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
    payload = tmp_path / "job-source.json"
    payload.write_bytes(JOB_BYTES)
    key = job_key(JOB_UUID)
    record = JobDefinitionRecord(
        schema_version=1,
        kind="job-definition",
        run_id=RUN_ID,
        created_at=handle.manifest.created_at,
        job_uuid=JOB_UUID,
        job_index=1,
        job_key=key,
        definition_id=DEFINITION_ID,
        jobflow_version="0.3.1",
        payload={
            "path": f"jobs/{key}/index-1/definitions/{DEFINITION_ID}/job.json",
            "sha256": hashlib.sha256(JOB_BYTES).hexdigest(),
            "size_bytes": len(JOB_BYTES),
        },
        origin="original_flow",
        source=handle.flow.payload.model_dump(),
    )
    definition = publish_job_definition(root, record, payload)
    return SimpleNamespace(
        root=root,
        path=handle.path,
        handle=handle,
        definition=definition,
        payload=payload,
        sources=sources,
    )


def timestamp(attempt_run, offset):
    instant = datetime.fromisoformat(attempt_run.handle.manifest.created_at)
    return (
        (instant + timedelta(seconds=offset))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def attempt_record(attempt_run, **changes):
    data = {
        "schema_version": 1,
        "kind": "execution-attempt",
        "run_id": RUN_ID,
        "created_at": timestamp(attempt_run, 1),
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": job_key(JOB_UUID),
        "attempt_id": ATTEMPT_ID,
        "definition_id": DEFINITION_ID,
        "definition_sha256": attempt_run.definition.record.payload.sha256,
        "consumer_code_sha256": (
            attempt_run.handle.manifest.request.workflow.consumer_code_sha256
        ),
        "worker_runtime_sha256": (
            attempt_run.handle.manifest.request.runtime.worker_runtime_sha256
        ),
    }
    data.update(changes)
    return AttemptRecord.model_validate(data)


def invocation_record(attempt_run, **changes):
    data = {
        "schema_version": 1,
        "kind": "worker-invocation",
        "run_id": RUN_ID,
        "created_at": timestamp(attempt_run, 2),
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": job_key(JOB_UUID),
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }
    data.update(changes)
    return InvocationRecord.model_validate(data)


def attempt_or_invocation(request, attempt_run):
    mode = request.param
    if mode == "invocation":
        reserve_attempt(attempt_run.root, attempt_record(attempt_run))
        record = invocation_record(attempt_run)
        filename = "invocation.json"
        operation = "register"
    else:
        record = attempt_record(attempt_run)
        filename = "attempt.json"
        operation = "reserve"

    return SimpleNamespace(
        run=attempt_run,
        mode=mode,
        record=record,
        filename=filename,
        operation=operation,
    )


def publish(attempt_or_invocation, record=None):
    record = attempt_or_invocation.record if record is None else record
    function = (
        reserve_attempt
        if attempt_or_invocation.mode == "attempt"
        else register_invocation
    )
    return function(attempt_or_invocation.run.root, record)


def read(attempt_or_invocation, **changes):
    values = {
        "run_id": RUN_ID,
        "job_uuid": attempt_or_invocation.record.job_uuid,
        "job_index": attempt_or_invocation.record.job_index,
        "attempt_id": attempt_or_invocation.record.attempt_id,
    }
    if attempt_or_invocation.mode == "invocation":
        values["invocation_id"] = attempt_or_invocation.record.invocation_id
    values.update(changes)
    function = (
        read_attempt if attempt_or_invocation.mode == "attempt" else read_invocation
    )
    return function(attempt_or_invocation.run.root, **values)


def paths(attempt_or_invocation):
    function = (
        attempts._attempt_paths
        if attempt_or_invocation.mode == "attempt"
        else attempts._invocation_paths
    )
    return function(attempt_or_invocation.run.handle, attempt_or_invocation.record)


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


def rewrite(path, mutate):
    data = json.loads(path.read_bytes())
    mutate(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def assert_publication_error(
    error,
    attempt_or_invocation,
    *,
    phase,
    uncertain=False,
    operation=None,
):
    assert error.run_id == RUN_ID
    assert error.job_key == job_key(JOB_UUID)
    assert error.job_index == 1
    assert error.attempt_id == ATTEMPT_ID
    assert error.invocation_id == (
        INVOCATION_ID if attempt_or_invocation.mode == "invocation" else None
    )
    assert error.path == paths(attempt_or_invocation)[1]
    assert error.phase == phase
    assert error.operation == (
        attempt_or_invocation.operation if operation is None else operation
    )
    assert error.publication_uncertain is uncertain
    assert "same identity and original record" in str(error)
    assert "Preserve evidence" in str(error)
    assert JOB_UUID not in str(error)
