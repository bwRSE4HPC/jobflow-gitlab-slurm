"""Explicit scenario preparation and observations; no collected tests."""

from tests.support.filesystem import descendant_metadata_and_bytes as snapshot
from tests.support.processes import python_child as process

__all__ = ["process", "snapshot"]

import hashlib
import json
from types import SimpleNamespace

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    publish_job_definition,
    read_job_definition,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

OTHER_RUN_ID = "00000000-0000-4000-8000-000000000099"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

SECOND_ID = "bbbbbbbb-0000-4000-8000-000000000003"

JOB_UUID = "../../opaque-job-é"

FLOW_BYTES = b'{ "@module": "do_not_import_definition_flow" }\r\n'

JOB_BYTES = b'{ "@module": "do_not_import_definition_job", "value": 42 }\r\n'


def definition_run(tmp_path):
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
                "name": "definition-example",
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
    return SimpleNamespace(
        root=root,
        path=handle.path,
        handle=handle,
        payload=payload,
        sources=sources,
    )


def record_for(
    definition_run, *, definition_id=DEFINITION_ID, job_uuid=JOB_UUID, job_index=1
):
    key = job_key(job_uuid)
    return JobDefinitionRecord(
        schema_version=1,
        kind="job-definition",
        run_id=RUN_ID,
        created_at=definition_run.handle.manifest.created_at,
        job_uuid=job_uuid,
        job_index=job_index,
        job_key=key,
        definition_id=definition_id,
        jobflow_version="0.3.1",
        payload={
            "path": (
                f"jobs/{key}/index-{job_index}/definitions/{definition_id}/job.json"
            ),
            "sha256": hashlib.sha256(JOB_BYTES).hexdigest(),
            "size_bytes": len(JOB_BYTES),
        },
        origin="original_flow",
        source=definition_run.handle.flow.payload.model_dump(),
    )


def destination(definition_run, record=None):
    record = record_for(definition_run) if record is None else record
    return (definition_run.path / record.payload.path).parent


def publish(definition_run, record=None):
    record = record_for(definition_run) if record is None else record
    return publish_job_definition(definition_run.root, record, definition_run.payload)


def read(definition_run, record=None):
    record = record_for(definition_run) if record is None else record
    return read_job_definition(
        definition_run.root,
        RUN_ID,
        record.job_uuid,
        record.job_index,
        record.definition_id,
    )


def rewrite_metadata(path, mutate):
    data = json.loads(path.read_bytes())
    mutate(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def assert_publication_error(
    error,
    definition_run,
    *,
    phase,
    uncertain=False,
    operation="publish",
):
    assert error.run_id == RUN_ID
    assert error.job_key == job_key(JOB_UUID)
    assert error.job_index == 1
    assert error.definition_id == DEFINITION_ID
    assert error.path == destination(definition_run)
    assert error.phase == phase
    assert error.operation == operation
    assert error.publication_uncertain is uncertain
    assert "same definition ID" in str(error)
    assert "Preserve evidence" in str(error)
    assert JOB_UUID not in str(error)
