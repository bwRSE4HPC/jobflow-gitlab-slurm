"""Explicit scenario preparation and observations; no collected tests."""

from tests.support.processes import python_child as process

__all__ = ["process"]

import hashlib
import json
from types import SimpleNamespace

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.runs.storage import (
    create_run,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

OTHER_ID = "00000000-0000-4000-8000-000000000002"

FLOW_BYTES = b'{ "@module": "never_import_me", "job_uuid": "unchanged" }\r\n'


def run_inputs(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    for path, payload in zip(
        sources, (FLOW_BYTES, b"opaque consumer bytes", b"\x00opaque runtime bytes")
    ):
        path.write_bytes(payload)
    digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in sources]
    site = SiteConfig.model_validate(
        {
            "schema_version": 1,
            "site_id": "example-cluster",
            "slurm": {
                "cluster_name": "example-slurm",
                "account": "example-account",
                "partitions": ["short", "batch"],
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
                "name": "toy-flow",
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


def create(run_inputs, **kwargs):
    return create_run(
        run_inputs.site,
        run_inputs.request,
        *run_inputs.sources,
        run_id=RUN_ID,
        **kwargs,
    )


def rewrite(path, mutate):
    data = json.loads(path.read_text(encoding="utf-8"))
    mutate(data)
    path.write_text(json.dumps(data), encoding="utf-8")
