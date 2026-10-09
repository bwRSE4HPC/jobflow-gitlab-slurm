"""Explicit scenario preparation and observations; no collected tests."""

import hashlib
from types import SimpleNamespace

import yaml

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.runs.storage import (
    create_run,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

OTHER_ID = "00000000-0000-4000-8000-000000000002"

FLOW_BYTES = b'{ "@module": "do_not_import_this_consumer" }\r\n'


def discovery_inputs(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sites = tmp_path / "sites"
    sites.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    for path, payload in zip(
        sources, (FLOW_BYTES, b"consumer bytes", b"\x00runtime bytes")
    ):
        path.write_bytes(payload)
    digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in sources]
    site_data = {
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
    request_data = {
        "schema_version": 1,
        "site_id": "example-cluster",
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
        "policy": {"allocated_cpu_hour_budget": "0.000000000000000001"},
    }
    site_file = sites / "example-cluster.yaml"
    site_file.write_text(yaml.safe_dump(site_data), encoding="utf-8")
    request_file = tmp_path / "request.yaml"
    request_file.write_text(yaml.safe_dump(request_data), encoding="utf-8")
    return SimpleNamespace(
        root=root,
        sites=sites,
        sources=sources,
        site=SiteConfig.model_validate(site_data),
        request=RunRequest.model_validate(request_data),
        site_file=site_file,
        request_file=request_file,
    )


def create(discovery_inputs, run_id=RUN_ID):
    return create_run(
        discovery_inputs.site,
        discovery_inputs.request,
        *discovery_inputs.sources,
        run_id=run_id,
    )


def create_arguments(discovery_inputs):
    return [
        "create-run",
        str(discovery_inputs.request_file),
        "--sites-directory",
        str(discovery_inputs.sites),
        "--run-id",
        RUN_ID,
        "--flow",
        str(discovery_inputs.sources[0]),
        "--consumer-code",
        str(discovery_inputs.sources[1]),
        "--worker-runtime",
        str(discovery_inputs.sources[2]),
    ]


def inspect_arguments(discovery_inputs):
    return ["inspect-run", RUN_ID, "--runs-root", str(discovery_inputs.root)]


def list_arguments(discovery_inputs):
    return ["list-runs", "--runs-root", str(discovery_inputs.root)]
