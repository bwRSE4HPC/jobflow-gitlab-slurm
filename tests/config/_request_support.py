"""Explicit scenario preparation and observations; no collected tests."""

import yaml


def request_data():
    return {
        "schema_version": 1,
        "site_id": "example-cluster",
        "workflow": {
            "name": "toy-flow",
            "serialized_flow_sha256": "a" * 64,
            "consumer_code_sha256": "b" * 64,
        },
        "runtime": {"worker_runtime_sha256": "c" * 64},
        "resources": {
            "partition": "short",
            "nodes": 1,
            "tasks_per_node": 2,
            "cpus_per_task": 1,
            "memory_mb_per_node": 4096,
            "walltime_seconds": 1800,
        },
    }


def site_data():
    return {
        "schema_version": 1,
        "site_id": "example-cluster",
        "slurm": {
            "cluster_name": "example-slurm",
            "account": "example-account",
            "partitions": ["short", "batch"],
            "default_partition": "short",
        },
        "storage": {"provider": "posix", "runs_root": "/shared/example/runs"},
        "worker": {"launch_mode": "apptainer", "apptainer_command": "apptainer"},
    }


def _write_yaml_pair(tmp_path, request=None, site=None):
    request_path = tmp_path / "request.yaml"
    site_path = tmp_path / "site.yaml"
    request_path.write_text(
        yaml.safe_dump(request if request is not None else request_data()),
        encoding="utf-8",
    )
    site_path.write_text(
        yaml.safe_dump(site if site is not None else site_data()),
        encoding="utf-8",
    )
    return request_path, site_path
