"""Explicit scenario preparation and observations; no collected tests."""

SITE = {
    "schema_version": 1,
    "site_id": "example-cluster",
    "slurm": {
        "cluster_name": "example-slurm-cluster",
        "account": "example-allocation",
        "partitions": ["short", "batch"],
        "default_partition": "short",
    },
    "storage": {"provider": "posix", "runs_root": "/shared/example/runs"},
    "worker": {"launch_mode": "apptainer", "apptainer_command": "apptainer"},
}
