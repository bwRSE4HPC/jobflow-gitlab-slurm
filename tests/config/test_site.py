"""Offline validation tests for the public site binding."""

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.config.site import SiteConfig


def site_data():
    return {
        "schema_version": 1,
        "site_id": "example-cluster",
        "slurm": {
            "cluster_name": "example-slurm-cluster",
            "account": "example-allocation",
            "partitions": ["short", "batch"],
            "default_partition": "short",
        },
        "storage": {
            "provider": "posix",
            "runs_root": "/shared/example/jobflow-runs",
        },
        "worker": {
            "launch_mode": "apptainer",
            "apptainer_command": "apptainer",
        },
    }


def test_valid_site():
    site = SiteConfig.model_validate(site_data())
    assert site.slurm.default_partition == "short"
    assert site.storage.runs_root == "/shared/example/jobflow-runs"


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_unsupported_or_wrong_type_version(version):
    data = site_data()
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        SiteConfig.model_validate(data)


def test_unknown_top_level_and_nested_keys():
    data = site_data()
    data["surprise"] = "x"
    with pytest.raises(ValidationError, match="Extra inputs"):
        SiteConfig.model_validate(data)

    data = site_data()
    data["slurm"]["surprise"] = "x"
    with pytest.raises(ValidationError, match="Extra inputs"):
        SiteConfig.model_validate(data)


@pytest.mark.parametrize(
    ("partitions", "default"),
    [([], "short"), (["short", "short"], "short"), (["short"], "batch")],
)
def test_invalid_partitions(partitions, default):
    data = site_data()
    data["slurm"]["partitions"] = partitions
    data["slurm"]["default_partition"] = default
    with pytest.raises(ValidationError):
        SiteConfig.model_validate(data)


@pytest.mark.parametrize(
    "root",
    ["relative/path", "/", "/shared/../runs", "/shared//runs", "/shared/runs/"],
)
def test_unsafe_runs_root(root):
    data = site_data()
    data["storage"]["runs_root"] = root
    with pytest.raises(ValidationError):
        SiteConfig.model_validate(data)


@pytest.mark.parametrize(
    "command",
    ["apptainer --cleanenv", "../apptainer", "/bin/../apptainer", "sh;true"],
)
def test_unsafe_worker_command(command):
    data = site_data()
    data["worker"]["apptainer_command"] = command
    with pytest.raises(ValidationError):
        SiteConfig.model_validate(data)


def test_unsupported_provider_and_launch_mode():
    data = site_data()
    data["storage"]["provider"] = "s3"
    data["worker"]["launch_mode"] = "native"
    with pytest.raises(ValidationError) as error:
        SiteConfig.model_validate(data)
    assert len(error.value.errors()) == 2


@pytest.mark.parametrize(
    "command",
    [
        "/usr/bin/apptainer --cleanenv",
        "/usr/bin/apptainer;true",
        "/usr/bin/apptainer\n",
    ],
)
def test_absolute_worker_command_rejects_arguments_and_metacharacters(command):
    data = site_data()
    data["worker"]["apptainer_command"] = command

    with pytest.raises(
        ValidationError,
        match="must be an executable name or absolute path",
    ):
        SiteConfig.model_validate(data)
