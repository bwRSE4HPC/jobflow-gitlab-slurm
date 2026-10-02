"""Offline run request validation and site matching."""

from decimal import Decimal

import pytest
import yaml
from pydantic import ValidationError

from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.loader import load_request_file
from jobflow_gitlab_slurm.request import RunRequest, validate_for_site
from jobflow_gitlab_slurm.site import SiteConfig


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


def test_valid_request_and_unbounded_default():
    request = RunRequest.model_validate(request_data())
    site = SiteConfig.model_validate(site_data())
    assert validate_for_site(request, site) is request
    assert request.policy.allocated_cpu_hour_budget is None


def test_exact_budget_and_json_round_trip():
    data = request_data()
    data["policy"] = {
        "allocated_cpu_hour_budget": "12.5",
        "notification_email": "user@kit.edu",
    }
    request = RunRequest.model_validate(data)
    assert request.policy.allocated_cpu_hour_budget == Decimal("12.5")
    assert (
        request.model_dump(mode="json")["policy"]["allocated_cpu_hour_budget"] == "12.5"
    )
    assert RunRequest.model_validate(request.model_dump(mode="json")) == request


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_bad_schema_version(version):
    data = request_data()
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


@pytest.mark.parametrize("digest", ["A" * 64, "a" * 63, "not-a-digest"])
def test_bad_digest(digest):
    data = request_data()
    data["workflow"]["serialized_flow_sha256"] = digest
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


@pytest.mark.parametrize(
    "field",
    [
        "nodes",
        "tasks_per_node",
        "cpus_per_task",
        "memory_mb_per_node",
        "walltime_seconds",
    ],
)
def test_nonpositive_resource(field):
    data = request_data()
    data["resources"][field] = 0
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


@pytest.mark.parametrize("budget", [0, 12.5, "0", "-1", "NaN", "Infinity", "1e3"])
def test_bad_budget(budget):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": budget}
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


@pytest.mark.parametrize("email", ["a@b", "a@kit.edu\n", "a;b@kit.edu", "a..b@kit.edu"])
def test_bad_email(email):
    data = request_data()
    data["policy"] = {"notification_email": email}
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


def test_unknown_and_missing_fields():
    data = request_data()
    data["resources"]["surprise"] = 1
    with pytest.raises(ValidationError, match="Extra inputs"):
        RunRequest.model_validate(data)

    data = request_data()
    del data["runtime"]
    with pytest.raises(ValidationError):
        RunRequest.model_validate(data)


def test_site_and_partition_mismatch():
    request = RunRequest.model_validate(request_data())
    site = SiteConfig.model_validate(site_data())
    wrong_site = site.model_copy(update={"site_id": "different"})
    with pytest.raises(ValueError, match="site_id"):
        validate_for_site(request, wrong_site)

    wrong_partition = request.resources.model_copy(update={"partition": "gpu"})
    wrong_request = request.model_copy(update={"resources": wrong_partition})
    with pytest.raises(ValueError, match="partition"):
        validate_for_site(wrong_request, site)


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


def test_request_file_and_cli(tmp_path, capsys):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": "12.5"}
    request_path, site_path = _write_yaml_pair(tmp_path, request=data)
    assert load_request_file(request_path).policy.allocated_cpu_hour_budget == Decimal(
        "12.5"
    )
    assert main(["validate-request", str(request_path), str(site_path)]) == 0
    assert "Run request validation passed" in capsys.readouterr().out


def test_request_file_duplicate_key(tmp_path, capsys):
    request_path, site_path = _write_yaml_pair(tmp_path)
    request_path.write_text(
        request_path.read_text(encoding="utf-8") + "site_id: duplicate\n",
        encoding="utf-8",
    )
    assert main(["validate-request", str(request_path), str(site_path)]) == 2
    assert "duplicate YAML key" in capsys.readouterr().err


def test_request_file_unquoted_budget(tmp_path, capsys):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": "12.5"}
    request_path, site_path = _write_yaml_pair(tmp_path, request=data)
    content = request_path.read_text(encoding="utf-8")
    assert "allocated_cpu_hour_budget: '12.5'" in content
    request_path.write_text(
        content.replace(
            "allocated_cpu_hour_budget: '12.5'", "allocated_cpu_hour_budget: 12.5"
        ),
        encoding="utf-8",
    )
    assert main(["validate-request", str(request_path), str(site_path)]) == 2
    assert "quoted decimal string" in capsys.readouterr().err


def test_request_cli_site_mismatch(tmp_path, capsys):
    site = site_data()
    site["site_id"] = "other-cluster"
    request_path, site_path = _write_yaml_pair(tmp_path, site=site)
    assert main(["validate-request", str(request_path), str(site_path)]) == 2
    assert "site_id" in capsys.readouterr().err


@pytest.mark.parametrize("name", [" toy-flow", "toy-flow ", "\ttoy-flow", "toy-flow\n"])
def test_workflow_name_rejects_surrounding_whitespace(name):
    data = request_data()
    data["workflow"]["name"] = name

    with pytest.raises(ValidationError, match="surrounding whitespace"):
        RunRequest.model_validate(data)


def test_explicit_unbounded_budget_and_no_notification():
    data = request_data()
    data["policy"] = {
        "allocated_cpu_hour_budget": None,
        "notification_email": None,
    }

    request = RunRequest.model_validate(data)

    assert request.policy.allocated_cpu_hour_budget is None
    assert request.policy.notification_email is None
    assert RunRequest.model_validate(request.model_dump(mode="json")) == request


@pytest.mark.parametrize(
    ("budget", "expected"),
    [
        (Decimal("12.50"), "12.50"),
        (Decimal("0.000000000000000001"), "0.000000000000000001"),
        (Decimal("1E+6"), "1000000"),
    ],
)
def test_direct_decimal_budget_preserves_precision(budget, expected):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": budget}

    request = RunRequest.model_validate(data)

    assert request.policy.allocated_cpu_hour_budget == budget
    assert request.model_dump()["policy"]["allocated_cpu_hour_budget"] == budget

    serialized = request.model_dump(mode="json")
    assert serialized["policy"]["allocated_cpu_hour_budget"] == expected
    assert RunRequest.model_validate(serialized) == request
    assert RunRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize(
    "budget",
    [
        Decimal(0),
        Decimal(-1),
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
    ],
)
def test_direct_decimal_budget_must_be_finite_and_positive(budget):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": budget}

    with pytest.raises(ValidationError, match="finite and positive"):
        RunRequest.model_validate(data)
