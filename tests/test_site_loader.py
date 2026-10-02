"""File and CLI checks for offline site validation."""

import runpy
import sys

import pytest
import yaml

from jobflow_gitlab_slurm import cli
from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.loader import load_site_file

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


def test_load_and_cli_success(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    path.write_text(yaml.safe_dump(SITE), encoding="utf-8")

    assert load_site_file(path).site_id == "example-cluster"
    assert main(["validate-site", str(path)]) == 0
    assert "Site validation passed" in capsys.readouterr().out


def test_duplicate_key_rejected(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    content = yaml.safe_dump(SITE)
    path.write_text(content + "site_id: duplicate\n", encoding="utf-8")

    assert main(["validate-site", str(path)]) == 2
    assert "duplicate YAML key" in capsys.readouterr().err


def test_unknown_key_rejected(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    content = yaml.safe_dump(SITE)
    path.write_text(content + "unknown: value\n", encoding="utf-8")

    assert main(["validate-site", str(path)]) == 2
    assert "Site validation failed" in capsys.readouterr().err


def test_unsafe_yaml_tag_rejected(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    path.write_text("!!python/object/apply:os.system ['true']\n", encoding="utf-8")

    assert main(["validate-site", str(path)]) == 2
    assert "could not determine a constructor" in capsys.readouterr().err


def test_missing_file_reports_error(tmp_path, capsys):
    assert main(["validate-site", str(tmp_path / "missing.yaml")]) == 2
    assert "Site validation failed" in capsys.readouterr().err


@pytest.mark.parametrize("key", ["1", "true"])
def test_non_string_yaml_mapping_key_rejected(tmp_path, capsys, key):
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(SITE) + f"{key}: unexpected\n",
        encoding="utf-8",
    )

    with pytest.raises(
        yaml.constructor.ConstructorError,
        match="mapping keys must be strings",
    ):
        load_site_file(path)

    assert main(["validate-site", str(path)]) == 2
    assert "mapping keys must be strings" in capsys.readouterr().err


@pytest.mark.parametrize("available", [True, False])
def test_cli_module_entrypoint(tmp_path, monkeypatch, capsys, available):
    path = tmp_path / "site.yaml"
    if available:
        path.write_text(yaml.safe_dump(SITE), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        ["jobflow-gitlab-slurm", "validate-site", str(path)],
    )

    with pytest.raises(SystemExit) as error:
        runpy.run_path(cli.__file__, run_name="__main__")

    assert error.value.code == (0 if available else 2)

    captured = capsys.readouterr()
    if available:
        assert "Site validation passed: example-cluster" in captured.out
        assert captured.err == ""
    else:
        assert "Site validation failed:" in captured.err
        assert captured.out == ""
