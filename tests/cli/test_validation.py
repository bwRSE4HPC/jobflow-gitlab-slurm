"""cli / test_validation contracts."""

from decimal import Decimal

import pytest
import yaml

from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.config.loader import load_request_file, load_site_file
from tests.config import _request_support as request_setup
from tests.support import configuration as configuration_setup


def test_load_and_cli_success(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    path.write_text(yaml.safe_dump(configuration_setup.SITE), encoding="utf-8")

    assert load_site_file(path).site_id == "example-cluster"
    assert main(["validate-site", str(path)]) == 0
    assert "Site validation passed" in capsys.readouterr().out


def test_duplicate_key_rejected(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    content = yaml.safe_dump(configuration_setup.SITE)
    path.write_text(content + "site_id: duplicate\n", encoding="utf-8")

    assert main(["validate-site", str(path)]) == 2
    assert "duplicate YAML key" in capsys.readouterr().err


def test_unknown_key_rejected(tmp_path, capsys):
    path = tmp_path / "site.yaml"
    content = yaml.safe_dump(configuration_setup.SITE)
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
        yaml.safe_dump(configuration_setup.SITE) + f"{key}: unexpected\n",
        encoding="utf-8",
    )

    with pytest.raises(
        yaml.constructor.ConstructorError,
        match="mapping keys must be strings",
    ):
        load_site_file(path)

    assert main(["validate-site", str(path)]) == 2
    assert "mapping keys must be strings" in capsys.readouterr().err


def test_request_file_and_cli(tmp_path, capsys):
    data = request_setup.request_data()
    data["policy"] = {"allocated_cpu_hour_budget": "12.5"}
    request_path, site_path = request_setup._write_yaml_pair(tmp_path, request=data)
    assert load_request_file(request_path).policy.allocated_cpu_hour_budget == Decimal(
        "12.5"
    )
    assert main(["validate-request", str(request_path), str(site_path)]) == 0
    assert "Run request validation passed" in capsys.readouterr().out


def test_request_file_duplicate_key(tmp_path, capsys):
    request_path, site_path = request_setup._write_yaml_pair(tmp_path)
    request_path.write_text(
        request_path.read_text(encoding="utf-8") + "site_id: duplicate\n",
        encoding="utf-8",
    )
    assert main(["validate-request", str(request_path), str(site_path)]) == 2
    assert "duplicate YAML key" in capsys.readouterr().err


def test_request_file_unquoted_budget(tmp_path, capsys):
    data = request_setup.request_data()
    data["policy"] = {"allocated_cpu_hour_budget": "12.5"}
    request_path, site_path = request_setup._write_yaml_pair(tmp_path, request=data)
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
    site = request_setup.site_data()
    site["site_id"] = "other-cluster"
    request_path, site_path = request_setup._write_yaml_pair(tmp_path, site=site)
    assert main(["validate-request", str(request_path), str(site_path)]) == 2
    assert "site_id" in capsys.readouterr().err
