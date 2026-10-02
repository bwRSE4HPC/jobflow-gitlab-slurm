"""Offline discovery and selection of consumer site bindings."""

import pytest
import yaml
from pydantic import ValidationError

from jobflow_gitlab_slurm.registry import SiteRegistry


def _site(site_id):
    return {
        "schema_version": 1,
        "site_id": site_id,
        "slurm": {
            "cluster_name": site_id,
            "account": "example-account",
            "partitions": ["short"],
            "default_partition": "short",
        },
        "storage": {"provider": "posix", "runs_root": "/shared/example/runs"},
        "worker": {"launch_mode": "apptainer", "apptainer_command": "apptainer"},
    }


def _write_site(directory, filename, site_id):
    path = directory / filename
    path.write_text(yaml.safe_dump(_site(site_id)), encoding="utf-8")
    return path


def test_discovers_and_selects_in_sorted_order(tmp_path):
    _write_site(tmp_path, "zeta.yaml", "zeta")
    _write_site(tmp_path, "alpha.yaml", "alpha")

    registry = SiteRegistry.from_directory(tmp_path)
    assert registry.site_ids == ("alpha", "zeta")
    selected = registry.select("alpha")
    assert selected.site_id == "alpha"
    selected.slurm.account = "changed"
    assert registry.select("alpha").slurm.account == "example-account"


def test_empty_directory_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="no YAML files"):
        SiteRegistry.from_directory(tmp_path)


@pytest.mark.parametrize("entry_name", ["README.md", "nested"])
def test_unexpected_entries_are_rejected(tmp_path, entry_name):
    _write_site(tmp_path, "alpha.yaml", "alpha")
    path = tmp_path / entry_name
    if entry_name == "nested":
        path.mkdir()
    else:
        path.write_text("not a site", encoding="utf-8")
    with pytest.raises(ValueError, match="regular .yaml file"):
        SiteRegistry.from_directory(tmp_path)


def test_symlinked_file_is_rejected(tmp_path):
    target = _write_site(tmp_path, "alpha.yaml", "alpha")
    (tmp_path / "other.yaml").symlink_to(target)
    with pytest.raises(ValueError, match="regular .yaml file"):
        SiteRegistry.from_directory(tmp_path)


def test_symlinked_directory_is_rejected(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    _write_site(actual, "alpha.yaml", "alpha")
    link = tmp_path / "sites-link"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        SiteRegistry.from_directory(link)


def test_duplicate_site_id_is_rejected(tmp_path):
    _write_site(tmp_path, "first.yaml", "same")
    _write_site(tmp_path, "second.yaml", "same")
    with pytest.raises(ValueError, match="duplicate site_id"):
        SiteRegistry.from_directory(tmp_path)


def test_filename_must_match_site_id(tmp_path):
    _write_site(tmp_path, "wrong.yaml", "alpha")
    with pytest.raises(ValueError, match="filename must match site_id"):
        SiteRegistry.from_directory(tmp_path)


def test_invalid_site_file_is_rejected(tmp_path):
    (tmp_path / "alpha.yaml").write_text("site_id: alpha\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        SiteRegistry.from_directory(tmp_path)


def test_unknown_site_is_rejected(tmp_path):
    _write_site(tmp_path, "alpha.yaml", "alpha")
    registry = SiteRegistry.from_directory(tmp_path)
    with pytest.raises(KeyError, match="unknown site_id"):
        registry.select("missing")
