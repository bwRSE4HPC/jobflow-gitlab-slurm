"""Direct file-loader contracts, independent of CLI reporting."""

import pytest
import yaml
from pydantic import ValidationError

from jobflow_gitlab_slurm.config.loader import load_request_file, load_site_file
from tests.config import _request_support as requests
from tests.support import configuration


@pytest.mark.parametrize("kind", ["site", "request"])
def test_loader_returns_validated_models(tmp_path, kind):
    data = configuration.SITE if kind == "site" else requests.request_data()
    loader = load_site_file if kind == "site" else load_request_file
    path = tmp_path / f"{kind}.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    assert loader(path).site_id == "example-cluster"


@pytest.mark.parametrize("kind", ["site", "request"])
@pytest.mark.parametrize("damage", ["duplicate", "unknown", "unsafe-tag", "non-string"])
def test_loader_rejects_invalid_yaml_or_schema(tmp_path, kind, damage):
    data = configuration.SITE if kind == "site" else requests.request_data()
    loader = load_site_file if kind == "site" else load_request_file
    text = yaml.safe_dump(data)
    if damage == "duplicate":
        text += "site_id: duplicate\n"
    elif damage == "unknown":
        text += "unknown: value\n"
    elif damage == "unsafe-tag":
        text = "!!python/object/apply:os.system ['true']\n"
    else:
        text += "true: unexpected\n"
    path = tmp_path / f"{kind}.yaml"
    path.write_text(text, encoding="utf-8")
    expected = (
        ValidationError if damage == "unknown" else yaml.constructor.ConstructorError
    )
    with pytest.raises(expected):
        loader(path)


@pytest.mark.parametrize("loader", [load_site_file, load_request_file])
def test_missing_loader_input_preserves_filesystem(tmp_path, loader):
    with pytest.raises(FileNotFoundError):
        loader(tmp_path / "missing.yaml")
    assert list(tmp_path.iterdir()) == []
