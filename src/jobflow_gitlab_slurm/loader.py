"""Load a site binding from YAML without constructing Python objects."""

from pathlib import Path

import yaml

from jobflow_gitlab_slurm.request import RunRequest
from jobflow_gitlab_slurm.site import SiteConfig


class _StrictSafeLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        self.flatten_mapping(node)
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise yaml.constructor.ConstructorError(
                    None, None, "mapping keys must be strings", key_node.start_mark
                )
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate YAML key: {key}", key_node.start_mark
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def load_site_file(path: str | Path) -> SiteConfig:
    with Path(path).open(encoding="utf-8") as stream:
        document = yaml.load(stream, Loader=_StrictSafeLoader)
    return SiteConfig.model_validate(document)


def load_request_file(path: str | Path) -> RunRequest:
    with Path(path).open(encoding="utf-8") as stream:
        document = yaml.load(stream, Loader=_StrictSafeLoader)
    return RunRequest.model_validate(document)
