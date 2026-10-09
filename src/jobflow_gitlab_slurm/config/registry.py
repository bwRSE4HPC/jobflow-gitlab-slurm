"""Deterministic, offline discovery of validated consumer site bindings."""

from pathlib import Path

from jobflow_gitlab_slurm.config.loader import load_site_file
from jobflow_gitlab_slurm.config.site import SiteConfig


class SiteRegistry:
    """Index one trusted, pinned directory of site YAML files by site_id."""

    def __init__(self, sites: dict[str, SiteConfig]) -> None:
        self._sites = dict(sites)

    @classmethod
    def from_directory(cls, directory: str | Path) -> "SiteRegistry":
        root = Path(directory)
        if root.is_symlink() or not root.is_dir():
            raise ValueError(f"invalid or symlinked sites directory: {root}")

        entries = sorted(root.iterdir(), key=lambda entry: entry.name)
        if not entries:
            raise ValueError(f"sites directory contains no YAML files: {root}")

        sites: dict[str, SiteConfig] = {}
        filenames: dict[str, str] = {}
        for entry in entries:
            if entry.is_symlink() or not entry.is_file() or entry.suffix != ".yaml":
                raise ValueError(f"site entry must be a regular .yaml file: {entry}")
            site = load_site_file(entry)
            if site.site_id in sites:
                raise ValueError(f"duplicate site_id: {site.site_id}")
            sites[site.site_id] = site
            filenames[site.site_id] = entry.stem

        for site_id, stem in filenames.items():
            if stem != site_id:
                raise ValueError(
                    f"site filename must match site_id: {stem}.yaml declares {site_id}"
                )
        return cls(sites)

    @property
    def site_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._sites))

    def select(self, site_id: str) -> SiteConfig:
        try:
            return self._sites[site_id].model_copy(deep=True)
        except KeyError:
            raise KeyError(f"unknown site_id: {site_id}") from None
