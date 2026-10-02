"""Strict, offline models for the version-one HPC site binding."""

import re
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Identifier = Annotated[
    str, Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


def _absolute_posix_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not path.is_absolute()
        or value == "/"
        or path.as_posix() != value
        or ".." in path.parts
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("must be a canonical absolute POSIX path below /")
    return value


class SlurmSite(_StrictModel):
    cluster_name: Identifier
    account: Identifier
    partitions: list[Identifier] = Field(min_length=1)
    default_partition: Identifier

    @model_validator(mode="after")
    def check_partitions(self) -> "SlurmSite":
        if len(self.partitions) != len(set(self.partitions)):
            raise ValueError("partitions must not contain duplicates")
        if self.default_partition not in self.partitions:
            raise ValueError("default_partition must be in partitions")
        return self


class StorageSite(_StrictModel):
    provider: Literal["posix"]
    runs_root: str

    @field_validator("runs_root")
    @classmethod
    def check_runs_root(cls, value: str) -> str:
        return _absolute_posix_path(value)


class WorkerSite(_StrictModel):
    launch_mode: Literal["apptainer"]
    apptainer_command: str

    @field_validator("apptainer_command")
    @classmethod
    def check_command(cls, value: str) -> str:
        if value.startswith("/"):
            if not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
                raise ValueError("must be an executable name or absolute path")
            return _absolute_posix_path(value)
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", value):
            raise ValueError("must be an executable name or absolute path")
        return value


class SiteConfig(_StrictModel):
    schema_version: int
    site_id: Identifier
    slurm: SlurmSite
    storage: StorageSite
    worker: WorkerSite

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported site schema_version")
        return value
