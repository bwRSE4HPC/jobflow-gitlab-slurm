"""Strict version-one run request and site cross-checks."""

import re
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from jobflow_gitlab_slurm.config.site import Identifier, SiteConfig

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PositiveInt = Annotated[int, Field(gt=0)]
_BUDGET = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?")
_EMAIL = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._%+-]*[A-Za-z0-9])?"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z]{2,63}"
)


class _RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class WorkflowIdentity(_RequestModel):
    name: str = Field(min_length=1)
    serialized_flow_sha256: Digest
    consumer_code_sha256: Digest

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("workflow name must not have surrounding whitespace")
        return value


class RuntimeIdentity(_RequestModel):
    worker_runtime_sha256: Digest


class Resources(_RequestModel):
    partition: Identifier
    nodes: PositiveInt
    tasks_per_node: PositiveInt
    cpus_per_task: PositiveInt
    memory_mb_per_node: PositiveInt
    walltime_seconds: PositiveInt


class RunPolicy(_RequestModel):
    allocated_cpu_hour_budget: Decimal | None = None
    notification_email: str | None = None

    @field_validator("allocated_cpu_hour_budget", mode="before")
    @classmethod
    def check_budget(cls, value: object) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            budget = value
        elif isinstance(value, str) and _BUDGET.fullmatch(value):
            budget = Decimal(value)
        else:
            raise ValueError("budget must be a quoted decimal string")
        if not budget.is_finite() or budget <= 0:
            raise ValueError("budget must be finite and positive")
        return budget

    @field_serializer("allocated_cpu_hour_budget", when_used="json-unless-none")
    def serialize_budget(self, value: Decimal) -> str:
        return format(value, "f")

    @field_validator("notification_email")
    @classmethod
    def check_email(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) > 254 or not _EMAIL.fullmatch(value) or ".." in value
        ):
            raise ValueError("notification_email must be one email address")
        return value


class RunRequest(_RequestModel):
    schema_version: int
    site_id: Identifier
    workflow: WorkflowIdentity
    runtime: RuntimeIdentity
    resources: Resources
    policy: RunPolicy = Field(default_factory=RunPolicy)

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported run request schema_version")
        return value


def validate_for_site(request: RunRequest, site: SiteConfig) -> RunRequest:
    """Reject a request targeting another site or disallowed partition."""
    if request.site_id != site.site_id:
        raise ValueError("run site_id does not match selected site")
    if request.resources.partition not in site.slurm.partitions:
        raise ValueError("run partition is not allowed by selected site")
    return request
