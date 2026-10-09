#!/usr/bin/env bash
set -euo pipefail

JGS_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
JGS_CHECK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/jgs-wheel.XXXXXX")"
JGS_WHEEL_ENV="$JGS_CHECK_ROOT/venv"

printf 'Wheel-check directory: %s\n' "$JGS_CHECK_ROOT"

shopt -s nullglob
JGS_WHEELS=("$JGS_REPO_ROOT"/dist/*.whl)
if [ "${#JGS_WHEELS[@]}" -ne 1 ]; then
  printf 'Expected one wheel in %s/dist; run uv build --no-sources first.\n' \
    "$JGS_REPO_ROOT" >&2
  exit 1
fi

uv export \
  --project "$JGS_REPO_ROOT" \
  --locked \
  --no-dev \
  --no-emit-project \
  --format requirements-txt \
  --output-file "$JGS_CHECK_ROOT/requirements.txt"

uv venv --python 3.13 "$JGS_WHEEL_ENV"

uv pip install \
  --python "$JGS_WHEEL_ENV/bin/python" \
  --require-hashes \
  --requirements "$JGS_CHECK_ROOT/requirements.txt"

uv pip install \
  --python "$JGS_WHEEL_ENV/bin/python" \
  --no-deps \
  "${JGS_WHEELS[0]}"

uv pip check --python "$JGS_WHEEL_ENV/bin/python"

cd -- "$JGS_CHECK_ROOT"
unset PYTHONPATH PYTHONHOME

"$JGS_WHEEL_ENV/bin/python" -I - "$JGS_REPO_ROOT/pyproject.toml" <<'PY'
import subprocess
import sys
import sysconfig
import tomllib
from importlib import import_module, metadata
from pathlib import Path

project = tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["project"]
installed_version = metadata.version(project["name"])
if installed_version != project["version"]:
    raise RuntimeError(
        f"Wheel version {installed_version} differs from {project['version']}"
    )

environment = Path(sys.prefix).resolve()
if environment == Path(sys.base_prefix).resolve():
    raise RuntimeError("Wheel verification must run inside its isolated environment")

for name in (
    "jobflow_gitlab_slurm",
    "jobflow_gitlab_slurm.config",
    "jobflow_gitlab_slurm.persistence",
    "jobflow_gitlab_slurm.persistence._filesystem",
    "jobflow_gitlab_slurm.persistence.runs",
    "jobflow_gitlab_slurm.persistence.queries",
    "jobflow_gitlab_slurm.persistence.attempts",
    "jobflow_gitlab_slurm.persistence.journal",
    "jobflow_gitlab_slurm.persistence.bundles",
    "jobflow_gitlab_slurm.persistence.publication",
    "jobflow_gitlab_slurm.persistence.recovery",
    "jobflow_gitlab_slurm.persistence.artifacts",
    "jobflow_gitlab_slurm.persistence.attempts.records",
    "jobflow_gitlab_slurm.persistence.attempts.storage",
    "jobflow_gitlab_slurm.persistence.bundles.inspection",
    "jobflow_gitlab_slurm.persistence.bundles.records",
    "jobflow_gitlab_slurm.persistence.bundles.storage",
    "jobflow_gitlab_slurm.cli",
    "jobflow_gitlab_slurm.persistence.attempts.definitions",
    "jobflow_gitlab_slurm.persistence.queries.discovery",
    "jobflow_gitlab_slurm.persistence.journal.records",
    "jobflow_gitlab_slurm.persistence.queries.inspection",
    "jobflow_gitlab_slurm.persistence.attempts.ownership",
    "jobflow_gitlab_slurm.persistence.journal.storage",
    "jobflow_gitlab_slurm.config.loader",
    "jobflow_gitlab_slurm.persistence.publication.registration",
    "jobflow_gitlab_slurm.persistence.publication.records",
    "jobflow_gitlab_slurm.persistence.publication.storage",
    "jobflow_gitlab_slurm.persistence.recovery.registration",
    "jobflow_gitlab_slurm.persistence.recovery.receipts",
    "jobflow_gitlab_slurm.persistence.recovery.records",
    "jobflow_gitlab_slurm.persistence.recovery.requests",
    "jobflow_gitlab_slurm.persistence.runs.records",
    "jobflow_gitlab_slurm.persistence.recovery.operations",
    "jobflow_gitlab_slurm.config.registry",
    "jobflow_gitlab_slurm.config.request",
    "jobflow_gitlab_slurm.config.site",
    "jobflow_gitlab_slurm.persistence.runs.storage",
):
    module = import_module(name)
    location = Path(module.__file__).resolve()
    if not location.is_relative_to(environment):
        raise RuntimeError(f"{name} was imported outside the wheel environment: {location}")
    print(f"{name}: {location}")

for artifact in metadata.files(project["name"]) or ():
    if "tests" in artifact.parts:
        raise RuntimeError(f"Test/support code was packaged in the runtime wheel: {artifact}")

import_module("jobflow")
print(f"jobflow: {metadata.version('jobflow')}")

command = Path(sysconfig.get_path("scripts")) / "jobflow-gitlab-slurm"
subprocess.run([str(command), "--help"], check=True)
print(f"Wheel verification passed: {project['name']} {installed_version}")
PY
