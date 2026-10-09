"""Explicit scenario preparation and observations; no collected tests."""

import hashlib
import json
import os
import stat
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.attempts.definitions import publish_job_definition
from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    provision_invocation_lock,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    register_invocation,
    reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    inspect_invocation_bundle,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

OTHER_ID = "dddddddd-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-bundle-job-é"

FLOW_BYTES = b'{ "@module": "do_not_import_bundle_flow" }\r\n'

JOB_BYTES = b'{ "@module": "do_not_import_bundle_job" }\r\n'

RECEIPT_BYTES = b"opaque receipt; not a scheduler schema"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(data):
    return json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def reference(path, data):
    return {"path": path, "sha256": digest(data), "size_bytes": len(data)}


def timestamp(run, offset):
    instant = datetime.fromisoformat(run.manifest.created_at)
    return (
        (instant + timedelta(seconds=offset))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def bundle_invocation(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    source_bytes = (FLOW_BYTES, b"consumer", b"\0runtime")
    for path, data in zip(sources, source_bytes):
        path.write_bytes(data)

    site = SiteConfig.model_validate(
        {
            "schema_version": 1,
            "site_id": "example-cluster",
            "slurm": {
                "cluster_name": "example-slurm",
                "account": "example-account",
                "partitions": ["short"],
                "default_partition": "short",
            },
            "storage": {"provider": "posix", "runs_root": str(root)},
            "worker": {
                "launch_mode": "apptainer",
                "apptainer_command": "apptainer",
            },
        }
    )
    request = RunRequest.model_validate(
        {
            "schema_version": 1,
            "site_id": site.site_id,
            "workflow": {
                "name": "bundle-inspection-example",
                "serialized_flow_sha256": digest(source_bytes[0]),
                "consumer_code_sha256": digest(source_bytes[1]),
            },
            "runtime": {"worker_runtime_sha256": digest(source_bytes[2])},
            "resources": {
                "partition": "short",
                "nodes": 1,
                "tasks_per_node": 2,
                "cpus_per_task": 1,
                "memory_mb_per_node": 4096,
                "walltime_seconds": 1800,
            },
        }
    )
    run = create_run(site, request, *sources, run_id=RUN_ID)
    common = {
        "schema_version": 1,
        "run_id": RUN_ID,
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": job_key(JOB_UUID),
    }

    source = tmp_path / "job-source.json"
    source.write_bytes(JOB_BYTES)
    definition_record = JobDefinitionRecord.model_validate(
        {
            **common,
            "kind": "job-definition",
            "created_at": run.manifest.created_at,
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference(
                f"jobs/{common['job_key']}/index-1/"
                f"definitions/{DEFINITION_ID}/job.json",
                JOB_BYTES,
            ),
            "origin": "original_flow",
            "source": run.flow.payload.model_dump(),
        }
    )
    definition = publish_job_definition(root, definition_record, source)
    attempt = reserve_attempt(
        root,
        AttemptRecord.model_validate(
            {
                **common,
                "kind": "execution-attempt",
                "created_at": timestamp(run, 1),
                "attempt_id": ATTEMPT_ID,
                "definition_id": DEFINITION_ID,
                "definition_sha256": definition.record.payload.sha256,
                "consumer_code_sha256": request.workflow.consumer_code_sha256,
                "worker_runtime_sha256": request.runtime.worker_runtime_sha256,
            }
        ),
    )
    invocation = register_invocation(
        root,
        InvocationRecord.model_validate(
            {
                **common,
                "kind": "worker-invocation",
                "created_at": timestamp(run, 2),
                "attempt_id": ATTEMPT_ID,
                "invocation_id": INVOCATION_ID,
            }
        ),
    )
    parameters = (root, RUN_ID, JOB_UUID, 1, ATTEMPT_ID, INVOCATION_ID)
    provision_invocation_lock(*parameters)
    receipt = attempt.path / "slurm-receipt.json"
    receipt.write_bytes(RECEIPT_BYTES)

    return SimpleNamespace(
        root=root,
        run=run,
        definition=definition,
        attempt=attempt,
        invocation=invocation,
        receipt=receipt,
        parameters=parameters,
    )


def make_bundle(
    bundle_invocation,
    name="published",
    *,
    manifest=True,
    marker=True,
    optional=True,
):
    """Construct inert fixture evidence, not an installed publication API."""
    unit = bundle_invocation.invocation.path / name
    unit.mkdir()
    payloads = {
        "job-document.json": b'{"output":null,"@module":"do_not_import_payload"}',
        "response.json": b"inert Response bytes; deliberately not JSON",
    }
    if optional:
        payloads.update(
            {
                "files/empty.bin": b"",
                "files/nested/result-é.txt": b"opaque application bytes",
                "data/blob.bin": b"\0opaque additional-store bytes",
            }
        )
    for payload_name, data in payloads.items():
        path = unit / payload_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    invocation = bundle_invocation.invocation.record
    attempt = bundle_invocation.attempt.record
    manifest_record = BundleManifest.model_validate(
        {
            "schema_version": 1,
            "kind": "bundle-manifest",
            "run_id": RUN_ID,
            "created_at": timestamp(bundle_invocation.run, 3),
            "job_uuid": JOB_UUID,
            "job_index": 1,
            "job_key": invocation.job_key,
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": attempt.definition_sha256,
            "consumer_code_sha256": attempt.consumer_code_sha256,
            "worker_runtime_sha256": attempt.worker_runtime_sha256,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                bundle_invocation.receipt.relative_to(
                    bundle_invocation.run.path
                ).as_posix(),
                RECEIPT_BYTES,
            ),
            "document": reference("job-document.json", payloads["job-document.json"]),
            "response": reference("response.json", payloads["response.json"]),
            "files": tuple(
                reference(name, data)
                for name, data in sorted(payloads.items())
                if name.startswith("files/")
            ),
            "additional_data": tuple(
                {
                    "store_name": "results",
                    "blob_uuid": "opaque-blob",
                    "artifact": reference(name, data),
                }
                for name, data in sorted(payloads.items())
                if name.startswith("data/")
            ),
        }
    )
    manifest_bytes = encode_bundle_manifest(manifest_record)
    commit_record = BundleCommit.model_validate(
        {
            "schema_version": 1,
            "kind": "bundle-commit",
            "run_id": RUN_ID,
            "created_at": timestamp(bundle_invocation.run, 4),
            "job_uuid": JOB_UUID,
            "job_index": 1,
            "job_key": invocation.job_key,
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
            "manifest": reference("payload-manifest.json", manifest_bytes),
        }
    )
    manifest_path = unit / "payload-manifest.json"
    commit_path = unit / "COMMIT.json"
    if manifest:
        manifest_path.write_bytes(manifest_bytes)
    if marker:
        commit_path.write_bytes(encode_bundle_commit(commit_record))
    return SimpleNamespace(
        unit=unit,
        manifest=manifest_record,
        commit=commit_record,
        manifest_path=manifest_path,
        commit_path=commit_path,
    )


def inspect(bundle_invocation):
    return inspect_invocation_bundle(*bundle_invocation.parameters)


def issue_codes(report):
    return {issue.code for issue in report.issues}


def write_manifest(bundle, data, *, rebind=False):
    encoded = canonical(data)
    bundle.manifest_path.write_bytes(encoded)
    if rebind:
        commit = json.loads(bundle.commit_path.read_bytes())
        commit["manifest"] = reference("payload-manifest.json", encoded)
        bundle.commit_path.write_bytes(canonical(commit))


def snapshot(root):
    result = []
    for path in sorted((root, *root.rglob("*"))):
        metadata = path.lstat()
        data = path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None
        result.append(
            (
                path.relative_to(root).as_posix(),
                metadata.st_mode,
                metadata.st_ino,
                metadata.st_mtime_ns,
                data,
            )
        )
    return tuple(result)


def child_inspection(bundle_invocation, cwd):
    script = """
import json
import sys

from jobflow_gitlab_slurm.persistence.bundles.inspection import inspect_invocation_bundle
from jobflow_gitlab_slurm.persistence.attempts.ownership import InvocationBusyError
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError

root, run_id, job_uuid, index, attempt_id, invocation_id = sys.argv[1:]
try:
    report = inspect_invocation_bundle(
        root, run_id, job_uuid, int(index), attempt_id, invocation_id
    )
except (InvocationBusyError, RunBusyError) as error:
    print(type(error).__name__)
    sys.exit(3)
print(json.dumps(report.to_report(), sort_keys=True))
"""
    environment = {
        **os.environ,
        "PYTHONPATH": str(
            Path(__import__("jobflow_gitlab_slurm").__file__).resolve().parent.parent
        ),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return subprocess.run(
        [sys.executable, "-c", script, *map(str, bundle_invocation.parameters)],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
