"""Explicit scenario preparation and observations; no collected tests."""

import errno
import hashlib
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
    owned_invocation,
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
from jobflow_gitlab_slurm.persistence.bundles import storage
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    inspect_invocation_bundle,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.bundles.storage import (
    publish_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

OTHER_ID = "dddddddd-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-publication-job-é"

FLOW_BYTES = b'{ "@module": "do_not_import_publication_flow" }\r\n'

JOB_BYTES = b'{ "@module": "do_not_import_publication_job" }\r\n'

RECEIPT_BYTES = b"opaque receipt; not a scheduler schema"

SECRET = "PRIVATE_FAILURE_MESSAGE"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def reference(path, data):
    return {"path": path, "sha256": digest(data), "size_bytes": len(data)}


def timestamp(run, offset):
    instant = datetime.fromisoformat(run.manifest.created_at)
    return (
        (instant + timedelta(seconds=offset))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def commit_for(manifest, created_at):
    data = manifest.model_dump()
    identity = {
        field: data[field]
        for field in (
            "run_id",
            "job_uuid",
            "job_index",
            "job_key",
            "attempt_id",
            "invocation_id",
        )
    }
    return BundleCommit.model_validate(
        {
            **identity,
            "schema_version": 1,
            "kind": "bundle-commit",
            "created_at": created_at,
            "manifest": reference(
                "payload-manifest.json",
                encode_bundle_manifest(manifest),
            ),
        }
    )


def bundle_staging(tmp_path):
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
                "name": "publication-example",
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

    job_source = tmp_path / "job-source.json"
    job_source.write_bytes(JOB_BYTES)
    definition = publish_job_definition(
        root,
        JobDefinitionRecord.model_validate(
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
        ),
        job_source,
    )
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

    staging = invocation.path / "staging"
    staging.mkdir()
    payloads = {
        "job-document.json": b'{"output":null,"@module":"do_not_import_payload"}',
        "response.json": b"inert Response bytes; deliberately not JSON",
        "files/empty.bin": b"",
        "files/nested/result-é.txt": b"opaque application bytes",
        "data/blob.bin": b"\0opaque additional-store bytes",
    }
    for payload_name, data in payloads.items():
        path = staging / payload_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    manifest = BundleManifest.model_validate(
        {
            **common,
            "kind": "bundle-manifest",
            "created_at": timestamp(run, 3),
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": attempt.record.definition_sha256,
            "consumer_code_sha256": attempt.record.consumer_code_sha256,
            "worker_runtime_sha256": attempt.record.worker_runtime_sha256,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                receipt.relative_to(run.path).as_posix(),
                RECEIPT_BYTES,
            ),
            "document": reference("job-document.json", payloads["job-document.json"]),
            "response": reference("response.json", payloads["response.json"]),
            "files": tuple(
                reference(payload_name, data)
                for payload_name, data in sorted(payloads.items())
                if payload_name.startswith("files/")
            ),
            "additional_data": (
                {
                    "store_name": "results",
                    "blob_uuid": "opaque-blob",
                    "artifact": reference("data/blob.bin", payloads["data/blob.bin"]),
                },
            ),
        }
    )
    commit = commit_for(manifest, timestamp(run, 4))
    (staging / "payload-manifest.json").write_bytes(encode_bundle_manifest(manifest))

    return SimpleNamespace(
        root=root,
        run=run,
        attempt=attempt,
        invocation=invocation,
        parameters=parameters,
        receipt=receipt,
        staging=staging,
        published=invocation.path / "published",
        manifest=manifest,
        commit=commit,
        payloads=payloads,
    )


def publish(bundle_staging, manifest=None, commit=None):
    with owned_invocation(*bundle_staging.parameters) as ownership:
        return publish_owned_bundle(
            ownership,
            bundle_staging.manifest if manifest is None else manifest,
            bundle_staging.commit if commit is None else commit,
        )


def inspect(bundle_staging):
    return inspect_invocation_bundle(*bundle_staging.parameters)


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


def install_failure(bundle_staging, monkeypatch, phase):
    def fail(*args, **kwargs):
        raise OSError(errno.EIO, SECRET)

    helpers = {
        "payload-flush": "flush_payloads",
        "receipt-flush": "flush_scheduler_receipt",
        "bundle-directory-flush": "flush_directories",
    }
    if phase in helpers:
        monkeypatch.setattr(storage, helpers[phase], fail)
        return

    invocation_phases = {
        "invocation-directory-flush": 1,
        "rename-parent-flush": 2,
        "marker-parent-flush": 3,
        "final-parent-flush": 4,
    }
    if phase in invocation_phases:
        original = storage._sync_directory
        calls = 0

        def directory(path):
            nonlocal calls
            if path == bundle_staging.invocation.path:
                calls += 1
                if calls == invocation_phases[phase]:
                    fail()
            original(path)

        monkeypatch.setattr(storage, "_sync_directory", directory)
        return

    if phase in ("published-directory-flush", "final-directory-flush"):
        original = storage._sync_directory
        expected_call = 1 if phase == "published-directory-flush" else 2
        calls = 0

        def directory(path):
            nonlocal calls
            if path == bundle_staging.published:
                calls += 1
                if calls == expected_call:
                    fail()
            original(path)

        monkeypatch.setattr(storage, "_sync_directory", directory)
        return

    if phase in ("destination-check", "marker-destination-check"):
        original = storage._require_absent
        target = (
            bundle_staging.published
            if phase == "destination-check"
            else bundle_staging.published / "COMMIT.json"
        )

        def absent(path):
            if path == target:
                raise ValueError(SECRET)
            original(path)

        monkeypatch.setattr(storage, "_require_absent", absent)
        return

    if phase in ("directory-rename", "marker-rename"):
        original = os.rename
        target = (
            bundle_staging.published
            if phase == "directory-rename"
            else bundle_staging.published / "COMMIT.json"
        )

        def rename(source, destination):
            if destination == target:
                fail()
            original(source, destination)

        monkeypatch.setattr(storage.os, "rename", rename)
        return

    if phase == "marker-create":
        monkeypatch.setattr(storage.tempfile, "mkstemp", fail)
        return

    if phase == "marker-write":
        original = os.fdopen

        def fdopen(descriptor, mode, *args, **kwargs):
            if mode == "wb":
                fail()
            return original(descriptor, mode, *args, **kwargs)

        monkeypatch.setattr(storage.os, "fdopen", fdopen)
        return

    if phase == "marker-file-flush":
        original_temporary = storage.tempfile.mkstemp
        original_sync = os.fsync
        marker_descriptors = []

        def temporary(*args, **kwargs):
            descriptor, name = original_temporary(*args, **kwargs)
            marker_descriptors.append(descriptor)
            return descriptor, name

        def sync(descriptor):
            if descriptor in marker_descriptors:
                fail()
            original_sync(descriptor)

        monkeypatch.setattr(storage.tempfile, "mkstemp", temporary)
        monkeypatch.setattr(storage.os, "fsync", sync)
        return

    assert phase == "final-verification"
    monkeypatch.setattr(storage, "_verified_handle", fail)


PHASES = (
    "payload-flush",
    "receipt-flush",
    "bundle-directory-flush",
    "invocation-directory-flush",
    "destination-check",
    "directory-rename",
    "published-directory-flush",
    "rename-parent-flush",
    "marker-create",
    "marker-write",
    "marker-file-flush",
    "marker-parent-flush",
    "marker-destination-check",
    "marker-rename",
    "final-directory-flush",
    "final-parent-flush",
    "final-verification",
)


CHILD_SCRIPT = """
import json
import os
import sys
from pathlib import Path

from jobflow_gitlab_slurm.persistence.bundles import storage
from jobflow_gitlab_slurm.persistence.bundles.inspection import inspect_invocation_bundle
from jobflow_gitlab_slurm.persistence.bundles.records import (
    decode_bundle_commit,
    decode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    owned_invocation,
)

root, run_id, job_uuid, index, attempt_id, invocation_id, manifest, commit, mode = (
    sys.argv[1:]
)
parameters = (root, run_id, job_uuid, int(index), attempt_id, invocation_id)
manifest = decode_bundle_manifest(Path(manifest).read_bytes())
commit = decode_bundle_commit(Path(commit).read_bytes())
original_rename = os.rename

def rename(source, destination):
    name = Path(destination).name
    if name == "published" and mode == "before-directory":
        os._exit(79)
    if name == "COMMIT.json" and mode == "before-marker":
        os._exit(79)
    original_rename(source, destination)
    if name == "published" and mode == "after-directory":
        os._exit(79)
    if name == "COMMIT.json" and mode == "after-marker":
        os._exit(79)

storage.os.rename = rename
try:
    with owned_invocation(*parameters) as ownership:
        storage.publish_owned_bundle(ownership, manifest, commit)
except InvocationBusyError:
    print("InvocationBusyError")
    sys.exit(3)
print(json.dumps(inspect_invocation_bundle(*parameters).to_report()))
"""


def child_publish(bundle_staging, tmp_path, mode="normal"):
    manifest_source = tmp_path / "expected-manifest.json"
    commit_source = tmp_path / "expected-commit.json"
    manifest_source.write_bytes(encode_bundle_manifest(bundle_staging.manifest))
    commit_source.write_bytes(encode_bundle_commit(bundle_staging.commit))
    environment = {
        **os.environ,
        "PYTHONPATH": str(
            Path(__import__("jobflow_gitlab_slurm").__file__).resolve().parent.parent
        ),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD_SCRIPT,
            *map(str, bundle_staging.parameters),
            str(manifest_source),
            str(commit_source),
            mode,
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
