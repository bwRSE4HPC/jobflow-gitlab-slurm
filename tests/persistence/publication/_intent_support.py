"""Explicit scenario preparation and observations; no collected tests."""

import errno
import hashlib
import os
import stat
import subprocess
import sys
from datetime import datetime, timedelta
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
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication import storage
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.storage import (
    publish_owned_publication_intent,
    read_owned_publication_intent,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

INTENT_ID = "dddddddd-0000-4000-8000-000000000005"

OTHER_ID = "eeeeeeee-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-intent-job-é"

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


def intent_staging(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    source_bytes = (b'{"@module":"never_import_flow"}', b"consumer", b"runtime")
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
                "name": "intent-example",
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
    job_source = tmp_path / "job.json"
    job_bytes = b'{"@module":"never_import_job"}'
    job_source.write_bytes(job_bytes)
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
                    job_bytes,
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

    receipt_bytes = b"opaque scheduler evidence"
    receipt = attempt.path / "slurm-receipt.json"
    receipt.write_bytes(receipt_bytes)
    payloads = {
        "job-document.json": b'{"output":null,"@module":"never_import_document"}',
        "response.json": b"opaque response bytes",
    }
    identity = {
        **common,
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }
    manifest = BundleManifest.model_validate(
        {
            **identity,
            "kind": "bundle-manifest",
            "created_at": timestamp(run, 3),
            "definition_id": DEFINITION_ID,
            "definition_sha256": attempt.record.definition_sha256,
            "consumer_code_sha256": attempt.record.consumer_code_sha256,
            "worker_runtime_sha256": attempt.record.worker_runtime_sha256,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                receipt.relative_to(run.path).as_posix(), receipt_bytes
            ),
            "document": reference("job-document.json", payloads["job-document.json"]),
            "response": reference("response.json", payloads["response.json"]),
            "files": (),
            "additional_data": (),
        }
    )
    commit = BundleCommit.model_validate(
        {
            **identity,
            "kind": "bundle-commit",
            "created_at": timestamp(run, 4),
            "manifest": reference(
                "payload-manifest.json", encode_bundle_manifest(manifest)
            ),
        }
    )
    intent = PublicationIntentRecord(
        schema_version=1,
        kind="publication-intent",
        intent_id=INTENT_ID,
        created_at=timestamp(run, 5),
        expected_manifest=manifest,
        expected_commit=commit,
    )
    return SimpleNamespace(
        root=root,
        run=run,
        invocation=invocation,
        parameters=parameters,
        intent=intent,
        path=invocation.path / "publication-intent.json",
        published=invocation.path / "published",
        payloads=payloads,
    )


def publish(intent_staging, intent=None):
    with owned_invocation(*intent_staging.parameters) as ownership:
        return publish_owned_publication_intent(
            ownership, intent_staging.intent if intent is None else intent
        )


def read(intent_staging):
    with owned_invocation(*intent_staging.parameters) as ownership:
        return read_owned_publication_intent(ownership)


def snapshot(root):
    observations = []
    for path in sorted((root, *root.rglob("*"))):
        metadata = path.lstat()
        if stat.S_ISREG(metadata.st_mode):
            data = path.read_bytes()
        elif stat.S_ISLNK(metadata.st_mode):
            data = os.readlink(path)
        else:
            data = None
        observations.append(
            (
                path.relative_to(root).as_posix(),
                metadata.st_mode,
                metadata.st_ino,
                metadata.st_mtime_ns,
                data,
            )
        )
    return tuple(observations)


def make_staging(intent_staging):
    staging = intent_staging.invocation.path / "staging"
    staging.mkdir()
    for name, data in intent_staging.payloads.items():
        (staging / name).write_bytes(data)
    (staging / "payload-manifest.json").write_bytes(
        encode_bundle_manifest(intent_staging.intent.expected_manifest)
    )
    return staging


def install_failure(intent_staging, monkeypatch, phase):
    def fail(*args, **kwargs):
        raise OSError(errno.EIO, SECRET)

    if phase == "temporary-create":
        monkeypatch.setattr(storage.tempfile, "mkstemp", fail)
    elif phase == "temporary-check":
        monkeypatch.setattr(storage, "_check_temporary", fail)
    elif phase == "temporary-write":
        original = os.fdopen

        def fdopen(descriptor, mode, *args, **kwargs):
            if mode == "wb":
                fail()
            return original(descriptor, mode, *args, **kwargs)

        monkeypatch.setattr(storage.os, "fdopen", fdopen)
    elif phase == "temporary-file-flush":
        monkeypatch.setattr(storage.os, "fsync", fail)
    elif phase in {"temporary-parent-flush", "final-parent-flush"}:
        original = storage._sync_directory
        count = 0
        target = 1 if phase == "temporary-parent-flush" else 2

        def sync(path):
            nonlocal count
            count += 1
            if count == target:
                fail()
            original(path)

        monkeypatch.setattr(storage, "_sync_directory", sync)
    elif phase in {"published-path-check", "intent-destination-check"}:
        original = storage._require_absent

        def absent(invocation, path, code):
            if phase == "published-path-check" and (
                path == intent_staging.published
                and list(intent_staging.invocation.path.glob(".publication-intent-*"))
            ):
                fail()
            if phase == "intent-destination-check" and path == intent_staging.path:
                fail()
            original(invocation, path, code)

        monkeypatch.setattr(storage, "_require_absent", absent)
    elif phase == "intent-rename":
        monkeypatch.setattr(storage.os, "rename", fail)
    elif phase == "final-file-flush":
        monkeypatch.setattr(storage, "_sync_file", fail)
    else:
        assert phase == "final-verification"
        original = storage._read

        def read_existing(ownership, path):
            if path.exists():
                fail()
            return original(ownership, path)

        monkeypatch.setattr(storage, "_read", read_existing)


PHASES = (
    "temporary-create",
    "temporary-check",
    "temporary-write",
    "temporary-file-flush",
    "temporary-parent-flush",
    "published-path-check",
    "intent-destination-check",
    "intent-rename",
    "final-file-flush",
    "final-parent-flush",
    "final-verification",
)


CHILD_SCRIPT = """
import os
import sys
from pathlib import Path

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.publication.records import (
    decode_publication_intent,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.storage import (
    publish_owned_publication_intent,
    read_owned_publication_intent,
)

root, run_id, job_uuid, attempt_id, invocation_id, source, mode = sys.argv[1:]
intent = decode_publication_intent(Path(source).read_bytes())
arguments = (Path(root), run_id, job_uuid, 1, attempt_id, invocation_id)
original_rename = os.rename

def interrupted_rename(source_path, destination):
    if Path(destination).name == "publication-intent.json":
        if mode == "before":
            os._exit(79)
        original_rename(source_path, destination)
        if mode == "after":
            os._exit(79)
    else:
        original_rename(source_path, destination)

os.rename = interrupted_rename
try:
    with owned_invocation(*arguments) as ownership:
        if mode == "read":
            handle = read_owned_publication_intent(ownership)
        else:
            handle = publish_owned_publication_intent(ownership, intent)
        assert encode_publication_intent(handle.record) == (
            encode_publication_intent(intent)
        )
        print(handle.sha256)
except InvocationBusyError:
    print("InvocationBusyError")
    sys.exit(3)
"""


def child(intent_staging, tmp_path, mode):
    source = tmp_path / "expected-intent.json"
    source.write_bytes(encode_publication_intent(intent_staging.intent))
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD_SCRIPT,
            str(intent_staging.root),
            RUN_ID,
            JOB_UUID,
            ATTEMPT_ID,
            INVOCATION_ID,
            str(source),
            mode,
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
