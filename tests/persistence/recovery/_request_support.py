"""Explicit scenario preparation and observations; no collected tests."""

from tests.support.filesystem import tree_identity_and_content as snapshot

__all__ = ["snapshot"]

import hashlib
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
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.storage import (
    publish_owned_publication_intent,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryRequestRecord,
    encode_recovery_request,
)
from jobflow_gitlab_slurm.persistence.recovery.requests import (
    persist_owned_recovery_request,
    read_owned_recovery_request,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

INTENT_ID = "dddddddd-0000-4000-8000-000000000005"

RECOVERY_ID = "eeeeeeee-0000-4000-8000-000000000006"

EVENT_ID = "ffffffff-0000-4000-8000-000000000007"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-recovery-storage-job-é"

SECRET = "PRIVATE_FAILURE_MESSAGE"

PHASES = (
    "recoveries-directory-create",
    "recovery-directory-create",
    "temporary-create",
    "temporary-check",
    "temporary-write",
    "temporary-file-flush",
    "temporary-parent-flush",
    "request-destination-check",
    "request-rename",
    "final-file-flush",
    "recovery-directory-flush",
    "recoveries-directory-flush",
    "invocation-directory-flush",
    "final-verification",
)


ACK_PHASES = (
    "final-file-flush",
    "recovery-directory-flush",
    "recoveries-directory-flush",
    "invocation-directory-flush",
    "final-verification",
)


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


def recovery_staging(tmp_path):
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
    run_request = RunRequest.model_validate(
        {
            "schema_version": 1,
            "site_id": site.site_id,
            "workflow": {
                "name": "recovery-storage-example",
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
    run = create_run(site, run_request, *sources, run_id=RUN_ID)
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
                "consumer_code_sha256": run_request.workflow.consumer_code_sha256,
                "worker_runtime_sha256": run_request.runtime.worker_runtime_sha256,
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
        "response.json": b"opaque response",
    }
    qualified = {
        **common,
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }
    manifest = BundleManifest.model_validate(
        {
            **qualified,
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
            **qualified,
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
    intent_path = invocation.path / "publication-intent.json"
    with owned_invocation(*parameters) as ownership:
        publish_owned_publication_intent(ownership, intent)

    staging = invocation.path / "staging"
    staging.mkdir()
    for filename, data in payloads.items():
        (staging / filename).write_bytes(data)
    (staging / "payload-manifest.json").write_bytes(encode_bundle_manifest(manifest))

    recovery_root = invocation.path / "recoveries"
    unit = recovery_root / RECOVERY_ID
    request = RecoveryRequestRecord.model_validate(
        {
            **qualified,
            "kind": "bundle-recovery-request",
            "created_at": timestamp(run, 6),
            "recovery_id": RECOVERY_ID,
            "journal_event_id": EVENT_ID,
            "actor": {"kind": "operator", "identifier": "operator-1"},
            "intent_id": INTENT_ID,
            "intent": reference(
                intent_path.relative_to(run.path).as_posix(),
                encode_publication_intent(intent),
            ),
            "observed_status": "staging_only",
            "action": "publish_staging",
        }
    )
    return SimpleNamespace(
        root=root,
        run=run,
        invocation=invocation,
        parameters=parameters,
        intent=intent,
        intent_path=intent_path,
        request=request,
        recovery_root=recovery_root,
        unit=unit,
        path=unit / "request.json",
        staging=staging,
        published=invocation.path / "published",
    )


def persist(recovery_staging, request=None):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        return persist_owned_recovery_request(
            ownership, recovery_staging.request if request is None else request
        )


def read(recovery_staging, recovery_id=RECOVERY_ID):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        return read_owned_recovery_request(ownership, recovery_id)


def prepare_state(recovery_staging, state):
    if state != "staging_only":
        recovery_staging.staging.rename(recovery_staging.published)
    if state == "committed":
        (recovery_staging.published / "COMMIT.json").write_bytes(
            encode_bundle_commit(recovery_staging.intent.expected_commit)
        )
    actions = {
        "staging_only": "publish_staging",
        "published_uncommitted": "complete_marker",
        "committed": "acknowledge_commit",
    }
    recovery_staging.request = RecoveryRequestRecord.model_validate(
        {
            **recovery_staging.request.model_dump(),
            "observed_status": state,
            "action": actions[state],
        }
    )
    return (
        recovery_staging.staging
        if state == "staging_only"
        else recovery_staging.published
    )


def prepare_temporary(recovery_staging, data, suffix="retained"):
    recovery_staging.unit.mkdir(parents=True, exist_ok=True)
    path = recovery_staging.unit / f".recovery-request-{suffix}"
    path.write_bytes(data)
    return path


def child(recovery_staging, tmp_path, mode):
    source = tmp_path / "retained-request.json"
    source.write_bytes(encode_recovery_request(recovery_staging.request))
    script = """
import os
import sys
from pathlib import Path

from jobflow_gitlab_slurm.persistence.recovery import requests as storage
from jobflow_gitlab_slurm.persistence.attempts.ownership import owned_invocation
from jobflow_gitlab_slurm.persistence.recovery.records import (
    decode_recovery_request,
    encode_recovery_request,
)

mode, source, *parameters = sys.argv[1:]
parameters[3] = int(parameters[3])
request = decode_recovery_request(Path(source).read_bytes())
if mode != "normal":
    original = storage.os.rename

    def interrupted(source, destination):
        if mode == "after":
            original(source, destination)
        os._exit(71)

    storage.os.rename = interrupted

with owned_invocation(*parameters) as ownership:
    handle = storage.persist_owned_recovery_request(ownership, request)
    reopened = storage.read_owned_recovery_request(
        ownership, request.recovery_id
    )
    assert handle == reopened
    print(encode_recovery_request(reopened.record).decode())
"""
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            script,
            mode,
            str(source),
            *(str(value) for value in recovery_staging.parameters),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
