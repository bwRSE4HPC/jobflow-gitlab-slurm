"""Explicit scenario preparation and observations; no collected tests."""

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.operations import (
    recover_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    encode_recovery_request,
)
from tests.persistence.recovery._request_support import (
    prepare_state,
    reference,
    timestamp,
)

ALLOWED = (
    ("staging_only", "staging_only"),
    ("staging_only", "published_uncommitted"),
    ("staging_only", "committed"),
    ("published_uncommitted", "published_uncommitted"),
    ("published_uncommitted", "committed"),
    ("committed", "committed"),
)


REGRESSIONS = (
    ("published_uncommitted", "staging_only"),
    ("committed", "staging_only"),
    ("committed", "published_uncommitted"),
)


PHASES = (
    "request-acknowledgment",
    "intent-file-flush",
    "recovery-reinspection",
    "staging-payload-flush",
    "staging-scheduler-receipt-flush",
    "staging-directory-flush",
    "staging-parent-flush",
    "published-destination-check",
    "directory-rename",
    "renamed-directory-flush",
    "rename-parent-flush",
    "published-reinspection",
    "published-payload-flush",
    "published-scheduler-receipt-flush",
    "published-directory-flush",
    "published-parent-flush",
    "marker-create",
    "marker-check",
    "marker-write",
    "marker-file-flush",
    "marker-parent-flush",
    "marker-ready-check",
    "marker-destination-check",
    "marker-rename",
    "marker-final-file-flush",
    "marker-final-directory-flush",
    "marker-final-parent-flush",
    "committed-verification",
    "marker-evidence-verification",
    "receipt-completion",
)


def receipt_for(context):
    identity = {
        field: getattr(context.request, field) for field in requests._IDENTITY_FIELDS
    }
    request_path = (
        context.invocation.path
        / "recoveries"
        / context.request.recovery_id
        / "request.json"
    )
    return RecoveryReceiptRecord.model_validate(
        {
            **identity,
            "schema_version": 1,
            "kind": "bundle-recovery-receipt",
            "created_at": timestamp(context.run, 7),
            "recovery_id": context.request.recovery_id,
            "request": reference(
                request_path.relative_to(context.run.path).as_posix(),
                encode_recovery_request(context.request),
            ),
            "bundle_manifest": reference(
                (context.published / "payload-manifest.json")
                .relative_to(context.run.path)
                .as_posix(),
                encode_bundle_manifest(context.intent.expected_manifest),
            ),
            "bundle_commit": reference(
                (context.published / "COMMIT.json")
                .relative_to(context.run.path)
                .as_posix(),
                encode_bundle_commit(context.intent.expected_commit),
            ),
            "result": "committed_bundle_verified",
        }
    )


def recoverable_bundle(recovery_staging):
    recovery_staging.audit = recovery_staging.unit / "receipt.json"
    recovery_staging.receipt = receipt_for(recovery_staging)
    return recovery_staging


def recover(context, request=None, receipt=None):
    with owned_invocation(*context.parameters) as ownership:
        return recover_owned_bundle(
            ownership,
            context.request if request is None else request,
            receipt=context.receipt if receipt is None else receipt,
        )


def retain_request(context):
    with owned_invocation(*context.parameters) as ownership:
        requests.persist_owned_recovery_request(ownership, context.request)


def original_state(context, status):
    prepare_state(context, status)
    context.receipt = receipt_for(context)
    retain_request(context)


def current_state(context, status):
    source = context.staging if context.staging.exists() else context.published
    destination = context.staging if status == "staging_only" else context.published
    if source != destination:
        source.rename(destination)
    marker = destination / "COMMIT.json"
    if marker.exists():
        marker.unlink()
    if status == "committed":
        marker.write_bytes(encode_bundle_commit(context.intent.expected_commit))


def payload_snapshot(context):
    unit = context.staging if context.staging.exists() else context.published
    paths = (
        unit / "job-document.json",
        unit / "response.json",
        unit / "payload-manifest.json",
        context.invocation.attempt.path / "slurm-receipt.json",
    )
    return tuple(
        (
            path.name,
            path.read_bytes(),
            path.stat().st_ino,
            path.stat().st_mtime_ns,
        )
        for path in paths
    )


def marker_temporary(context, data, suffix="retained"):
    path = context.invocation.path / f".bundle-commit-{suffix}"
    path.write_bytes(data)
    return path
