"""Explicit scenario preparation and observations; no collected tests."""

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.receipts import (
    persist_owned_recovery_receipt,
    read_owned_recovery_receipt,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
)
from tests.persistence.recovery._request_support import (
    RECOVERY_ID,
    reference,
    timestamp,
)

BUNDLE_PHASES = (
    "request-acknowledgment",
    "intent-file-flush",
    "bundle-payload-flush",
    "scheduler-receipt-flush",
    "bundle-directory-flush",
    "bundle-parent-flush",
    "bundle-verification",
)


PUBLICATION_PHASES = (
    "temporary-create",
    "temporary-check",
    "temporary-write",
    "temporary-file-flush",
    "temporary-parent-flush",
    "receipt-destination-check",
    "receipt-rename",
    "final-file-flush",
    "recovery-directory-flush",
    "recoveries-directory-flush",
    "invocation-directory-flush",
    "final-verification",
)


PHASES = (*BUNDLE_PHASES, *PUBLICATION_PHASES)

ACK_PHASES = (*BUNDLE_PHASES, *PUBLICATION_PHASES[7:])


def committed_recovery_bundle(recovery_staging):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        requests.persist_owned_recovery_request(ownership, recovery_staging.request)
        bundles.publish_owned_bundle(
            ownership,
            recovery_staging.intent.expected_manifest,
            recovery_staging.intent.expected_commit,
        )

    identity = {
        field: getattr(recovery_staging.request, field)
        for field in requests._IDENTITY_FIELDS
    }
    recovery_staging.audit = recovery_staging.unit / "receipt.json"
    recovery_staging.receipt = RecoveryReceiptRecord.model_validate(
        {
            **identity,
            "schema_version": 1,
            "kind": "bundle-recovery-receipt",
            "created_at": timestamp(recovery_staging.run, 7),
            "recovery_id": RECOVERY_ID,
            "request": reference(
                recovery_staging.path.relative_to(recovery_staging.run.path).as_posix(),
                recovery_staging.path.read_bytes(),
            ),
            "bundle_manifest": reference(
                (recovery_staging.published / "payload-manifest.json")
                .relative_to(recovery_staging.run.path)
                .as_posix(),
                encode_bundle_manifest(recovery_staging.intent.expected_manifest),
            ),
            "bundle_commit": reference(
                (recovery_staging.published / "COMMIT.json")
                .relative_to(recovery_staging.run.path)
                .as_posix(),
                encode_bundle_commit(recovery_staging.intent.expected_commit),
            ),
            "result": "committed_bundle_verified",
        }
    )
    return recovery_staging


def persist(context, receipt=None):
    with owned_invocation(*context.parameters) as ownership:
        return persist_owned_recovery_receipt(
            ownership,
            context.receipt if receipt is None else receipt,
        )


def read(context, recovery_id=RECOVERY_ID):
    with owned_invocation(*context.parameters) as ownership:
        return read_owned_recovery_receipt(ownership, recovery_id)


def temporary(context, data, suffix="retained"):
    path = context.unit / f".recovery-receipt-{suffix}"
    path.write_bytes(data)
    return path
