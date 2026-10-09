"""integration / test_recovery_operations_handoff contracts."""

import subprocess
import sys

import pytest

from jobflow_gitlab_slurm.persistence.recovery.operations import (
    BundleRecoveryIntegrityError,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    encode_recovery_receipt,
    encode_recovery_request,
)
from tests.persistence.recovery import _operation_support as operation_setup
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    ("phase", "when"),
    [
        ("request-acknowledgment", "after"),
        ("directory-rename", "before"),
        ("directory-rename", "after"),
        ("marker-create", "after"),
        ("marker-file-flush", "after"),
        ("marker-rename", "before"),
        ("marker-rename", "after"),
        ("receipt-completion", "before"),
        ("receipt-completion", "after"),
    ],
)
def test_separate_process_interruption_preserves_identity_and_evidence(
    recoverable_bundle, phase, when
):
    context = recoverable_bundle
    request_file = context.root.parent / "expected-request.json"
    receipt_file = context.root.parent / "expected-receipt.json"
    request_file.write_bytes(encode_recovery_request(context.request))
    receipt_file.write_bytes(encode_recovery_receipt(context.receipt))
    before_payloads = operation_setup.payload_snapshot(context)
    script = """
import os
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.recovery import operations as recovery
from jobflow_gitlab_slurm.persistence.attempts.ownership import owned_invocation
from jobflow_gitlab_slurm.persistence.recovery.records import (
    decode_recovery_request,
    decode_recovery_receipt,
)

root, run, job, index, attempt, invocation, request_file, receipt_file, phase, when = sys.argv[1:]
request = decode_recovery_request(Path(request_file).read_bytes())
receipt = decode_recovery_receipt(Path(receipt_file).read_bytes())
original = recovery._step

def exit_at(progress, current_phase, operation, *args):
    if current_phase == phase and when == "before":
        os._exit(73)
    result = original(progress, current_phase, operation, *args)
    if current_phase == phase and when == "after":
        os._exit(73)
    return result

recovery._step = exit_at
with owned_invocation(root, run, job, int(index), attempt, invocation) as ownership:
    recovery.recover_owned_bundle(ownership, request, receipt=receipt)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            *(str(value) for value in context.parameters),
            str(request_file),
            str(receipt_file),
            phase,
            when,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr

    if phase == "marker-create":
        before_retry = request_setup.snapshot(context.root)
        with pytest.raises(BundleRecoveryIntegrityError) as caught:
            operation_setup.recover(context)
        assert caught.value.code == "invalid_temporary_marker"
        assert request_setup.snapshot(context.root) == before_retry
        assert not context.audit.exists()
    else:
        assert operation_setup.recover(context).record == context.receipt
        assert operation_setup.recover(context).record == context.receipt
        assert context.audit.read_bytes() == receipt_file.read_bytes()

    assert context.path.read_bytes() == request_file.read_bytes()
    assert operation_setup.payload_snapshot(context) == before_payloads
    assert tuple(context.recovery_root.iterdir()) == (context.unit,)
