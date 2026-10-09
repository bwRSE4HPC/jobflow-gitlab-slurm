"""Cross-process persistence composition using inert, caller-retained inputs."""

import hashlib
import json
import os
import stat
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    publish_job_definition,
    read_job_definition,
)
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
    read_attempt,
    read_invocation,
    register_invocation,
    reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.registration import (
    PublicationJournalConflictError,
    PublicationJournalIntegrityError,
    register_owned_publication_event,
)
from jobflow_gitlab_slurm.persistence.publication.storage import (
    publish_owned_publication_intent,
)
from jobflow_gitlab_slurm.persistence.recovery.operations import recover_owned_bundle
from jobflow_gitlab_slurm.persistence.recovery.receipts import (
    read_owned_recovery_receipt,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    RecoveryRequestRecord,
    encode_recovery_request,
)
from jobflow_gitlab_slurm.persistence.recovery.registration import (
    register_owned_recovery_event,
)
from jobflow_gitlab_slurm.persistence.recovery.requests import (
    read_owned_recovery_request,
)
from jobflow_gitlab_slurm.persistence.runs.storage import create_run, open_run

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

INTENT_ID = "dddddddd-0000-4000-8000-000000000005"

RECOVERY_ID = "eeeeeeee-0000-4000-8000-000000000006"

RECOVERY_EVENT_ID = "ffffffff-0000-4000-8000-000000000007"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-lifecycle-job-é"

CRASH_STATUS = 73

FLOW_BYTES = b'{ "@module": "never_import_lifecycle_flow" }\r\n'

JOB_BYTES = b'{ "@module": "never_import_lifecycle_job" }\r\n'

RECEIPT_BYTES = b"opaque scheduler receipt, not evidence of Slurm success"

PAYLOADS = {
    "job-document.json": b'{"@module":"never_import_lifecycle_document"}',
    "response.json": b"opaque response; intentionally not scientific JSON",
    "files/empty.bin": b"",
    "files/nested/result-é.bin": b"\0opaque application output",
}


CHILD_SCRIPT = """
import runpy
import sys
from pathlib import Path

namespace = runpy.run_path(sys.argv[1])
namespace["execute_stage"](Path(sys.argv[2]), sys.argv[3], sys.argv[4])
"""


def reference(path, data):
    return {
        "path": path,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def write_plan(base, plan):
    (base / "plan.json").write_text(
        json.dumps(plan, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )


def read_plan(base):
    return json.loads((base / "plan.json").read_text(encoding="utf-8"))


def model_from_plan(plan, name, model):
    return model.model_validate_json(json.dumps(plan[name]))


def parameters(base):
    return (
        base / "runs",
        RUN_ID,
        JOB_UUID,
        1,
        ATTEMPT_ID,
        INVOCATION_ID,
    )


def run_path(base):
    return base / "runs" / RUN_ID


def invocation_path(base):
    return (
        run_path(base)
        / "jobs"
        / job_key(JOB_UUID)
        / "index-1"
        / "attempts"
        / ATTEMPT_ID
        / "invocations"
        / INVOCATION_ID
    )


def timestamp(run, offset):
    instant = datetime.fromisoformat(run.manifest.created_at)
    return (
        (instant + timedelta(seconds=offset))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def snapshot(root):
    observations = {}
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        if stat.S_ISREG(metadata.st_mode):
            observations[path.relative_to(root).as_posix()] = (
                metadata.st_mode,
                metadata.st_ino,
                metadata.st_mtime_ns,
                path.read_bytes(),
            )
    return observations


def metadata_snapshot(base):
    return {
        name: observation
        for name, observation in snapshot(base / "runs").items()
        if not {"staging", "published", "events"}.intersection(Path(name).parts)
        and Path(name).name != "journal-head.json"
    }


def assert_preserved(before, after):
    for name, observation in before.items():
        assert after[name] == observation, name


def bundle_snapshot(base):
    invocation = invocation_path(base)
    unit = invocation / "published"
    if not unit.exists():
        unit = invocation / "staging"
    return {
        name: observation
        for name, observation in snapshot(unit).items()
        if name != "COMMIT.json"
    }


def run_stage(base, stage, boundary=""):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD_SCRIPT,
            str(Path(__file__).resolve()),
            str(base),
            stage,
            boundary,
        ],
        cwd=base,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    expected = CRASH_STATUS if boundary else 0
    assert result.returncode == expected, (
        f"stage={stage}; boundary={boundary}; "
        f"returncode={result.returncode}\n{result.stdout}\n{result.stderr}"
    )
    return result


@pytest.fixture
def lifecycle(tmp_path):
    base = tmp_path
    root = base / "runs"
    root.mkdir()
    inputs = base / "inputs"
    inputs.mkdir()

    for name, data in {
        "flow.json": FLOW_BYTES,
        "job.json": JOB_BYTES,
        "consumer.whl": b"opaque consumer artifact",
        "runtime.sif": b"\0opaque runtime artifact",
    }.items():
        (inputs / name).write_bytes(data)

    site = {
        "schema_version": 1,
        "site_id": "example-cluster",
        "slurm": {
            "cluster_name": "example-slurm",
            "account": "example-account",
            "partitions": ["short"],
            "default_partition": "short",
        },
        "storage": {
            "provider": "posix",
            "runs_root": str(root),
        },
        "worker": {
            "launch_mode": "apptainer",
            "apptainer_command": "apptainer",
        },
    }
    request = {
        "schema_version": 1,
        "site_id": "example-cluster",
        "workflow": {
            "name": "opaque-lifecycle-example",
            "serialized_flow_sha256": reference("", FLOW_BYTES)["sha256"],
            "consumer_code_sha256": reference(
                "", (inputs / "consumer.whl").read_bytes()
            )["sha256"],
        },
        "runtime": {
            "worker_runtime_sha256": reference(
                "", (inputs / "runtime.sif").read_bytes()
            )["sha256"],
        },
        "resources": {
            "partition": "short",
            "nodes": 1,
            "tasks_per_node": 2,
            "cpus_per_task": 1,
            "memory_mb_per_node": 4096,
            "walltime_seconds": 1800,
        },
    }
    write_plan(base, {"site": site, "request": request})
    assert list(root.iterdir()) == []
    return base


def retain_records(base):
    """Retain caller inputs once, outside the run's authoritative evidence."""
    plan = read_plan(base)
    run = open_run(base / "runs", RUN_ID, verify_external=True)
    common = {
        "schema_version": 1,
        "run_id": RUN_ID,
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": job_key(JOB_UUID),
    }
    prefix = f"jobs/{common['job_key']}/index-1"
    attempt_prefix = f"{prefix}/attempts/{ATTEMPT_ID}"

    definition = JobDefinitionRecord.model_validate(
        {
            **common,
            "kind": "job-definition",
            "created_at": timestamp(run, 0),
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference(
                f"{prefix}/definitions/{DEFINITION_ID}/job.json",
                JOB_BYTES,
            ),
            "origin": "original_flow",
            "source": run.flow.payload.model_dump(),
        }
    )
    attempt = AttemptRecord.model_validate(
        {
            **common,
            "kind": "execution-attempt",
            "created_at": timestamp(run, 1),
            "attempt_id": ATTEMPT_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": definition.payload.sha256,
            "consumer_code_sha256": run.manifest.request.workflow.consumer_code_sha256,
            "worker_runtime_sha256": run.manifest.request.runtime.worker_runtime_sha256,
        }
    )
    invocation = InvocationRecord.model_validate(
        {
            **common,
            "kind": "worker-invocation",
            "created_at": timestamp(run, 2),
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
        }
    )
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
            "definition_sha256": attempt.definition_sha256,
            "consumer_code_sha256": attempt.consumer_code_sha256,
            "worker_runtime_sha256": attempt.worker_runtime_sha256,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                f"{attempt_prefix}/slurm-receipt.json", RECEIPT_BYTES
            ),
            "document": reference("job-document.json", PAYLOADS["job-document.json"]),
            "response": reference("response.json", PAYLOADS["response.json"]),
            "files": tuple(
                reference(name, data)
                for name, data in sorted(PAYLOADS.items())
                if name.startswith("files/")
            ),
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
    plan.update(
        {
            "definition": definition.model_dump(mode="json"),
            "attempt": attempt.model_dump(mode="json"),
            "invocation": invocation.model_dump(mode="json"),
            "intent": intent.model_dump(mode="json"),
            "expected_events": [],
            "expected_hold": "integrity",
        }
    )
    write_plan(base, plan)


def bootstrap(base, *, abrupt=False):
    boundary = "returned" if abrupt else ""
    run_stage(base, "run", boundary)
    retain_records(base)

    for stage in ("definition", "attempt", "invocation", "lock"):
        before = snapshot(base / "runs")
        run_stage(base, stage, boundary)
        assert_preserved(before, snapshot(base / "runs"))

    before = snapshot(base / "runs")
    run_stage(base, "verify-metadata")
    assert snapshot(base / "runs") == before


def verify_metadata(base, plan):
    run = open_run(base / "runs", RUN_ID, verify_external=True)
    assert run.manifest.request == model_from_plan(plan, "request", RunRequest)
    assert (run.path / run.flow.payload.path).read_bytes() == FLOW_BYTES

    definition = read_job_definition(*parameters(base)[:4], DEFINITION_ID)
    assert definition.record == model_from_plan(plan, "definition", JobDefinitionRecord)
    assert (run.path / definition.record.payload.path).read_bytes() == JOB_BYTES

    attempt = read_attempt(*parameters(base)[:5])
    assert attempt.record == model_from_plan(plan, "attempt", AttemptRecord)

    invocation = read_invocation(*parameters(base))
    assert invocation.record == model_from_plan(plan, "invocation", InvocationRecord)
    assert invocation.attempt.record == attempt.record
    assert invocation.attempt.definition.record == definition.record
    assert (invocation.path / ".bundle.lock").is_file()


def prepare_staging(ownership, intent):
    """Test-only byte preparation, not a worker or production staging API."""
    invocation = ownership.invocation
    receipt = invocation.attempt.path / "slurm-receipt.json"
    receipt.write_bytes(RECEIPT_BYTES)

    staging = invocation.path / "staging"
    staging.mkdir()
    for name, data in PAYLOADS.items():
        target = staging / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    (staging / "payload-manifest.json").write_bytes(
        encode_bundle_manifest(intent.expected_manifest)
    )


def retain_recovery(base, status):
    plan = read_plan(base)
    run = open_run(base / "runs", RUN_ID)
    intent = model_from_plan(plan, "intent", PublicationIntentRecord)
    qualified = {
        field: getattr(intent.expected_manifest, field)
        for field in (
            "schema_version",
            "run_id",
            "job_uuid",
            "job_index",
            "job_key",
            "attempt_id",
            "invocation_id",
        )
    }
    prefix = invocation_path(base).relative_to(run.path).as_posix()
    request = RecoveryRequestRecord.model_validate(
        {
            **qualified,
            "kind": "bundle-recovery-request",
            "created_at": timestamp(run, 6),
            "recovery_id": RECOVERY_ID,
            "journal_event_id": RECOVERY_EVENT_ID,
            "actor": {"kind": "operator", "identifier": "offline-test"},
            "intent_id": INTENT_ID,
            "intent": reference(
                f"{prefix}/publication-intent.json",
                encode_publication_intent(intent),
            ),
            "observed_status": status,
            "action": {
                "staging_only": "publish_staging",
                "published_uncommitted": "complete_marker",
            }[status],
        }
    )
    receipt = RecoveryReceiptRecord.model_validate(
        {
            **qualified,
            "kind": "bundle-recovery-receipt",
            "created_at": timestamp(run, 7),
            "recovery_id": RECOVERY_ID,
            "request": reference(
                f"{prefix}/recoveries/{RECOVERY_ID}/request.json",
                encode_recovery_request(request),
            ),
            "bundle_manifest": reference(
                f"{prefix}/published/payload-manifest.json",
                encode_bundle_manifest(intent.expected_manifest),
            ),
            "bundle_commit": reference(
                f"{prefix}/published/COMMIT.json",
                encode_bundle_commit(intent.expected_commit),
            ),
            "result": "committed_bundle_verified",
        }
    )
    plan["recovery_request"] = request.model_dump(mode="json")
    plan["recovery_receipt"] = receipt.model_dump(mode="json")
    write_plan(base, plan)


def install_crash_hook(boundary):
    """Expose three existing publication hand-offs in the child process only."""
    if boundary not in {
        "published-directory",
        "marker-staged",
        "journal-event",
    }:
        return

    if boundary == "journal-event":
        module = journal
        attribute = "_rename_new"
    else:
        module = bundles.os
        attribute = "rename"
    original = getattr(module, attribute)

    def intercepted(source, destination):
        destination = Path(destination)
        if boundary == "marker-staged" and destination.name == "COMMIT.json":
            os._exit(CRASH_STATUS)
        original(source, destination)
        if boundary == "published-directory" and destination.name == "published":
            os._exit(CRASH_STATUS)
        if boundary == "journal-event" and destination.parent.name == "events":
            os._exit(CRASH_STATUS)

    setattr(module, attribute, intercepted)


def reject_registration(base, ownership, intent, plan):
    expected = {
        "integrity": PublicationJournalIntegrityError,
        "conflict": PublicationJournalConflictError,
    }[plan["expected_hold"]]
    with pytest.raises(expected) as caught:
        register_owned_publication_event(ownership, intent)
    report = caught.value.to_report()
    assert report["intent_id"] == INTENT_ID
    assert not report["journal_registration_uncertain"]
    (base / "hold-report.json").write_text(json.dumps(report), encoding="utf-8")


def verify_final(base, ownership, intent, plan):
    report = inspection.inspect_owned_bundle(ownership)
    assert report.status == "committed"
    assert report.content_integrity == "valid"

    published = ownership.invocation.path / "published"
    for name, data in PAYLOADS.items():
        assert (published / name).read_bytes() == data
    assert (published / "payload-manifest.json").read_bytes() == (
        encode_bundle_manifest(intent.expected_manifest)
    )
    assert (published / "COMMIT.json").read_bytes() == (
        encode_bundle_commit(intent.expected_commit)
    )
    assert (ownership.invocation.attempt.path / "slurm-receipt.json").read_bytes() == (
        RECEIPT_BYTES
    )

    replay = journal.read_events(base / "runs", RUN_ID)
    assert [[event.event_id, event.event_type] for event in replay.events] == plan[
        "expected_events"
    ]
    assert [event.sequence for event in replay.events] == list(
        range(1, len(replay.events) + 1)
    )
    assert replay.head.last_sequence == len(replay.events)

    if "recovery_request" in plan:
        request = read_owned_recovery_request(ownership, RECOVERY_ID)
        receipt = read_owned_recovery_receipt(ownership, RECOVERY_ID)
        assert request.record == model_from_plan(
            plan, "recovery_request", RecoveryRequestRecord
        )
        assert receipt.record == model_from_plan(
            plan, "recovery_receipt", RecoveryReceiptRecord
        )


def execute_stage(base, stage, boundary):
    """Entry point loaded by runpy in a fresh, bounded child process."""
    plan = read_plan(base)
    install_crash_hook(boundary)

    if stage == "run":
        create_run(
            model_from_plan(plan, "site", SiteConfig),
            model_from_plan(plan, "request", RunRequest),
            base / "inputs/flow.json",
            base / "inputs/consumer.whl",
            base / "inputs/runtime.sif",
            run_id=RUN_ID,
        )
    else:
        execute_existing_stage(base, stage, plan)

    if boundary == "returned":
        os._exit(CRASH_STATUS)


def execute_existing_stage(base, stage, plan):
    metadata_actions = {
        "definition": lambda: publish_job_definition(
            base / "runs",
            model_from_plan(plan, "definition", JobDefinitionRecord),
            base / "inputs/job.json",
        ),
        "attempt": lambda: reserve_attempt(
            base / "runs",
            model_from_plan(plan, "attempt", AttemptRecord),
        ),
        "invocation": lambda: register_invocation(
            base / "runs",
            model_from_plan(plan, "invocation", InvocationRecord),
        ),
        "lock": lambda: provision_invocation_lock(*parameters(base)),
        "verify-metadata": lambda: verify_metadata(base, plan),
    }
    if stage in metadata_actions:
        metadata_actions[stage]()
        return

    verify_metadata(base, plan)
    intent = model_from_plan(plan, "intent", PublicationIntentRecord)
    with owned_invocation(*parameters(base)) as ownership:
        owned_actions = {
            "prepare": lambda: prepare_staging(ownership, intent),
            "intent": lambda: publish_owned_publication_intent(ownership, intent),
            "publish": lambda: bundles.publish_owned_bundle(
                ownership, intent.expected_manifest, intent.expected_commit
            ),
            "register": lambda: register_owned_publication_event(ownership, intent),
            "hold": lambda: reject_registration(base, ownership, intent, plan),
            "recover": lambda: recover_owned_bundle(
                ownership,
                model_from_plan(plan, "recovery_request", RecoveryRequestRecord),
                receipt=model_from_plan(
                    plan, "recovery_receipt", RecoveryReceiptRecord
                ),
            ),
            "recovery-register": lambda: register_owned_recovery_event(
                ownership,
                model_from_plan(plan, "recovery_request", RecoveryRequestRecord),
                receipt=model_from_plan(
                    plan, "recovery_receipt", RecoveryReceiptRecord
                ),
            ),
            "verify-final": lambda: verify_final(base, ownership, intent, plan),
        }
        owned_actions[stage]()


def prepare_publication(base):
    run_stage(base, "prepare")
    run_stage(base, "intent")
    return metadata_snapshot(base), bundle_snapshot(base)


def expect_events(base, *, recovered=False):
    plan = read_plan(base)
    plan["expected_events"] = []
    if recovered:
        plan["expected_events"].append([RECOVERY_EVENT_ID, "bundle.recovery_completed"])
    plan["expected_events"].append([INTENT_ID, "bundle.publication_completed"])
    write_plan(base, plan)


@pytest.mark.parametrize("abrupt", [False, True])
def test_metadata_handoffs_and_ordinary_publication(lifecycle, abrupt):
    base = lifecycle
    bootstrap(base, abrupt=abrupt)
    metadata, payloads = prepare_publication(base)
    retained_plan = (base / "plan.json").read_bytes()

    run_stage(base, "publish")
    run_stage(base, "register")
    before = snapshot(base / "runs")
    run_stage(base, "register")
    assert snapshot(base / "runs") == before
    assert (base / "plan.json").read_bytes() == retained_plan

    expect_events(base)
    run_stage(base, "verify-final")
    assert_preserved(metadata, metadata_snapshot(base))
    assert bundle_snapshot(base) == payloads


@pytest.mark.parametrize(
    ("stage", "boundary", "status"),
    [
        ("prepare", "returned", "staging_only"),
        ("publish", "published-directory", "published_uncommitted"),
        ("publish", "marker-staged", "published_uncommitted"),
    ],
)
def test_interrupted_publication_requires_explicit_recovery(
    lifecycle, stage, boundary, status
):
    base = lifecycle
    bootstrap(base)
    if stage == "prepare":
        run_stage(base, "prepare", boundary)
        run_stage(base, "intent")
    else:
        prepare_publication(base)
        run_stage(base, stage, boundary)

    metadata = metadata_snapshot(base)
    payloads = bundle_snapshot(base)
    before = snapshot(base / "runs")
    run_stage(base, "hold")
    assert snapshot(base / "runs") == before
    assert not (run_path(base) / "events").exists()

    candidates = list(invocation_path(base).glob(".bundle-commit-*"))
    candidate_observation = None
    if boundary == "marker-staged":
        assert len(candidates) == 1
        candidate_key = candidates[0].relative_to(base / "runs").as_posix()
        candidate_observation = metadata.pop(candidate_key)
        intent = model_from_plan(read_plan(base), "intent", PublicationIntentRecord)
        assert candidate_observation[3] == encode_bundle_commit(intent.expected_commit)

    retain_recovery(base, status)
    run_stage(base, "recover")
    after_recovery = snapshot(base / "runs")
    run_stage(base, "recover")
    assert snapshot(base / "runs") == after_recovery

    if candidate_observation is not None:
        assert not candidates[0].exists()
        marker = invocation_path(base) / "published/COMMIT.json"
        assert snapshot(marker.parent)["COMMIT.json"] == candidate_observation

    run_stage(base, "recovery-register")
    run_stage(base, "register")
    before_retry = snapshot(base / "runs")
    run_stage(base, "recovery-register")
    run_stage(base, "register")
    assert snapshot(base / "runs") == before_retry

    expect_events(base, recovered=True)
    run_stage(base, "verify-final")
    assert_preserved(metadata, metadata_snapshot(base))
    assert bundle_snapshot(base) == payloads


@pytest.mark.parametrize("boundary", ["returned", "journal-event"])
def test_committed_bundle_resumes_registration_without_republication(
    lifecycle, boundary
):
    base = lifecycle
    bootstrap(base)
    metadata, payloads = prepare_publication(base)

    if boundary == "returned":
        run_stage(base, "publish", boundary)
    else:
        run_stage(base, "publish")
        run_stage(base, "register", boundary)

    pending = run_path(base) / "events/000000000001.json"
    retained_event = (
        (pending.stat().st_ino, pending.read_bytes()) if pending.exists() else None
    )
    run_stage(base, "register")
    if retained_event is not None:
        assert (pending.stat().st_ino, pending.read_bytes()) == retained_event

    before = snapshot(base / "runs")
    run_stage(base, "register")
    assert snapshot(base / "runs") == before
    assert not (invocation_path(base) / "recoveries").exists()

    expect_events(base)
    run_stage(base, "verify-final")
    assert_preserved(metadata, metadata_snapshot(base))
    assert bundle_snapshot(base) == payloads


def test_recovery_receipt_resumes_separate_journal_registration(lifecycle):
    base = lifecycle
    bootstrap(base)
    metadata, payloads = prepare_publication(base)
    retain_recovery(base, "staging_only")

    run_stage(base, "recover", "returned")
    audit = invocation_path(base) / "recoveries" / RECOVERY_ID
    assert (audit / "request.json").is_file()
    assert (audit / "receipt.json").is_file()
    assert not (run_path(base) / "events").exists()

    retained_audit = snapshot(audit)
    run_stage(base, "recovery-register")
    run_stage(base, "register")
    assert snapshot(audit) == retained_audit

    expect_events(base, recovered=True)
    run_stage(base, "verify-final")
    assert_preserved(metadata, metadata_snapshot(base))
    assert bundle_snapshot(base) == payloads


@pytest.mark.parametrize(
    "damage",
    [
        "missing-intent",
        "changed-intent",
        "missing-document",
        "changed-document",
    ],
)
def test_existing_event_does_not_authorize_adoption_of_changed_evidence(
    lifecycle, damage
):
    base = lifecycle
    bootstrap(base)
    prepare_publication(base)
    run_stage(base, "publish")
    run_stage(base, "register")

    invocation = invocation_path(base)
    intent_path = invocation / "publication-intent.json"
    document = invocation / "published/job-document.json"
    plan = read_plan(base)

    if damage == "missing-intent":
        intent_path.rename(invocation / ".publication-intent-retained")
    elif damage == "changed-intent":
        changed = model_from_plan(plan, "intent", PublicationIntentRecord)
        changed = changed.model_copy(update={"intent_id": OTHER_ID})
        intent_path.write_bytes(encode_publication_intent(changed))
        plan["expected_hold"] = "conflict"
    elif damage == "missing-document":
        document.rename(invocation / "retained-document.bin")
    else:
        document.write_bytes(b"changed opaque output")
    write_plan(base, plan)

    before = snapshot(base / "runs")
    run_stage(base, "hold")
    run_stage(base, "hold")
    assert snapshot(base / "runs") == before
    assert not (invocation / "recoveries").exists()

    replay = journal.read_events(base / "runs", RUN_ID)
    assert [(event.event_id, event.event_type) for event in replay.events] == [
        (INTENT_ID, "bundle.publication_completed")
    ]
