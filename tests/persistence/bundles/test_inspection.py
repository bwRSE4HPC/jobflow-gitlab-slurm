"""persistence / bundles / test_inspection contracts."""

import builtins
import json
import os
import socket
import stat
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    InvocationLockIntegrityError,
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    job_key,
)
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    AttemptIntegrityError,
)
from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    inspect_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    locked_run,
)
from tests.persistence.bundles import _inspection_support as inspection_setup


def test_absent_is_not_execution_evidence(bundle_invocation):
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "absent"
    assert report.content_integrity == "not_evaluated"
    assert report.manifest_present is False
    assert report.commit_present is False
    assert not report.issues
    data = report.to_report()
    assert data["evidence"]["inspected_path"] is None
    assert data["jobflow_semantics"] == "not_evaluated"
    assert data["scheduler_state"] == "not_evaluated"
    assert data["result_eligibility"] == "not_evaluated"


@pytest.mark.parametrize(
    ("name", "marker", "status"),
    [
        ("staging", False, "staging_only"),
        ("published", False, "published_uncommitted"),
        ("published", True, "committed"),
    ],
)
@pytest.mark.parametrize("optional", [False, True])
def test_valid_evidence_classifications(
    bundle_invocation, name, marker, status, optional
):
    bundle = inspection_setup.make_bundle(
        bundle_invocation, name, marker=marker, optional=optional
    )
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == status
    assert report.content_integrity == "valid"
    assert report.manifest_present is True
    assert report.commit_present is marker
    assert report.inspected_path == bundle.unit
    assert not report.issues
    assert report.to_report()["result_eligibility"] == "not_evaluated"


@pytest.mark.parametrize("name", ["staging", "published"])
@pytest.mark.parametrize("marker", [False, True])
def test_missing_manifest(bundle_invocation, name, marker):
    inspection_setup.make_bundle(bundle_invocation, name, manifest=False, marker=marker)
    report = inspection_setup.inspect(bundle_invocation)
    expected = (
        "invalid"
        if marker
        else ("staging_only" if name == "staging" else "published_uncommitted")
    )
    assert report.status == expected
    assert report.content_integrity == ("invalid" if marker else "incomplete")
    assert inspection_setup.issue_codes(report) == (
        {"marker_in_staging"} if name == "staging" and marker else {"manifest_missing"}
    )


def test_staging_marker_is_invalid_even_with_valid_bytes(bundle_invocation):
    inspection_setup.make_bundle(bundle_invocation, "staging")
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert report.commit_present is True
    assert inspection_setup.issue_codes(report) == {"marker_in_staging"}


def test_both_directories_are_ambiguous_without_content_inspection(
    bundle_invocation, monkeypatch
):
    inspection_setup.make_bundle(bundle_invocation, "staging", marker=False)
    inspection_setup.make_bundle(bundle_invocation)
    (bundle_invocation.invocation.path / "staging" / "unsafe").symlink_to(
        bundle_invocation.receipt
    )

    def forbidden(*args, **kwargs):
        pytest.fail("ambiguous inventories must not be inspected")

    monkeypatch.setattr(inspection, "_inventory", forbidden)
    monkeypatch.setattr(inspection, "_decode", forbidden)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "ambiguous"
    assert report.content_integrity == "not_evaluated"
    assert report.staging_kind == "directory"
    assert report.published_kind == "directory"
    assert report.manifest_present is None
    assert report.commit_present is None
    assert report.inspected_path is None
    assert not report.issues


@pytest.mark.parametrize(
    "name",
    [
        "job-document.json",
        "response.json",
        "files/empty.bin",
        "files/nested/result-é.txt",
        "data/blob.bin",
        "receipt",
    ],
)
@pytest.mark.parametrize("marker", [False, True])
def test_missing_declared_artifacts(bundle_invocation, name, marker):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=marker)
    target = bundle_invocation.receipt if name == "receipt" else bundle.unit / name
    target.unlink()
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == ("invalid" if marker else "published_uncommitted")
    assert report.content_integrity == ("invalid" if marker else "incomplete")
    assert inspection_setup.issue_codes(report) == {"artifact_missing"}


def test_missing_necessary_directory_is_incomplete_without_marker(
    bundle_invocation,
):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=False)
    (bundle.unit / "files/nested/result-é.txt").unlink()
    (bundle.unit / "files/nested").rmdir()
    report = inspection_setup.inspect(bundle_invocation)
    assert report.content_integrity == "incomplete"
    assert inspection_setup.issue_codes(report) == {"artifact_missing"}


@pytest.mark.parametrize(
    "name",
    [
        "job-document.json",
        "response.json",
        "files/empty.bin",
        "files/nested/result-é.txt",
        "data/blob.bin",
        "receipt",
    ],
)
@pytest.mark.parametrize("marker", [False, True])
def test_changed_declared_bytes(bundle_invocation, name, marker):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=marker)
    target = bundle_invocation.receipt if name == "receipt" else bundle.unit / name
    target.write_bytes(b"changed bytes")
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert report.content_integrity == "invalid"
    assert "artifact_bytes_invalid" in inspection_setup.issue_codes(report)


def test_invalid_bytes_take_precedence_over_missing_bytes(bundle_invocation):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=False)
    (bundle.unit / "response.json").unlink()
    bundle_invocation.receipt.write_bytes(b"wrong receipt bytes")
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert report.content_integrity == "invalid"
    assert inspection_setup.issue_codes(report) == {
        "artifact_missing",
        "artifact_bytes_invalid",
    }


@pytest.mark.parametrize("role", ["document", "response", "scheduler_receipt"])
def test_declared_size_must_match_even_when_hash_matches(bundle_invocation, role):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    data = bundle.manifest.model_dump(mode="json")
    data[role]["size_bytes"] += 1
    inspection_setup.write_manifest(bundle, data, rebind=True)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"artifact_size_mismatch"}


def test_large_opaque_payload_is_verified(bundle_invocation):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    data = b"x" * (2 * 1024 * 1024 + 3)
    name = "files/nested/result-é.txt"
    (bundle.unit / name).write_bytes(data)
    manifest = bundle.manifest.model_dump(mode="json")
    manifest["files"][1] = inspection_setup.reference(name, data)
    inspection_setup.write_manifest(bundle, manifest, rebind=True)
    assert inspection_setup.inspect(bundle_invocation).status == "committed"


@pytest.mark.parametrize("marker", [False, True])
@pytest.mark.parametrize(
    "field",
    [
        "run_id",
        "job_uuid",
        "job_index",
        "attempt_id",
        "invocation_id",
        "definition_id",
        "definition_sha256",
        "consumer_code_sha256",
        "worker_runtime_sha256",
        "created_at",
    ],
)
def test_manifest_binding_mismatches(bundle_invocation, marker, field):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=marker)
    data = bundle.manifest.model_dump(mode="json")
    if field == "job_uuid":
        data[field] = "different-job"
        data["job_key"] = job_key(data[field])
    elif field == "job_index":
        data[field] = 2
    elif field.endswith("sha256"):
        data[field] = "f" * 64
    elif field == "created_at":
        data[field] = inspection_setup.timestamp(bundle_invocation.run, 1)
    else:
        data[field] = inspection_setup.OTHER_ID
    data["scheduler_receipt"]["path"] = (
        f"jobs/{data['job_key']}/index-{data['job_index']}/attempts/"
        f"{data['attempt_id']}/slurm-receipt.json"
    )
    inspection_setup.write_manifest(bundle, data, rebind=marker)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"bundle_binding_invalid"}


@pytest.mark.parametrize(
    "field",
    ["run_id", "job_uuid", "job_index", "attempt_id", "invocation_id", "created_at"],
)
def test_commit_identity_and_time_mismatches(bundle_invocation, field):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    data = bundle.commit.model_dump(mode="json")
    if field == "job_uuid":
        data[field] = "different-job"
        data["job_key"] = job_key(data[field])
    elif field == "job_index":
        data[field] = 2
    elif field == "created_at":
        data[field] = inspection_setup.timestamp(bundle_invocation.run, 2)
    else:
        data[field] = inspection_setup.OTHER_ID
    bundle.commit_path.write_bytes(inspection_setup.canonical(data))
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"bundle_binding_invalid"}


@pytest.mark.parametrize("field", ["sha256", "size_bytes"])
def test_commit_binds_actual_manifest_bytes(bundle_invocation, field):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    data = bundle.commit.model_dump(mode="json")
    data["manifest"][field] = (
        "f" * 64 if field == "sha256" else data["manifest"]["size_bytes"] + 1
    )
    bundle.commit_path.write_bytes(inspection_setup.canonical(data))
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"commit_manifest_mismatch"}


@pytest.mark.parametrize("record", ["manifest", "commit"])
@pytest.mark.parametrize(
    "damage",
    ["newline", "invalid_json", "duplicate_key", "unsupported_version", "secret"],
)
def test_invalid_metadata_is_sanitized(bundle_invocation, record, damage):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    path = bundle.manifest_path if record == "manifest" else bundle.commit_path
    original = path.read_bytes()
    if damage == "newline":
        encoded = original + b"\n"
    elif damage == "invalid_json":
        encoded = b"{"
    elif damage == "duplicate_key":
        encoded = b'{"schema_version":1,"schema_version":1}'
    elif damage == "unsupported_version":
        data = json.loads(original)
        data["schema_version"] = 999
        encoded = inspection_setup.canonical(data)
    else:
        encoded = b'{"PRIVATE_EXCEPTION_PAYLOAD":"secret",'
    path.write_bytes(encoded)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"invalid_metadata"}
    assert "PRIVATE_EXCEPTION_PAYLOAD" not in json.dumps(report.to_report())


def test_unsupported_manifest_jobflow_version_is_invalid(bundle_invocation):
    bundle = inspection_setup.make_bundle(bundle_invocation, marker=False)
    data = bundle.manifest.model_dump(mode="json")
    data["jobflow_version"] = "0.4.0"
    inspection_setup.write_manifest(bundle, data)
    assert inspection_setup.issue_codes(
        inspection_setup.inspect(bundle_invocation)
    ) == {"invalid_metadata"}


@pytest.mark.parametrize(
    ("name", "directory"),
    [
        ("extra.txt", False),
        (".publication-tmp", False),
        ("extra-directory", True),
        ("files/unused", True),
        ("data/unused", True),
        ("job-document.json", True),
        ("payload-manifest.json/unused", False),
    ],
)
def test_inventory_is_exhaustive(bundle_invocation, name, directory):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    target = bundle.unit / name
    if name == "job-document.json":
        target.unlink()
    if name == "payload-manifest.json/unused":
        # A required metadata file replaced by a directory is also invalid.
        bundle.manifest_path.unlink()
        bundle.manifest_path.mkdir()
    if directory:
        target.mkdir()
    else:
        target.write_bytes(b"undeclared")
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert report.content_integrity == "invalid"
    assert inspection_setup.issue_codes(report) <= {
        "inventory_mismatch",
        "invalid_metadata",
    }


@pytest.mark.parametrize(
    "name",
    [
        "job-document.json",
        "payload-manifest.json",
        "COMMIT.json",
        "files/nested",
        "files/empty.bin",
        "data/blob.bin",
    ],
)
def test_symlink_entries_are_rejected_without_following(bundle_invocation, name):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    target = bundle.unit / name
    if target.is_dir():
        (target / "result-é.txt").unlink()
        target.rmdir()
    else:
        target.unlink()
    target.symlink_to(bundle_invocation.attempt.path)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"unsafe_entry"}


@pytest.mark.parametrize("special", ["fifo", "socket", "device"])
def test_special_entries_are_rejected_without_opening(
    bundle_invocation, special, monkeypatch
):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    target = bundle.unit / "special"
    if special == "fifo":
        os.mkfifo(target)
        assert inspection_setup.issue_codes(
            inspection_setup.inspect(bundle_invocation)
        ) == {"unsafe_entry"}
    elif special == "socket":
        with (
            monkeypatch.context() as context,
            socket.socket(socket.AF_UNIX) as endpoint,
        ):
            context.chdir(bundle.unit)
            endpoint.bind("special")
            assert inspection_setup.issue_codes(
                inspection_setup.inspect(bundle_invocation)
            ) == {"unsafe_entry"}
    else:
        target.write_bytes(b"inert device stand-in")
        original = Path.lstat

        def lstat(path, *args, **kwargs):
            if path == target:
                return SimpleNamespace(st_mode=stat.S_IFCHR)
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "lstat", lstat)
        assert inspection_setup.issue_codes(
            inspection_setup.inspect(bundle_invocation)
        ) == {"unsafe_entry"}


def test_without_manifest_only_structure_is_evaluated(bundle_invocation):
    bundle = inspection_setup.make_bundle(
        bundle_invocation, manifest=False, marker=False
    )
    (bundle.unit / "unknown-empty-directory").mkdir()
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "published_uncommitted"
    assert report.content_integrity == "incomplete"
    assert inspection_setup.issue_codes(report) == {"manifest_missing"}


@pytest.mark.parametrize("entry_type", ["directory", "symlink"])
def test_receipt_must_be_a_regular_file(bundle_invocation, entry_type):
    inspection_setup.make_bundle(bundle_invocation)
    bundle_invocation.receipt.unlink()
    if entry_type == "directory":
        bundle_invocation.receipt.mkdir()
    else:
        bundle_invocation.receipt.symlink_to(
            bundle_invocation.definition.path / "job.json"
        )
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert inspection_setup.issue_codes(report) == {"unsafe_artifact_path"}


@pytest.mark.parametrize("location", ["run", "attempt"])
@pytest.mark.parametrize("kind", ["absent", "file", "unsafe"])
def test_receipt_parent_components_are_checked(
    bundle_invocation, location, kind, monkeypatch
):
    inspection_setup.make_bundle(bundle_invocation, marker=False)
    target = (
        bundle_invocation.run.path
        if location == "run"
        else bundle_invocation.attempt.path
    )
    original = inspection._kind

    with owned_invocation(*bundle_invocation.parameters) as ownership:

        def observed_kind(path):
            if path == target:
                return kind
            return original(path)

        monkeypatch.setattr(inspection, "_kind", observed_kind)
        report = inspect_owned_bundle(ownership)

    if kind == "absent":
        assert report.content_integrity == "incomplete"
        assert inspection_setup.issue_codes(report) == {"artifact_missing"}
    else:
        assert report.content_integrity == "invalid"
        assert inspection_setup.issue_codes(report) == {"unsafe_artifact_path"}


def test_missing_lock_is_not_created(bundle_invocation):
    lock = bundle_invocation.invocation.path / ".bundle.lock"
    lock.unlink()
    with pytest.raises(InvocationLockIntegrityError):
        inspection_setup.inspect(bundle_invocation)
    assert not lock.exists()


def test_invalid_parent_metadata_retains_existing_error_type(bundle_invocation):
    (bundle_invocation.invocation.path / "invocation.json").write_bytes(b"{")
    with pytest.raises(AttemptIntegrityError):
        inspection_setup.inspect(bundle_invocation)


def test_invocation_contention_propagates(bundle_invocation):
    with (
        owned_invocation(*bundle_invocation.parameters),
        pytest.raises(InvocationBusyError),
    ):
        inspection_setup.inspect(bundle_invocation)


def test_run_contention_propagates(bundle_invocation):
    with (
        locked_run(bundle_invocation.root, inspection_setup.RUN_ID),
        pytest.raises(RunBusyError),
    ):
        inspection_setup.inspect(bundle_invocation)
    # The failed acquisition must not leave invocation ownership retained.
    assert inspection_setup.inspect(bundle_invocation).status == "absent"


def test_expired_ownership_is_rejected(bundle_invocation):
    with owned_invocation(*bundle_invocation.parameters) as ownership:
        assert inspect_owned_bundle(ownership).status == "absent"
    with pytest.raises(InvocationOwnershipError):
        inspect_owned_bundle(ownership)


def test_foreign_process_ownership_is_rejected(bundle_invocation):
    with owned_invocation(*bundle_invocation.parameters) as ownership:
        original = ownership._lease.pid
        ownership._lease.pid = -1
        try:
            with pytest.raises(InvocationOwnershipError):
                inspect_owned_bundle(ownership)
        finally:
            ownership._lease.pid = original


def test_owned_helper_does_not_reacquire_locks(bundle_invocation, monkeypatch):
    inspection_setup.make_bundle(bundle_invocation)

    def forbidden(*args, **kwargs):
        pytest.fail("owned inspection must not reacquire ownership")

    with owned_invocation(*bundle_invocation.parameters) as ownership:
        monkeypatch.setattr(inspection, "owned_invocation", forbidden)
        assert inspect_owned_bundle(ownership).status == "committed"


def test_payload_verification_holds_invocation_but_not_run_lock(
    bundle_invocation, monkeypatch
):
    inspection_setup.make_bundle(bundle_invocation)
    original = inspection.verify_artifact
    checked = []

    def verify(path, expected):
        with locked_run(bundle_invocation.root, inspection_setup.RUN_ID):
            checked.append(Path(path))
        with pytest.raises(InvocationBusyError):
            inspection_setup.inspect(bundle_invocation)
        return original(path, expected)

    monkeypatch.setattr(inspection, "verify_artifact", verify)
    assert inspection_setup.inspect(bundle_invocation).status == "committed"
    assert len(checked) == 6


def test_inspection_does_not_flush_or_mutate_evidence(bundle_invocation, monkeypatch):
    inspection_setup.make_bundle(bundle_invocation)
    before = inspection_setup.snapshot(bundle_invocation.root)

    def forbidden(*args, **kwargs):
        pytest.fail("read-only inspection must not mutate or flush evidence")

    monkeypatch.setattr(os, "fsync", forbidden)
    monkeypatch.setattr(os, "rename", forbidden)
    monkeypatch.setattr(os, "replace", forbidden)
    assert inspection_setup.inspect(bundle_invocation).status == "committed"
    assert inspection_setup.inspect(bundle_invocation).status == "committed"
    assert inspection_setup.snapshot(bundle_invocation.root) == before


def test_opaque_payloads_do_not_import_consumer_code(bundle_invocation, monkeypatch):
    inspection_setup.make_bundle(bundle_invocation)
    original = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert not name.startswith("do_not_import")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "committed"
    assert report.to_report()["jobflow_semantics"] == "not_evaluated"


def test_report_is_frozen_detached_and_json_compatible(bundle_invocation):
    inspection_setup.make_bundle(bundle_invocation)
    report = inspection_setup.inspect(bundle_invocation)
    with pytest.raises(FrozenInstanceError):
        report.status = "invalid"
    data = report.to_report()
    assert json.loads(json.dumps(data)) == data
    assert data["identity"]["job_uuid"] == inspection_setup.JOB_UUID
    assert "inert Response bytes" not in json.dumps(data)
    data["identity"]["run_id"] = inspection_setup.OTHER_ID
    data["evidence"]["published"]["kind"] = "unsafe"
    data["issues"].append({"code": "fabricated", "path": "/"})
    fresh = report.to_report()
    assert fresh["identity"]["run_id"] == inspection_setup.RUN_ID
    assert fresh["evidence"]["published"]["kind"] == "directory"
    assert not fresh["issues"]


def test_issue_reports_are_detached(bundle_invocation):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    (bundle.unit / "response.json").unlink()
    report = inspection_setup.inspect(bundle_invocation)
    data = report.to_report()
    data["issues"][0]["code"] = "changed"
    assert report.to_report()["issues"][0]["code"] == "artifact_missing"


def test_independent_process_reads_committed_evidence(bundle_invocation, tmp_path):
    inspection_setup.make_bundle(bundle_invocation)
    result = inspection_setup.child_inspection(bundle_invocation, tmp_path)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "committed"
    assert report["identity"]["invocation_id"] == inspection_setup.INVOCATION_ID
    assert report["scheduler_state"] == "not_evaluated"


def test_independent_process_cannot_inspect_owned_invocation(
    bundle_invocation, tmp_path
):
    inspection_setup.make_bundle(bundle_invocation)
    with owned_invocation(*bundle_invocation.parameters):
        result = inspection_setup.child_inspection(bundle_invocation, tmp_path)
    assert result.returncode == 3, result.stderr
    assert result.stdout.strip() == "InvocationBusyError"


def test_independent_process_observes_run_contention(bundle_invocation, tmp_path):
    with locked_run(bundle_invocation.root, inspection_setup.RUN_ID):
        result = inspection_setup.child_inspection(bundle_invocation, tmp_path)
    assert result.returncode == 3, result.stderr
    assert result.stdout.strip() == "RunBusyError"
