"""Metadata identity, deterministic site snapshots, and detached round trips."""

import hashlib
import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.runs.records import (
    ExternalArtifact,
    ExternalArtifacts,
    FlowEnvelope,
    RunManifest,
    SiteSnapshot,
    site_snapshot_bytes,
    snapshot_site,
    validate_run_records,
)
from tests.persistence.runs import _run_support as run_setup

RUN_ID = "00000000-0000-4000-8000-000000000001"

CREATED = "2026-10-02T12:00:00.000000Z"


def site_data():
    return {
        "schema_version": 1,
        "site_id": "example-cluster",
        "slurm": {
            "cluster_name": "example-slurm",
            "account": "example-account",
            "partitions": ["short", "batch"],
            "default_partition": "short",
        },
        "storage": {"provider": "posix", "runs_root": "/shared/example/runs"},
        "worker": {"launch_mode": "apptainer", "apptainer_command": "apptainer"},
    }


def request_data():
    return {
        "schema_version": 1,
        "site_id": "example-cluster",
        "workflow": {
            "name": "toy-flow",
            "serialized_flow_sha256": "a" * 64,
            "consumer_code_sha256": "b" * 64,
        },
        "runtime": {"worker_runtime_sha256": "c" * 64},
        "resources": {
            "partition": "short",
            "nodes": 1,
            "tasks_per_node": 2,
            "cpus_per_task": 1,
            "memory_mb_per_node": 4096,
            "walltime_seconds": 1800,
        },
    }


def manifest_data():
    return {
        "schema_version": 1,
        "kind": "run-manifest",
        "run_id": RUN_ID,
        "created_at": CREATED,
        "backend_version": "0.1.0.dev0",
        "jobflow_version": "0.3.1",
        "request": request_data(),
        "site_snapshot": snapshot_site(SiteConfig.model_validate(site_data())),
    }


def flow_data():
    return {
        "schema_version": 1,
        "kind": "original-flow",
        "run_id": RUN_ID,
        "created_at": CREATED,
        "payload": {"path": "flow/payload.json", "sha256": "a" * 64, "size_bytes": 0},
    }


def test_records_round_trip_and_match():
    manifest = RunManifest.model_validate(manifest_data())
    flow = FlowEnvelope.model_validate(flow_data())
    assert RunManifest.model_validate_json(manifest.model_dump_json()) == manifest
    assert FlowEnvelope.model_validate_json(flow.model_dump_json()) == flow
    assert validate_run_records(manifest, flow) is None
    assert manifest.workspace_expires_at is None
    with pytest.raises(ValidationError, match="frozen"):
        manifest.backend_version = "changed"
    with pytest.raises(ValidationError, match="frozen"):
        flow.payload.size_bytes = 1


@pytest.mark.parametrize("budget", [None, "12.50", "0.000000000000000001"])
def test_request_is_detached_and_budget_round_trips(budget):
    data = request_data()
    data["policy"] = {"allocated_cpu_hour_budget": budget}
    request = RunRequest.model_validate(data)
    values = manifest_data()
    values["request"] = request
    manifest = RunManifest.model_validate(values)
    assert manifest.request is not request
    restored = RunManifest.model_validate_json(manifest.model_dump_json())
    expected = None if budget is None else Decimal(budget)
    assert restored.request.policy.allocated_cpu_hour_budget == expected


def test_site_json_v1_encoding_and_key_order():
    site = SiteConfig.model_validate(site_data())
    reversed_data = {
        key: dict(reversed(tuple(value.items()))) if isinstance(value, dict) else value
        for key, value in reversed(tuple(site_data().items()))
    }
    other = SiteConfig.model_validate(reversed_data)
    payload = site_snapshot_bytes(site)
    assert payload == site_snapshot_bytes(other)
    assert payload == json.dumps(
        site.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    assert not payload.endswith(b"\n")
    assert snapshot_site(site).sha256 == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("change", ["account", "partition-order", "unicode-root"])
def test_site_changes_affect_digest_without_aliasing(change):
    site = SiteConfig.model_validate(site_data())
    snapshot = snapshot_site(site)
    if change == "account":
        site.slurm.account = "other-account"
    elif change == "partition-order":
        site.slurm.partitions.reverse()
    else:
        site.storage.runs_root = "/shared/é/runs"
        assert b"\\u00e9" not in site_snapshot_bytes(site)
    assert snapshot_site(site).sha256 != snapshot.sha256
    assert snapshot.to_site() == SiteConfig.model_validate(site_data())
    detached = snapshot.to_site()
    detached.slurm.partitions.append("gpu")
    assert "gpu" not in snapshot.to_site().slurm.partitions


@pytest.mark.parametrize("bad", ["pretty", "wrong-digest", "duplicate-key", "encoding"])
def test_invalid_snapshot_is_rejected(bad):
    snapshot = snapshot_site(SiteConfig.model_validate(site_data()))
    data = snapshot.model_dump()
    if bad == "pretty":
        data["content"] = json.dumps(site_data(), indent=2)
    elif bad == "wrong-digest":
        data["sha256"] = "0" * 64
    elif bad == "duplicate-key":
        data["content"] = data["content"].replace(
            '"schema_version":1', '"schema_version":1,"schema_version":1', 1
        )
    else:
        data["encoding"] = "site-json-v2"
    with pytest.raises(ValidationError):
        SiteSnapshot.model_validate(data)


@pytest.mark.parametrize(
    "model,fixture", [(RunManifest, manifest_data), (FlowEnvelope, flow_data)]
)
@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_unsupported_or_wrong_type_schema(model, fixture, version):
    data = fixture()
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize(
    "model,fixture", [(RunManifest, manifest_data), (FlowEnvelope, flow_data)]
)
@pytest.mark.parametrize(
    "change", ["unknown", "missing-schema", "wrong-kind", "run-id"]
)
def test_invalid_record_identity(model, fixture, change):
    data = fixture()
    if change == "unknown":
        data["unexpected"] = True
    elif change == "missing-schema":
        del data["schema_version"]
    elif change == "wrong-kind":
        data["kind"] = "other-record"
    else:
        data["run_id"] = "not-a-canonical-uuid4"
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize(
    "timestamp",
    [
        "invalid",
        "2026-10-02T12:00:00",
        "2026-10-02T14:00:00.000000+02:00",
        "2026-10-02T12:00:00Z",
        "2026-10-02T12:00:00.000000+00:00",
    ],
)
def test_noncanonical_creation_timestamp(timestamp):
    data = flow_data()
    data["created_at"] = timestamp
    with pytest.raises(ValidationError):
        FlowEnvelope.model_validate(data)


@pytest.mark.parametrize("expiry", [None, "2026-12-01T12:00:00.000000Z"])
def test_valid_expiry(expiry):
    data = manifest_data()
    data["workspace_expires_at"] = expiry
    manifest = RunManifest.model_validate(data)
    assert manifest.workspace_expires_at == expiry


@pytest.mark.parametrize("expiry", [CREATED, "2026-10-01T12:00:00.000000Z"])
def test_expiry_must_follow_creation(expiry):
    data = manifest_data()
    data["workspace_expires_at"] = expiry
    with pytest.raises(ValidationError, match="later than creation"):
        RunManifest.model_validate(data)


@pytest.mark.parametrize(
    "field,value",
    [
        ("backend_version", ""),
        ("backend_version", " version"),
        ("backend_version", "v\n1"),
        ("jobflow_version", "0.3.2"),
    ],
)
def test_invalid_versions(field, value):
    data = manifest_data()
    data[field] = value
    with pytest.raises(ValidationError):
        RunManifest.model_validate(data)


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", "../payload.json"),
        ("sha256", "A" * 64),
        ("size_bytes", -1),
        ("size_bytes", True),
    ],
)
def test_invalid_payload_reference(field, value):
    data = flow_data()
    data["payload"][field] = value
    with pytest.raises(ValidationError):
        FlowEnvelope.model_validate(data)


@pytest.mark.parametrize("change", ["site", "partition"])
def test_manifest_binding_mismatch(change):
    data = manifest_data()
    if change == "site":
        data["request"]["site_id"] = "other-site"
    else:
        data["request"]["resources"]["partition"] = "gpu"
    with pytest.raises(ValidationError):
        RunManifest.model_validate(data)


@pytest.mark.parametrize("change", ["run_id", "created_at", "sha256"])
def test_cross_record_mismatch(change):
    manifest = RunManifest.model_validate(manifest_data())
    data = flow_data()
    if change == "run_id":
        data["run_id"] = "00000000-0000-4000-8000-000000000002"
    elif change == "created_at":
        data["created_at"] = "2026-10-02T12:00:01.000000Z"
    else:
        data["payload"]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="does not match"):
        validate_run_records(manifest, FlowEnvelope.model_validate(data))


def test_unsafe_model_copy_is_revalidated():
    manifest = RunManifest.model_validate(manifest_data())
    unsafe = manifest.model_copy(update={"schema_version": 2})
    with pytest.raises(ValidationError, match="unsupported record"):
        validate_run_records(unsafe, FlowEnvelope.model_validate(flow_data()))


def test_unsafe_request_copy_is_revalidated():
    data = manifest_data()
    request = RunRequest.model_validate(request_data())
    data["request"] = request.model_copy(update={"site_id": "other-site"})
    with pytest.raises(ValidationError, match="site_id"):
        RunManifest.model_validate(data)


def test_unsafe_snapshot_copy_is_revalidated():
    data = manifest_data()
    data["site_snapshot"] = data["site_snapshot"].model_copy(
        update={"sha256": "0" * 64}
    )
    with pytest.raises(ValidationError, match="SHA-256 mismatch"):
        RunManifest.model_validate(data)


def test_mutated_site_is_revalidated_before_snapshot():
    site = SiteConfig.model_validate(site_data())
    site.slurm.partitions.append("short")
    with pytest.raises(ValidationError, match="duplicates"):
        snapshot_site(site)


@pytest.mark.parametrize(
    "path", ["/", "relative/file", "/a//b", "/a/../b", "/a/", "/a\nb"]
)
def test_external_reference_path_validation(path):
    with pytest.raises(ValidationError):
        ExternalArtifact(path=path, sha256="a" * 64, size_bytes=0)


def test_external_reference_models_are_strict_and_frozen(run_inputs):
    handle = run_setup.create(run_inputs)
    record = handle.artifacts
    assert ExternalArtifacts.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(ValidationError, match="frozen"):
        record.consumer_code.size_bytes = 1
    for value in (True, "1", 2):
        data = record.model_dump(mode="json")
        data["schema_version"] = value
        with pytest.raises(ValidationError):
            ExternalArtifacts.model_validate(data)
