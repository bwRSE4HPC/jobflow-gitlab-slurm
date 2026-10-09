# Record contract v1

Status: **scope and site-json-v1 encoding approved on 2026-10-02; models
implemented and user-verified locally**. This slice
defines metadata, not run creation, atomic publication, or locking.

## Common envelope fields

`schema_version` is the required integer `1`; missing, unknown, and wrong-type
versions are rejected. `kind` is the record discriminator. `run_id` is a
canonical lowercase UUIDv4 string; the creation caller generates it once and
retains it across interruptions.
`created_at` uses the UTC format `YYYY-MM-DDTHH:MM:SS.ffffffZ`. IDs and
timestamps are supplied to these models, not generated as a side effect.
Unknown fields are rejected and models disallow field reassignment.

## Original-flow envelope

`FlowEnvelope` describes `flow/original.json`, with kind `original-flow` and
the common envelope fields. Its `payload` reference has exactly these fields:

- `path`: fixed `flow/payload.json`, relative to the run root;
- `sha256`: lowercase SHA-256 of the exact payload bytes;
- `size_bytes`: nonnegative integer.

The envelope does not parse or reserialize the payload, verify its bytes, or
claim that it is executable jobflow JSON. Those are separate boundaries.

## Site snapshot and site-json-v1

`SiteSnapshot` stores `encoding: site-json-v1`, canonical JSON text in
`content`, and its lowercase SHA-256. The content is a string rather than a
mutable nested site object. `to_site()` returns a fresh validated `SiteConfig`.
Mutating the caller's site or the returned copy cannot change the snapshot.

Encoding is exactly:

```python
json.dumps(
    validated_site.model_dump(mode="json"),
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
    allow_nan=False,
).encode("utf-8")
```

No newline is added. Dictionary key order is normalized; list order is
preserved. The source site is revalidated before encoding. Loading a snapshot
checks its schema, exact canonical content, and digest. Noncanonical text,
including duplicate keys, is rejected. This is a project-specific encoding,
not a claim of RFC canonical-JSON compliance.

## Run manifest

`RunManifest` describes `run.json`, with kind `run-manifest` and the common
envelope fields. It additionally carries:

- `backend_version`: nonempty version label without whitespace;
- `jobflow_version`: the initial compatibility target `0.3.1`;
- `request`: a detached and revalidated `RunRequest`;
- `site_snapshot`: the validated `SiteSnapshot`;
- `workspace_expires_at`: optional canonical UTC timestamp, later than
  creation when provided; `null`/omitted means expiry is not recorded.

The request must match the snapshot's site ID and permitted partition. Budget
serialization retains the existing exact plain-decimal contract. Backend
version compatibility is checked separately by the storage loader. Runtime
paths are recorded separately in external-artifact references; the manifest
model itself does not establish filesystem availability.

## Cross-record checks and limits

### External artifact references

Approved on 2026-10-07; implementation installed by the user and tests reported
passing, followed by confirmation of coverage and packaging checks.
`artifacts/references.json` uses schema version
`1`, kind `external-artifacts`, and the same `run_id`/`created_at` as the manifest.
Its `consumer_code` and `worker_runtime` references each contain a canonical
absolute POSIX `path`, lowercase `sha256`, and nonnegative integer `size_bytes`.
Hashes must match the corresponding immutable request fields. These artifacts
are verified at creation but not copied into each run. Their paths must remain
available to the relevant controller/worker, and bytes must be reverified before
execution; a recorded reference does not guarantee retention or immutability.

The storage loader rejects duplicate JSON keys, nonfinite constants, unsafe
file types, incompatible schemas, and inconsistent identities. Initial backend
compatibility is an exact match to the installed package version, without
automatic migration. A read-only reopen checks stored Flow bytes and metadata;
external verification is an explicit option to avoid repeatedly hashing a large
runtime image just to inspect a run. Reopening does not prove executable Flow
validity, workspace lifetime, scheduler access, or cross-host filesystem safety.

`validate_run_records` revalidates both records and requires matching run IDs,
creation timestamps, and workflow payload/request digests. A digest field or
byte count is still a claim until artifact verification checks actual bytes.

Frozen models describe records but do not provide on-disk immutability.
Initial run publication, locking, reopening, and local interrupted-creation
tests are implemented in the separate storage slice. Opaque result-bundle
publication recovery is implemented separately in cycle 002; worker execution,
parse-only scientific repair, and cross-host verification remain later gates.
No workflow callables are
imported or executed by these models.
