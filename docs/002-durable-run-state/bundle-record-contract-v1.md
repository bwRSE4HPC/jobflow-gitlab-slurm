# Bundle manifest and completion-marker contract v1

Status (2026-10-08): **model-only slice approved and installed; full local
acceptance verification user-reported passing**. This refines the approved
[attempt record contract](attempt-record-contract-v1.md), not its execution
or recovery interfaces. Work remains in cycle
[002-durable-run-state](../002-durable-run-state.md).

## Scope

Supply strict immutable models, deterministic encoding/decoding, and pure
cross-record validation for an invocation bundle. There is no filesystem I/O,
publication/finalization API, CLI change, JobStore adapter, scientific execution,
or scheduler lookup in this slice. Existing schemas remain unchanged.

A model-valid manifest declares required evidence. A model-valid marker binds
that declaration. Neither proves that the files exist, are valid jobflow
objects, or have been durably published. Actual bundle integrity, jobflow
semantic validity, scheduler outcome, and selected result are separate checks.

## Path namespaces and artifact roles

Define `BundleArtifactReference` with safe POSIX `path`, lowercase SHA-256,
and nonnegative strict integer `size_bytes`. It uses the existing lexical
path rules, but its path is relative to the invocation's published bundle,
not the run root. No absolute path, traversal, empty/normalized component,
backslash, NUL, or unencodable UTF-8 is accepted. Storage readers must separately
reject unsafe file types and path components; lexical validation does not
establish filesystem safety.

For an invocation, the logical publication path is:

```text
jobs/<job_key>/index-<job_index>/attempts/<attempt_id>/
  invocations/<invocation_id>/published/
```

The paths below describe the intended layout, not installed directories:

```text
published/
  job-document.json
  response.json
  files/...                   declared application files
  data/...                    declared additional-store payloads
  payload-manifest.json
  COMMIT.json
```

Required document/Response references have fixed paths `job-document.json`
and `response.json` and positive byte counts. A legitimate null output still
requires a nonempty serialized document and full Response. Missing references
or empty required JSON artifacts are rejected; payload contents are opaque
to these models.

Application references must lie strictly below `files/`. Additional-store
payload references must lie strictly below `data/`. Every declared payload
path is unique; no declared file path may be an ancestor of another file
path. A declared payload cannot alias the top-level manifest or marker.
The model does not assert that an inventory is exhaustive: future worker and
reader contracts must detect undeclared required files/data and unsafe entries.

## Additional-store payload references

`AdditionalDataReference` is a frozen nested model with:

- `store_name`: exact nonempty UTF-8-encodable string;
- `blob_uuid`: exact nonempty UTF-8-encodable string;
- `artifact`: a bundle-relative reference below `data/`.

Names are not normalized or used directly to construct paths. Store/blob
identity is the pair `(store_name, blob_uuid)` and must be unique within a
manifest. This slice pins exact opaque bytes; it does not define their jobflow
serialization or implement store access. The Stage 3 adapter must verify blob
identity, job association, and reference resolution before exposing a result.
Representing an empty additional-data inventory does not prove that a document
contains no blob references. Unsupported store configurations must still fail
explicitly before job execution.

## BundleManifest (`kind: bundle-manifest`)

Required fields:

- `schema_version: 1`, `kind`, `created_at` with existing canonical UTC format;
- `run_id`, exact `job_uuid`, positive strict `job_index`, validated `job_key`;
- `attempt_id`, `invocation_id`, and `definition_id` (backend UUIDv4 strings);
- `definition_sha256`, `consumer_code_sha256`, `worker_runtime_sha256`;
- `jobflow_version: "0.3.1"`;
- `scheduler_receipt`: a **run-relative** `RecordArtifactReference` at
  `jobs/<job_key>/index-<job_index>/attempts/<attempt_id>/slurm-receipt.json`,
  with positive size, binding this bundle to a separate scheduler receipt;
- `document` and `response`: the required bundle-relative references above;
- `files`: an explicitly supplied immutable collection, which may be empty;
- `additional_data`: an explicitly supplied immutable collection, which may
  be empty.

All inventory entries are immutable nested models. Python construction uses
tuples; JSON uses arrays. `files` must be ordered lexicographically by path;
`additional_data` must be ordered lexicographically by artifact path. Wrong
ordering is rejected rather than silently reordered. Metadata can therefore
have a deterministic representation without altering opaque payload bytes.

The scheduler reference is a claim about a receipt's identity and bytes, not
proof of Slurm acceptance or terminal success. A later loader/reconciler must
verify the referenced record and match the actual scheduler association.
Offline tests can use inert receipt metadata without fabricating a live job.
Backend-version compatibility remains pinned by the run manifest; checking
that manifest is a loader responsibility, not established by this model alone.

## BundleCommit (`kind: bundle-commit`)

Required fields:

- `schema_version: 1`, `kind`, caller-supplied canonical UTC `created_at`;
- `run_id`, exact `job_uuid`, positive strict `job_index`, validated `job_key`;
- `attempt_id` and `invocation_id`;
- `manifest`: bundle-relative reference with fixed path
  `payload-manifest.json`, positive byte count, and SHA-256 of the exact
  canonical manifest bytes, including its full envelope and inventories.

Do not add a `success: true` field: marker validity is not jobflow or scheduler
success. The marker has no self-referential checksum. A future journal record
may bind its exact bytes; checksum binding is integrity evidence, not a
signature, authenticated timestamp, or protection against coordinated rewrite.

## Encoding and pure validation

Use `bundle-json-v1` for backend manifest/marker bytes: UTF-8 JSON, sorted
object keys, compact separators, unescaped Unicode, no nonfinite numbers,
and no trailing newline. Arrays retain their required order. There is no Monty
decoding, consumer import, or opaque-payload normalization. This is a project
encoding, not a claim of RFC canonical-JSON compliance.

Approved helpers:

- `encode_bundle_manifest` and `encode_bundle_commit`: revalidate models,
  then return canonical bytes;
- `decode_bundle_manifest` and `decode_bundle_commit`: accept bytes, reject
  malformed/duplicate-key/nonfinite JSON and unknown/wrong-type fields or
  versions, validate models, and require the exact canonical byte representation;
- `validate_bundle_records(definition, attempt, invocation, manifest, commit)`:
  revalidate all five records, reuse `validate_attempt_records`, and compare
  all run/job/attempt/invocation identities, selected definition ID/digest,
  code/runtime digests, and jobflow version. Require nondecreasing creation
  times: invocation, manifest, marker, allowing equality. Re-encode the manifest
  and compare its exact size and SHA-256 with the marker's reference.

The helper receives both records: a standalone marker model cannot prove that
its referenced manifest agrees. Even a matching pair does not verify payload
files, scheduler receipts, ownership, jobflow semantics, or publication. Retry
identity, bundle finalization, writer exclusion, and durable acknowledgment
belong to the later publication slice.

## Files, risks, and acceptance

Approved affected files:

- new `src/jobflow_gitlab_slurm/persistence/bundles/records.py`;
- new `tests/persistence/bundles/test_records.py`;
- one new module import in `ci/check-wheel.sh`;
- documentation updates recording approval, user application, and evidence.

No dependency/lockfile or existing source/test changes are intended.

Acceptance checks:

- Strict versions/types/IDs/timestamps and frozen/detached nested models;
  Python and JSON round trips; Unicode identity preservation.
- Both required references are present and nonempty; empty optional
  inventories are explicit and valid.
- Reject path traversal, wrong namespaces, duplicate paths, file/descendant
  collisions, duplicate store/blob identities, and unordered inventories.
- Reject manifest/marker and attempt identity/digest/version/time mismatches;
  reject unsafe model copies through revalidation.
- Verify exact canonical encoding, independent manifest digest/size calculation,
  and rejection of duplicate JSON keys, noncanonical bytes, malformed/nonfinite
  JSON, unknown fields, and unsupported schemas.
- Preserve current tests/CLI behavior; pass the existing local coverage and
  packaging gates after user application. No result can be advertised as
  execution-ready merely because these model checks pass.

Main risks are confusing the two path namespaces, assuming declarations prove
semantic completeness, and treating a marker as success or authorization.
Tests and docstrings must keep these boundaries explicit. Remote CI, actual
bundle publication/crash recovery, additional-store semantics, and cross-host
durability remain separate gates.

## Verification status (2026-10-08)

The user reports the full local acceptance gate passed: tests, the agreed
100% combined statement/branch coverage gate, Ruff formatting/lint, CLI help,
shell syntax, wheel/sdist build, isolated wheel verification, and diff checks.
The successful final test count was not supplied; the assistant did not
independently repeat this successful verification.

This completes the model/codec checkpoint, not Stage 2. Packaging and pure
metadata validation do not establish execution compatibility. Remote CI,
bundle publication/recovery, and live filesystem gates remain pending.
