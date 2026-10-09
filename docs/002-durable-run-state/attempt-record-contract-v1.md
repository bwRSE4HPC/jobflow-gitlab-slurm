# Job, attempt, invocation, and result-bundle contract v1

Status (2026-10-08): **contract approved; identity/provenance models installed;
local verification user-reported passing**. The user approved the
persistence/worker/scheduler boundary and then this contract on 2026-10-08.
Read-only inspection confirms the models, tests, and wheel import entry are
present. Bundle models are installed under the separate
[bundle record contract](bundle-record-contract-v1.md); their full local
acceptance gate is user-reported passing. Original-Flow
[job-definition storage](definition-storage-v1.md) is installed and locally
user-verified. Attempt/invocation storage is installed with assistant-executed
tests, coverage, and packaging passing; final local acceptance is user-reported
passing.
Bundle inspection/publication, retained intent/audit storage, explicit recovery,
and journal association are installed in separate cycle 002 slices. Their full
local gate was assistant-verified on 2026-10-09; see the
[review record](review-and-verification.md).
This contract extends the
[run-state design](run-state-v1.md) and preserves
[Decision 0002](../decisions/0002-attempt-publication-and-recovery.md).

## Scope and implementation order

Stage 2 supplies durable identities, record envelopes, result-bundle storage,
and read-only inspection. Stage 3 supplies the jobflow store adapter, payload
serialization/semantic validation, and worker. Stage 4 supplies scheduler
evidence and eligibility/reconciliation. No stage may treat a filesystem
marker as sufficient authorization to execute descendants.

The identity/provenance slice implements only those models in this document
and their offline tests. A subsequent installed model slice supplies bundle
manifests/markers. Job-definition publication/reopening is installed separately;
attempt/invocation storage, bundle publication/recovery, and completion-event
association are installed separately. Worker execution and any future
attempt/bundle CLI require further reviewed slices. No
existing run, record, or journal schema is changed by the model slice.

The installed filesystem slice is
[attempt/invocation storage v1](attempt-storage-v1.md). It specifies
ownership, parent/request bindings, atomic metadata publication, exact retries,
and extensible directories. Local tests, coverage, and packaging pass;
the user reports final local acceptance passed;
reservation is not execution authorization or a scheduling lease.

## Identity

- `run_id`, `definition_id`, `attempt_id`, and `invocation_id` use the existing
  canonical lowercase UUIDv4 contract. Callers supply identities once and
  retain them across interruptions; model construction generates nothing.
- `job_uuid` preserves the exact nonempty, UTF-8-encodable jobflow string.
  It is not normalized, lowercased, or constrained to backend UUIDv4 syntax.
  `job_index` is a strict integer greater than or equal to one, never a boolean.
- `(job_uuid, job_index)` identifies a jobflow job within the run. An execution
  retry changes `attempt_id`, not `job_index`; jobflow replacement-index
  semantics belong to the Stage 3 compatibility tests.
- `job_key` is the lowercase SHA-256 of `job_uuid.encode("utf-8")`. It is a
  filesystem locator, not the job's identity or an authorization check.
  Record loaders must compare the original identity as well as the key.

Read-only inspection of installed jobflow source found that its constructor
accepts a string UUID, its UID helper supports UUID1 and UUID4, and its default
job index is one. This motivates separating jobflow IDs from backend IDs; it
does not establish execution compatibility or support every possible custom
jobflow index.

## Common record rules

The first three models are frozen, strict, reject unknown fields,
and revalidate instances. Each requires `schema_version: 1`, an exact `kind`,
`run_id`, and caller-supplied `created_at` using the existing canonical UTC
microsecond timestamp contract. Missing or unsupported versions fail closed.
Tuples or immutable nested models are used rather than mutable record lists.

Artifact references contain `path`, lowercase `sha256`, and a nonnegative
strict integer `size_bytes`. Paths are run-relative POSIX paths: no leading
slash, empty component, `.`/`..`, backslash, or NUL, and no normalization of
input. Model validation does not follow paths, verify bytes, or prove that
parent directories and files are safe. Those are storage-loader checks.

### JobDefinitionRecord (`kind: job-definition`)

Required fields beyond the common envelope:

- `definition_id`, `job_uuid`, `job_index`, and derived/validated `job_key`;
- `jobflow_version`, initially exactly `0.3.1`;
- `payload`: artifact reference to the exact serialized job definition,
  with fixed derived path
  `jobs/<job_key>/index-<job_index>/definitions/<definition_id>/job.json`;
- `origin`: `original_flow`, `dynamic_response`, or `amendment`;
- `source`: a run-relative artifact reference identifying the source Flow,
  full Response, or amendment record from which the definition was selected.

The original Flow remains unchanged. A definition is a separately pinned
snapshot, never a call to a maker to recreate a job. For `original_flow`, the
source path is `flow/payload.json`; its digest/size must agree with the run's
Flow envelope when records are linked. Source-role validation for dynamic
responses and amendments belongs to their later semantic contracts.
The model does not deserialize the job, import callables, establish dependency
equivalence, or approve an amendment.

### AttemptRecord (`kind: execution-attempt`)

Required fields beyond the common envelope:

- `attempt_id`, `job_uuid`, `job_index`, and validated `job_key`;
- `definition_id` and `definition_sha256`, referring to the selected job
  payload bytes, not to an ambiguously named metadata checksum;
- `consumer_code_sha256` and `worker_runtime_sha256` for this attempt.

This record fixes what an attempt intends to execute. It is not a Slurm
submission receipt, proof of user authorization, or evidence of execution.
Submission tokens, resolved resource requests, scheduler receipts, and
amendment approvals belong to separate records specified before their I/O
or execution interfaces are implemented. A future amendment may pin different
code/runtime artifacts only after its authorization/impact checks pass; the
initial attempt must agree with the immutable request.

### InvocationRecord (`kind: worker-invocation`)

Required fields beyond the common envelope:

- `invocation_id`, `attempt_id`, `job_uuid`, `job_index`, and validated `job_key`.

Each worker process entry uses a new invocation ID, including site-triggered
restarts. Multiple invocations may belong to one attempt, but they cannot
overwrite one another. Scheduler association is a separate verified record,
not a fabricated Slurm ID needed to construct an offline invocation model.
The model does not claim that an invocation is active, terminal, or successful.

### Cross-record validation

A pure helper revalidates the supplied definition, attempt, and invocation;
checks identical run/job UUID/index/key; matches definition ID and payload
digest to the attempt; and matches invocation attempt ID. Creation times must
be nondecreasing: definition, then attempt, then invocation. Equal timestamps
are permitted. The helper checks metadata consistency only, not filesystem
contents, Slurm state, ownership, authorization, or graph semantics.

## Result bundle: model refinement and subsequent storage slices

The approved [bundle record contract](bundle-record-contract-v1.md) specifies
concrete manifest/marker fields and pure validation. That model-only refinement
is installed; its full local acceptance gate is user-reported passing. Publication I/O
remains a later separately reviewed operation.

Each invocation owns a private staging area and a unique published bundle.
The proposed bundle contains:

- one jobflow output document, including a legitimate null output if that
  is what the job returned;
- the full serialized `Response`, including dynamic instructions, stop flags,
  `stored_data`, and directory information where present;
- all declared retained application files;
- any declared additional-store data, with explicit store/blob identities;
- a manifest covering the preceding artifacts' paths, exact byte digests,
  sizes, run/job/attempt/invocation identities, selected definition,
  consumer/runtime digests, and scheduler association;
- a final `COMMIT.json` binding the invocation and exact manifest digest.

Missing output document is different from a document whose output is null.
The manifest cannot bless a document without its full Response, or the reverse.
The full Response is separate even when its output duplicates document data.
Files and additional-store blobs must not have unresolved or expired references
outside the retained bundle's declared lifetime.

The Stage 2 bundle reader checks backend envelopes and exact bytes without
executing Monty decoding or importing consumer callables. Stage 3 must validate
jobflow document identities, decode/re-encode compatibility, reference/blob
resolution, and Response semantics before admitting a bundle to a result view.
A checksummed opaque payload alone is not a valid jobflow result.

Opaque additional-data artifact retention is implemented by the bundle layer;
the jobflow additional-store adapter is not implemented. Its encoding and lookup
contract must be specified before the store adapter is advertised as general
purpose. If an initial worker cannot support a store configuration, it must
reject it explicitly before executing the job, never silently omit its data.

## Ownership, publication, and recovery constraints

The controller reserves attempts under the existing per-run lock. A worker
writes only its own invocation; it must not hold that run lock for scientific
execution. Workers do not concurrently mutate shared result indexes.
The installed [invocation ownership protocol](bundle-storage-v1.md) provides
cooperating-writer exclusion and invocation-before-run lock ordering for
publication I/O. It does not establish orphan-writer quiescence or scheduler
termination; worker/controller integration must supply those checks.

Publication must preserve same-filesystem atomic rename, a completion marker
written last, and the accepted file/directory flush requirements. Its success
acknowledgment, conflict behavior, and safe retry identity are specified and
locally crash-tested in the separate publication/recovery slices. Live no-writer
evidence remains a worker/controller integration obligation.

The bundle and its journal registration cannot be one atomic rename. A future
controller must discover a published but unregistered bundle and register it
idempotently; absence of an event never proves absence of calculation output.
Conflicting invocation completion claims are held for inspection, not selected
by timestamp, directory order, or first valid marker.

Backend integrity, jobflow semantic validity, scheduler outcome, and result
selection remain separate findings. Recovery of intact bytes may finish
publication after terminal/no-writer checks, but does not override a failed
or unknown scheduler outcome. Parse-only repair, failed-job amendments, and
new scientific attempts retain the accepted separate evidence and approval
requirements. No partial state authorizes an implicit calculation rerun.

## Next model-slice files and acceptance checks

Approved model-only files:

- `src/jobflow_gitlab_slurm/persistence/attempts/records.py`: the identity/provenance models,
  path/reference validation, job-key helper, and pure cross-record validator;
- `tests/persistence/attempts/test_records.py`: positive, negative, round-trip, and
  mutation/revalidation regressions;
- `ci/check-wheel.sh`: import the new module in the existing wheel check;
- these contract/progress documents: record application and actual evidence.

Acceptance: preserve UUID1/custom job IDs exactly and give unsafe-looking IDs
safe hash locators; reject malformed backend IDs, indices/booleans, timestamps,
schema versions, unsafe paths, derived-key/path mismatches, and cross-record
identity/digest/time conflicts. Test legitimate independent invocation records
without treating them as selected results, frozen/detached model behavior, and
JSON round trips. Existing tests and CLI behavior must remain unchanged.

Run the existing local checks, statement/branch coverage gate, and build/wheel
checks after user application. No dependency change, installation, runtime
probe, source edit, or check execution is authorized by this design document.
Local tests cannot establish cross-host HPC durability; that live gate remains
pending.

### Verification evidence (2026-10-08)

The user reports the supplied verification sequence passed: new model tests,
the full suite with the 100% statement/branch coverage gate, Ruff formatting
and lint, CLI help, shell syntax, wheel/sdist build, isolated wheel verification,
and diff checks. The final test count was not supplied. The assistant confirmed
installation through read-only inspection and did not rerun checks. No remote
CI, jobflow execution, bundle publication, or live cross-host verification is
claimed. These models do not persist attempts or authorize a calculation.
