# Attempt and invocation storage v1

Status (2026-10-08): **implementation installed; full local acceptance gate
passed**. Tests, coverage, and packaging were verified by the assistant;
the user reports the final remaining quality checks passed. Original-Flow job-definition
storage is installed and locally user-verified. This slice extends the approved
[identity/provenance contract](attempt-record-contract-v1.md), without changing
its schemas or introducing execution.

## Scope and API

New module: `src/jobflow_gitlab_slurm/persistence/attempts/storage.py`.

Implemented operations:

- `reserve_attempt(runs_root, record)` publishes or acknowledges an immutable
  `AttemptRecord` linked to an already-published job definition.
- `read_attempt(runs_root, run_id, job_uuid, job_index, attempt_id)` verifies
  an existing attempt and its referenced definition.
- `register_invocation(runs_root, record)` publishes or acknowledges an
  immutable `InvocationRecord` linked to an already-published attempt.
- `read_invocation(runs_root, run_id, job_uuid, job_index, attempt_id,
  invocation_id)` verifies an invocation and its attempt/definition chain.

Return frozen handles with the published path, validated record, and linked
definition/attempt handles as applicable. Returned handles retain no lock and
are not guarantees of future byte integrity.

Reservation means persisted execution intention, not a scheduling lease,
Slurm acceptance, or permission to run. Invocation registration records an
identity, not proof of a live worker, successful execution, or writer exclusion.
The APIs do not submit jobs, decode jobflow payloads, import consumer code,
check budget policy, or evaluate execution readiness.

## Ownership and locking

The controller owns attempt reservation. A future worker registers its own
invocation at process entry; each actual process entry, including a scheduler
restart, receives a new invocation ID. An explicit retry of that entry's
metadata publication retains the same ID and timestamp. Registration never
creates its missing parent attempt automatically.

Each public operation takes the existing nonblocking per-run lock once through
`locked_run`. Linked definitions must be loaded under that same lock using
the package-internal locked definition reader, not by calling a public API
that reacquires the lock. No existing public API or source-module refactor
is proposed. Internal helper reuse is package-local, not a consumer interface.

The lock covers only short metadata operations. It is released before any
scientific execution; `RunBusyError` propagates with no implicit wait/retry.
Later orchestration defers/retries using stable identities.

The future reconciler enforces at most one active execution attempt per job.
These storage primitives cannot enforce that policy because records contain
no authoritative scheduler/terminal state. They preserve multiple distinct
attempts/invocations as evidence, without selecting a winner or automatically
relaunching a calculation. Worker/finalizer exclusion requires a later
invocation-level locking contract.

## Layout and publication units

```text
<run-root>/jobs/<job_key>/index-<job_index>/
  definitions/<definition_id>/...      already-published definition
  attempts/<attempt_id>/
    attempt.json                      immutable AttemptRecord
    invocations/<invocation_id>/
      invocation.json                 immutable InvocationRecord
```

Each new attempt or invocation is initially staged as a private directory
containing its required metadata file, then published by same-filesystem
rename under the existing run lock. Staging siblings use reserved prefixes
`.staging-attempt-<attempt_id>-` and `.staging-invocation-<invocation_id>-`.
They are never adopted or cleaned up implicitly.

The required metadata remains immutable, but the surrounding directory is
extensible: attempts will later gain submission records, and invocations
will gain worker staging/published output. Readers check required metadata
and the path components they traverse; they do not claim those other children
are valid or require the whole directory to contain only the original record.
An exact metadata retry neither rewrites nor validates unrelated worker files.
Bundle inventory validation remains a separate contract.

## Validation and provenance

Before creating containers or staging, revalidate caller models and load the
required parent records under the run lock. Readers perform equivalent checks:

- Preserve the exact job UUID; verify its hash locator, index, run identity,
  backend IDs, schema, and canonical timestamp syntax against the requested
  location. Reject unsafe model copies, symlinks, non-directory containers,
  and non-regular metadata files without blocking on FIFOs.
- Load the selected definition and verify its stored payload bytes using the
  installed definition-storage checks. The definition must exist before an
  attempt can be reserved.
- Bind attempt `definition_id` and `definition_sha256` to that definition,
  with matching run/job identity. Require definition time <= attempt time.
- For this initial original-Flow slice, attempt consumer-code and runtime
  digests must equal the immutable run request. Changed-runtime/amendment
  attempts are explicitly unsupported until their authorization contract is
  implemented; equality alone does not prove runtime availability.
- An invocation must match its published attempt's run/job/attempt identity.
  Require attempt time <= invocation time, and validate the full linked
  definition/attempt/invocation records with the installed pure validator.

Use the existing UTF-8 `model_dump_json()` record-writing convention. Readers
reject duplicate JSON keys, malformed/nonfinite JSON, unknown fields, and
unsupported versions. Metadata operations do not reverify the external wheel
or runtime image; execution-time verification remains mandatory future work.
Jobflow graph membership, dependency readiness, and scheduler association are
not established by these checks.

## Stable retries and interrupted publication

Caller-supplied IDs and complete records, including timestamps, are retained
across retries. Lookup/retry identity includes run, exact job UUID/index, attempt,
and invocation where applicable; a bare UUID is not a lookup into a global index.
New publication flushes the metadata file and private directory,
renames the unit only to an absent identity-derived target, then flushes parent
and newly created ancestor directories through the run root before success.
The trusted-root, cooperating-writer assumption remains unchanged.

For an existing target, revalidate stored metadata and all parent bindings.
An exact matching record is acknowledged after re-establishing the required
metadata/directory flushes, without rewriting it. A different valid record
under the same ID is a conflict. Missing, corrupt, unsafe, or inconsistent
published metadata is held as an integrity failure, never overwritten.

Publication/acknowledgment failures report run/job/attempt/invocation identity
as applicable, operation, phase, published/staging paths, conservative
publication uncertainty, and an inspection/same-ID retry hint. Preserve
remaining staging and worker evidence. If an explicit retry finds no target,
it may stage the same record anew without deleting older stages. An existing
target may only be acknowledged after full validation.

An attempt and its later invocation are separate publication units. A reserved
attempt without an invocation is a valid metadata state, not proof that nothing
ran or permission to submit again. These operations append no journal event;
later reconciliation must discover records and register domain events
idempotently. Absence of an event or invocation is never scheduler evidence.

## Affected files, risks, and acceptance

Approved copy/paste delivery:

- New `src/jobflow_gitlab_slurm/persistence/attempts/storage.py`.
- New `tests/persistence/attempts/`.
- One module import in `ci/check-wheel.sh`.
- Documentation recording approval, installation, evidence, and limitations.

No dependency, lockfile, CLI, existing record schema, worker, scheduler,
journal mutation, or existing source-module change is proposed.

Acceptance: second-process reopening; exact retry acknowledgment/conflicts;
missing or invalid parent rejection before writes; definition/request/time
binding; unsafe identity/path/file-type rejection; corrupt metadata and parent
payload detection; flush ordering; rename/acknowledgment failure injection;
abrupt process exit; process-level contention; multiple distinct invocations
without result selection; preserved extensible-directory evidence; and
non-mutating reads. Pass the full existing local coverage and packaging gates.

Main risks are nested run-lock acquisition, treating reservation as execution
authorization, confusing a publication retry with a new process entry, and
mistaking metadata acknowledgment for bundle finalization. Tests must cover
these boundaries without Slurm, consumer imports, or scientific execution.
On the user's explicit verification request, the assistant ran 152 focused
tests and the full 1,267-test suite successfully, with 100% statement and branch
coverage. Locked environment synchronization, formatting checks, shell syntax,
CLI help, wheel/sdist build, isolated wheel verification, and diff whitespace
checks passed. The user subsequently reports the verification gate passed,
completing local acceptance. The assistant did not repeat that final check.
No source, tests, or CI definitions were edited during verification.
Cross-host guarantees, live/remote CI, scheduling policy, amendments, bundle
finalization, and result eligibility remain separate gates.
This slice does not close Stage 2.
