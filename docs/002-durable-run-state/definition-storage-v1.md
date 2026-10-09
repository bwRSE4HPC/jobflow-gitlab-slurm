# Job-definition storage v1

Status (2026-10-08): **slice approved, installed, and locally user-verified**.
The user reports the supplied verification sequence passes. This slice builds
on the installed run lock, artifact helpers, and
[identity/provenance models](attempt-record-contract-v1.md).

## Scope and ordering

Persist and reopen one immutable job definition with its exact opaque payload.
Start with `origin: original_flow` only. Dynamic-response and amendment origins
remain valid model values but are explicitly rejected by this initial storage
API until their source-role and authorization contracts are implemented.

Follow with separately reviewed attempt reservation/invocation registration,
then bundle publication and interrupted-publication recovery. This separates
short controller metadata operations from the worker/finalizer locking needed
for application output. No stage may equate stored inputs with execution
authorization or a selected result.

## Approved API and ownership

New module: `src/jobflow_gitlab_slurm/persistence/attempts/definitions.py`.

- `publish_job_definition(runs_root, record, payload_source)` validates,
  stages, publishes, or acknowledges an exact already-published definition.
- `read_job_definition(runs_root, run_id, job_uuid, job_index, definition_id)`
  reopens and verifies that definition without writing or repairing anything.

Both return a frozen handle containing the verified record and its published
directory. The returned handle does not retain the lock or guarantee that
bytes cannot subsequently change. Later execution must reverify its inputs.

The controller owns publication. Each public operation acquires the existing
nonblocking per-run lock once through `locked_run`; it must not nest another
operation that reacquires that lock. Busy propagates as `RunBusyError` with
no implicit wait/retry loop. Future orchestration decides when to retry.
No new invocation lock, worker, journal mutation, or submission is introduced.

The caller supplies and retains the complete `JobDefinitionRecord`, including
its definition ID and timestamp, across retries. The API generates neither a
new logical identity nor a new timestamp on retry.

## Layout and validation

The implemented definition unit is:

```text
<run-root>/jobs/<job_key>/index-<job_index>/definitions/<definition_id>/
  definition.json       immutable JobDefinitionRecord
  job.json              unchanged supplied job payload
```

Private sibling directories named `.staging-<definition_id>-<random>` hold
unpublished evidence. Parent containers are not definition publication units.
There is no completion marker: the complete definition directory is the
atomic publication unit, unlike a later worker result bundle.

Before writing, revalidate the supplied model and bind it to the locked run:

- Run identity and jobflow version agree with the run manifest.
- The exact job UUID, job index, hash locator, and definition ID determine
  the destination; arbitrary job UUID strings never become path components.
- Definition time is not earlier than the run's creation time.
- `original_flow` source path, digest, and size agree with the stored Flow
  envelope. The stored Flow bytes are verified by `locked_run`.
- The payload source is a regular non-symlink file, and the copied bytes
  match the definition's declared digest and size.

Backend metadata uses the revalidated model's UTF-8 `model_dump_json()` bytes,
following the existing run-record writing convention. Readers reject duplicate
JSON keys, malformed/nonfinite JSON, unknown fields, and unsupported versions.
Job payload bytes are never decoded or reserialized. Stage 3 must establish
that this payload actually represents the declared job from the original Flow;
matching provenance hashes alone cannot establish that semantic relationship.

Check each backend-controlled directory component without following symlinks;
reject symlinks and non-directory containers. Reject unsafe record/payload file
types without blocking on FIFOs. The existing trusted-root, cooperating-writer
assumption remains; this is not an adversarial filesystem sandbox.

## Publication, retries, and failures

1. Validate caller metadata and the locked run before creating job containers.
2. Create/verify containers under the run lock; retain any partial setup.
3. Stage metadata and verified payload privately on the destination filesystem.
   Flush both files and the staging directory before publication.
4. Under the same lock, require the destination to be absent and rename the
   whole staging directory to the identity-derived destination. Never replace
   a previously published definition.
5. Flush the destination's parent and newly created ancestor directories,
   including their entries in the run root, before acknowledging success.

An existing destination is handled explicitly:

- Revalidate stored metadata, source/run bindings, and actual payload bytes.
- If the stored record agrees with every supplied field and the payload is
  intact, acknowledge the same definition after re-establishing required flushes.
  No records, timestamps, or payload bytes are rewritten.
- Exact acknowledgment does not require the original payload source to remain
  available: the stored payload is verified against the unchanged record.
- Changed content/provenance under the same ID is a conflict. Invalid or
  incomplete published contents are held as integrity failures, not overwritten.

A failed publication reports run/job/definition identity, operation/phase,
published and staging paths where available, and conservative publication
uncertainty. Preserve remaining staging evidence; perform no automatic cleanup
or adoption of an abandoned staging directory. A caught artifact-helper failure
may remove the incomplete private payload file it created, as already specified
by that helper; the definition staging directory and other evidence are retained.

After an uncertain rename/flush outcome, inspection and explicit retry use the
same definition ID and original record. If the target is absent, a retry may
create a fresh private stage without deleting older stages. If it is present,
only exact validated acknowledgment is permitted. This retries an input-storage
operation, never a calculation. Parent-container existence alone is not evidence
that a definition was published.

## Affected files and acceptance checks

Approved copy/paste delivery:

- New `src/jobflow_gitlab_slurm/persistence/attempts/definitions.py`.
- New `tests/persistence/attempts/`.
- One module import in the existing `ci/check-wheel.sh` verification list.
- Documentation recording installation and verification when reported.

No dependency, lockfile, CLI, existing schema, or execution changes are proposed.
Reuse existing primitives where appropriate; any necessary changes to an
existing source module require separate discussion before delivery.

Acceptance includes exact-byte publication and second-process reopening;
stable-ID acknowledgment and conflicts; invalid input before directory creation;
unsafe path/file-type rejection; corruption/missing-artifact detection; flush
ordering and rename-boundary fault injection; abrupt process exit; competing
publisher exclusion; retained failure evidence; and read-only reopening.
Tests use inert payloads and real local files/processes, without scientific
execution, Slurm, GitLab credentials, or consumer imports. Preserve existing
tests and pass the agreed full local coverage and packaging gates.

Cross-host filesystem guarantees, graph membership, attempt scheduling,
worker ownership, bundle finalization, and result eligibility remain separate
gates. This slice does not close Stage 2.

## Verification evidence (2026-10-08)

The user reports the supplied local sequence passes: focused definition-storage
tests, the full suite with the agreed 100% combined statement/branch coverage
gate, Ruff formatting/lint, shell syntax, CLI help, wheel/sdist build, isolated
wheel verification, and diff checks. Read-only inspection confirms the module,
tests, and wheel import entry are present. The successful final test count was
not supplied; the assistant did not independently execute these checks.

This verifies the local checkpoint only. Cross-host durability, graph membership,
attempt/invocation storage, worker execution, and result eligibility remain
separate gates. Attempt/invocation storage is now locally accepted as recorded
in [002-durable-run-state](../002-durable-run-state.md); no remote CI result for
that cycle is recorded.
