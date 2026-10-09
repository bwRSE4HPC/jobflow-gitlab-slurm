# Run-state v1: filesystem contract

Status: **accepted design on 2026-09-28; offline persistence implemented and
locally accepted through cycle 002 lifecycle tests**. Run creation/reopening,
anchored journal/inspection, immutable definition/attempt/invocation storage,
opaque bundle publication, and explicit recovery/audit/event registration are
installed. Worker/JobStore semantics and scheduler integration remain planned;
live filesystem verification and cycle 002 integration gates remain pending.
See [progress](../progress.md) and the [cycle overview](../002-durable-run-state.md)
for slice-specific evidence. This refines the
accepted recovery policy in [Decision 0002](../decisions/0002-attempt-publication-and-recovery.md).
The first slice targets `jobflow==0.3.1` and one shared POSIX run workspace.
The exact filesystem and locking behavior must pass the site doctor before a
live cluster uses it.

## Identity and record rules

The workspace provider returns one absolute `<run-root>` per run. A random
`run_id` identifies that run independently of GitLab pipeline and Slurm IDs.
The original jobflow `Flow` and run request are immutable. Each logical job is
identified by `(job_uuid, job_index)`; each execution attempt has a separate
`attempt_id`, and each worker invocation has a separate `invocation_id` so a
site-triggered Slurm restart cannot overwrite earlier evidence. Repair actions
have their own IDs and link to the original attempt. No record is reconstructed
by calling a workflow maker again. The controller permits at most one active
execution attempt for a logical job; if multiple invocations claim completion
for the same attempt, it retains the evidence and requires reconciliation
rather than choosing one silently.

Every JSON record has `schema_version: 1`, `kind`, `run_id`, its applicable job
and attempt IDs, a creation timestamp, and the versions/digests needed to
interpret it. Unknown schema versions fail closed. The run manifest also pins
the backend version, jobflow version, consumer-code revision, runtime digest,
site ID, and workspace expiry when known. The run request records bounded
resources and policy, including any compute-hour budget. A later budget
increase is a new policy event, not a rewrite of the original request.
Backend records carrying jobflow objects use versioned envelopes. The approved
[artifact contract v1](artifact-contract-v1.md) makes one distinction: original
Flow bytes are preserved as an opaque input artifact in `flow/payload.json`,
referenced by the versioned `flow/original.json` envelope. That payload is not
a backend state record and must not be reserialized during staging.

Paths below are illustrative (the invariants above matter more than
the spelling). The implemented per-run lock is outside this tree at
`<runs_root>/.locks/<run_id>.lock`. The new
[attempt record contract](attempt-record-contract-v1.md) specifies a SHA-256
`job_key` instead of placing arbitrary jobflow UUID strings into paths;
its identity/provenance and bundle models are installed and locally
user-verified. [Job-definition storage](definition-storage-v1.md) is also
installed and locally user-verified for original-Flow inputs.
[Attempt/invocation metadata storage](attempt-storage-v1.md),
[bundle publication](bundle-publication-v1.md), and
[explicit recovery](bundle-recovery-v1.md) are installed. The tree below remains
an illustrative full-system design: submission intents, repairs, amendments,
and report projections are not installed. Authoritative implemented definitions,
invocation metadata, publication intents, and `recoveries/` audit paths are
specified in the corresponding cycle 002 contracts, not by the illustrative
`repairs/` path below.

```text
<run-root>/
  run.json                         immutable request and version manifest
  flow/original.json               versioned original-flow envelope
  flow/payload.json                unchanged supplied serialized Flow bytes
  journal-head.json                implemented run-local journal tail anchor
  events/000000000001.json         immutable, ordered controller decisions
  jobs/<job-key>/index-<n>/attempts/<attempt-id>/
    intent.json                    durable submission token, before sbatch
    slurm-receipt.json             Slurm ID if acceptance was recorded
    invocations/<invocation-id>/
      staging/                     worker-only, possibly incomplete
        job-document.json          JobStore-compatible output document
        response.json              full serialized jobflow Response
        files/                     retained application output, or manifest refs
        payload-manifest.json       file hashes and identity checks
      published/                   staging renamed here when validated
        ...
        COMMIT.json                final validity marker, written last
    repairs/<repair-id>/            immutable, linked repair evidence
  amendments/<amendment-id>/        immutable corrected-definition and approval record
  reports/attempts/<attempt-id>/<revision>.json  immutable report history
  reports/latest.json              derived user-facing status projection
```

The authoritative state is the immutable request, flow, attempt records,
commits, and ordered events. `reports/latest.json` and any accelerated index
are replaceable projections, not the source of truth. The controller appends
an event when it applies a committed dynamic `Response`; the event references
the response and records the newly introduced UUIDs. Replaying reconciliation
must not apply that response twice.

An accepted [failed-job amendment](../decisions/0003-failed-job-amendments.md)
is another append-only event. It selects a pinned corrected definition for a
new attempt of the same logical job while leaving the original flow and
failed attempt unchanged. The controller may reuse committed ancestor outputs
only after its impact checks pass. The exact jobflow UUID/index behavior must
pass a compatibility test before the amendment interface is enabled.

## Publication and visibility

The worker resolves parent references only from validated committed
documents. Its store adapter stages `Job.run`'s output document under its own
invocation; an ordinary writable `JSONStore` is not the production adapter.
After `Job.run` returns, the worker stages the full `Response`, required
files, and their manifest. The worker validates IDs, schema, serialization,
and file integrity before renaming `staging` to a unique `published` path on
the **same filesystem**. It writes `COMMIT.json` last through an atomic
temporary-file rename. Where supported, it flushes files and parent
directories before acknowledging success. A committed result becomes eligible
for downstream scheduling only after the corresponding Slurm job is terminal
and successful too. A marker alone cannot override a failed scheduler result.

A controller holding the per-run lock may finish publication if all durable
payloads are valid but the marker is missing, after confirming the Slurm job
is terminal and no writer remains. If the output document exists
without the full `Response`, the attempt is incomplete; its document must
remain invisible to descendants. Partial or invalid payloads are preserved
for inspection. The filesystem provider must demonstrate cross-host locking,
same-filesystem atomic rename, and visibility on the actual controller and
compute-node paths; a failed capability check blocks live use. Python
documents atomic successful POSIX replacement, but this design does not
assume a particular HPC filesystem supplies the needed durability without a
site test: [rename](https://docs.python.org/3/library/os.html#os.replace),
[file locks](https://docs.python.org/3/library/fcntl.html#fcntl.flock).

## Reconciliation and failure reporting

Before `sbatch`, the controller persists `intent.json` with a stable token.
If submission is accepted but its receipt is missing, the next controller
queries Slurm using that token and a site-verified lookup method. An
ambiguous result stays `unknown`; it does not submit a second job. Once a
Slurm job is terminal, reconciliation follows Decision 0002: finalize an
intact payload, offer a configured parse-only repair from durable output, or
hold for inspection and explicit authorization of a new calculation attempt.
The original attempt and any failed repairs remain inspectable.

Each reconciliation produces the structured failure report specified in
Decision 0002. It distinguishes scheduler state, worker/publication state,
and scientific validation; names evidence paths and expiry; identifies held
descendants; and states whether the next action is automatic, parse-only,
an eligible amendment, requires user approval, or consumes a new allocation.
Public CI logs contain only the concise, secret-safe view.

## Review and acceptance gates

- Verify the layout can represent a two-job non-VASP flow, a dynamic addition,
  a missing response, and a parse-only repair without changing original IDs.
- Crash-test each write boundary and repeated controller invocations. Exactly
  one committed result and one applied dynamic response may emerge.
- Confirm that no staged document can satisfy a downstream reference before
  its complete attempt is committed and Slurm success is verified.
- Test lock exclusion and rename visibility from two independent processes on
  different hosts of the target shared filesystem. If the provider cannot
  establish these properties, fail closed or use a separately specified
  storage adapter; do not silently fall back to unprotected files.
- Keep the report useful when scheduler accounting is delayed, the workspace
  has expired, or only partial scientific files survive.
- Test an approved corrected definition for a failed child, reusing a
  committed parent without changing the original flow or duplicating the
  corrected attempt; reject changes outside the narrow amendment scope.

The approved [record contract v1](record-contract-v1.md) specifies the first
`RunManifest`, `FlowEnvelope`, and `SiteSnapshot` models and the exact site
encoding. These models are installed and user-verified locally. Run
creation/publication and local locking tests are now implemented separately in
`storage.py`; the creation, inspection, and discovery CLI commands are also
installed and locally user-verified.

The creation/reopening slice was approved on 2026-10-07, installed by the user,
and its tests, 100% coverage, and local packaging checks are reported passing.
For the existing POSIX provider its layout is `<runs_root>/<run_id>/`, with persistent locks
at `<runs_root>/.locks/<run_id>.lock` outside the publication rename. Lock files
must not be removed while this root is in use. Private sibling
`.staging-<run_id>-<random>/` directories are not published runs and may contain
interrupted creation evidence. Only cooperating writers in a trusted root are
supported; this is not an adversarial filesystem sandbox. The caller retains
one UUIDv4 before creation and uses it for inspection/recovery after interruption.
An existing ID is never overwritten by creation. Workspace allocation and
automatic staging cleanup are not part of this slice.

## CLI/discovery slice: local checkpoint verified

Accepted and installed on 2026-10-07. The user reports the supplied local
verification sequence passes, including coverage and packaging. No checks
were independently executed by the assistant for this slice. Interfaces:

This section records the initial metadata-only CLI checkpoint. The following
journal-aware extension defines the current additional checks and exit behavior.

- `create-run REQUEST --sites-directory DIRECTORY --run-id UUID --flow FILE
  --consumer-code FILE --worker-runtime FILE [--workspace-expires-at TIMESTAMP]`
  selects the site from the immutable request's `site_id`. The caller supplies
  and retains the UUID; creation does not silently generate a new identity.
- `inspect-run UUID --runs-root DIRECTORY [--verify-external]` validates one
  run without importing or executing workflow code.
- `list-runs --runs-root DIRECTORY [--verify-external]` scans immediate entries
  deterministically, ignoring only `.locks` and `.staging-*` names. Unexpected
  entries and invalid published candidates appear explicitly in the report.

Existing validation commands retain their human-readable output and exit codes.
New commands emit JSON reports: success/list output on stdout, command failures
on stderr. Exit codes are `0` for successful creation/inspection or a list with
only valid entries, `2` for configuration/I/O/invalid-entry failures, `3` for
lock contention (including a list with busy entries but no invalid entries),
and `4` for uncertain creation publication. An empty valid root is a successful
empty list. Argument-parser usage errors remain ordinary argparse diagnostics.

Listing verifies stored Flow bytes, with external-artifact checks only when
requested. `valid`, `busy`, and `invalid` are metadata/discovery classifications,
not scientific or Slurm states. A root scan is not an atomic snapshot: each
candidate is checked under its own existing lock, and entries may change
between checks. No discovery failure authorizes submission, scientific reruns,
staging cleanup, or creation of missing roots/locks. The first implementation
collects the report in memory; very large registries may later need pagination.

## Journal-aware inspection extension: local checkpoint verified

Approved and installed on 2026-10-07; the user reports the supplied local
verification sequence passed, including coverage and packaging. No checks were
independently executed by the assistant for this slice. The extension adds default journal validation
to `inspect-run` and `list-runs` without changing their arguments, metadata
fields/counts, creation, or validation commands. `inspection.py` reads metadata
and the journal during one `locked_run` context; it does not release/reacquire
the lock between observations. Its result retains no lock. Each discovery entry
gets the same inspection, but the directory-wide listing stays non-atomic.

Journal classifications are separate from metadata:

- `uninitialized`: neither journal head nor events directory exists.
- `valid`: full history matches its head, including an initialized zero head.
- `incomplete`: exactly one valid unanchored event; explicit matching retry is
  required, not performed by inspection.
- `invalid`: unsafe, unreadable, or inconsistent journal evidence.
- `not_checked`: metadata/lock validation prevented journal inspection.

The nested `journal` JSON report contains schema/version discriminator,
status, whether a check was attempted, head/events paths, verified committed
count, head and last-event checksums, pending-event identity/sequence/checksum/
path where applicable, problem path, error type/reason, and a recovery hint.
Invalid history uses null counts/checksums rather than asserting an empty
history. No event payload or chained decoder exception is emitted. Existing
`execution_state: not_evaluated` remains unchanged: a valid journal is not a
scientific success or scheduling authorization.

Listing keeps `counts` for metadata and adds `journal_counts`. Exit precedence
is invalid metadata/journal (2), incomplete publication (4), busy (3), otherwise
0. For valid metadata, inspection emits its JSON report on stdout even when
journal findings require exit 2 or 4. Metadata/lock/root errors keep the existing
stderr command-error convention and identify the journal as `not_checked`.
Creation's existing uncertain-publication exit 4 is unchanged.

All reads preserve evidence and perform no repair, initialization, flush,
cleanup, import of consumer callables, scientific rerun, or Slurm operation.
Full-history validation remains linear in journal size. Code and tests are
installed and locally user-verified. The final test count was not supplied;
remote CI and live filesystem verification remain pending for this extension.
