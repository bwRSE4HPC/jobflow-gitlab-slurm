# Run-state v1: filesystem contract

Status: **accepted design on 2026-09-28, not an implemented schema**. This refines the
accepted recovery policy in [Decision 0002](decisions/0002-attempt-publication-and-recovery.md).
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
Serialized jobflow objects are carried inside these versioned envelopes, not
published as bare, unversioned Monty JSON files.

Paths below are illustrative (the invariants above matter more than
the spelling):

```text
<run-root>/
  run.json                         immutable request and version manifest
  flow/original.json               immutable serialized jobflow Flow
  events/000000000001.json         immutable, ordered controller decisions
  locks/reconcile.lock             per-run controller lock
  jobs/<job-uuid>/index-<n>/attempts/<attempt-id>/
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

An accepted [failed-job amendment](decisions/0003-failed-job-amendments.md)
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

This document does not yet choose the concrete Python classes or CLI names.
Those belong in the smallest package-slice proposal before code is written.
