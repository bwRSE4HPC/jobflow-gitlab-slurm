# Decision 0002: attempt publication, recovery, and failure reports

Status: accepted for the first implementation slice on 2026-09-28; not implemented.

## Boundary

The first compatibility target is `jobflow==0.3.1`. A controller that
deserializes a flow must be able to import its consumer-defined callables;
the initial consumer will provide a lightweight importable workflow module
without loading VASP or other scientific executables on the controller.
Broader jobflow-version support and a data-only controller envelope require
separate compatibility work.

Each logical job has immutable, uniquely identified execution attempts. The
worker stages its jobflow output document, full `Response` (including dynamic
instructions), required output files, and a manifest tying them to the run,
job UUID/index, attempt, runtime, and Slurm job ID. A result is visible to
dependent jobs only after these records pass validation and a final completion
marker is published on the shared filesystem. The storage adapter must stage
`Job.run`'s document rather than expose it before the `Response` is ready.
The worker exits successfully only after publication. The original attempt
is retained; repair actions create a separate, linked audit trail.

Publication is necessary but not sufficient for downstream scheduling: the
corresponding Slurm job must also be terminal and successful, as specified in
[run-state v1](../run-state-v1.md). Finalization or parse-only repair does not
by itself override a failed or unknown scheduler outcome.

## Terminal Slurm job with no valid completion marker

The controller first confirms a terminal scheduler state, allows for any
site-specific accounting/filesystem visibility delay, and inspects the
attempt under the per-run lock. It blocks descendants and never implicitly
requeues or resubmits the scientific calculation.

1. If both staged records and required files are intact, validate their
   identity and integrity and finish publication idempotently. Record who or
   what recovered the commit.
2. If records are incomplete but durable application output exists, preserve
   the original attempt and offer an application-specific **parse-only**
   repair. Such a repair must be explicitly configured and must not launch the
   scientific executable. It publishes a new repair record only after
   validating the scientific output and reconstructing the required jobflow
   result and dynamic instructions. A failed repair retains its own record;
   a corrected parser may make a new repair attempt, but never overwrites the
   earlier one. Otherwise, require operator approval.
3. If durable output is absent, expired, or cannot be validated, mark the
   attempt `needs_attention`. There is no generic no-rerun recovery. A new
   calculation attempt or checkpoint continuation requires explicit user
   authorization and budget; it is never disguised as a repair.

An output document without a valid full `Response` is not a successful job.
A full `Response` without its validated output document is not sufficient
either. If Slurm accounting or an accepted submission cannot be reconciled,
retain an explicit `unknown` outcome and do not submit a duplicate job.
The same policy applies if node-local scratch disappeared with the allocation:
unpublished data cannot be reconstructed from a scheduler state alone.

## User-facing failure report

Every failed, timed-out, cancelled, unknown, or incomplete attempt must have
one stable machine-readable report and a concise human view, both derived
from the authoritative run workspace. They must include:

- run/workflow ID, job name and UUID/index, attempt ID, cluster, Slurm job ID,
  scheduler state and exit code, and observed worker phase;
- a precise classification and explanation that distinguishes scheduler
  termination from jobflow publication failure;
- the paths and availability of the attempt manifest, logs, staged records,
  scientific files, and any repair trail, plus workspace expiry when known;
- downstream impact (which work is held), recovery already attempted, and
  the next safe action: finalize, inspect, parse-only repair, an eligible
  [failed-job amendment](0003-failed-job-amendments.md), or explicitly
  authorize a new calculation/checkpoint continuation;
- whether that action needs user approval, a new Slurm allocation, or more
  compute budget. Never label a proposed repair as proven until validation
  has passed.

The report may include a bounded diagnostic excerpt, but must not copy
credentials, proprietary input, licensed data, or full scientific output into
public CI logs. GitLab status jobs should display the concise view and a path
to the workspace evidence. An expired workspace is reported as such, not as
a successful or recoverable run.

Illustrative human view (not an implemented command or an actual run):

```text
Run example-17 / Job relax (uuid ...), attempt 1: NEEDS_ATTENTION
Slurm: TIMEOUT, job 12345; worker result: incomplete publication
Evidence: /shared/runs/example-17/jobs/.../attempts/1/ (logs and manifest)
Missing: validated response.json; descendants held: 2
Repair: parse-only unavailable; no durable scientific output was validated
Next action: inspect evidence, then explicitly authorize a new calculation
Budget: new allocation required; workspace expiry: 2026-11-27
```

## Acceptance checks

Offline tests must inject crashes before document write, between document
and response write, before completion-marker publication, and after marker
publication. Repeated reconciliation must commit a complete attempt once,
keep incomplete attempts blocked, preserve the original and repair trails,
and never rerun a scientific function implicitly. Live smoke tests must
verify the user report and evidence paths for at least a forced failure and
a timeout before scientific production use.
