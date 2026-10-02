# Run request v1: offline contract

Status: **accepted design on 2026-09-29; model, strict YAML loading,
single-site cross-check, and `validate-request` CLI implemented**. Artifact
verification, durable run creation, and budget amendment are not implemented.
The user-approved architecture calls for an immutable run request with
bounded resources, pinned workflow/runtime inputs, optional notification,
and an optional compute-hour budget. This document fixes the first schema
target for subsequent execution and recovery tests.

## Fields

| Field | Rule and reason |
| --- | --- |
| `schema_version` | Required integer `1`; unknown versions and unknown keys fail closed. |
| `site_id` | Required; must equal the validated site binding's `site_id`. |
| `workflow.name` | Human-readable, nonempty label for reports. |
| `workflow.serialized_flow_sha256` | Required lowercase SHA-256 of the exact serialized jobflow `Flow` bytes stored as `flow/original.json`; the manager must verify the bytes, not merely trust the field. |
| `workflow.consumer_code_sha256` | Required lowercase SHA-256 of the pinned importable consumer-code artifact available to the controller and worker; its identity must be verified at staging. This need not be a Git commit and remains application-neutral. |
| `runtime.worker_runtime_sha256` | Required lowercase SHA-256 of the staged worker runtime artifact (for example, a SIF); the launch plan must verify it. It is not an image tag. |
| `resources.partition` | Required and must be one of the site's allowed partitions; the consumer may prefill the site's default before constructing the request. |
| `resources.nodes`, `tasks_per_node`, `cpus_per_task` | Required positive integers. They map to Slurm resource requests; site/partition maximum checks occur separately. |
| `resources.memory_mb_per_node` | Required positive integer, explicitly per node. |
| `resources.walltime_seconds` | Required positive integer, avoiding ambiguous Slurm time-string parsing in the durable request. |
| `policy.allocated_cpu_hour_budget` | Optional `null`/omitted means unbounded. Otherwise a **quoted positive finite decimal string** such as `"12.5"`, never a binary floating-point value. This is a run-wide cap on newly submitted work, measured in allocated CPU-hours, not an HPC billing amount. |
| `policy.notification_email` | Optional `null`/omitted means no Slurm mail. Validate a conservative address syntax and reject controls/newlines; delivery is site-dependent. |

The site binding supplies the account and approved partitions. The request
does not duplicate the account, runner tag, GitLab token, storage root, module
names, or arbitrary shell commands. The manager, not the user, creates a
random `run_id` and records backend/jobflow versions, runtime paths, and
workspace expiry in the immutable run manifest. A hash field is a claim
until its corresponding artifact has been checked during run creation.

The request is immutable after creation. A budget increase is a separate
append-only policy event linked to the same run; it does not reconstruct the
flow or re-execute committed jobs. Notification changes, if supported later,
also require an explicit event rather than an in-place request edit.

### Budget increase operation

A budget increase is **not** a replacement run request and is distinct from
an amendment to a failed job definition. The user supplies the target run
(or selects it from status output) and a new **absolute total** allocated
CPU-hour ceiling, for example from `"20"` to `"30"`. The user does not
re-enter the workflow, site, runtime, partition, or resource fields; these
remain in the pinned original request and site snapshot. The tool should
show the current total, consumed amount, in-flight reservation/uncertainty,
and proposed new total before the user confirms.

Under the per-run lock, the manager checks that the workspace is still
resumable, the actor is authorized, the new finite ceiling is greater than
the effective prior ceiling, and no conflicting policy update has intervened.
It then appends a versioned event containing a generated idempotency key,
actor, timestamp, expected prior policy revision, and new total. A retry of
the same approved request cannot add budget twice. The original request and
earlier policy events remain unchanged. The next reconciliation may submit
ready jobs if the new ceiling leaves sufficient headroom; it does not modify
already submitted Slurm jobs or rerun committed jobflow jobs.

This operation does not alter per-job resources. Changing those is a
separate, explicitly reviewed definition/resource amendment where eligible,
or a derived run; it must not be smuggled into a budget top-up. An expired
workspace, ambiguous active submission, or missing accounting evidence must
be reported rather than treated as a successful continuation. If the
original budget is unbounded, an "increase" is inapplicable; imposing a new
limit would be a different policy operation.

## Validation boundary

The model's **offline** tests reject missing or extra fields, wrong types,
unsupported schema versions, invalid/non-lowercase digests, nonpositive
resources, a site/partition mismatch, unquoted or nonfinite budgets, and
control characters in email. The file loader reuses the strict SafeLoader
so duplicate YAML keys cannot silently override values. None of these
checks calls Slurm, reads licensed data, or proves that the referenced
artifacts exist. A later staging check verifies hashes and paths; the site
doctor/smoke gates verify actual account, partition, and filesystem behavior.

One Slurm allocation executes one jobflow job in the initial backend. The
resource request is therefore the per-job allocation envelope, while the
compute-hour budget applies across all allocations in the run. A policy
decision about charging completed, cancelled, or unknown allocations belongs
to the later accounting/reconciler stage and must be tested there.
