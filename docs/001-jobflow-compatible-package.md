# Cycle 001: jobflow-compatible package

Branch: `001-jobflow-compatible-package`.
Status: closed; squash-merged through PR #1 as `ded57b3`.

This document records the completed Stage 1 scope and hand-off retrospectively
under the branch-matching naming policy. The [overall plan](implementation-plan.md)
owns stage ordering; [progress](progress.md) owns the current checkpoint.
The next cycle is [002-durable-run-state](002-durable-run-state.md).
Refined Stage 1 contract documentation lives in
[001-jobflow-compatible-package/](001-jobflow-compatible-package/).

## Scope and implemented interfaces

Establish an installable, application-neutral offline package before introducing
durable execution state:

- Strict single-site and run-request models, safe YAML loading, and cross-validation.
- Exact compute-budget JSON serialization without decimal precision loss.
- Deterministic discovery of validated regular `*.yaml` site files from a
  consumer-owned directory, with duplicate site IDs rejected.
- `SiteRegistry` and CLI commands `validate-site` and `validate-request`.
- Public offline lint, test, coverage reporting, distribution build, and
  isolated-wheel import/CLI checks.

The initial compatibility target is `jobflow==0.3.1` with Python 3.13.
Contracts: [HPC binding](hpc-binding.md), [run request v1](001-jobflow-compatible-package/run-request-v1.md),
[jobflow compatibility](jobflow-compatibility.md), and
[testing strategy](testing-strategy.md).

Site discovery is a derived index, not another hand-maintained configuration
file. GitLab runner selection remains downstream CI bootstrap configuration:
it happens before a controller can discover site files.

## Acceptance evidence and limitations

On 2026-10-02, an assistant local recheck of `b979ec5` recorded 86 passing
tests, 100% combined statement/branch coverage, locked environment synchronization,
Ruff formatting/lint, shell syntax, wheel/sdist build, and isolated-wheel
verification. The user reported a passing GitHub Actions pipeline; no run URL
was recorded. These are historical checkpoints, not the current suite size.

This verifies configuration and packaging, not backend jobflow execution,
Slurm integration, or live HPC support. Automated package release/publication
and type checking were not configured.

## Exploratory evidence informing later cycles

The following user-reported probes used `jobflow==0.3.1` on 2026-09-28. They
inform the design but are not automated backend compatibility tests.

| Boundary | Observation | Limitation |
| --- | --- | --- |
| Static serialization | A two-job Flow was loaded in a second process with unchanged UUIDs and parent reference, then produced `root/a` and `root/a/b`. | Does not establish dynamic semantics or scheduler recovery. |
| Dynamic addition | An importable planner survived serialization and returned `Response(addition=...)`; planner and added-job outputs were `planned` and `42`. | Only addition was explored; replacement, detour, and stop semantics remain acceptance work. |
| Cross-process result reuse | A parent wrote `42` through maggma `JSONStore`; another process reopened it and ran the child to `84` without rerunning the parent. | Sequential reuse only, not production durability, concurrent safety, or crash recovery. |
| Full Response hand-off | A full Response was serialized separately, reloaded with the same added-job identity, and the added job produced `42`. | Does not establish atomic publication of Response and output document. |
| Callable imports | The defining fixture module had to be importable while deserializing/executing the planner. | Does not establish a safe controller boundary for heavy scientific consumer imports. |

[Decision 0002](decisions/0002-attempt-publication-and-recovery.md) records the
accepted pinned compatibility target, lightweight controller imports,
complete attempt publication, no implicit scientific rerun, and useful
failure/recovery reporting. [Decision 0003](decisions/0003-failed-job-amendments.md)
records narrow, audited recovery by amendment in the same run. Amendments
and compute-hour budget top-ups remain designs, not installed functionality.

`JSONStore` is not selected for production: it rewrites its file on updates,
and concurrent-writer safety is unproven. Coordinated history/head rollback
and whole-workspace loss require an external witness; this is deferred to
later integration. Local tests cannot prove cross-host lock, rename, or
durability guarantees.

## Hand-off

Stage 2 starts from the merged baseline on `002-durable-run-state`.
It must retain the selected site's normalized snapshot and digest so later
configuration edits cannot silently alter an active run. Controller and worker
versions must eventually be pinned and recorded in the run manifest.

The public package remains independent of private HPC consumer configuration;
see [decision 0001](decisions/0001-public-package-private-hpc-consumers.md).
