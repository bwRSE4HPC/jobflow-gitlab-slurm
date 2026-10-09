# Staged implementation plan

This document owns major stages, dependencies, scope, and acceptance gates.
Cycle-specific slices, mechanisms, rationale, risks, and acceptance evidence
belong in the matching cycle document. [Progress](progress.md) records the
current checkpoint and pending checks.

## Development cycles

Cycle branches use sequential, zero-padded names `XXX-branch-name`.
Their overview documents are `docs/XXX-branch-name.md`, with exactly the same
stem. Refined slice documents and cycle-owned contracts live in
`docs/XXX-branch-name/`; their descriptive/versioned filenames are preserved.
Cross-cycle architecture, testing, binding, compatibility, and decision records
remain outside those folders. Create each cycle overview when cycle planning
begins; start the branch from current `main` only after the preceding cycle has
merged. Reviewed, verified cycles
are integrated through a user-directed squash-merge.

- [001-jobflow-compatible-package](001-jobflow-compatible-package.md):
  Stage 1 complete; squash-merged through PR #1 as `ded57b3`.
- [002-durable-run-state](002-durable-run-state.md):
  Stage 2 implementation slices locally accepted; assistant cycle review and
  full local working-tree gate passed on 2026-10-09. User documentation review,
  committed candidate identification, remote CI, and explicit close-out/merge
  approval remain pending.

Versioned contract files (`*-v1.md`) and numbered records in `decisions/`
have independent naming/version schemes; they are not development cycles.
Future cycle names are agreed when planning starts rather than reserved here.

## Stages and acceptance gates

A stage is complete only when its acceptance checks pass. Later stages must
not silently break an earlier run-state schema.

| Stage | Deliverable | Acceptance gate |
| --- | --- | --- |
| 0. Freeze contracts | Review the [architecture](architecture.md), [site binding](hpc-binding.md), [run-request v1](001-jobflow-compatible-package/run-request-v1.md), [jobflow behavior](jobflow-compatibility.md), [testing strategy](testing-strategy.md), [run-state v1 design](002-durable-run-state/run-state-v1.md), and [failure-report contract](decisions/0002-attempt-publication-and-recovery.md). | A non-VASP example run and its failed-attempt report can be described entirely using these contracts; unresolved questions are recorded rather than guessed in code. |
| 1. Package and offline tests | Installable Python package, typed public interfaces, single-site and run-request validation, deterministic site discovery from a consumer configuration directory, CLI skeleton, lint/test CI. | A clean environment installs the package, rejects invalid site/run-request files and duplicate site IDs, and selects a site without HPC access. |
| 2. Durable flow state | Exact opaque Flow/job bytes, run discovery, qualified records, anchored journal, ownership, atomic bundle publication, explicit recovery/audit, and schema/version rejection. | Fresh processes preserve identities and bytes; cooperating writers are excluded; publication/recovery retries retain evidence and event identity; incompatible state fails clearly. JobStore semantics and live filesystem verification remain later gates. |
| 3. Worker and jobflow semantics | One-job worker, output-reference resolution, persistent results, dynamic `Response` handling, file hand-off, and narrow [failed-job amendments](decisions/0003-failed-job-amendments.md). | Jobflow-specific rows in [compatibility contract](jobflow-compatibility.md) pass without Slurm, including restart and corrected-definition tests. |
| 4. Slurm adapter and reconciler | Submit/query/cancel, deterministic submission token, recovery of accepted-but-unrecorded submissions, distinct terminal states, and labeled allocated CPU-hour accounting for optional pause/top-up budgets. | Simulated Slurm crash windows and duplicate controller invocations produce no duplicate active job; unknown outcomes stay `unknown`; adding budget resumes the same run. |
| 5. GitLab controller integration | Short-lived scheduled reconciliation, protected-runner consumer template, structured status, and manual cancel. | A test GitLab consumer with simulated Slurm advances a toy flow across separate pipeline invocations; no long-running Python manager is needed. |
| 6. Live HPC binding | Complete `posix` + Apptainer binding, offline validator, non-submitting doctor, opt-in smoke and recovery probes. | All four [verification gates](hpc-binding.md#verification-gates) pass on one real Slurm cluster using a non-VASP flow. |
| 7. Scientific consumer | Pin a tested package commit in `vaspxatomate2`; add the VASP/atomate2 adaptive KSPACING demonstration and any required site workspace provider. | The same run resumes after controller restart, preserves outputs, accounts for compute time, and respects a raised budget without rebuilding the flow. |
| 8. Generalization review | Document supported jobflow semantics, storage limits, concurrency limits, and site adapters; discuss interfaces with jobflow maintainers. | Public examples and tests support a non-VASP user without KIT-specific code or documentation. |

## Integration boundaries

The approved persistence/execution boundary separates three responsibilities:

- Stage 2: exact opaque Flow/job-definition bytes; immutable identities and
  metadata; bundle inventories and completion evidence; writer ownership;
  publication, read-only inspection, and explicit interrupted-publication
  recovery. Offline tests use inert payloads and separate processes.
- Stage 3: production `JobStore` and one-job worker; output references,
  full `Response` serialization, dynamic/stop semantics, file hand-off,
  and amendments. Additional-store data must be explicitly supported or rejected.
- Stage 4: scheduler evidence, result eligibility/selection, submission
  recovery, reconciliation, and compute-budget policy.

Metadata validity, byte integrity, jobflow semantics, scheduler success, and
selected result are separate checks. A completion marker alone cannot release
a dependent job. Missing journal registration cannot establish that a
calculation never ran. Whole-lifecycle persistence tests are installed with a
passing assistant-executed local gate on 2026-10-09; cycle 002 remote CI and
close-out remain pending. The approved source/test reorganization is implemented
and locally verified; its clean-break import migration does not change persisted
formats or execution functionality. The approved
producer boundary leaves payload preparation and enforcement in Stage 3;
Stage 2 consumes complete opaque staging without adding a staging builder.

Keep live HPC integration in an access-controlled downstream consumer until
generic smoke and recovery gates pass. A KIT GitLab mirror is optional unless
network/deployment policy requires one. Consumers pin a commit or release;
compatible controller/worker versions are recorded in each run manifest.

Directory-based site discovery was delivered in Stage 1, before durable run
creation. The selected normalized site snapshot and digest must be retained
with each run. Runner selection remains downstream CI bootstrap configuration,
not runtime discovery.

Out of scope for the first working backend: job packing, Slurm arrays,
multi-cluster migration of a live run, archival, and an always-on controller.
