# Staged implementation plan

Status: proposed. A stage is complete only when its acceptance checks pass;
the next stage may refine interfaces but must not silently break an earlier
run-state schema.

| Stage | Deliverable | Acceptance gate |
| --- | --- | --- |
| 0. Freeze contracts | Review the [architecture](architecture.md), [site binding](hpc-binding.md), [run-request v1](run-request-v1.md), [jobflow behavior](jobflow-compatibility.md), [testing strategy](testing-strategy.md), [run-state v1 design](run-state-v1.md), and [failure-report contract](decisions/0002-attempt-publication-and-recovery.md). | A non-VASP example run and its failed-attempt report can be described entirely using these contracts; unresolved questions are recorded rather than guessed in code. |
| 1. Package and offline tests | Installable Python package, typed public interfaces, single-site and run-request validation, deterministic site discovery from a consumer configuration directory, CLI skeleton, lint/test CI. | A clean environment installs the package, rejects invalid site/run-request files and duplicate site IDs, and selects a site without HPC access. |
| 2. Durable flow state | Original serialized flow, workspace registry, result store, job/event records, locking, atomic publication, and migration/version checks. | Restarted processes see identical UUIDs and results; concurrent controller attempts serialize; incompatible state fails clearly. |
| 3. Worker and jobflow semantics | One-job worker, output-reference resolution, persistent results, dynamic `Response` handling, file hand-off, and narrow [failed-job amendments](decisions/0003-failed-job-amendments.md). | Jobflow-specific rows in [compatibility contract](jobflow-compatibility.md) pass without Slurm, including restart and corrected-definition tests. |
| 4. Slurm adapter and reconciler | Submit/query/cancel, deterministic submission token, recovery of accepted-but-unrecorded submissions, distinct terminal states, and labeled allocated CPU-hour accounting for optional pause/top-up budgets. | Simulated Slurm crash windows and duplicate controller invocations produce no duplicate active job; unknown outcomes stay `unknown`; adding budget resumes the same run. |
| 5. GitLab controller integration | Short-lived scheduled reconciliation, protected-runner consumer template, structured status, and manual cancel. | A test GitLab consumer with simulated Slurm advances a toy flow across separate pipeline invocations; no long-running Python manager is needed. |
| 6. Live HPC binding | Complete `posix` + Apptainer binding, offline validator, non-submitting doctor, opt-in smoke and recovery probes. | All four [verification gates](hpc-binding.md#verification-gates) pass on one real Slurm cluster using a non-VASP flow. |
| 7. Scientific consumer | Pin a tested package commit in `vaspxatomate2`; add the VASP/atomate2 adaptive KSPACING demonstration and any required site workspace provider. | The same run resumes after controller restart, preserves outputs, accounts for compute time, and respects a raised budget without rebuilding the flow. |
| 8. Generalization review | Document supported jobflow semantics, storage limits, concurrency limits, and site adapters; discuss interfaces with jobflow maintainers. | Public examples and tests support a non-VASP user without KIT-specific code or documentation. |

Keep the live KIT integration in a separate access-controlled consumer project
until the generic smoke and recovery gates pass. A GitHub source repository
does not need a KIT GitLab mirror unless network or deployment policy requires
one. The consumer pins a commit or release; controller and worker must use
compatible backend versions recorded in each run manifest.

Implement directory-based site discovery **after** one run request can be
validated against one site and **before** durable run creation. The registry
is a derived in-memory index of validated, regular `*.yaml` files in a pinned
consumer directory, not a second hand-maintained configuration file. Run
creation must persist the selected site's normalized snapshot and digest so
later edits cannot silently change an active run. GitLab runner selection
remains consumer CI bootstrap configuration because it happens before a job
can scan the repository directory.

Out of scope for the first working slice: job packing, Slurm arrays,
multi-cluster migration of a live run, archival, and an always-on controller.
