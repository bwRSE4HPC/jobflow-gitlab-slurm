# Architecture

Status (2026-10-09): execution architecture remains proposed. The offline
validation and persistence foundation is installed: site/run-request discovery,
artifact verification, pinned site snapshots, run creation/reopening, qualified
definition/attempt/invocation storage, ownership, anchored journal operations,
opaque bundle publication, retained intent storage, explicit recovery/audit,
and completion-event registration. Read-only inspection and cross-process
lifecycle tests are also installed. Local slice gates are accepted on recorded
evidence; assistant cycle review and full local verification passed on
2026-10-09. Cycle 002 integration gates remain pending. See [progress](progress.md)
for verification evidence and [cycle 002](002-durable-run-state.md) for scope.

No production `JobStore` adapter, worker, producer enforcement, execution-status
projection, scheduler-aware result selection, Slurm submission/reconciliation,
or GitLab controller integration is implemented. Persistence inspection reports
filesystem integrity, not scientific success or scheduler eligibility. The
responsibilities below describe the intended complete system, not current
execution support.

## Boundary and responsibilities

The package supplies reusable jobflow orchestration and a Slurm adapter. A
downstream consumer supplies the workflow code, worker runtime, run request,
and a site binding described in [HPC binding](hpc-binding.md). A GitLab
project supplies the protected controller pipeline and runner; Slurm supplies
compute allocations. No continuously running Python manager or database is
required by the initial design.

1. A consumer creates a run from a **pinned**, serializable jobflow `Flow` and
   an immutable run request. The request identifies the worker runtime,
   resources, and selected site. The creation manifest additionally pins the
   selected site's snapshot and the backend/jobflow versions.
2. A scheduled or manually triggered, short-lived GitLab controller job finds
   active runs, obtains a per-run lock, reconciles recorded work with Slurm,
   and submits ready jobs. It must never execute scientific jobs itself.
3. A Slurm worker runs one jobflow `Job` per allocation initially, resolves its
   input references, executes it, and publishes its document and required
   output files before acknowledging completion.
4. The next controller invocation incorporates the result, applies any
   dynamic `Response`, and schedules newly ready jobs. Independent ready jobs
   may be submitted concurrently once the basic recovery model is proven.

The controller needs the backend package and Slurm access, but must not run
VASP or other scientific executables. Deserializing a jobflow graph may also
require importable consumer workflow definitions. The first-slice probe
confirmed this boundary with a temporary module supplied on `PYTHONPATH`;
the accepted initial contract supplies lightweight consumer definitions on
the controller, without importing VASP-heavy runtime code there. A data-only
scheduling envelope remains future work. The worker needs the
backend and the consumer's pinned runtime. The backend must not assume that
a compute node can reach GitLab or an OCI registry: a consumer may stage the
runtime image before submission.

## Durable state and recovery

The run workspace is authoritative; GitLab logs and artifacts are diagnostic
copies. A run must retain the original flow with stable job UUIDs, backend and
schema versions, immutable input/runtime identifiers, job states, submission
receipts, output documents, file locations, and an append-only event history.
Do not rebuild an in-progress flow by calling its maker again.

State changes use a per-run lock and atomic publish/rename on a shared
filesystem that supports those operations. A reconciliation pass must be safe
to repeat. In particular, a crash between `sbatch` acceptance and recording
its job ID must not silently create another expensive job: use a stable
submission token recorded before submitting, put it in Slurm job metadata,
and query Slurm for it before retrying. If the outcome cannot be established,
leave the job in an explicit `unknown` state for inspection.

Slurm acceptance means **submitted**, not successful. Completion requires a
terminal Slurm status **and** a valid worker completion record. Missing
accounting or an expired workspace is not interpreted as success. Cancellation,
timeout, and application failure have distinct terminal states.

The accepted first-slice contract is [Decision 0002](decisions/0002-attempt-publication-and-recovery.md):
stage the output document, full `Response`, and required files under an
immutable attempt ID; expose them to descendants only after validation and a
completion marker. A terminal Slurm job with an incomplete attempt is held for
safe finalization, parse-only repair, or explicit user-authorized relaunch.
Each outcome has a user-facing evidence report. Neither Slurm requeue nor
automatic scientific recomputation is a recovery operation.
The concrete [run-state v1 layout](002-durable-run-state/run-state-v1.md) has been accepted.
Run metadata, exact original Flow bytes, external-artifact references,
per-run locks, the anchored event journal, and original-Flow job-definition
storage and attempt/invocation metadata storage are implemented. Completion
bundle publication, explicit same-identity recovery/audit, and completion-event
registration are installed and locally verified. The jobflow result-store
adapter, worker integration, and scheduler-aware recovery remain planned. The illustrative layout
is not a claim that every planned path exists.
The accepted [failed-job amendment](decisions/0003-failed-job-amendments.md)
allows an explicit corrected definition and new attempt in the same run only
when the failed job has no committed output and no descendant has started.
It does not mutate the original flow or automatically follow a changed branch.

One Slurm job per jobflow job is the first supported scheduling granularity.
Packing or arrays can follow after the state model has been validated.
An optional compute-hour budget is run policy, not a property of a Slurm
account. The first accounting metric can be allocated CPU-hours derived from
Slurm records; it must be labeled as such, not presented as the site's billed
cost. Reaching a budget pauses new submissions, and adding budget resumes the
same durable run.

## Portable core versus integration code

| Portable package | Downstream consumer / HPC site |
| --- | --- |
| Jobflow graph and response semantics | Workflow functions, makers, scientific policy |
| Durable state format and reconciliation | Runtime image and application dependencies |
| Slurm submit/query/cancel adapter | Account, partitions, runner registration |
| Site-binding schema and diagnostics | Site file, storage provisioning, optional adapter |
| Generic worker entry point | Licensed executables, modules, pseudopotentials |

The initial filesystem workspace provider assumes a shared POSIX directory.
Other workspace lifecycles (for example, an expiring allocation service) must
enter through a provider interface that can create, resolve, list, and report
the expiry of runs. A site-specific provider belongs in the consumer unless
its behavior proves generally useful.

The proposed [retention and recovery refinement](architecture-refinement-retention-and-recovery.md)
separates workspace expiry, independent loss/rollback detection, operational
protection, archival, restoration, and scientific continuation. These are not
installed capabilities. The small project-directory catalog/witness is deferred
until a persistence mechanism addressing workspace expiry receives explicit
approval; additional storage services remain outside the current setup.

See [jobflow compatibility](jobflow-compatibility.md) for the behavior this
manager must preserve. Jobflow's public documentation describes `Job`, `Flow`,
references, and stores; it does not specify a single manager plugin protocol,
so compatibility is established by behavior tests against the supported
jobflow version.
