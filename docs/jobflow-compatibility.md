# Jobflow compatibility contract

Status: proposed behavior; not yet implemented. There is no assumption that
jobflow offers a formal manager plugin interface. The backend targets a
documented, pinned jobflow release and demonstrates compatibility through
black-box tests against its `Job`, `Flow`, `Response`, `OutputReference`, and
`JobStore` behavior.

| Behavior | First proof required |
| --- | --- |
| Static dependency graph | A child waits for its parent, then receives the parent's resolved output. |
| Job identity | UUIDs and indices remain stable after controller restart; a run is not reconstructed by calling makers again. |
| Results and references | Output is persisted in a `JobStore`-compatible form before dependent jobs are released; missing references fail visibly. |
| Dynamic responses | `addition`, `replace`, and `detour` create the correct subsequent work without duplicate execution. |
| Stop semantics | `stop_children` and `stop_jobflow` prevent the appropriate downstream submissions. |
| Files | A dependent job can access explicitly retained predecessor files at a stable path, including after the first allocation ends. |
| Terminal outcomes | Slurm failure, timeout, cancellation, and successful application exit map to distinct durable states; success additionally requires valid jobflow output. |
| Recovery | Repeated scheduled reconciliation and controller crashes do not create duplicate active Slurm jobs or lose a completed response. |
| Incomplete attempts | An output document alone cannot release descendants; a complete validated `Response` and completion marker are required. Incomplete attempts retain their evidence and are not rerun implicitly. |
| Failure reporting | Every terminal non-success or unknown outcome identifies the run/job/attempt, scheduler result, evidence locations, downstream impact, and a truthful repair-or-relaunch action. |
| Failed-job amendment | An explicitly approved corrected child definition consumes its committed parent's output without rerunning the parent; original attempts remain inspectable and out-of-scope changes fail closed. |

The package should ship tiny, application-neutral example flows covering
these cases. A VASP/atomate2 flow is a downstream integration test, not the
only evidence of jobflow compatibility. Features not yet passing must be
listed as explicit limitations and rejected at validation time when possible.

The persistence implementation must be tested with process restarts and real
filesystem writes; an in-memory `JobStore` cannot prove cross-allocation
recovery. File publication and document publication form one logical job
completion boundary, even if they require separate atomic operations.
The accepted recovery and reporting policy is in
[Decision 0002](decisions/0002-attempt-publication-and-recovery.md).
The narrow user-correction contract is [Decision 0003](decisions/0003-failed-job-amendments.md).

References: [jobflow workflow model](https://materialsproject.github.io/jobflow/),
[dynamic flows](https://materialsproject.github.io/jobflow/tutorials/5-dynamic-flows.html),
and [local manager API](https://materialsproject.github.io/jobflow/jobflow.managers.html).
