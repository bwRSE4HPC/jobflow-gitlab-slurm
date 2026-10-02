# Decision 0003: explicit amendments to failed jobs

Status: accepted for the first implementation slice on 2026-09-28; not implemented.

## Scope

A user may correct the definition of a **failed, uncommitted** job and
continue the same run without rerunning validated ancestors. This is an
explicit amendment, not an edit of `flow/original.json` and not an automatic
pickup of the current repository branch. The original definition, execution
attempt, logs, and any partial output remain immutable evidence.

The first slice permits an amendment only if all of these checks pass under
the per-run lock:

- the target Slurm job is terminal and has no active or ambiguous submission;
- the target has no committed valid output, and no descendant has started;
- the corrected definition retains the target's incoming dependencies and
  expected output contract, so committed parent results remain applicable;
- the corrected workflow and its importable code/runtime are pinned by an
  immutable commit or content digest and pass validation;
- the user has reviewed the impact and explicitly authorized any new compute
  allocation and budget change.

If the dependency graph, parent inputs, output contract, or already-started
descendants change, this narrow same-run operation is rejected. A future
derived-run/fork mechanism may reuse selected validated checkpoints, but it
must not retroactively rewrite a completed branch. Where jobflow identity or
reference compatibility cannot be demonstrated, reject the amendment rather
than guessing how to remap UUIDs or indices.

## Amendment record and execution

The controller first produces a non-submitting impact report: target job
UUID/index and failed attempt, definition diff or digests, committed ancestors
to reuse, descendants held, proposed runtime revision, expected Slurm
resources, and whether the action is parse-only or a new scientific
calculation. The user then authorizes a specific amendment request. Repeating
the same request is idempotent; conflicting requests require inspection.

An append-only amendment record captures the reason, approving identity,
timestamp, original and corrected definition digests, code/runtime versions,
validation evidence, and parent output identities. It produces a new
`attempt_id` for the target logical job and an event identifying the effective
definition for that attempt. The original flow and failed attempt are not
overwritten. The implementation must demonstrate that downstream
`OutputReference` resolution still targets the corrected committed result;
it must not rely on an untested use of jobflow `Response.replace` for this
user-driven operation.

A correction to postprocessing can use [parse-only repair](0002-attempt-publication-and-recovery.md)
when retained scientific files suffice, without launching the scientific
executable. A changed scientific input (for example, a VASP INCAR setting)
requires a new calculation attempt. If durable files are absent, a parser
correction cannot recreate them. Any repair failure keeps its own record and
returns the run to `needs_attention`; it never silently starts VASP.

## Operator view and acceptance checks

The failure report must identify whether an amendment is eligible, why it is
rejected if not, which successful results would be reused, and whether the
proposed continuation launches a scientific job or consumes additional
compute budget. The accepted amendment, its new attempt, and subsequent
outcome remain visible from the same run.

Offline tests must prove: a corrected failed child consumes a committed
parent output without rerunning the parent; a duplicate amendment request
does not create a second attempt; corrections to a committed job or one with
a started descendant are refused; changed dependencies are refused; and
the old and new code revisions remain distinguishable. A non-VASP live
consumer must exercise an amendment before claiming the feature works on
Slurm.
