# Progress record

Updated: 2026-10-09. This document is the current checkpoint, not a log of
minor test/debugging iterations. The [overall plan](implementation-plan.md)
owns stage ordering. Cycle documents retain scope, mechanisms, acceptance
criteria, and historical slice evidence:
[001-jobflow-compatible-package](001-jobflow-compatible-package.md) and
[002-durable-run-state](002-durable-run-state.md). Refined slice and contract
documents are grouped in the matching cycle folders; links below point to
those documents.

## Current state

Stage 1 is complete. Cycle `001-jobflow-compatible-package` was squash-merged
through PR #1 as `ded57b3`. The current branch is `002-durable-run-state`;
Stage 2 implementation slices are locally accepted. The user reported the cycle
review complete on 2026-10-08. On 2026-10-09 the assistant reviewed the installed
cycle, corrected documentation inconsistencies, and executed the full local
gate successfully. The subsequently authorized autonomous structural refactor
is implemented and freshly verified, including domain-oriented source/test
hierarchies, explicit internal contracts, shared filesystem mechanics, and
dependency checks. No blocking code finding was identified within the reviewed
scope. User documentation review, committed merge-candidate identification,
and remote CI remain outstanding; close-out/merge is not approved.

Installed Stage 2 persistence covers:

- Exact-byte artifacts, immutable run/site metadata, POSIX run
  creation/reopening, and directory discovery.
- Anchored journal replay, append, matching-tail recovery, and journal-aware
  read-only inspection.
- Qualified job-definition/attempt/invocation metadata and immutable storage.
- Invocation ownership, opaque bundle inspection, ordinary publication,
  retained publication intents, recovery audit/request/receipt storage,
  and explicit same-identity bundle recovery.
- Recovery-journal and ordinary-publication-journal association, installed with
  their local acceptance gates
  user-reported passing.
- Whole-lifecycle cross-process persistence tests, installed with their local
  verification gate user-reported passing.

All installed slices through the lifecycle tests have passing local
acceptance evidence. See the [cycle 002 slice record](002-durable-run-state.md#implementation-slices-and-acceptance-evidence)
for the distinction between user reports and assistant-executed checks.
No production `JobStore` adapter, worker, scheduler-aware result selection,
Slurm submission, budget top-up, amendment execution, or GitLab reconciler
is implemented.

## Latest verification evidence

Assistant-executed on 2026-10-09, in the actual working tree on
`002-durable-run-state`, based on `ded57b3a5c40cbc33f61d5c8612356731e1ab330`.
The cycle's source/tests include uncommitted and untracked files; the base commit
alone does not identify the verified implementation. Commands, a verification
input fingerprint, review scope, and findings are retained in the
[cycle review and verification record](002-durable-run-state/review-and-verification.md).

| Gate | Evidence |
| --- | --- |
| Locked development environment | Existing locked Python 3.13.14 environment used; synchronization passed at the preceding cycle-review gate. No dependency changes. |
| Formatting/lint, shell syntax, CLI help, tracked-diff whitespace | Passed; 135 Python files already formatted. Installed CLI checked by the wheel gate. |
| Full offline tests | 2,353 passed in 172.96 seconds, including all 12 persistence-lifecycle cases and offline architecture checks. |
| Agreed coverage gate | 100% statements and branches: 3,636/3,636 statements and 742/742 branches; explicit `--cov-fail-under=100` passed. |
| Migration inventory and encoded bytes | All 2,324 original collected cases retained; all 14 representative encoding samples match the pre-refactor baseline exactly. |
| Wheel/sdist build and isolated-wheel verification | Passed; built wheel imported outside the checkout, dependency compatibility and installed CLI checked. |
| Local Markdown file/directory links | Fresh post-refactor check recorded in the verification record. External URLs and heading anchors are not checked. |
| Cycle review | Assistant code/documentation review completed on 2026-10-09; no blocking code finding identified. |
| Close-out documentation review | Pending user review of the updated documents. |
| Exact merge-candidate verification | Working tree verified; candidate commit not yet recorded. Repeat affected gates after substantive source/test/configuration changes. |
| Cycle 002 remote CI | Pending; no result recorded. |
| Live cross-host filesystem verification | Pending Stage 6 deployment gate. |

These are local executed checks, not remote CI or HPC-readiness evidence.
Automated package release/publication and type checking remain unconfigured.
Public CI reports coverage but does not enforce a 100% threshold; this known
automation gap does not change the agreed local 100% statement/branch gate.

## Persistence boundaries and limitations

Cycle 002 supplies opaque storage and integrity evidence, not jobflow semantics
or scheduler eligibility. Stage 3 supplies the store/worker and Stage 4 the
reconciler and budgets. See the [cycle boundary](002-durable-run-state.md#persistence-behavior-and-boundaries)
and the [versioned contracts](002-durable-run-state.md#slice-and-contract-map).

Filesystem completion, audit completion, and journal anchoring are separate
transactions. Missing registration does not authorize calculation replay.
Cooperating-writer exclusion, trusted run roots, and same-filesystem publication
are assumptions. Local tests do not prove cross-host visibility, lock behavior,
or power-loss durability. Coordinated history/head rollback and whole-workspace
loss require independently retained evidence to detect; recovery of lost bytes
additionally requires a retained copy.

The [retention and recovery architecture refinement](architecture-refinement-retention-and-recovery.md)
records the proposed separation of observation, protection, archival, restore,
and derived-run continuation. These extensions are not implemented or included
in cycle 002 close-out. A small catalog/witness outside expiring workspaces is
plausible but deferred until explicit approval of a persistence mechanism
addressing workspace expiry. Additional storage-service integration remains
beyond the current setup; no new implementation cycle is approved.

## Integration gates and next checkpoint

- Public offline CI: cycle 001 user-reported passing on 2026-10-02; no run URL
  recorded. Cycle 002 remote CI remains pending.
- GitHub clone from the UC3 controller: user-reported successful; no reproducible
  network probe recorded. Image-builder reachability remains an assumption.
- Live GitLab/HPC consumer, cross-host filesystem probes, worker recovery,
  and Slurm idempotency: no verified integration result.
- Historical downstream imports on 2026-09-28: Python 3.13.14, jobflow 0.3.1,
  atomate2 0.1.5, maggma 0.74.0, monty 2026.7.16. This is not current deployment
  verification; exploratory jobflow observations are recorded in
  [cycle 001](001-jobflow-compatible-package.md#exploratory-evidence-informing-later-cycles).

The [read-only lifecycle review](002-durable-run-state/lifecycle-review.md)
is complete. Its dedicated test-only cross-process lifecycle slice and retention
of payload preparation/producer enforcement in Stage 3 were approved on
2026-10-08. Lifecycle tests are installed and their local gate is user-reported
passing. The assistant's 2026-10-09 full-suite rerun includes all lifecycle cases.

On 2026-10-09 the user adopted the refined
[source/test organization](002-durable-run-state/package-organization.md):
separate `runs/` foundations and `queries/`, pure record-model ownership,
explicit internal/dependency contracts, bounded filesystem-helper extraction,
architecture checks, behavioral test splits, scoped support/fixtures, and
explicit collection scope. The selected Python import migration is a clean
break without old-path wrappers. Component subprocess tests remain with their
contract; lifecycle composition belongs under integration.

The user subsequently authorized autonomous edits and verification of the full
bounded refactor. Source/test moves, semantic test splits, explicit helper and
fixture ownership, dependency checks, wheel-path adaptations, and documentation
updates are complete. CLI behavior, persisted identities/paths/formats, and
failure classifications are retained; old flat Python imports intentionally
break without compatibility wrappers. The fresh local gate above covers the
reorganized tree. No temporary test checkout was created; the existing isolated
wheel gate was the sole separate installation.

Next action: review the updated close-out documents, identify the final candidate
through user-directed Git operations, and run cycle-specific remote CI.
Stage 2 close-out and Git/remote operations still require explicit instructions.

### Stage 2 close-out work remaining

User review and assistant review are recorded; the local working-tree gate has
passed. Remaining gates are:

1. User review of the updated documentation and Stage 3 hand-off in the
   [cycle overview](002-durable-run-state.md#close-out-summary-and-stage-3-hand-off).
2. Confirm final quality, full offline tests, agreed 100% combined coverage,
   and packaging gates cover the final code/configuration revision. Record the
   candidate commit and results; repeat affected gates after substantive changes.
3. User-directed commit/push and pull request, then passing `Offline checks`
   on the latest PR revision. Record the CI run URL and commit; CI reports
   coverage but does not enforce the agreed local 100% threshold.
4. Explicit close-out approval and user-directed squash-merge. Record the PR
   and merge reference afterward. Begin Stage 3 planning from updated `main`
   only after merging; its branch name and slice scope remain to be agreed.

Live cross-host verification remains a Stage 6 gate. Local close-out must
retain this limitation explicitly, not claim HPC readiness or power-loss durability.
