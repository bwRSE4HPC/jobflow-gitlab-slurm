# Cycle 002 review and local verification

## Historical pre-refactor review

The evidence in this section and the original gate below precedes the source/test
reorganization. Fresh refactor evidence is recorded at the end of this document;
historical counts and fingerprints are retained unchanged.

Date: 2026-10-09. The user explicitly requested code review, documentation
updates, and execution of the verification gate. Only documentation was edited;
source, tests, CI, and dependency files were not changed. No Git operations were
performed.

## Reviewed scope and result

Reviewed the installed cycle against its accepted opaque-persistence boundary:
artifact/run storage, discovery and CLI inspection, qualified metadata,
invocation ownership, anchored journal, bundle inventory/publication, retained
intents, explicit recovery/audit, completion-event associations, lifecycle tests,
and packaging/CI integration. The base commit is
`ded57b3a5c40cbc33f61d5c8612356731e1ab330` on branch `002-durable-run-state`;
the cycle implementation is still uncommitted, including untracked files.
Review and verification therefore cover the working tree, not that commit alone.

No blocking code defect was identified within this review's scope. In particular,
the reviewed paths preserve retained expectations and identities, separate
filesystem/audit/journal acknowledgment, keep payload I/O outside the run lock,
use invocation-before-run lock ordering, and hold missing, conflicting,
ambiguous, or incomplete evidence without implicitly rerunning a calculation.
This is a bounded review conclusion, not proof that all defects are absent.

Findings and follow-up:

- **Documentation inconsistency, corrected:** several earlier contracts still
  described installed bundle publication, explicit recovery, audit storage,
  and event association as future work. Current checkpoint/architecture and
  slice hand-off statements now distinguish installed opaque mechanisms from
  unimplemented worker, JobStore, scheduler, and scientific-result semantics.
- **Known nonblocking CI automation gap:** `.github/workflows/ci.yml` reports
  coverage but does not enforce the agreed local 100% threshold. This review
  executed that threshold explicitly. Any CI enforcement change needs a
  separately approved configuration edit; remote CI remains pending.
- **Accepted integration boundaries, unchanged:** the low-level publisher
  does not enforce intent presence; Stage 3 must enforce producer ordering.
  Recovery authorization and orphan-writer quiescence are caller obligations.
  Stage 4 must establish scheduler evidence and result eligibility. Stage 6
  must verify cross-host filesystem behavior. These are not implemented worker
  or HPC guarantees, nor waived by the local test results.

## Executed gate

Run from the actual repository root, using Python 3.13.14 and the locked
development environment:

```bash
uv sync --locked --dev
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests
bash -n ci/check-wheel.sh
uv run --locked jobflow-gitlab-slurm --help
git diff --check

uv run --locked pytest -q \
  --cov=jobflow_gitlab_slurm --cov-branch --cov-report=term-missing \
  --cov-report=xml:ci-reports/coverage.xml \
  --junitxml=ci-reports/junit.xml --cov-fail-under=100

uv build --no-sources
TMPDIR=/var/tmp UV_LINK_MODE=copy bash ci/check-wheel.sh
```

All commands passed. Full suite: **2,324 passed in 166.50 seconds**, including
all 12 persistence-lifecycle cases. Coverage: **3,630/3,630 statements and
742/742 branches**, with no missed statements or partial branches. Ruff reported
56 files already formatted and no lint errors. Wheel/sdist builds succeeded;
isolated wheel imports, dependency compatibility, and installed CLI checks
passed outside the source checkout. The wheel gate retained its temporary
environment for inspection; no cleanup was performed.

JUnit and coverage evidence are in ignored `ci-reports/`; distributions are
in ignored `dist/`. Local Markdown file/directory link targets were also checked
before and after documentation updates; no missing target was found. This
check does not validate external URLs or heading anchors. `git diff --check`
checks tracked diffs only; Ruff includes the untracked Python source/tests.

Verification-input fingerprint (documentation excluded):

```text
7881ae36681e8302d2c029a88afc23c5941706e91fe2441f35dad8f30539a427
```

Reproduce the fingerprint from the repository root:

```bash
{ rg --files --hidden src tests ci .github; printf '%s\n' pyproject.toml uv.lock .python-version; } |
  LC_ALL=C sort | xargs sha256sum | sha256sum
```

The fingerprint identifies the reviewed local inputs, not a signed provenance
record or a substitute for recording the candidate commit and CI run.

## Remaining close-out gates

User review of the updated documentation; user-directed candidate commit/push
and pull request; passing `Offline checks` for that exact revision with its run
URL recorded; explicit close-out approval and user-directed squash-merge.
Repeat affected local checks if source/tests/configuration change after this gate.
No Git, publication, or HPC operations were performed.

Local process-interruption tests do not prove cross-host lock/rename visibility
or power-loss durability. The trusted-root/cooperating-writer contract remains
in force. Coordinated rollback of journal history and its head, or loss of the
whole workspace, requires an independent witness to detect. Future Stage 3/4/6
gates remain required before advertising workflow execution or HPC readiness.

The subsequent [retention and recovery architecture refinement](../architecture-refinement-retention-and-recovery.md)
separates detection from restoration of lost bytes and records deferred
mitigation proposals. It does not close these limitations or change the
verification evidence above.

## Autonomous organization refactor: fresh verification

Date: 2026-10-09. The user explicitly authorized implementation of the approved
[organization](package-organization.md), including source/test/configuration
edits, local verification, and documentation updates in the actual checkout.
No branch, commit, push, merge, remote-service, or HPC operation was performed.
The working tree remains uncommitted; unrelated existing work was preserved.

Implemented scope:

- Domain-oriented source ownership, pure external-artifact models in run
  records, and minimal inert initializers, without old-import wrappers.
- Bounded descriptor/flush extraction with preserved flags, cleanup, and order;
  deliberate bundle durability interfaces and a caller-locked journal reader.
- Responsibility-based test splits, scoped fixtures and explicit support,
  separated CLI/composition checks, and static dependency/cycle/test-import
  checks with deliberately invalid examples.
- CLI/subprocess/monkeypatch import adaptations, explicit pytest collection
  scope with unchanged import mode, installed-wheel targets, and current docs.

Package name/version, console command, CLI behavior, persisted identities,
paths, formats, permissions, locks, and failure classifications are unchanged.
The Python import paths deliberately change to the approved hierarchy. No
dependency upgrade, persistence migration, worker, or scheduler feature is added.

Executed final gate:

```bash
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests
bash -n ci/check-wheel.sh
git diff --check
uv run --locked pytest -q \
  --cov=jobflow_gitlab_slurm --cov-branch --cov-report=term-missing \
  --cov-report=xml:ci-reports/coverage.xml \
  --junitxml=ci-reports/junit.xml --cov-fail-under=100
uv build --no-sources
TMPDIR=/var/tmp UV_LINK_MODE=copy bash ci/check-wheel.sh
```

Observed results: **2,353 passed in 172.96 seconds**; **100% statements and
branches (3,636/3,636 statements; 742/742 branches)**, with no missed or partial
branches. Ruff reported 135 files already formatted and no lint errors. Shell
syntax and tracked-diff whitespace checks passed. Wheel/sdist build and the
isolated installed-wheel import, dependency, and CLI checks passed. Neither
distribution contains test/support code. The existing wheel check was the only
separate installation; no test checkout copy or alternate test environment was
created. All 37 Markdown documents' local file/directory links resolve;
external URLs and heading anchors were not checked.

Migration evidence is independent of count equality: the inventory maps all
711 original test functions, and collected-case multiset comparison finds no
missing original scenario among the 2,324 baseline cases. All **14**
representative encoded-record samples match the pre-refactor baseline exactly.
Added cases cover extracted filesystem mechanics, direct loader behavior, and
architecture constraints. This is sampled encoding evidence, not an exhaustive
proof of every possible record's encoding. Local inventory/comparison and
verification reports are retained in ignored `ci-reports/`.

Fresh verification-input fingerprint, using the command above:

```text
854ab2c7e27027592a19ebb5915cf7946ecd3711011b00221d2745a8debcdc21
```

Historical pre-refactor evidence above remains unchanged. Remote CI, candidate
commit identification, merge approval, and live cross-host durability remain
unverified. The retention/independent-witness gaps and Stage 3/4/6 boundaries
are unchanged; structural organization does not close them.

## Committed candidate checkpoint

The user reports committing/pushing the cycle and successful `Offline checks`.
Read-only inspection identifies candidate
`a48020e8b3ce81a75c83e2b901d7d42c89449ada` on `002-durable-run-state`, also
present in the local remote-tracking ref. Its verification-input fingerprint
matches the fresh refactor gate exactly. The working tree was clean before this
documentation update. No tests or builds were repeated; the unchanged fingerprint
ties the recorded local results to this candidate. CI success is user-reported;
no run URL was supplied and remote CI was not independently inspected.

No technical blocker to cycle integration is identified. Final documentation
review, a passing required check on the final PR head, and explicit user-directed
squash-merge remain. No merge or other Git mutation was performed. Live HPC,
cross-host durability, and retention/witness extensions remain later gates.
