# Testing strategy

Status (2026-10-09): **offline foundation implemented and locally verified;
execution tests planned**. The assistant's cycle 002 code/documentation review
and full local quality, test/coverage, and packaging gate passed. The
[review record](002-durable-run-state/review-and-verification.md) retains commands
and results; [progress](progress.md) owns the checkpoint. User documentation
review, committed candidate identification, and cycle-specific remote
`Offline checks` CI remain pending. Historical slice evidence remains in the
cycle overview rather than being duplicated here.

Installed tests cover configuration, opaque artifacts and metadata, POSIX run
storage/discovery, anchored journal operations, qualified definitions/attempts/
invocations, ownership, bundle inspection/publication, retained intent/audit,
explicit recovery, and completion-event association. Cross-process lifecycle
tests exercise empty-root creation, metadata hand-offs, publication, recovery,
registration, and conservative evidence holds. Inert payloads test persistence,
not jobflow execution or scheduler semantics.

Cycle 001 GitHub Actions success is user-reported; no cycle 002 remote result
is recorded. Cross-host filesystem tests, worker semantics, and Slurm integration
remain unimplemented. This document defines the additional evidence required
before claiming jobflow execution compatibility or live HPC support.

## Upstream baseline and our additional obligations

The [jobflow contribution guide](https://github.com/materialsproject/jobflow/blob/main/CONTRIBUTING.md)
asks contributors to use NumPy-style code, update documentation when needed,
and write `pytest` tests for new features; its CI runs proposed changes. The
[Materials Project contributor guide](https://docs.materialsproject.org/community/getting-involved/contributor-guide)
also calls for a regression test for each bug fix and functional tests for
new features. Jobflow's current [project configuration](https://github.com/materialsproject/jobflow/blob/main/pyproject.toml)
uses pytest, coverage, and Ruff. These are **upstream alignment targets**, not
an assertion that this separate repository inherits upstream's exact CI or
coverage threshold. Recheck the guides and supported Python versions before
proposing an upstream PR.

For this backend, a passing unit suite alone is insufficient: the central
claim is that a real jobflow graph can advance over separate controller and
worker processes without losing identities, duplicating calculations, or
exposing incomplete results. The test suite therefore separates pure logic,
jobflow compatibility, durable-state faults, simulated orchestration, and
opt-in live HPC verification.

## Test layers and where they run

| Layer | Required evidence | Default location |
| --- | --- | --- |
| Offline unit tests | Versioned site/run-request schemas, unknown-key rejection, resource bounds, paths, state transitions, budget arithmetic, and precise diagnostics. | Public GitHub CI and local development; no Slurm, GitLab, network, or licensed software. |
| Jobflow contract tests | Use installed, pinned `jobflow==0.3.1` objects and tiny importable Python callables; compare observable semantics with jobflow's local execution where appropriate. | Public CI, including fresh-process serialization tests. |
| Durable-state integration | Real files in temporary workspaces; separate processes; crash injection at each publication boundary; concurrent reconciliation attempts. | Public CI on a local filesystem, then a site filesystem probe. |
| Simulated controller/Slurm tests | Fake submit/query/cancel responses and repeated scheduled-controller invocations, including delayed or contradictory accounting. | Public CI; no actual `sbatch`. |
| Packaging and quality | Wheel/sdist build, clean-wheel import, lint/format checks, and test/coverage reporting are implemented; type checking remains planned. | Public GitHub CI and local development. |
| Live HPC gates | Identity, shared-path, lock/rename, Apptainer/runtime, one bounded toy submission, terminal accounting, restart, and recovery. | Explicitly triggered in an access-controlled downstream consumer; never in public PR CI. |
| Scientific consumer | atomate2/VASP render, run, parse, and adaptive KSPACING behavior. | Downstream `vaspxatomate2` project after generic HPC gates pass. |

Each test names the contract it proves. The public suite must use no VASP
binary, POTCAR, KIT account, real Slurm allocation, or privileged runner.
The live suite must not be triggered merely by importing the package or by a
public pull request.

## Jobflow compatibility matrix

The [compatibility contract](jobflow-compatibility.md) is the source of truth
for supported behavior. Every row below needs at least one positive test and
one relevant negative or replay test before it is advertised as supported.
Unsupported jobflow behavior must be rejected clearly, not silently ignored.

| Contract | Minimum test fixture and assertion |
| --- | --- |
| Static graph and output references | Two-job parent/child flow; child receives the committed parent value, not an unresolved reference; a missing parent result blocks the child. |
| Identity across processes | Serialize once, load in a second process, and reconcile repeatedly; flow/job UUIDs and indices stay fixed without calling the maker again. |
| Store semantics | Persist a jobflow-compatible output document; reopen in another process and resolve a child's reference; staged or malformed documents remain invisible. |
| Dynamic `Response` | Cover `addition`, `replace`, and `detour` separately; serialize the full response, restart, and prove each new job is introduced and run at most once in the correct order. |
| Stop semantics | `stop_children` and `stop_jobflow` prevent the appropriate descendants from being submitted, including after restart. |
| File hand-off | A child can read only declared retained predecessor files at a stable path after the predecessor allocation ends; missing or altered files fail visibly. |
| Failure and amendment | A failed child leaves committed ancestors reusable; an explicitly approved corrected definition creates one new attempt and preserves the original flow and evidence. Out-of-scope amendments fail closed. |

Test callables belong in an **importable fixture module**, not in `__main__`
or an inline shell Python snippet, because serialized jobflow functions must
load in another process. Use tiny arithmetic/string/file jobs so tests expose
backend behavior instead of application-specific physics.

## Persistence, concurrency, and recovery tests

Run the state machine against a temporary run root that follows
[run-state v1](002-durable-run-state/run-state-v1.md). Inject failure immediately before and after
each durable boundary: request/flow write, submission intent, Slurm receipt,
job document, full `Response`, file manifest, publication rename, `COMMIT`,
dynamic-response event, and latest-report projection. Restart a **new
process** after each interruption. Required invariant: descendants see a
result only after a complete validated commit and successful terminal Slurm
state; no replay creates a second committed result or applies a dynamic
response twice.

Run two reconcilers against the same run to prove lock exclusion. Simulate an
accepted `sbatch` whose receipt was not written: lookup by the stable token
must recover the existing job, or leave the outcome `unknown`; it must not
blindly resubmit. Simulate missing `Response`, corrupt JSON, missing files,
conflicting worker invocations, expired workspace, unavailable accounting,
timeout, cancellation, and a completed application with failed parsing. Keep
the original evidence and verify the user-facing report names run/job/attempt,
scheduler and publication states, held descendants, evidence paths, and a
truthful repair-or-relaunch action.

The local-filesystem suite cannot establish cross-host HPC filesystem
semantics. The downstream doctor/smoke tests must separately demonstrate
lock exclusion and rename visibility from controller and compute-node hosts.
If those capabilities are not proven, live submission is blocked.

## Deterministic harness rules

- Inject a clock, ID generator, Slurm adapter, and filesystem fault hook so
  crash windows are repeatable; keep one small integration path using real
  subprocesses and files rather than mocks alone.
- Use fake Slurm responses for pending, running, completed, failed, timed out,
  cancelled, unknown, duplicate-token, and accounting-lag cases. Assert the
  number of **submission calls**, not only the final state.
- Never rely on test order, a persistent developer database, wall-clock sleep,
  or the availability of a particular KIT cluster. Temporary directories and
  fake state are isolated per test.
- Preserve a minimal redacted fixture for every regression. A bug fix adds a
  failing-before/fixed-after test, as requested by Materials Project guidance.
- Test both unbounded compute-hour budget and a finite budget that pauses the
  same run, records consumed allocated CPU-hours, accepts an explicit top-up,
  and resumes without rebuilding or rerunning committed jobs.
- Check that diagnostics and public CI artifacts do not print tokens, licensed
  inputs, proprietary output, or full environment dumps.

## CI and release gates

The first implementation uses `.github/workflows/ci.yml` on a GitHub
hosted Ubuntu runner and a reusable `ci/check-wheel.sh` packaging check. Both
files are tracked. The local close-out check on 2026-10-02 passed shell syntax,
Ruff formatting/lint, 86 tests with 100% combined statement/branch coverage,
wheel/sdist build, and isolated wheel installation/import/CLI checks. Locked
environment synchronization was also verified during
[001-jobflow-compatible-package](001-jobflow-compatible-package.md). Reports are
written to ignored `ci-reports/`. The user reported successful GitHub
execution; a run URL has not been recorded.

Cycle 002's pre-refactor gate passed 2,324 tests on 2026-10-09. Following the
approved autonomous reorganization, the fresh assistant-executed gate passed
2,353 tests with 100% statement and branch coverage and successful quality and
packaging checks. All original collected scenarios remain represented, and
14 representative record encodings match exactly. The
[review record](002-durable-run-state/review-and-verification.md)
identifies the checked working tree and commands. Cycle-specific remote CI and
live HPC verification remain pending; local process-interruption tests do not
establish power-loss durability or cross-host filesystem semantics.

The workflow runs on pushes, pull requests, and manual dispatch, retains
JUnit/coverage reports for seven days and verified distributions for fourteen
days, and uses read-only repository permissions. These artifacts are
verification outputs; publishing a release to a package index is separate
work. It uses no HPC credentials or licensed software.

The implemented public CI sequence is: synchronize a locked development
environment; run formatting/lint and shell-syntax checks; run offline
pytest with branch-coverage reporting; build source and wheel distributions;
and import the **built wheel** in a clean environment. A build from the source
checkout alone does not prove that the distribution wheel contains all
required package modules. The wheel check imports each current package module,
checks dependency compatibility, and runs the installed console command from
a fresh environment outside the checkout. The current target is the pinned
Python 3.13/jobflow 0.3.1 pair; add
other Python and jobflow versions only after their compatibility tests pass.
Do not imply support from a dependency resolver alone.

No arbitrary overall coverage percentage is claimed as an upstream
requirement. The current 100% result is observed coverage for the small
offline package, not proof of backend execution correctness. GitHub CI
reports coverage but does not enforce a minimum. The optional local
`--cov-fail-under=100` flag was used for the
[001-jobflow-compatible-package](001-jobflow-compatible-package.md) close-out check; no
repository-wide threshold or type checker has been configured.

Review missing branches in identity, publication, replay,
submission recovery, and amendment logic explicitly; these safety-critical
invariants require direct tests even if headline coverage is high. A feature
PR is not ready when its documented contract row has no test, a failure can
lead to implicit recalculation, or a terminal state has no useful report.

To reproduce the current local checks:

```bash
uv sync --locked --dev
bash -n ci/check-wheel.sh
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests
mkdir -p ci-reports
uv run --locked pytest -q \
  --cov=jobflow_gitlab_slurm \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:ci-reports/coverage.xml \
  --junitxml=ci-reports/junit.xml
uv build --no-sources
bash ci/check-wheel.sh
```

The wheel check expects exactly one wheel in `dist/` and retains its temporary
environment for inspection. A uv hardlink-to-copy fallback warning on
different filesystems is not a test failure.

Live HPC tests are separately authorized and budgeted. They follow the four
[HPC binding verification gates](hpc-binding.md#verification-gates): offline
validation, non-submitting doctor, bounded toy smoke, and recovery. Record
the site/runtime versions, Slurm ID, run path, observed state, and sanitized
failure evidence. The private consumer adds VASP/atomate2 tests only after the
generic non-VASP path passes. A live success on one site does not replace the
offline suite or establish portability to another cluster.

## Review evidence

For a significant change, the PR description should state the changed
contract rows, exact offline commands and results, wheel-build result,
version pair tested, and any live gate performed or deliberately deferred.
For a bug report, retain upstream's reproducible example, expected versus
actual behavior, and traceback; add a redacted run/job/attempt ID, state
records, scheduler result, and backend/jobflow versions when relevant. Never
publish site credentials or licensed scientific data in this public repo.
