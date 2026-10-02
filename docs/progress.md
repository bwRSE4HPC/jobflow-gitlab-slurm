# Progress record

Updated: 2026-10-02. This record distinguishes observations from proposals.
Update it after each guided checkpoint; do not mark a gate complete from an
assumption or from a command that only checks part of the path.

## Current state

Latest coverage-completion check (2026-10-02): the expanded suite has 85 tests,
with 84 passing and one failing; combined statement/branch coverage reaches
100%. The failing test exposes a budget JSON round-trip mismatch: a valid
`Decimal("0.000000000000000001")` serializes as `"1E-18"`, which the validator
rejects. Ruff reports two FURB157 findings for `Decimal("0")`/`Decimal("-1")`,
and three test files need formatting. Locked synchronization passes. These
findings supersede the earlier green 65-test checkpoint below; no production
or test changes were made during this check. Resolve them and rerun the checks
before claiming the expanded suite is green.

| Item | Status and evidence |
| --- | --- |
| Public source repository | Confirmed locally: `origin` is `https://github.com/bwRSE4HPC/jobflow-gitlab-slurm.git`; development branch is `001-jobflow-compatible-package`. |
| Package implementation | User-reported `uv lock`, `uv sync --locked`, and `uv build --no-sources` succeeded on 2026-09-28. Bounded local and isolated built-wheel imports confirmed `jobflow_gitlab_slurm` and `jobflow 0.3.1` load. The strict site and run-request models, shared YAML loader, single-site cross-check, `validate-site`/`validate-request` CLI commands, and directory-based site registry are present. User-reported 65 tests passed on 2026-10-02. The registry test import-order error was corrected by the user; a local check confirmed Ruff lint and formatting pass for all 10 source/test files. No execution backend is implemented. |
| Design documents | Drafted locally on the development branch and not committed. The user accepted Decisions 0002/0003 and run-state v1 as design contracts. The [testing strategy](testing-strategy.md) now records intended upstream-aligned and backend-specific gates; none of this implies working functionality. |
| GitHub access from UC3 controller | User-reported successful clone. The repository does not yet contain a recorded, reproducible network probe. |
| GitHub access from image builder | Assumed for planning; not verified. |
| GitHub Actions wheel | Distribution build and artifact upload are configured in the local workflow; remote execution and release publication are not verified. |
| Public offline CI | `.github/workflows/ci.yml` and `ci/check-wheel.sh` are present. On 2026-10-02, local locked synchronization, shell syntax, Ruff formatting/lint, all 65 tests, wheel/sdist build, and isolated wheel verification passed. Combined statement/branch coverage is 96%; JUnit/coverage reports and the wheel-check log are in `ci-reports/`. The fresh wheel environment imported every package module, loaded jobflow 0.3.1, passed dependency compatibility checking, and ran the CLI help. A GitHub Actions run remains pending. |
| Live KIT GitLab consumer | Proposed; no integration result is recorded here. |
| Static jobflow serialization boundary | User-reported probe passed on 2026-09-28 with jobflow 0.3.1: a two-job `Flow` was saved to `/tmp/jgs-flow.PTuz9N/flow.json`, loaded in a second Python process with unchanged job UUIDs and parent reference, then executed to produce `root/a` and `root/a/b`. This does not yet test dynamic `Response`, a persistent store, or Slurm recovery. |
| Dynamic jobflow boundary | User-reported corrected probe passed on 2026-09-28 with jobflow 0.3.1. An importable planner function was serialized in a one-job flow, loaded in a second Python process with planner UUID `9b73b14f-43ca-434e-a4b6-04c42dfa19fc`, and returned `Response(addition=...)`; the added job UUID was `85b3ec1c-9ac8-416c-b5e0-0877778cd190`, with outputs `planned` and `42`. The first no-fixture shortcut failed because passing the `Response` dataclass *class* as `Job.function` triggers a monty `TypeError`; it is not needed for the corrected design. |
| Persistent-store/restart boundary | User-reported corrected probe passed on 2026-09-28: `Job.run(store=...)` executed parent UUID `a042dd5c-8120-498f-a0ba-df7ec1bb2be6` and wrote output `42` to `/tmp/jgs-store.XlWzfJ/documents.json` through writable maggma `JSONStore`; a second Python process reopened the store, read that output, and ran the dependent child to output `84` without rerunning the parent. The first attempt with `run_locally(parent)` failed before storage because the parent already belonged to the serialized `Flow`. This proves sequential cross-process result reuse, not production durability or crash recovery. |
| Dynamic response hand-off | User-reported split-process probe passed on 2026-09-28: planner UUID `7083668e-0731-4af4-b455-49d60e6431d2` ran and its full `Response` was saved separately to `/tmp/jgs-response.StJHpo/response.json`; a second process loaded the same added-job UUID `1003f9c9-a698-48c0-9ac8-bd9e49c1566a`, saw one stored planner output document, and ran the added job to output `42`. This proves a response transport shape, not atomic publication of the response and output document. |
| Consumer callable import boundary | The dynamic-flow probe supplied `/tmp/jgs-dynamic.ivv7WZ` on `PYTHONPATH` while deserializing and executing its importable `dynamic_fixture.plan` callable. A controller that deserializes such a flow must therefore have the defining module importable, unless a future data-only scheduling envelope avoids that import. The probe does not establish a safe way to import VASP-heavy consumer code on the controller. |
| Initial contract and recovery policy | User accepted the first-slice `jobflow==0.3.1` target, lightweight consumer imports on the controller, complete attempt publication, and no implicit scientific rerun on 2026-09-28. [Decision 0002](decisions/0002-attempt-publication-and-recovery.md) records these choices and the requested user-facing failure report. No implementation or failure-injection test exists yet. |
| Run-state layout | User accepted the [run-state v1](run-state-v1.md) design on 2026-09-28. It is not implemented; cross-host lock and rename behavior remain site verification gates. |
| User-corrected workflow definitions | User approved the narrow same-run recovery-by-amendment policy on 2026-09-28; [Decision 0003](decisions/0003-failed-job-amendments.md) records its scope, audit trail, and failure-report requirements. No amendment code or compatibility test exists yet. |
| Slurm submission recovery | Open. No backend adapter or crash test exists. |
| Existing `vaspxatomate2` virtual environment | Rebuilt on 2026-09-28. Fresh `.venv` imports Python 3.13.14, jobflow 0.3.1, atomate2 0.1.5, maggma 0.74.0, and monty 2026.7.16. `uv lock --check` passes and the consumer worktree remains clean. |
| System Python | User-reported `/usr/bin/python3` is 3.14.4, incompatible with the consumer's Python `>=3.13,<3.14` requirement. The project environment now uses uv-managed Python 3.13.14 instead. |

## Next checkpoint: public offline CI and packaging checks

The static graph, dynamic addition, sequential cross-process result reuse,
and response hand-off probes pass with jobflow 0.3.1. The user approved the
first package slice; its bootstrap install, import, distribution build, and
isolated wheel import now pass. The first import command was interrupted while
loading jobflow's dependencies, not rejected by an import error; a bounded
retry loaded both packages in about one second.
The strict site-binding and run-request models, shared YAML loader,
single-site cross-check, both offline CLI commands, and site registry pass
65 user-reported tests. The [run-request v1 fields](run-request-v1.md) were
accepted on 2026-09-29. The user approved deterministic discovery from a
consumer configuration directory on 2026-10-02: only immediate regular
`*.yaml` files, no symlinks or unexpected entries, one file per matching
`site_id`, and exact ID selection. The registry patch is now present in this
checkout, and its import-order correction now passes local Ruff checks. The
Stage 1 public GitHub Actions workflow is now present locally. It synchronizes
the locked Python 3.13 environment, checks formatting and lint, runs offline
tests with branch-coverage reports, builds wheel/sdist artifacts, and checks
the built wheel from a clean environment outside the source checkout. This
workflow uses no HPC credentials or licensed software. The reusable
wheel check exports locked runtime dependencies, installs them with hash
verification in a fresh environment, installs only the built package wheel,
and checks installed module paths and the console command outside the source
checkout. On 2026-10-02 its full local execution passed, together with locked
synchronization, shell syntax, formatting, lint, all 65 tests (96% combined
statement/branch coverage), and wheel/sdist build. The only observed packaging
warning was uv falling back from hardlinks to copies across filesystems;
verification still succeeded. The GitHub-hosted workflow must still be run
after pushing. Automatic release
publication and a type-checker configuration remain separate future work;
no results for those are claimed. Once the CI and packaging checks pass,
Stage 2 begins with durable run creation that verifies artifact hashes and
pins the selected site snapshot. Amendment support still requires its
identity, impact, provenance, and reuse tests.

The backend must still establish locking, atomic publication, crash-window
recovery, and Slurm idempotency. `JSONStore` is not selected for production:
it rewrites its file on updates, and concurrent-writer safety is unproven.

Record observed results here. Documentation-only corrections may be made as
evidence arrives; package and repository test code require explicit approval.
