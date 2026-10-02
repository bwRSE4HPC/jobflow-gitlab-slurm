# Progress record

Updated: 2026-10-02. This record distinguishes observations from proposals.
Update it after each guided checkpoint; do not mark a gate complete from an
assumption or from a command that only checks part of the path.

## Current state

Cycle 001 close-out checkpoint (2026-10-02): the user reported a passing
GitHub pipeline. A local recheck of commit `b979ec5` confirmed Ruff formatting
and lint, all 86 tests, 100% combined statement/branch coverage, wheel/sdist
build, shell syntax, and isolated wheel verification. The earlier budget
JSON round-trip failure and test lint/format findings are resolved. The
serializer emits plain decimal strings without precision loss; exponent
notation remains rejected in YAML/JSON input. Stage 1 is complete; no
execution backend or live HPC support is claimed. Merge to `main` and the
next development branch remain user actions.

| Item | Status and evidence |
| --- | --- |
| Public source repository | Confirmed locally: `origin` is `https://github.com/bwRSE4HPC/jobflow-gitlab-slurm.git`; development branch is `001-jobflow-compatible-package`. |
| Package implementation | Strict site and run-request models, safe YAML loading with duplicate/non-string key rejection, single-site cross-check, `validate-site`/`validate-request` CLI commands, directory-based site registry, and exact budget JSON serialization are implemented. Local recheck: 86 tests pass with 100% combined statement/branch coverage, and Ruff lint/format checks pass for all 10 source/test files. No execution backend is implemented. |
| Design documents | Tracked in Git on the development branch. The user accepted Decisions 0002/0003 and run-state v1 as design contracts. The [testing strategy](testing-strategy.md) distinguishes implemented offline checks from intended execution and live HPC gates. |
| GitHub access from UC3 controller | User-reported successful clone. The repository does not yet contain a recorded, reproducible network probe. |
| GitHub access from image builder | Assumed for planning; not verified. |
| GitHub Actions wheel | Wheel/sdist build, fresh-environment wheel verification, and distribution artifact upload are configured. The user reports the pipeline passes. Automated release/package-index publication is not configured. |
| Public offline CI | `.github/workflows/ci.yml` and `ci/check-wheel.sh` are tracked. The user reported successful GitHub execution on 2026-10-02; no run URL is recorded. Local recheck confirms 86 passing tests at 100% coverage, shell syntax, Ruff checks, build, and isolated wheel verification. JUnit/coverage reports and wheel-check log are in ignored `ci-reports/`. The fresh wheel environment imports every package module, loads jobflow 0.3.1, passes dependency compatibility checking, and runs CLI help. CI reports coverage but does not enforce a 100% threshold. |
| Live KIT GitLab consumer | Proposed; no integration result is recorded here. |
| Static jobflow serialization boundary | User-reported probe passed on 2026-09-28 with jobflow 0.3.1: a two-job `Flow` was saved to `/tmp/jgs-flow.PTuz9N/flow.json`, loaded in a second Python process with unchanged job UUIDs and parent reference, then executed to produce `root/a` and `root/a/b`. This probe alone does not test dynamic `Response`, a persistent store, or Slurm recovery; the separate probes below cover some of these boundaries. |
| Dynamic jobflow boundary | User-reported corrected probe passed on 2026-09-28 with jobflow 0.3.1. An importable planner function was serialized in a one-job flow, loaded in a second Python process with planner UUID `9b73b14f-43ca-434e-a4b6-04c42dfa19fc`, and returned `Response(addition=...)`; the added job UUID was `85b3ec1c-9ac8-416c-b5e0-0877778cd190`, with outputs `planned` and `42`. The first no-fixture shortcut failed because passing the `Response` dataclass *class* as `Job.function` triggers a monty `TypeError`; it is not needed for the corrected design. |
| Persistent-store/restart boundary | User-reported corrected probe passed on 2026-09-28: `Job.run(store=...)` executed parent UUID `a042dd5c-8120-498f-a0ba-df7ec1bb2be6` and wrote output `42` to `/tmp/jgs-store.XlWzfJ/documents.json` through writable maggma `JSONStore`; a second Python process reopened the store, read that output, and ran the dependent child to output `84` without rerunning the parent. The first attempt with `run_locally(parent)` failed before storage because the parent already belonged to the serialized `Flow`. This proves sequential cross-process result reuse, not production durability or crash recovery. |
| Dynamic response hand-off | User-reported split-process probe passed on 2026-09-28: planner UUID `7083668e-0731-4af4-b455-49d60e6431d2` ran and its full `Response` was saved separately to `/tmp/jgs-response.StJHpo/response.json`; a second process loaded the same added-job UUID `1003f9c9-a698-48c0-9ac8-bd9e49c1566a`, saw one stored planner output document, and ran the added job to output `42`. This proves a response transport shape, not atomic publication of the response and output document. |
| Consumer callable import boundary | The dynamic-flow probe supplied `/tmp/jgs-dynamic.ivv7WZ` on `PYTHONPATH` while deserializing and executing its importable `dynamic_fixture.plan` callable. A controller that deserializes such a flow must therefore have the defining module importable, unless a future data-only scheduling envelope avoids that import. The probe does not establish a safe way to import VASP-heavy consumer code on the controller. |
| Initial contract and recovery policy | User accepted the first-slice `jobflow==0.3.1` target, lightweight consumer imports on the controller, complete attempt publication, and no implicit scientific rerun on 2026-09-28. [Decision 0002](decisions/0002-attempt-publication-and-recovery.md) records these choices and the requested user-facing failure report. No implementation or failure-injection test exists yet. |
| Run-state layout | User accepted the [run-state v1](run-state-v1.md) design on 2026-09-28. It is not implemented; cross-host lock and rename behavior remain site verification gates. |
| User-corrected workflow definitions | User approved the narrow same-run recovery-by-amendment policy on 2026-09-28; [Decision 0003](decisions/0003-failed-job-amendments.md) records its scope, audit trail, and failure-report requirements. No amendment code or compatibility test exists yet. |
| Slurm submission recovery | Open. No backend adapter or crash test exists. |
| Existing `vaspxatomate2` virtual environment | Historical observation on 2026-09-28: rebuilt `.venv` imported Python 3.13.14, jobflow 0.3.1, atomate2 0.1.5, maggma 0.74.0, and monty 2026.7.16; `uv lock --check` passed and the consumer worktree was clean. Not rechecked for this package close-out. |
| System Python | User-reported `/usr/bin/python3` is 3.14.4, incompatible with the consumer's Python `>=3.13,<3.14` requirement. The project environment now uses uv-managed Python 3.13.14 instead. |

## Cycle 001 completed; next checkpoint: durable run creation

The installable package, offline contracts, site registry, public CI, and
isolated packaging checks complete Stage 1. The earlier manual jobflow probes
established serialization, dynamic addition, sequential cross-process result
reuse, and response hand-off with jobflow 0.3.1; they are exploratory evidence,
not automated execution-manager compatibility tests.

Merge cycle 001 through a pull request after committing the documentation
close-out, then create `002-durable-run-state` from updated `main`. No merge,
branch creation, or push is performed as part of this documentation update.

Stage 2 begins with a reviewed proposal for durable run creation:

- Verify the exact serialized flow, consumer-code artifact, and worker-runtime
  bytes against the immutable request's SHA-256 claims.
- Persist the original request, flow, selected site's normalized snapshot and
  digest, and version/identity metadata without rebuilding the flow.
- Implement atomic run publication and per-run locking; partial creation,
  concurrent access, and unknown schemas must fail safely.
- Add process-restart and injected-write-failure tests before introducing
  worker execution or Slurm submission.

One remaining design detail must be resolved in that proposal: the concrete
versioned serialized-flow envelope and the exact bytes its request digest
identifies. The current contracts require exact-byte hashing and envelopes
but do not fix their encoding. This is a Stage 2 input, not a Stage 1 test
failure; do not silently choose a reserialization convention in code.

The current validator checks digest syntax, not artifact contents. Budget
top-up and failed-job amendment remain designs requiring identity, impact,
provenance, and reuse tests. Automatic release publication, type checking,
and live filesystem verification remain separate future work.

During this cycle, coverage tests exposed a valid small Decimal becoming
rejected exponent notation after JSON serialization. The plain-decimal
serializer and small/large Decimal round-trip regression tests resolve that
failure without relaxing the input contract. The earlier lint findings were
also corrected. No known failure remains in the current offline check suite.

The backend must still establish locking, atomic publication, crash-window
recovery, and Slurm idempotency. `JSONStore` is not selected for production:
it rewrites its file on updates, and concurrent-writer safety is unproven.

Record observed results here. Documentation-only corrections may be made as
evidence arrives; package and repository test code require explicit approval.
