# jobflow-gitlab-slurm

A proposed jobflow manager that uses short-lived GitLab CI controller jobs to
submit and reconcile Slurm workers. It is intended for general jobflow
workflows; VASP and atomate2 are downstream demonstrations, not dependencies
of this package.

The first development cycle delivers an **installable offline validation
package**: strict site/run-request models and YAML loading, validation CLI
commands, a directory-based site registry, and public GitHub Actions checks.
The current suite has 86 passing tests and 100% statement/branch coverage;
the user reported a passing GitHub pipeline on 2026-10-02.

There is no execution manager or supported HPC deployment yet. Durable run
state is the next development cycle. The examples in the design documents
specify intended interfaces unless explicitly marked implemented.

## Offline use and development

Use Python 3.13 and uv. From the repository root:

```bash
uv sync --locked --dev
uv run --locked jobflow-gitlab-slurm --help
```

With your own consumer site and request files (these paths are illustrative):

```bash
uv run --locked jobflow-gitlab-slurm validate-site hpc/sites/example-cluster.yaml
uv run --locked jobflow-gitlab-slurm validate-request request.yaml hpc/sites/example-cluster.yaml
```

These commands validate configuration only; they do not verify artifact bytes,
contact Slurm, or submit a job. See [HPC binding](docs/hpc-binding.md) and
[run-request v1](docs/run-request-v1.md) for the file contracts.

Before submitting changes:

```bash
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests
uv run --locked pytest -q --cov=jobflow_gitlab_slurm --cov-branch --cov-report=term-missing
uv build --no-sources
bash ci/check-wheel.sh
```

GitHub Actions runs offline checks on pushes, pull requests, and manual
dispatch. Its distribution artifacts are test outputs, not a published
package release. See the [testing strategy](docs/testing-strategy.md) for
coverage policy and the separately authorized live HPC gates.

## Contributing

Contributions are welcome through GitHub issues and pull requests. Read the
[contribution guide](CONTRIBUTING.md) for development setup, required checks,
design-change review, and handling sensitive information.

## Design documents

- [Architecture](docs/architecture.md): component boundaries, durable state,
  and recovery model.
- [Run-state v1 design](docs/run-state-v1.md): accepted filesystem layout,
  identity, publication, and failure-report contract; not implemented.
- [Run-request v1](docs/run-request-v1.md): accepted workflow, runtime,
  resource, and budget contract; offline file validation implemented,
  execution pending.
- [HPC binding](docs/hpc-binding.md): the proposed user-facing site file,
  prerequisites, configuration ownership, and verification gates.
- [Jobflow compatibility](docs/jobflow-compatibility.md): behavior the manager
  must preserve and the tests that demonstrate it.
- [Testing strategy](docs/testing-strategy.md): upstream-aligned pytest
  expectations and additional durability, recovery, and HPC verification.
- [Implementation plan](docs/implementation-plan.md): staged delivery and
  acceptance criteria.
- [Progress record](docs/progress.md): observed state, unresolved checks, and
  the next guided checkpoint.
- [Decision 0001](docs/decisions/0001-public-package-private-hpc-consumers.md):
  separation of public package development from site-specific HPC consumers.
- [Decision 0002](docs/decisions/0002-attempt-publication-and-recovery.md):
  accepted first-slice publication, recovery, and user-facing failure-report policy.
- [Decision 0003](docs/decisions/0003-failed-job-amendments.md): explicit,
  audited corrections to failed, uncommitted jobs without rerunning ancestors.

The public repository must not contain site credentials, proprietary inputs,
licensed binaries, or site-specific runner configuration. Live HPC integration
belongs in an access-controlled downstream consumer project.

Licensed under the [MIT License](LICENSE).
