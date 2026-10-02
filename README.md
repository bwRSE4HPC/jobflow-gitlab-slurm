# jobflow-gitlab-slurm

A proposed jobflow manager that uses short-lived GitLab CI controller jobs to
submit and reconcile Slurm workers. It is intended for general jobflow
workflows; VASP and atomate2 are downstream demonstrations, not dependencies
of this package.

This repository has an **initial installable package skeleton**, offline site
and run-request validation commands, a directory-based site registry, and
design documents. There is no execution manager or supported HPC deployment
yet. The examples in the design
documents specify intended interfaces unless explicitly marked implemented.

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
