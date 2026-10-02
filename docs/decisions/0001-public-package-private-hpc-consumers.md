# Decision 0001: public package, separate HPC consumers

Status: proposed for acceptance before implementation.

The source of this package is a public GitHub repository. General orchestration
code, documentation, and offline tests are developed here. The package does
not require a mirror on KIT GitLab merely to gain runner access.

An access-controlled downstream GitLab consumer owns the runner, protected
pipeline and schedule, [site binding](../hpc-binding.md), secret settings,
and live HPC tests. A small non-VASP consumer proves the backend first;
`vaspxatomate2` later demonstrates the scientific use case. Both pin an exact
package commit or release. No consumer automatically follows a moving branch.

Rationale: this keeps site configuration and licensed application details out
of the public repository, permits maintainers to inspect and contribute to
the general package, and limits the trusted shell runner to code reviewed in
the consumer project. A mirror or internal package registry remains an option
if KIT nodes cannot fetch the public source.

Consequence: public CI alone cannot claim live Slurm support. A separate,
explicit integration gate must run in the consumer; its results should be
reported back without copying credentials, proprietary data, or internal
paths into this repository.
