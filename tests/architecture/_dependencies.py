"""Static internal-import checks; not a dynamic-import security boundary."""

import ast
from pathlib import Path

PACKAGE = "jobflow_gitlab_slurm"
FOUNDATIONS = {"config", "runs", "artifacts", "_filesystem"}
ALLOWED = {
    "config": {"config"},
    "_filesystem": {"_filesystem"},
    "artifacts": {"artifacts", "_filesystem"},
    "runs": FOUNDATIONS,
    "attempts": FOUNDATIONS | {"attempts"},
    "journal": FOUNDATIONS | {"journal"},
    "bundles": FOUNDATIONS | {"attempts", "bundles"},
    "publication": FOUNDATIONS | {"attempts", "bundles", "journal", "publication"},
    "recovery": FOUNDATIONS
    | {"attempts", "bundles", "journal", "publication", "recovery"},
    "queries": {"config", "runs", "journal", "queries"},
    "cli": set(),
}


def module_name(path: Path, root: Path) -> str:
    """Return the qualified name, treating initializers as their package."""
    parts = path.relative_to(root).with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def import_edges(text: str, current: str, modules: set[str], *, package=False):
    """Resolve literal imports, including aliases and package-relative imports."""
    result = set()
    parent = current if package else current.rpartition(".")[0]

    def retain(candidate):
        while candidate:
            if candidate in modules:
                result.add(candidate)
                break
            candidate = candidate.rpartition(".")[0]

    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                retain(alias.name)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                ancestors = parent.split(".")
                prefix = ".".join(ancestors[: len(ancestors) - node.level + 1])
                base = ".".join(part for part in (prefix, base) if part)
            retain(base)
            for alias in node.names:
                candidate = f"{base}.{alias.name}" if base else alias.name
                if candidate in modules:
                    result.add(candidate)
    return result


def domain(module: str) -> str:
    """Classify runtime owners; empty package initializers are neutral."""
    parts = module.split(".")[1:]
    if not parts or parts == ["persistence"]:
        return "package"
    if parts[0] == "persistence":
        return parts[1]
    return parts[0]


def violations(graph: dict[str, set[str]]) -> list[str]:
    """Report forbidden direction and storage dependencies of record schemas."""
    problems = []
    for source, targets in graph.items():
        owner = domain(source)
        for target in targets:
            dependency = domain(target)
            if dependency == "package":
                continue
            if owner != "cli" and dependency not in ALLOWED.get(owner, set()):
                problems.append(f"{source} -> {target}: forbidden direction")
            initializer = target == f"{PACKAGE}.persistence.{dependency}"
            if (
                source.endswith(".records")
                and dependency != "config"
                and not initializer
                and not target.endswith(".records")
            ):
                problems.append(
                    f"{source} -> {target}: records depend on implementation"
                )
    return problems


def cycles(graph: dict[str, set[str]]) -> list[tuple[str, ...]]:
    """Find cycles independently of direction policy."""
    found = []
    completed = set()

    def visit(node, active):
        if node in active:
            found.append((*active[active.index(node) :], node))
            return
        if node in completed:
            return
        for target in sorted(graph.get(node, ())):
            visit(target, (*active, node))
        completed.add(node)

    for node in sorted(graph):
        visit(node, ())
    return found


def collected_test_imports(graph: dict[str, set[str]]) -> list[tuple[str, str]]:
    """Reject any test/support module importing a collected test module."""
    return [
        (source, target)
        for source, targets in graph.items()
        for target in sorted(targets)
        if target.rsplit(".", 1)[-1].startswith("test_")
    ]
