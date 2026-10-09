"""Actual dependency boundaries and negative examples for the static checker."""

import ast
from pathlib import Path

import pytest

from tests.architecture._dependencies import (
    collected_test_imports,
    cycles,
    import_edges,
    module_name,
    violations,
)

ROOT = Path(__file__).resolve().parents[2]


def graph_for(directory, import_root):
    paths = {module_name(path, import_root): path for path in directory.rglob("*.py")}
    return {
        name: import_edges(
            path.read_text(encoding="utf-8"),
            name,
            set(paths),
            package=path.name == "__init__.py",
        )
        for name, path in paths.items()
    }


def test_runtime_dependencies_follow_accepted_direction():
    graph = graph_for(ROOT / "src/jobflow_gitlab_slurm", ROOT / "src")
    assert not violations(graph)
    assert not cycles(graph)


def test_runtime_initializers_are_inert_and_root_has_no_legacy_modules():
    package = ROOT / "src/jobflow_gitlab_slurm"
    assert {path.name for path in package.glob("*.py")} == {"__init__.py", "cli.py"}
    for path in package.rglob("__init__.py"):
        body = ast.parse(path.read_text(encoding="utf-8")).body
        assert all(
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            for node in body
        ), path


def test_test_modules_never_import_collected_tests():
    graph = graph_for(ROOT / "tests", ROOT)
    assert not collected_test_imports(graph)


@pytest.mark.parametrize(
    "text",
    [
        "from jobflow_gitlab_slurm.persistence.recovery import operations as work",
        "import jobflow_gitlab_slurm.persistence.recovery.operations as work",
        "from ..recovery import operations as work",
    ],
)
def test_aliases_and_relative_imports_cannot_hide_forbidden_edges(text):
    source = "jobflow_gitlab_slurm.persistence.runs.storage"
    target = "jobflow_gitlab_slurm.persistence.recovery.operations"
    graph = {source: import_edges(text, source, {source, target})}
    assert target in graph[source]
    assert violations(graph)


def test_records_cannot_depend_on_storage_in_their_own_domain():
    assert violations(
        {
            "jobflow_gitlab_slurm.persistence.runs.records": {
                "jobflow_gitlab_slurm.persistence.runs.storage"
            }
        }
    )


def test_cycles_are_detected_even_with_allowed_domain_edges():
    left = "jobflow_gitlab_slurm.persistence.runs.left"
    right = "jobflow_gitlab_slurm.persistence.runs.right"
    graph = {left: {right}, right: {left}}
    assert not violations(graph)
    assert cycles(graph)


def test_package_relative_imports_and_initializers_are_resolved():
    source = "tests.persistence.runs"
    target = f"{source}.test_records"
    edges = import_edges(
        "from . import test_records", source, {source, target}, package=True
    )
    assert collected_test_imports({source: edges}) == [(source, target)]


def test_external_imports_are_not_internal_dependencies():
    assert (
        import_edges(
            "import os\nfrom pydantic import BaseModel",
            "tests.example",
            {"tests.example"},
        )
        == set()
    )
