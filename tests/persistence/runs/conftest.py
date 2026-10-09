"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.runs._run_support import (
    run_inputs as _build_run_storage_inputs,
)


@pytest.fixture
def run_inputs(tmp_path):
    return _build_run_storage_inputs(tmp_path)
