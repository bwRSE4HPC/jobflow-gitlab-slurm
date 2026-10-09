"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.attempts._attempt_support import MODES
from tests.persistence.attempts._attempt_support import (
    attempt_or_invocation as _build_attempt_case,
)
from tests.persistence.attempts._attempt_support import (
    attempt_run as _build_attempt_run,
)
from tests.persistence.attempts._definition_support import (
    definition_run as _build_definition_run,
)


@pytest.fixture
def definition_run(tmp_path):
    return _build_definition_run(tmp_path)


@pytest.fixture
def attempt_run(tmp_path):
    return _build_attempt_run(tmp_path)


@pytest.fixture(params=MODES)
def attempt_or_invocation(request, attempt_run):
    return _build_attempt_case(request, attempt_run)
