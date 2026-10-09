"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.journal._append_support import (
    append_run as _build_append_run,
)
from tests.persistence.journal._replay_support import (
    replay_run as _build_replay_run,
)


@pytest.fixture
def replay_run(tmp_path):
    return _build_replay_run(tmp_path)


@pytest.fixture
def append_run(tmp_path):
    return _build_append_run(tmp_path)
