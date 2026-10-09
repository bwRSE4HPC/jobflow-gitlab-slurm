"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.recovery._receipt_support import (
    committed_recovery_bundle as _build_committed_recovery_bundle,
)
from tests.persistence.recovery._request_support import (
    recovery_staging as _build_recovery_staging,
)
from tests.support.inspection import (
    inspection_runs as _build_inspection_runs,
)
from tests.support.runs import (
    discovery_inputs as _build_discovery_inputs,
)


@pytest.fixture
def discovery_inputs(tmp_path):
    return _build_discovery_inputs(tmp_path)


@pytest.fixture
def inspection_runs(tmp_path):
    return _build_inspection_runs(tmp_path)


@pytest.fixture
def recovery_staging(tmp_path):
    return _build_recovery_staging(tmp_path)


@pytest.fixture
def committed_recovery_bundle(recovery_staging):
    return _build_committed_recovery_bundle(recovery_staging)
