"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.recovery._operation_support import (
    recoverable_bundle as _build_recovery_operation_recovery_case,
)
from tests.persistence.recovery._registration_support import (
    recovered_bundle_for_registration as _build_recovery_registration_journal_case,
)


@pytest.fixture
def recoverable_bundle(recovery_staging):
    return _build_recovery_operation_recovery_case(recovery_staging)


@pytest.fixture
def recovered_bundle_for_registration(committed_recovery_bundle):
    return _build_recovery_registration_journal_case(committed_recovery_bundle)
