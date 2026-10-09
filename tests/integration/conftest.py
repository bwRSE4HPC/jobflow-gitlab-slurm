"""Explicit domain scenarios shared with cross-process lifecycle checks."""

import pytest

from tests.persistence.publication._registration_support import (
    published_bundle_for_registration as _build_publication_case,
)
from tests.persistence.recovery._operation_support import (
    recoverable_bundle as _build_recovery_case,
)
from tests.persistence.recovery._registration_support import (
    recovered_bundle_for_registration as _build_journal_case,
)


@pytest.fixture
def recoverable_bundle(recovery_staging):
    return _build_recovery_case(recovery_staging)


@pytest.fixture
def published_bundle_for_registration(recovery_staging):
    return _build_publication_case(recovery_staging)


@pytest.fixture
def recovered_bundle_for_registration(committed_recovery_bundle):
    return _build_journal_case(committed_recovery_bundle)
