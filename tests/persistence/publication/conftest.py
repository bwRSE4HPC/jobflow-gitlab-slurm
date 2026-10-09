"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.publication._intent_support import (
    intent_staging as _build_intent_case,
)
from tests.persistence.publication._registration_support import (
    published_bundle_for_registration as _build_publication_registration_publication_case,
)


@pytest.fixture
def intent_staging(tmp_path):
    return _build_intent_case(tmp_path)


@pytest.fixture
def published_bundle_for_registration(recovery_staging):
    return _build_publication_registration_publication_case(recovery_staging)
