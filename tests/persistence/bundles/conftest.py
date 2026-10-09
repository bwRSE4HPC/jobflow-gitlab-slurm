"""Explicit function-scoped scenario fixtures for this domain."""

import pytest

from tests.persistence.bundles._inspection_support import (
    bundle_invocation as _build_bundle_inspection_case,
)
from tests.persistence.bundles._publication_support import (
    bundle_staging as _build_bundle_publication_case,
)


@pytest.fixture
def bundle_invocation(tmp_path):
    return _build_bundle_inspection_case(tmp_path)


@pytest.fixture
def bundle_staging(tmp_path):
    return _build_bundle_publication_case(tmp_path)
