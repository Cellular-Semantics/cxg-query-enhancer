"""
Pytest configuration and shared fixtures for all tests.

This module provides test fixtures that ensure test isolation by:
- Clearing LRU caches between tests
- Using temporary cache directories to avoid polluting the real user cache
- Preventing test cross-contamination from cached data

Integration tests (marked with @pytest.mark.integration) skip cache cleaning
to test real caching behavior and network connectivity.
"""

import pytest
import os
from unittest.mock import patch


def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers",
        "unit: Fast unit tests with mocked dependencies (default)",
    )
    config.addinivalue_line(
        "markers",
        "integration: Integration tests requiring network connectivity (slow)",
    )
    config.addinivalue_line(
        "markers", "slow: Particularly slow tests that download large datasets"
    )


@pytest.fixture(autouse=True)
def clean_test_environment(request, tmp_path, monkeypatch):
    """
    Automatically applied to unit tests to ensure test isolation.

    Integration tests skip this to test real caching behavior.
    """
    # Skip cache cleaning for integration tests
    if "integration" in request.keywords:
        yield
        return

    # Import locally to avoid top-level import errors if dependencies are missing
    from cxg_query_enhancer.enhancer import _cached_expansion, _get_census_terms

    # 1. Clear the LRU caches before the test starts
    _get_census_terms.cache_clear()
    _cached_expansion.cache_clear()

    # 2. Redirect the on-disk cache to a temporary directory
    from cxg_query_enhancer.enhancer import CACHE_DIR_ENV_VAR

    monkeypatch.setenv(CACHE_DIR_ENV_VAR, str(tmp_path / "test_cache"))

    yield

    # 3. Cleanup: Clear LRU caches again after test
    _get_census_terms.cache_clear()
    _cached_expansion.cache_clear()


@pytest.fixture
def mock_census_terms():
    """
    Convenience fixture that mocks _get_census_terms to return a controlled set of IDs.
    """
    with patch("cxg_query_enhancer.enhancer._get_census_terms") as mock:
        yield mock
