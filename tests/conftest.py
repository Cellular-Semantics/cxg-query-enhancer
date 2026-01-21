"""
Pytest configuration and shared fixtures for all tests.

This module provides test fixtures that ensure test isolation by:
- Clearing LRU caches between tests
- Using temporary cache directories to avoid polluting the real .cache/
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
    from cxg_query_enhancer.enhancer import _get_census_terms

    # 1. Clear the LRU cache before the test starts
    _get_census_terms.cache_clear()

    # 2. Setup temporary cache directory
    temp_cache_dir = tmp_path / "test_cache"
    temp_cache_dir.mkdir(exist_ok=True)

    # 3. Patch os.path.join
    # We use a variadic signature (*args) to mimic the real os.path.join
    original_join = os.path.join

    def patched_join(path, *paths):
        """Redirect paths starting with .cache to temp directory"""
        if path == ".cache":
            # If the start is .cache, use our temp dir instead
            return original_join(str(temp_cache_dir), *paths)
        return original_join(path, *paths)

    # Patch where it is imported/used in your module
    monkeypatch.setattr("cxg_query_enhancer.enhancer.os.path.join", patched_join)

    # 4. Patch os.makedirs
    original_makedirs = os.makedirs

    def patched_makedirs(name, mode=0o777, exist_ok=False):
        """Redirect creation of .cache directory to temp directory"""
        if name == ".cache":
            name = str(temp_cache_dir)
        return original_makedirs(name, mode=mode, exist_ok=exist_ok)

    monkeypatch.setattr("cxg_query_enhancer.enhancer.os.makedirs", patched_makedirs)

    yield

    # 5. Cleanup: Clear LRU cache again after test
    _get_census_terms.cache_clear()


@pytest.fixture
def mock_census_terms():
    """
    Convenience fixture that mocks _get_census_terms to return a controlled set of IDs.
    """
    with patch("cxg_query_enhancer.enhancer._get_census_terms") as mock:
        yield mock
