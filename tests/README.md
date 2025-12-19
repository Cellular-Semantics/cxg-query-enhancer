# Test Suite Documentation

## Overview

This directory contains the test suite for the cxg-query-enhancer project. The tests are organized to ensure complete isolation between test runs and prevent cross-contamination from cached data.

## Test Files

- **`test_enhance.py`**: Tests for the main `enhance()` function and query rewriting logic
- **`test_ontology_extractor.py`**: Tests for the `OntologyExtractor` class and SPARQL query generation
- **`test_sparql_client.py`**: Tests for the `SPARQLClient` wrapper
- **`test_process_category.py`**: Tests for the `process_category` helper function
- **`test_error_handling.py`**: Comprehensive error handling tests covering SPARQL failures, unsupported organisms, invalid categories, and census errors
- **`conftest.py`**: Shared pytest configuration and fixtures

## Test Isolation Strategy

### The Problem

The `_get_census_terms()` function uses two levels of caching:
1. **In-memory LRU cache** (`@lru_cache`) - persists across tests in the same session
2. **On-disk file cache** (`.cache/` directory) - persists across test sessions

Without proper isolation, tests can pass due to **stale cached data** rather than the intended mocked behavior, leading to false positives when:
- Mocks are removed
- Mock behavior changes
- Tests run in different orders

### The Solution

The `conftest.py` file provides an **auto-use fixture** (`clean_test_environment`) that runs automatically for every test and ensures:

1. **LRU cache is cleared** before and after each test
2. **Cache directory is redirected** to a temporary location that's cleaned up after each test
3. **No shared state** exists between tests

### How It Works

```python
@pytest.fixture(autouse=True)
def clean_test_environment(tmp_path, monkeypatch):
    # Clear in-memory cache
    _get_census_terms.cache_clear()
    
    # Redirect .cache/ to temp directory
    temp_cache_dir = tmp_path / "test_cache"
    # ... monkeypatch os.path.join and os.makedirs ...
    
    yield  # Run the test
    
    # Clean up
    _get_census_terms.cache_clear()
```

## Running Tests

### Run all tests
```bash
poetry run pytest tests/
```

### Run specific test file
```bash
poetry run pytest tests/test_enhance.py
```

### Run with verbose output
```bash
poetry run pytest tests/ -v
```

### Run specific test
```bash
poetry run pytest tests/test_enhance.py::TestEnhance::test_enhance_with_labels_and_filtering -v
```

## Writing New Tests

### Basic Test Structure

Tests should use the standard pytest/unittest structure:

```python
import unittest
from unittest.mock import patch
from cxg_query_enhancer import enhance

class TestMyFeature(unittest.TestCase):
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_something(self, mock_census_terms):
        # Arrange
        mock_census_terms.return_value = {"CL:0000001", "CL:0000002"}
        
        # Act
        result = enhance("cell_type in ['neuron']")
        
        # Assert
        self.assertIn("CL:0000001", result)
```

### Using the Mock Census Terms Fixture

For convenience, you can use the `mock_census_terms` fixture:

```python
def test_with_fixture(mock_census_terms):
    mock_census_terms.return_value = {"CL:0000001"}
    # Your test code...
```

### Important: Always Mock `_get_census_terms`

When writing tests that involve census filtering:

✅ **DO**: Mock `_get_census_terms` to return a controlled set
```python
@patch("cxg_query_enhancer.enhancer._get_census_terms")
def test_something(self, mock_census):
    mock_census.return_value = {"CL:0000540"}
```

❌ **DON'T**: Mock `_filter_ids_against_census` (won't work due to default parameters)
```python
# This won't work properly in the new code!
@patch("cxg_query_enhancer.enhancer._filter_ids_against_census")
```

## Test Coverage

To check test coverage:

```bash
poetry run pytest tests/ --cov=src/cxg_query_enhancer --cov-report=html
```

This generates a coverage report in `htmlcov/index.html`.

## Debugging Tests

### Run tests with print output
```bash
poetry run pytest tests/ -s
```

### Run tests with logging
```bash
poetry run pytest tests/ --log-cli-level=DEBUG
```

### Drop into debugger on failure
```bash
poetry run pytest tests/ --pdb
```

## Continuous Integration

Tests are automatically run in CI/CD pipelines. Ensure all tests pass locally before pushing:

```bash
# Run the full test suite
poetry run pytest tests/

# Check for any warnings
poetry run pytest tests/ -v --strict-warnings
```
