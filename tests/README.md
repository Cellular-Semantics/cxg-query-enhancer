# Test Suite Documentation

## Overview

This directory contains the test suite for the cxg-query-enhancer project. The tests are organized into separate directories for unit and integration tests, with proper isolation to prevent cross-contamination from cached data.

## Directory Structure

```
tests/
├── unit/               # Fast unit tests with mocked dependencies
│   ├── test_enhance.py
│   ├── test_ontology_extractor.py
│   ├── test_sparql_client.py
│   ├── test_process_category.py
│   └── test_error_handling.py
├── integration/        # Slow integration tests with real network calls
│   └── test_integration.py
├── conftest.py         # Shared pytest configuration and fixtures
└── README.md          # This file
```

## Test Files

### Unit Tests (Fast, Mocked) - `tests/unit/`
- **`test_enhance.py`**: Tests for the main `enhance()` function and query rewriting logic
- **`test_ontology_extractor.py`**: Tests for the `OntologyExtractor` class and SPARQL query generation
- **`test_sparql_client.py`**: Tests for the `SPARQLClient` wrapper
- **`test_process_category.py`**: Tests for the `process_category` helper function
- **`test_error_handling.py`**: Comprehensive error handling tests covering SPARQL failures, unsupported organisms, invalid categories, and census errors

### Integration Tests (Slow, Real Network Calls) - `tests/integration/`
- **`test_integration.py`**: End-to-end integration tests with real Ubergraph and Census connections. Includes performance benchmarks.

### Configuration
- **`conftest.py`**: Shared pytest configuration and fixtures (includes marker definitions)

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

### Run all unit tests (fast, mocked)
```bash
poetry run pytest tests/unit/ -v
# or exclude integration tests
poetry run pytest tests/ -m "not integration"
```

### Run all tests including integration tests (slow)
```bash
poetry run pytest tests/ -v
```

### Run only integration tests
```bash
poetry run pytest tests/integration/ -v
# or using markers
poetry run pytest tests/ -m integration
```

### Run specific test file
```bash
poetry run pytest tests/unit/test_enhance.py -v
poetry run pytest tests/integration/test_integration.py -v
```

### Run with verbose output
```bash
poetry run pytest tests/ -v
```

### Run specific test
```bash
poetry run pytest tests/unit/test_enhance.py::TestEnhance::test_enhance_with_labels_and_filtering -v
```

### Skip slow tests
```bash
poetry run pytest tests/ -m "not slow"
```

### Run with performance tracking (integration tests)
```bash
poetry run pytest tests/integration/test_integration.py -v -s
# -s shows print output and logging
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

## Performance Testing

### Integration Test Performance Metrics

The integration tests in `test_integration.py` include automatic performance tracking:

- **Connectivity tests**: Validate network access to Ubergraph and Census (~5-15s each)
- **Functional tests**: Test correctness of query enhancement (~10-30s each)
- **Performance benchmarks**: Measure end-to-end performance with real data

Performance metrics are automatically logged during test execution:

```bash
poetry run pytest tests/test_integration.py -v -s
```

Expected performance (cold cache, first run):
- Simple queries (single category): < 15 seconds
- Complex queries (multiple categories): < 60 seconds  
- Full benchmark (cell_type + tissue): < 120 seconds

Expected performance (warm cache, cached data):
- All queries: < 10 seconds

### Performance Regression Detection

The `test_end_to_end_performance_benchmark` test warns if performance degrades beyond thresholds:
- Cold cache: > 120 seconds triggers warning
- Warm cache: > 10 seconds fails test

### Running Performance Tests Only

```bash
# Run all integration tests with performance tracking
poetry run pytest tests/test_integration.py::TestPerformance -v -s

# Run specific performance benchmark
poetry run pytest tests/test_integration.py::TestPerformance::test_end_to_end_performance_benchmark -v -s
```

### Interpreting Performance Results

Performance can vary based on:
1. **Network speed**: Slower connections increase Census download time
2. **Census service load**: Higher server load increases latency
3. **Query complexity**: More terms = more parallel SPARQL queries
4. **Cache state**: Cold cache requires data download; warm cache reads from disk

If tests are consistently slow:
1. Check internet connection
2. Verify Ubergraph endpoint is responsive
3. Verify Census service is accessible
4. Clear cache and retry: `rm -rf .cache/`

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
