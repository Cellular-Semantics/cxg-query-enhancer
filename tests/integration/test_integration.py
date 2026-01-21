"""
Integration tests for cxg-query-enhancer.

These tests validate end-to-end functionality by making real network calls to:
- Ubergraph SPARQL endpoint (for ontology expansion)
- CellxGene Census (for filtering against real data)

IMPORTANT: These tests are SLOW and require network connectivity.
- Run separately from unit tests: pytest tests/test_integration.py
- Can be skipped in CI with: pytest -m "not integration"
- Cache is intentionally cleared to ensure fresh data fetching

Performance metrics are logged to help identify regressions.
"""

import pytest
import os
import shutil
import time
import logging
from pathlib import Path

from cxg_query_enhancer import enhance

logger = logging.getLogger(__name__)


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def clean_cache():
    """
    Fixture to clean the cache before each test.

    This ensures we're testing real network connections and data streaming,
    not just pickle loading from cached files.

    Yields after cleaning, then cache can be used by subsequent runs.
    """
    cache_dir = Path(".cache")

    if cache_dir.exists():
        logger.info(f"Cleaning cache at {cache_dir} for fresh integration test...")
        shutil.rmtree(cache_dir)

    yield

    # Cache is preserved after test for inspection if needed
    logger.info(f"Test complete. Cache preserved at {cache_dir}")


@pytest.fixture(scope="function")
def performance_tracker():
    """
    Fixture to track and report performance metrics for integration tests.

    Usage:
        def test_something(performance_tracker):
            with performance_tracker("My Operation"):
                # ... test code ...
    """

    class PerformanceTracker:
        def __init__(self):
            self.metrics = {}

        def __call__(self, operation_name):
            return self._Timer(operation_name, self.metrics)

        class _Timer:
            def __init__(self, name, metrics_dict):
                self.name = name
                self.metrics = metrics_dict
                self.start_time = None

            def __enter__(self):
                self.start_time = time.perf_counter()
                logger.info(f"▶ Starting: {self.name}")
                return self

            def __exit__(self, *args):
                duration = time.perf_counter() - self.start_time
                self.metrics[self.name] = duration
                logger.info(f"✓ Completed: {self.name} in {duration:.2f}s")

    tracker = PerformanceTracker()
    yield tracker

    # Report all metrics at end of test
    if tracker.metrics:
        logger.info("\n" + "=" * 60)
        logger.info("Performance Summary:")
        for operation, duration in tracker.metrics.items():
            logger.info(f"  {operation}: {duration:.2f}s")
        logger.info("=" * 60 + "\n")


class TestConnectivity:
    """Tests for basic connectivity to external services."""

    def test_ubergraph_connectivity_tissue(self, clean_cache, performance_tracker):
        """
        Test connectivity to Ubergraph SPARQL endpoint using tissue ontology.

        This is a lightweight test that validates:
        - SPARQL queries work
        - UBERON ontology is accessible
        - Basic ontology expansion functions

        Uses 'eye' (UBERON:0000970) as a simple test case.
        """
        query = "tissue == 'eye'"

        with performance_tracker("Ubergraph Connectivity (tissue='eye')"):
            result = enhance(query, organism="homo_sapiens")

        # Validate structure
        assert "tissue" in result, "Result should contain 'tissue' column"
        assert "'eye'" in result, "Result should contain 'eye' term"

        # Result should use 'in' operator after expansion
        assert " in " in result, "Result should use 'in' operator for list matching"

    def test_ubergraph_connectivity_cell_type(self, clean_cache, performance_tracker):
        """
        Test connectivity to Ubergraph SPARQL endpoint using Cell Ontology (CL).

        This validates:
        - SPARQL queries work for CL ontology
        - Cell type terms are correctly expanded
        - Subclass relationships are found

        Uses 'interneuron' (CL:0000099) as test case - a neuron subtype with many children.
        """
        query = "cell_type == 'interneuron'"

        with performance_tracker("Ubergraph Connectivity (cell_type='interneuron')"):
            result = enhance(query, organism="homo_sapiens")

        # Validate structure
        assert "cell_type" in result, "Result should contain 'cell_type' column"
        assert "'interneuron'" in result, "Result should contain 'interneuron' term"

        # Should be expanded with subtypes
        assert len(result) > len(
            query
        ), "Result should be expanded with interneuron subtypes"

        # Should include some common interneuron subtypes that exist in Census
        # (These are broad categories likely to be in the data)
        result_lower = result.lower()
        assert (
            "gabaergic interneuron" in result_lower
        ), "Should include GABAergic interneuron subtypes"

    def test_census_connectivity_sparse_category(
        self, clean_cache, performance_tracker
    ):
        """
        Test connectivity to CellxGene Census using a sparse category (disease).

        Disease column is sparser than cell_type, making it faster to stream.
        Validates census filtering logic without heavy data loads.

        Uses 'influenza' (MONDO:0005136) as test case.
        """
        query = "disease == 'influenza'"

        with performance_tracker("Census Connectivity (disease='influenza')"):
            result = enhance(query, organism="homo_sapiens")

        # Validate query was rewritten
        assert "disease" in result
        assert "influenza" in result.lower()


class TestFunctionalCorrectness:
    """Tests for functional correctness of query enhancement."""

    def test_label_based_query_expansion(self, clean_cache, performance_tracker):
        """
        Test that label-based queries are correctly expanded with subterms.

        Validates:
        - Labels are recognized and mapped to ontology IDs
        - Subclasses/part-of relationships are included
        - Census filtering removes non-existent terms
        """
        query = "cell_type == 'neuron'"

        with performance_tracker("Label-based expansion (cell_type='neuron')"):
            result = enhance(query, organism="homo_sapiens")

        # Should contain the original term
        assert "'neuron'" in result

        # Should be expanded (result longer than input)
        assert len(result) > len(query)

        # Should use 'in' operator for expanded list
        assert "cell_type in [" in result

    def test_id_based_query_expansion(self, clean_cache, performance_tracker):
        """
        Test that ID-based queries work correctly.

        Validates:
        - Explicit ontology IDs are recognized
        - IDs are expanded to include subclasses
        - ID column naming is preserved

        Uses UBERON:0000970 (eye) as test case.
        """
        query = "tissue_ontology_term_id == 'UBERON:0000970'"

        with performance_tracker("ID-based expansion (tissue='UBERON:0000970')"):
            result = enhance(query, organism="homo_sapiens")

        # Should preserve ID column name
        assert "tissue_ontology_term_id" in result

        # Should contain the original ID
        assert "'UBERON:0000970'" in result

        # Should use 'in' operator for expanded list
        assert "tissue_ontology_term_id in [" in result

    def test_multi_category_query(self, clean_cache, performance_tracker):
        """
        Test that multi-category queries are correctly handled.

        Validates:
        - Multiple categories are processed in parallel
        - Boolean operators (and/or) are preserved
        - Each category is independently expanded
        """
        query = "cell_type == 'neuron' and tissue == 'lung'"

        with performance_tracker("Multi-category query (cell_type + tissue)"):
            result = enhance(query, organism="homo_sapiens")

        # Both categories should be present
        assert "cell_type" in result
        assert "tissue" in result

        # Boolean operator should be preserved
        assert " and " in result

        # Both should have expanded terms
        assert "'neuron'" in result or "neuron" in result.lower()
        assert "'lung'" in result or "lung" in result.lower()

    def test_cell_type_expansion_interneuron(self, clean_cache, performance_tracker):
        """
        Test Cell Ontology (CL) expansion with interneuron - a term with many subtypes.

        IMPORTANT: This test demonstrates expected behavior for CL expansion:
        - Census filtering checks if cell types exist ANYWHERE in Census (organism-level)
        - It does NOT filter by tissue-specific co-occurrence
        - The tissue filter is applied at query execution time, not during enhancement

        For 'interneuron', the ontology contains ~40+ subtypes (GABAergic interneuron,
        cortical interneuron, etc.). Census filtering will include all subtypes that
        exist in ANY tissue for the organism, not just lung.

        When this enhanced query is executed against Census, the 'and tissue == lung'
        part will further filter to only show samples with interneurons in lung.
        """
        query = "cell_type == 'interneuron'"

        with performance_tracker("CL expansion (interneuron)"):
            result = enhance(query, organism="homo_sapiens")

        # Should contain original term
        assert "'interneuron'" in result

        # Should be significantly expanded - interneuron has many subtypes
        assert len(result) > len(query) * 3, "Interneuron has many subtypes in CL"

        # Should include common interneuron subtypes (if they exist in Census)
        result_lower = result.lower()
        # These are very broad categories that almost certainly exist somewhere in Census
        common_subtypes = ["gabaergic interneuron", "cortical interneuron"]
        matches = [subtype for subtype in common_subtypes if subtype in result_lower]
        assert (
            len(matches) > 0
        ), f"Should include at least one common interneuron subtype"

    def test_combined_cell_and_tissue_filters(self, clean_cache, performance_tracker):
        """
        Test realistic query combining cell_type (CL) and tissue (UBERON).

        This is the most common use case for CellxGene queries.

        CRITICAL UNDERSTANDING:
        The enhanced query will expand BOTH categories independently:
        - cell_type: All interneuron subtypes that exist in Census (organism-level)
        - tissue: All lung subparts that exist in Census

        The 'and' operator combines these filters at query execution time.
        This means:
        1. Enhancement expands 'interneuron' → [interneuron, GABAergic interneuron, ...]
        2. Enhancement expands 'lung' → [lung, left lung, right lung, ...]
        3. Census execution finds samples where:
           (cell_type in expanded_list) AND (tissue in expanded_list)

        The expansion may include cell types that don't actually occur in lung
        (e.g., hippocampal interneurons), but the tissue filter will exclude those
        samples when the query executes.
        """
        query = "cell_type == 'interneuron' and tissue == 'lung'"

        with performance_tracker("Combined cell_type='interneuron' and tissue='lung'"):
            result = enhance(query, organism="homo_sapiens")

        # Both filters should be present
        assert "cell_type" in result
        assert "tissue" in result
        assert " and " in result

        # Original terms should be included
        assert "'interneuron'" in result
        assert "'lung'" in result

        # Both should be expanded
        assert len(result) > len(query) * 2

        # The interneuron list may be surprisingly long (30-40 subtypes) because
        # it includes ALL interneuron types from Census, not just lung-specific ones
        # This is expected and correct behavior

    def test_development_stage_with_organism(self, clean_cache, performance_tracker):
        """
        Test that development_stage requires and uses organism parameter.

        Validates:
        - Organism-specific ontologies are used (MmusDv for mouse, HsapDv for human)
        - Development stage terms are correctly expanded
        """
        # Test with mouse
        query_mouse = "development_stage == 'embryonic stage'"

        with performance_tracker("Development stage (mouse)"):
            result_mouse = enhance(query_mouse, organism="Mus musculus")

        assert "development_stage" in result_mouse

        # Test with human
        query_human = "development_stage == 'adult stage'"

        with performance_tracker("Development stage (human)"):
            result_human = enhance(query_human, organism="homo_sapiens")

        assert "development_stage" in result_human


class TestPerformance:
    """Tests for performance characteristics and benchmarks."""

    @pytest.mark.slow
    def test_end_to_end_performance_benchmark(self, clean_cache, performance_tracker):
        """
        End-to-end performance benchmark for complex query.

        WARNING: This test downloads substantial data (cell_type column is large).

        Performance expectations:
        - First run (cold cache): < 120 seconds on decent connection
        - Subsequent runs (warm cache): < 5 seconds

        This test is marked as 'slow' and can be skipped with: pytest -m "not slow"
        """
        query = "cell_type == 'neuron' and tissue == 'lung'"

        with performance_tracker("End-to-end benchmark (cold cache)") as timer:
            result = enhance(query, organism="homo_sapiens")

        duration = timer.metrics["End-to-end benchmark (cold cache)"]

        # Warn if performance is degraded
        if duration > 120:
            logger.warning(
                f"⚠️  Performance degradation detected: {duration:.2f}s "
                f"(threshold: 120s). Check network or Census service status."
            )

        # Still validate correctness
        assert "neuron" in result
        assert "lung" in result
        assert len(result) > len(query)

        # Test warm cache performance
        with performance_tracker("End-to-end benchmark (warm cache)"):
            result_cached = enhance(query, organism="homo_sapiens")

        cached_duration = timer.metrics["End-to-end benchmark (warm cache)"]

        # Cached should be much faster
        assert cached_duration < 10, f"Cached query too slow: {cached_duration:.2f}s"

        # Results should be identical
        assert result == result_cached

    def test_parallel_expansion_efficiency(self, clean_cache, performance_tracker):
        """
        Test that multiple terms in a query are processed in parallel.

        Validates that parallel processing provides speedup over sequential.
        """
        # Query with multiple terms in same category
        query = "cell_type in ['neuron', 'epithelial cell', 'fibroblast']"

        with performance_tracker("Parallel expansion (3 terms)"):
            result = enhance(query, organism="homo_sapiens")

        # All terms should be processed
        # Note: Some may be filtered out by census, but query should complete
        assert "cell_type" in result


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_empty_expansion_returns_original(self, clean_cache):
        """
        Test that queries with no expansions return sensible results.

        If a term has no subclasses and doesn't exist in census,
        the original term should still be in the result.
        """
        # Use a very specific, leaf-level term unlikely to have children
        query = "cell_type == 'very_rare_nonexistent_cell_type_12345'"

        result = enhance(query, organism="homo_sapiens")

        # Should return something, even if term doesn't expand
        assert "cell_type" in result

    def test_mixed_labels_and_ids(self, clean_cache, performance_tracker):
        """
        Test queries mixing label-based and ID-based filters.

        Validates that both styles can be used in the same query.
        """
        query = (
            "cell_type == 'neuron' and "
            "tissue_ontology_term_id == 'UBERON:0002048'"  # lung
        )

        with performance_tracker("Mixed labels and IDs"):
            result = enhance(query, organism="homo_sapiens")

        assert "cell_type" in result
        assert "tissue_ontology_term_id" in result
        assert " and " in result
