"""
Tests for error handling across the cxg-query-enhancer package.

This module ensures that the system properly handles and surfaces errors:
- SPARQL query failures
- Unsupported organisms
- Unsupported categories
- Invalid ontology prefixes
- Missing required parameters
"""

import unittest
from unittest.mock import patch, MagicMock
import logging

from cxg_query_enhancer import OntologyExtractor, SPARQLClient

logger = logging.getLogger(__name__)


class TestSPARQLErrorHandling(unittest.TestCase):
    """Tests for SPARQL client error handling."""

    def test_sparql_client_raises_runtime_error_on_query_failure(self):
        """Test that SPARQLClient.query raises RuntimeError when SPARQL query fails."""
        logger.info("Running: test_sparql_client_raises_runtime_error_on_query_failure")

        with patch("cxg_query_enhancer.enhancer.SPARQLWrapper") as mock_wrapper_class:
            # Mock the SPARQLWrapper instance
            mock_instance = MagicMock()
            mock_wrapper_class.return_value = mock_instance

            # Make query().convert() raise an exception
            mock_instance.query.return_value.convert.side_effect = Exception(
                "Network timeout"
            )

            client = SPARQLClient()

            with self.assertRaises(RuntimeError) as context:
                client.query("SELECT ?s WHERE { ?s a ?o }")

            self.assertIn("SPARQL query failed", str(context.exception))
            self.assertIn("Network timeout", str(context.exception))

    def test_sparql_client_raises_on_connection_error(self):
        """Test that SPARQLClient.query raises RuntimeError on connection errors."""
        logger.info("Running: test_sparql_client_raises_on_connection_error")

        with patch("cxg_query_enhancer.enhancer.SPARQLWrapper") as mock_wrapper_class:
            mock_instance = MagicMock()
            mock_wrapper_class.return_value = mock_instance

            # Simulate connection error
            mock_instance.query.side_effect = ConnectionError("Cannot reach endpoint")

            client = SPARQLClient()

            with self.assertRaises(RuntimeError) as context:
                client.query("SELECT ?s WHERE { ?s a ?o }")

            self.assertIn("SPARQL query failed", str(context.exception))


class TestOntologyExtractorErrorHandling(unittest.TestCase):
    """Tests for OntologyExtractor error handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_sparql_client = MagicMock(spec=SPARQLClient)
        self.extractor = OntologyExtractor(self.mock_sparql_client)

    def test_get_iri_prefix_raises_on_unsupported_organism(self):
        """Test that _get_iri_prefix raises ValueError for unsupported organisms."""
        logger.info("Running: test_get_iri_prefix_raises_on_unsupported_organism")

        with self.assertRaisesRegex(
            ValueError, "Unsupported organism 'Danio rerio' for 'development_stage'"
        ):
            self.extractor._get_iri_prefix("development_stage", "Danio rerio")

    def test_get_iri_prefix_raises_on_missing_organism_for_dev_stage(self):
        """Test that _get_iri_prefix raises ValueError when organism is missing for development_stage."""
        logger.info(
            "Running: test_get_iri_prefix_raises_on_missing_organism_for_dev_stage"
        )

        with self.assertRaisesRegex(
            ValueError, "The 'organism' parameter is required for 'development_stage'"
        ):
            self.extractor._get_iri_prefix("development_stage", organism=None)

    def test_get_iri_prefix_raises_on_unsupported_category(self):
        """Test that _get_iri_prefix raises ValueError for unsupported categories."""
        logger.info("Running: test_get_iri_prefix_raises_on_unsupported_category")

        with self.assertRaisesRegex(ValueError, "Unsupported category 'fake_category'"):
            self.extractor._get_iri_prefix("fake_category")

    def test_get_ontology_expansion_raises_on_invalid_iri_prefix(self):
        """Test that _get_ontology_expansion raises ValueError when IRI prefix has no mapping."""
        logger.info("Running: test_get_ontology_expansion_raises_on_invalid_iri_prefix")

        # Temporarily modify prefix_map to return an unmapped prefix
        self.extractor.prefix_map["test_category"] = "INVALID_PREFIX"

        with self.assertRaisesRegex(
            ValueError, "No ontology IRI found for prefix 'INVALID_PREFIX'"
        ):
            self.extractor._get_ontology_expansion("test_term", "test_category")

    def test_get_ontology_expansion_surfaces_sparql_errors(self):
        """Test that _get_ontology_expansion surfaces SPARQL query errors."""
        logger.info("Running: test_get_ontology_expansion_surfaces_sparql_errors")

        # Make the SPARQL client raise an error
        self.mock_sparql_client.query.side_effect = RuntimeError(
            "SPARQL query failed: Endpoint unreachable"
        )

        with self.assertRaises(RuntimeError) as context:
            self.extractor._get_ontology_expansion("CL:0000540", "cell_type")

        self.assertIn("SPARQL query failed", str(context.exception))

    def test_get_subclasses_raises_on_unsupported_category(self):
        """Test that get_subclasses raises ValueError for unsupported categories."""
        logger.info("Running: test_get_subclasses_raises_on_unsupported_category")

        with self.assertRaisesRegex(
            ValueError, "Unsupported category 'invalid_category'"
        ):
            self.extractor.get_subclasses("some_term", "invalid_category")

    def test_get_subclasses_raises_on_dev_stage_without_organism(self):
        """Test that get_subclasses raises ValueError for development_stage without organism."""
        logger.info("Running: test_get_subclasses_raises_on_dev_stage_without_organism")

        with self.assertRaisesRegex(
            ValueError, "The 'organism' parameter is required for 'development_stage'"
        ):
            self.extractor.get_subclasses(
                "some_stage", "development_stage", organism=None
            )


class TestEnhanceErrorHandling(unittest.TestCase):
    """Tests for enhance() function error handling."""

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    def test_enhance_handles_invalid_syntax(self, mock_expansion):
        """Test that enhance() returns original query on invalid syntax."""
        logger.info("Running: test_enhance_handles_invalid_syntax")

        from cxg_query_enhancer import enhance

        # Invalid Python expression
        invalid_query = "cell_type in ['neuron' AND disease == 'cancer'"

        result = enhance(invalid_query)

        # Should return original query when syntax is invalid
        self.assertEqual(result, invalid_query)

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_handles_sparql_failures_gracefully(
        self, mock_census, mock_expansion
    ):
        """Test that enhance() gracefully degrades when SPARQL fails during expansion.

        The function logs the error and returns the original query terms rather than
        crashing. This allows partial results when some terms fail.
        """
        logger.info("Running: test_enhance_handles_sparql_failures_gracefully")

        from cxg_query_enhancer import enhance

        # Mock expansion to raise an error
        mock_expansion.side_effect = RuntimeError("SPARQL query failed")
        mock_census.return_value = {"CL:0000540"}

        query = "cell_type in ['neuron']"

        # Should not crash - returns original terms when expansion fails
        result = enhance(query)

        # Should return the original query term since expansion failed
        self.assertIn("'neuron'", result)

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_unsupported_organism_for_dev_stage(
        self, mock_census, mock_expansion
    ):
        """Test that enhance() gracefully handles unsupported organism errors.

        When an unsupported organism is used with development_stage, the expansion
        fails and the original terms are returned rather than crashing.
        """
        logger.info("Running: test_enhance_with_unsupported_organism_for_dev_stage")

        from cxg_query_enhancer import enhance

        mock_expansion.side_effect = ValueError(
            "Unsupported organism 'Caenorhabditis elegans' for 'development_stage'"
        )
        mock_census.return_value = set()

        query = "development_stage_ontology_term_id in ['WBls:0000001']"

        # Should not crash - returns original terms when organism is unsupported
        result = enhance(query, organism="Caenorhabditis elegans")

        # Should return the original ID since expansion failed
        self.assertIn("'WBls:0000001'", result)


class TestCensusErrorHandling(unittest.TestCase):
    """Tests for census-related error handling."""

    @patch("cxg_query_enhancer.enhancer.cellxgene_census.open_soma")
    def test_get_census_terms_returns_none_on_census_error(self, mock_open_soma):
        """Test that _get_census_terms returns None when census access fails."""
        logger.info("Running: test_get_census_terms_returns_none_on_census_error")

        from cxg_query_enhancer.enhancer import _get_census_terms

        # Clear cache first
        _get_census_terms.cache_clear()

        # Mock census to raise an error
        mock_open_soma.side_effect = Exception("Census unavailable")

        result = _get_census_terms(
            "latest", "homo_sapiens", "cell_type_ontology_term_id"
        )

        # Should return None on error
        self.assertIsNone(result)

    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_filter_ids_returns_unfiltered_when_census_unavailable(
        self, mock_get_census
    ):
        """Test that _filter_ids_against_census returns original IDs when census is unavailable."""
        logger.info(
            "Running: test_filter_ids_returns_unfiltered_when_census_unavailable"
        )

        from cxg_query_enhancer.enhancer import _filter_ids_against_census

        # Mock census to return None (error condition)
        mock_get_census.return_value = None

        test_ids = ["CL:0000540", "CL:0000066"]
        result = _filter_ids_against_census(
            test_ids, "homo_sapiens", "latest", "cell_type_ontology_term_id"
        )

        # Should return all IDs with "Unknown Label"
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["ID"], "CL:0000540")
        self.assertEqual(result[0]["Label"], "Unknown Label")
        self.assertEqual(result[1]["ID"], "CL:0000066")
        self.assertEqual(result[1]["Label"], "Unknown Label")


if __name__ == "__main__":
    unittest.main()
