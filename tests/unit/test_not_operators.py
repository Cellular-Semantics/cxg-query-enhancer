import unittest
from unittest.mock import patch, MagicMock
from cxg_query_enhancer import enhance
import logging

logger = logging.getLogger(__name__)

# Basic logging setup for test output (optional)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class TestNotOperators(unittest.TestCase):
    """Test cases for != and not in operators"""

    # Test 1: Label-based query with != operator
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_not_equal_label(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        """
        Test enhance: != operator with label input
        """
        logger.info("Running: test_enhance_with_not_equal_label")
        # --- ARRANGE ---

        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {"CL:0000540", "CL:neuron_child"}
        mock_get_census_terms.return_value = allowed_by_census

        # 2. Mock OntologyExtractor._get_ontology_expansion
        def get_expansion_effect(term, category, organism=None):
            logger.info(f"MOCK _get_ontology_expansion for: {term}")
            if term == "neuron":
                return [
                    {"ID": "CL:0000540", "Label": "neuron"},
                    {"ID": "CL:neuron_child", "Label": "Neuron Child"},
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs for enhance
        query_filter = "cell_type != 'neuron'"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"NotEqual Label Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Should expand to: cell_type not in ['neuron', 'Neuron Child']
        self.assertIn("not in", rewritten_filter)
        self.assertIn("'Neuron Child'", rewritten_filter)
        self.assertIn("'neuron'", rewritten_filter)
        self.assertNotIn("!=", rewritten_filter)

    # Test 2: ID-based query with != operator
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_not_equal_id(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        logger.info("Running: test_enhance_with_not_equal_id")

        # --- ARRANGE ---
        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {"CL:0000540", "CL:child_540"}
        mock_get_census_terms.return_value = allowed_by_census

        # 2. Mock OntologyExtractor._get_ontology_expansion
        def get_expansion_effect(term_id, category, organism=None):
            logger.info(f"MOCK _get_ontology_expansion for: {term_id}")
            if term_id == "CL:0000540":
                return [
                    {"ID": "CL:0000540", "Label": "Label for 540"},
                    {"ID": "CL:child_540", "Label": "Child 540"},
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs
        query_filter = "cell_type_ontology_term_id != 'CL:0000540'"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"NotEqual ID Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Should expand to: cell_type_ontology_term_id not in ['CL:0000540', 'CL:child_540']
        self.assertIn("not in", rewritten_filter)
        self.assertIn("'CL:0000540'", rewritten_filter)
        self.assertIn("'CL:child_540'", rewritten_filter)
        self.assertNotIn("!=", rewritten_filter)

    # Test 3: Label-based query with 'not in' operator
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_not_in_labels(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        """
        Test enhance: not in operator with multiple label inputs
        """
        logger.info("Running: test_enhance_with_not_in_labels")
        # --- ARRANGE ---

        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {
            "CL:0000540",
            "CL:neuron_child",
            "CL:epitheliocyte_id",
            "CL:epitheliocyte_child",
        }
        mock_get_census_terms.return_value = allowed_by_census

        # 2. Mock OntologyExtractor._get_ontology_expansion
        def get_expansion_effect(term, category, organism=None):
            logger.info(f"MOCK _get_ontology_expansion for: {term}")
            if term == "neuron":
                return [
                    {"ID": "CL:0000540", "Label": "neuron"},
                    {"ID": "CL:neuron_child", "Label": "Neuron Child"},
                ]
            if term == "epitheliocyte":
                return [
                    {"ID": "CL:epitheliocyte_id", "Label": "epitheliocyte"},
                    {"ID": "CL:epitheliocyte_child", "Label": "Epitheliocyte Child"},
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs for enhance
        query_filter = "cell_type not in ['neuron', 'epitheliocyte']"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"NotIn Labels Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Should expand to include all subtypes
        self.assertIn("not in", rewritten_filter)
        self.assertIn("'Neuron Child'", rewritten_filter)
        self.assertIn("'neuron'", rewritten_filter)
        self.assertIn("'Epitheliocyte Child'", rewritten_filter)
        self.assertIn("'epitheliocyte'", rewritten_filter)

    # Test 4: ID-based query with 'not in' operator
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_not_in_ids(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        logger.info("Running: test_enhance_with_not_in_ids")

        # --- ARRANGE ---
        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {"CL:0000540", "CL:child_540", "CL:0000566", "CL:child_566"}
        mock_get_census_terms.return_value = allowed_by_census

        # 2. Mock OntologyExtractor._get_ontology_expansion
        def get_expansion_effect(term_id, category, organism=None):
            logger.info(f"MOCK _get_ontology_expansion for: {term_id}")
            if term_id == "CL:0000540":
                return [
                    {"ID": "CL:0000540", "Label": "Label for 540"},
                    {"ID": "CL:child_540", "Label": "Child 540"},
                ]
            if term_id == "CL:0000566":
                return [
                    {"ID": "CL:0000566", "Label": "Label for 566"},
                    {"ID": "CL:child_566", "Label": "Child 566"},
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs
        query_filter = "cell_type_ontology_term_id not in ['CL:0000540', 'CL:0000566']"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"NotIn IDs Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Should expand both IDs to include their children
        self.assertIn("not in", rewritten_filter)
        self.assertIn("'CL:0000540'", rewritten_filter)
        self.assertIn("'CL:child_540'", rewritten_filter)
        self.assertIn("'CL:0000566'", rewritten_filter)
        self.assertIn("'CL:child_566'", rewritten_filter)

    # Test 5: Mixed operators with both == and !=
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_mixed_operators(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        logger.info("Running: test_enhance_with_mixed_operators")

        # --- ARRANGE ---
        allowed_by_census = {
            "CL:0000540",
            "CL:neuron_child",
            "UBERON:0002107",
            "UBERON:liver_child",
        }
        mock_get_census_terms.return_value = allowed_by_census

        def get_expansion_effect(term, category, organism=None):
            if term == "neuron":
                return [
                    {"ID": "CL:0000540", "Label": "neuron"},
                    {"ID": "CL:neuron_child", "Label": "Neuron Child"},
                ]
            if term == "liver":
                return [
                    {"ID": "UBERON:0002107", "Label": "liver"},
                    {"ID": "UBERON:liver_child", "Label": "Liver Child"},
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs
        query_filter = "cell_type == 'neuron' and tissue != 'liver'"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"Mixed Operators Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # cell_type should use 'in', tissue should use 'not in'
        self.assertIn("cell_type in", rewritten_filter)
        self.assertIn("tissue not in", rewritten_filter)
        self.assertIn("'neuron'", rewritten_filter)
        self.assertIn("'Neuron Child'", rewritten_filter)
        self.assertIn("'liver'", rewritten_filter)
        self.assertIn("'Liver Child'", rewritten_filter)


if __name__ == "__main__":
    unittest.main()
