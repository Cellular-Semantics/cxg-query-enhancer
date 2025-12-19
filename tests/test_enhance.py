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


class TestEnhance(unittest.TestCase):
    # Test 1: Label-based query with filtering
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_labels_and_filtering(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        """
        Test enhance: label input, Ubergraph expansion (mocked), census filtering (mocked).
        """
        logger.info("Running: test_enhance_with_labels_and_filtering")
        # --- ARRANGE ---

        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {"CL:0000540", "CL:neuron_child"}  # ids that survive
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
                    {
                        "ID": "CL:epitheliocyte_child",
                        "Label": "Epitheliocyte Child",
                    },
                ]
            return []

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs for enhance
        query_filter = "cell_type in ['neuron', 'epitheliocyte']"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"Labels Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Based on mocks:
        # 'neuron' expands to CL:0000540 and CL:neuron_child. Both survive census.
        # 'epitheliocyte' expands to CL:epitheliocyte_id and CL:epitheliocyte_child. Neither survive.
        # So, the final list of labels should be 'neuron' and 'Neuron Child'.
        self.assertIn("'Neuron Child'", rewritten_filter)
        self.assertIn("'neuron'", rewritten_filter)
        self.assertNotIn("'epitheliocyte'", rewritten_filter)
        self.assertNotIn("'Epitheliocyte Child'", rewritten_filter)

    # Test 2: ID-based query with filtering
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_ids_and_filtering(
        self, mock_get_census_terms, mock_get_ontology_expansion
    ):
        logger.info("Running: test_enhance_with_ids_and_filtering")

        # --- ARRANGE ---
        # 1. Mock _get_census_terms to return a set of allowed IDs
        allowed_by_census = {"CL:0000540", "CL:child_566"}
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
        query_filter = "cell_type_ontology_term_id in ['CL:0000540', 'CL:0000566']"
        organism = "homo_sapiens"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"IDs Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # CL:0000540 expands to itself and CL:child_540. Census keeps only CL:0000540.
        # CL:0000566 expands to itself and CL:child_566. Census keeps only CL:child_566.
        self.assertIn("'CL:0000540'", rewritten_filter)
        self.assertIn("'CL:child_566'", rewritten_filter)
        self.assertNotIn("'CL:child_540'", rewritten_filter)
        self.assertNotIn("'CL:0000566'", rewritten_filter)

    # Test 3: Multiple categories with filtering (ROBUST SORTING VERSION)
    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer._get_census_terms")
    def test_enhance_with_multiple_categories_and_filtering(
        self,
        mock_get_census_terms,
        mock_get_ontology_expansion,
    ):
        logger.info("Running: test_enhance_with_multiple_categories_and_filtering")

        # --- ARRANGE ---

        # Define expected survivors for cell_type to test sorting
        # We add a Fake ID here to ensure we have a list of >1 items
        surviving_cell_types = ["CL:0000540", "CL:0000999"]

        # 1. Mock _get_census_terms to return a set of allowed IDs
        # Combine all allowed IDs for the mock
        all_allowed = set(
            surviving_cell_types
            + [
                "UBERON:0002107",
                "MONDO:0005148",
                "MmusDv:0000001_child",
            ]
        )
        mock_get_census_terms.return_value = all_allowed

        # 2. Mock _get_ontology_expansion
        def get_expansion_effect(term_id, category, organism=None):
            # For CL:0000540, return ITSELF + the FAKE ID + a CHILD (which dies in census)
            if term_id == "CL:0000540":
                return [
                    {"ID": "CL:0000540", "Label": "Neuron"},
                    {"ID": "CL:0000999", "Label": "Fake Surviving Sibling"},
                    {"ID": "CL:DEAD_CHILD", "Label": "Dead Child"},
                ]

            expansions = {
                "UBERON:0002107": [
                    {"ID": "UBERON:0002107_child", "Label": "UBERON Child"}
                ],
                "MONDO:0005148": [
                    {"ID": "MONDO:0005148_child", "Label": "MONDO Child"}
                ],
                "MmusDv:0000001": [
                    {"ID": "MmusDv:0000001_child", "Label": "MmusDv Child1"}
                ],
            }
            # Helper to return parent + expansions
            if term_id in expansions:
                return [{"ID": term_id, "Label": "Parent " + term_id}] + expansions[
                    term_id
                ]

            return [{"ID": term_id, "Label": "Parent " + term_id}]

        mock_get_ontology_expansion.side_effect = get_expansion_effect

        # Inputs
        query_filter = (
            "cell_type_ontology_term_id in ['CL:0000540'] and "
            "tissue_ontology_term_id in ['UBERON:0002107'] and "
            "disease_ontology_term_id in ['MONDO:0005148'] and "
            "development_stage_ontology_term_id in ['MmusDv:0000001']"
        )
        organism = "Mus musculus"

        # --- ACT ---
        rewritten_filter = enhance(query_filter, organism=organism)
        logger.info(
            f"Multi-Cat Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---

        # 1. Robust Assertion for the list (The fix we discussed)
        # We manually sort the expected list, just like the app does
        sorted_cells = sorted(surviving_cell_types)
        # We format it exactly like the app: 'ID1', 'ID2'
        formatted_cells = ", ".join(f"'{t}'" for t in sorted_cells)
        # We verify the full string
        self.assertIn(
            f"cell_type_ontology_term_id in [{formatted_cells}]", rewritten_filter
        )

        # 2. Assertions for single items (standard)
        self.assertIn("tissue_ontology_term_id in ['UBERON:0002107']", rewritten_filter)
        self.assertIn("disease_ontology_term_id in ['MONDO:0005148']", rewritten_filter)
        self.assertIn(
            "development_stage_ontology_term_id in ['MmusDv:0000001_child']",
            rewritten_filter,
        )


if __name__ == "__main__":
    unittest.main()
