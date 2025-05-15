import unittest
from unittest.mock import patch
from ontology_closure.onto_closure import obs_close
import logging

# Basic logging setup for test output (optional)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class TestObsClose(unittest.TestCase):
    # Test 1: Label-based query with filtering
    @patch("ontology_closure.onto_closure.OntologyExtractor.get_subclasses")
    @patch("ontology_closure.onto_closure.OntologyExtractor.get_ontology_id_from_label")
    @patch("ontology_closure.onto_closure._filter_ids_against_census")
    def test_obs_close_with_labels_and_filtering(
        self, mock_filter_census, mock_get_id_from_label, mock_get_subclasses
    ):
        """
        Test obs_close: label input, Ubergraph expansion (mocked), census filtering (mocked).
        """
        print("\nRunning: test_obs_close_with_labels_and_filtering")
        # --- ARRANGE ---

        # 1. Mock _filter_ids_against_census: only "CL:0000540" (neuron's ID) and its child "CL:neuron_child" survive
        def census_filter_effect(
            ids_to_filter, census_version, organism, ontology_column_name
        ):
            logging.info(
                f"MOCK _filter_ids_against_census called with: {ids_to_filter}"
            )
            allowed_by_census = ["CL:0000540", "CL:neuron_child"]  # ids that survive
            return [id_ for id_ in ids_to_filter if id_ in allowed_by_census]

        mock_filter_census.side_effect = census_filter_effect

        # 2. Mock OntologyExtractor.get_ontology_id_from_label
        def get_id_effect(label, category, organism):
            logging.info(f"MOCK get_ontology_id_from_label for: {label}")
            if label == "neuron":
                return "CL:0000540"
            if label == "epitheliocyte":
                return "CL:epitheliocyte_id"
            return None

        mock_get_id_from_label.side_effect = get_id_effect

        # 3. Mock OntologyExtractor.get_subclasses
        def get_subclasses_effect(term_id_or_label, category, organism=None):
            logging.info(f"MOCK get_subclasses for: {term_id_or_label}")
            if term_id_or_label == "CL:0000540":  # neuron's ID
                return [{"ID": "CL:neuron_child", "Label": "Neuron Child"}]
            if term_id_or_label == "CL:epitheliocyte_id":
                return [
                    {"ID": "CL:epitheliocyte_child", "Label": "Epitheliocyte Child"}
                ]
            return []

        mock_get_subclasses.side_effect = get_subclasses_effect

        # Inputs for obs_close
        query_filter = "cell_type in ['neuron', 'epitheliocyte']"
        categories = ["cell_type"]
        organism = "homo_sapiens"
        census_version = "mock_version"  # To trigger filtering

        # --- ACT ---
        rewritten_filter = obs_close(query_filter, categories, organism, census_version)
        logging.info(
            f"Labels Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Neuron (CL:0000540) + child (CL:neuron_child) -> both survive census mock.
        # Epitheliocyte (CL:epitheliocyte_id) + child (CL:epitheliocyte_child) -> neither survive census mock.
        self.assertIn("'neuron'", rewritten_filter)
        self.assertIn("'Neuron Child'", rewritten_filter)
        self.assertNotIn("'epitheliocyte'", rewritten_filter)
        self.assertNotIn("'Epitheliocyte Child'", rewritten_filter)

    # Test 2: ID-based query with filtering
    @patch("ontology_closure.onto_closure.OntologyExtractor.get_subclasses")
    # No need to mock get_ontology_id_from_label if main terms are IDs
    @patch("ontology_closure.onto_closure._filter_ids_against_census")
    def test_obs_close_with_ids_and_filtering(
        self, mock_filter_census, mock_get_subclasses
    ):
        print("\nRunning: test_obs_close_with_ids_and_filtering")

        # --- ARRANGE ---
        # 1. Mock _filter_ids_against_census: only CL:0000540 (parent) and CL:child_566 survive
        def census_filter_effect(
            ids_to_filter, census_version, organism, ontology_column_name
        ):
            logging.info(
                f"MOCK _filter_ids_against_census called with: {ids_to_filter}"
            )
            allowed_by_census = ["CL:0000540", "CL:child_566"]
            return [id_ for id_ in ids_to_filter if id_ in allowed_by_census]

        mock_filter_census.side_effect = census_filter_effect

        # 2. Mock OntologyExtractor.get_subclasses
        def get_subclasses_effect(term_id, category, organism=None):
            logging.info(f"MOCK get_subclasses for: {term_id}")
            if term_id == "CL:0000540":
                return [{"ID": "CL:child_540", "Label": "Child 540"}]
            if term_id == "CL:0000566":
                return [{"ID": "CL:child_566", "Label": "Child 566"}]
            return []

        mock_get_subclasses.side_effect = get_subclasses_effect

        # Inputs
        query_filter = "cell_type_ontology_term_id in ['CL:0000540', 'CL:0000566']"
        categories = ["cell_type"]
        organism = "homo_sapiens"
        census_version = "mock_version"

        # --- ACT ---
        rewritten_filter = obs_close(query_filter, categories, organism, census_version)
        logging.info(
            f"IDs Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # CL:0000540 + CL:child_540 -> filter keeps CL:0000540, discards CL:child_540
        # CL:0000566 + CL:child_566 -> filter discards CL:0000566, keeps CL:child_566
        self.assertIn("'CL:0000540'", rewritten_filter)
        self.assertNotIn("'CL:child_540'", rewritten_filter)
        self.assertNotIn("'CL:0000566'", rewritten_filter)
        self.assertIn("'CL:child_566'", rewritten_filter)

    # Test 3: Multiple categories with filtering
    @patch("ontology_closure.onto_closure.OntologyExtractor.get_subclasses")
    @patch(
        "ontology_closure.onto_closure.OntologyExtractor.get_ontology_id_from_label"
    )  # Needed if any inputs are labels
    @patch("ontology_closure.onto_closure._filter_ids_against_census")
    def test_obs_close_with_multiple_categories_and_filtering(
        self,
        mock_filter_census,
        mock_get_id_from_label,
        mock_get_subclasses,
    ):
        print("\nRunning: test_obs_close_with_multiple_categories_and_filtering")
        # --- ARRANGE ---
        # 1. Mock _filter_ids_against_census
        mock_filter_census.side_effect = (
            lambda ids_to_filter, census_version, organism, ontology_column_name: [
                id_
                for id_ in ids_to_filter
                if id_
                in [
                    "CL:0000540",
                    "UBERON:0002107",
                    "MONDO:0005148",
                    "MmusDv:0000001_child",  # Allow child of MmusDv term
                    "HsapDv:parent_dev_id",  # Allow a hypothetical HsapDv parent
                ]
            ]
        )

        # 2. Mock get_ontology_id_from_label (only if you plan to test labels in this multi-category test)
        #    For this example, all inputs are IDs, so this mock might not be strictly hit unless get_subclasses calls it.
        mock_get_id_from_label.return_value = (
            None  # Default, can be more specific if needed
        )

        # 3. Mock get_subclasses
        def get_subclasses_effect(term_id, category, organism=None):
            logging.info(f"MOCK get_subclasses for: {term_id} in category {category}")
            if term_id == "CL:0000540":
                return [{"ID": "CL:0000540_child", "Label": "CL Child"}]
            if term_id == "UBERON:0002107":
                return [{"ID": "UBERON:0002107_child", "Label": "UBERON Child"}]
            if term_id == "MONDO:0005148":
                return [{"ID": "MONDO:0005148_child", "Label": "MONDO Child"}]
            if term_id == "MmusDv:0000001":
                return [{"ID": "MmusDv:0000001_child", "Label": "MmusDv Child1"}]
            # Example for a HsapDv term if it were in the query
            # if term_id == "HsapDv:parent_dev_id": return [{"ID": "HsapDv_child", "Label": "HsapDv Child"}]
            return []

        mock_get_subclasses.side_effect = get_subclasses_effect

        # Inputs
        query_filter = (
            "cell_type_ontology_term_id in ['CL:0000540', 'CL:0000566'] and "
            "tissue_ontology_term_id in ['UBERON:0002107', 'UBERON:0001234'] and "
            "disease_ontology_term_id in ['MONDO:0005148', 'MONDO:0001234'] and "
            "development_stage_ontology_term_id in ['MmusDv:0000001', 'MmusDv:0000002']"
        )
        categories = ["cell_type", "tissue", "disease", "development_stage"]
        organism = "Mus musculus"  # This organism is used for Ubergraph dev stage context AND census
        census_version = "mock_version"

        # --- ACT ---
        rewritten_filter = obs_close(query_filter, categories, organism, census_version)
        logging.info(
            f"Multi-Cat Test - Original: {query_filter}\nRewritten: {rewritten_filter}"
        )

        # --- ASSERT ---
        # Based on mocks:
        # CL:0000540 (parent) survives census. Its child CL:0000540_child does not.
        # UBERON:0002107 (parent) survives. Its child UBERON:0002107_child does not.
        # MONDO:0005148 (parent) survives. Its child MONDO:0005148_child does not.
        # MmusDv:0000001 (parent) does not survive. Its child MmusDv:0000001_child DOES survive.

        self.assertIn("'CL:0000540'", rewritten_filter)
        self.assertNotIn("'CL:0000540_child'", rewritten_filter)
        self.assertNotIn(
            "'CL:0000566'", rewritten_filter
        )  # And its child (empty from mock_get_subclasses)

        self.assertIn("'UBERON:0002107'", rewritten_filter)
        self.assertNotIn("'UBERON:0002107_child'", rewritten_filter)
        self.assertNotIn("'UBERON:0001234'", rewritten_filter)

        self.assertIn("'MONDO:0005148'", rewritten_filter)
        self.assertNotIn("'MONDO:0005148_child'", rewritten_filter)
        self.assertNotIn("'MONDO:0001234'", rewritten_filter)

        self.assertNotIn(
            "'MmusDv:0000001'", rewritten_filter
        )  # Parent MmusDv ID is not in mock census allowed list
        self.assertIn(
            "'MmusDv:0000001_child'", rewritten_filter
        )  # Child MmusDv ID IS in mock census allowed list
        self.assertNotIn("'MmusDv:0000002'", rewritten_filter)


if __name__ == "__main__":
    unittest.main()
