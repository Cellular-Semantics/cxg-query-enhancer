import unittest
from unittest.mock import patch, MagicMock
import pandas as pd  # For testing extract_and_save_hierarchy if you go that far
from ontology_closure.onto_closure import OntologyExtractor, SPARQLClient

import logging

logging.basicConfig(level=logging.INFO)


class TestOntologyExtractor(unittest.TestCase):

    def setUp(self):
        """Executed before each test method."""
        # Create a mock for SPARQLClient. Its 'query' method will be configured per test.
        self.mock_sparql_client = MagicMock(spec=SPARQLClient)
        # Instantiate OntologyExtractor with the mock client and dummy root_ids/output_dir
        # as these are not the focus of most unit tests for get_ontology_id_from_label or get_subclasses.
        self.extractor = OntologyExtractor(
            self.mock_sparql_client, root_ids=[], output_dir="test_output"
        )

    # --- Tests for get_ontology_id_from_label ---
    # Test Case: Label successfully resolved for "cell_type"
    def test_get_id_from_label_valid_cell_type(self):
        print("\nRunning: test_get_id_from_label_valid_cell_type")
        # Arrange
        label = "neuron"
        category = "cell_type"
        expected_id = "CL:0000540"
        # Simulate SPARQLClient returning a result
        self.mock_sparql_client.query.return_value = [
            {"term": {"value": "http://purl.obolibrary.org/obo/CL_0000540"}}
        ]

        # Act
        actual_id = self.extractor.get_ontology_id_from_label(
            label, category, organism=None
        )

        # Assert
        self.assertEqual(actual_id, expected_id)
        self.mock_sparql_client.query.assert_called_once()
        # Optional: Assert the query string was as expected
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(f'FILTER(LCASE(?label) = LCASE("{label}"))', called_query)
        self.assertIn("obo/CL_", called_query)  # Check for correct prefix filter

    # Test Case: Label successfully resolved for "developmental_stage" (e.g., Mus Musculus)
    def test_get_id_from_label_development_stage_mus_musculus(self):
        print("\nRunning: test_get_id_from_label_development_stage_mus_musculus")
        # Arrange
        label = "embryonic stage"
        category = "development_stage"
        organism = "Mus musculus"  # or "mus_musculus"
        expected_id = "MmusDv:0000002"  # Example
        self.mock_sparql_client.query.return_value = [
            {"term": {"value": "http://purl.obolibrary.org/obo/MmusDv_0000002"}}
        ]

        # Act
        actual_id = self.extractor.get_ontology_id_from_label(label, category, organism)

        # Assert
        self.assertEqual(actual_id, expected_id)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn("obo/MmusDv_", called_query)

    # Test Case: Label not found for "cell_type"
    def test_get_id_from_label_not_found(self):
        print("\nRunning: test_get_id_from_label_not_found")
        # Arrange
        self.mock_sparql_client.query.return_value = (
            []
        )  # Simulate no results when the query method of mocked sparql_client is called during this specific test

        # Act
        actual_id = self.extractor.get_ontology_id_from_label(
            "non_existent_label",
            "cell_type",  # call the method with a test label ("non_existent_label") and category.
        )

        # Assert
        self.assertIsNone(actual_id)

    # Test Case: Unsupported category used
    def test_get_id_from_label_unsupported_category(self):
        print("\nRunning: test_get_id_from_label_unsupported_category")
        # No specific mock configuration is needed for self.mock_sparql_client.query
        # because we expect the function to raise an error *before* it even tries to make a SPARQL query.
        # Arrange / Act / Assert
        with self.assertRaisesRegex(ValueError, "Unsupported category 'faké_category'"):
            self.extractor.get_ontology_id_from_label("some_label", "faké_category")

    # Test Case: "developmental_stage" without organism
    def test_get_id_from_label_dev_stage_no_organism(self):
        print("\nRunning: test_get_id_from_label_dev_stage_no_organism")
        with self.assertRaisesRegex(
            ValueError, "The 'organism' parameter is required for 'development_stage'"
        ):
            self.extractor.get_ontology_id_from_label(
                "some_stage", "development_stage", organism=None
            )

    # --- Tests for get_subclasses ---
    # Test Case: Subclasses successfully retrieved for "cell_type" ID input
    def test_get_subclasses_for_id_input_success(self):
        print("\nRunning: test_get_subclasses_for_id_input_success")
        # Arrange
        term_id = "CL:0000540"
        category = "cell_type"
        # Simulate SPARQLClient returning subclasses
        self.mock_sparql_client.query.return_value = [
            {
                "term": {"value": "http://purl.obolibrary.org/obo/CL_child1"},
                "label": {"value": "Child Neuron 1"},
            },
            {
                "term": {"value": "http://purl.obolibrary.org/obo/CL_child2"},
                "label": {"value": "Child Neuron 2"},
            },
        ]
        expected_subclasses = [
            {"ID": "CL:child1", "Label": "Child Neuron 1"},
            {"ID": "CL:child2", "Label": "Child Neuron 2"},
        ]

        # Act
        actual_subclasses = self.extractor.get_subclasses(term_id, category)

        # Assert
        self.assertEqual(actual_subclasses, expected_subclasses)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(
            f'VALUES ?inputTerm {{ obo:{term_id.replace(":", "_")} }}', called_query
        )
        self.assertIn(
            "obo/CL_", called_query
        )  # Check for correct iri_prefix filter for children

    @patch(
        "ontology_closure.onto_closure.OntologyExtractor.get_ontology_id_from_label"
    )  # Mock the internal call
    # Test Case: Input is a label (e.g., "neuron"), subclasses found
    def test_get_subclasses_for_label_input_success(self, mock_get_id_label):
        print("\nRunning: test_get_subclasses_for_label_input_success")
        # Arrange
        term_label = "neuron"
        category = "cell_type"
        resolved_id = "CL:0000540"

        # Configure the mock for the internal get_ontology_id_from_label call
        mock_get_id_label.return_value = resolved_id

        # Configure the mock for the sparql_client.query call (made by get_subclasses itself)
        self.mock_sparql_client.query.return_value = [
            {
                "term": {"value": "http://purl.obolibrary.org/obo/CL_child1"},
                "label": {"value": "Child Neuron 1"},
            },
        ]
        expected_subclasses = [{"ID": "CL:child1", "Label": "Child Neuron 1"}]

        # Act
        actual_subclasses = self.extractor.get_subclasses(term_label, category)

        # Assert
        self.assertEqual(actual_subclasses, expected_subclasses)
        mock_get_id_label.assert_called_once_with(term_label, category, organism=None)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(
            f'VALUES ?inputTerm {{ obo:{resolved_id.replace(":", "_")} }}', called_query
        )

    # Test Case: No subclasses found for a valid ID
    def test_get_subclasses_no_subclasses_found(self):
        print("\nRunning: test_get_subclasses_no_subclasses_found")
        # Arrange
        term_id = "CL:0000540"
        category = "cell_type"
        self.mock_sparql_client.query.return_value = []  # Simulate no results

        # Act
        actual_subclasses = self.extractor.get_subclasses(term_id, category)

        # Assert
        self.assertEqual(actual_subclasses, [])

    # Test Case: Developmental stage ID input with correct organism (e.g., "MmusDv:..." with organism="Mus musculus")
    def test_get_subclasses_dev_stage_id_input_correct_prefix_handling(self):
        print(
            "\nRunning: test_get_subclasses_dev_stage_id_input_correct_prefix_handling"
        )
        # Test the fix for development stage IDs
        term_id = "MmusDv:0000001"  # Mouse ID
        category = "development_stage"
        organism = "Mus musculus"  # Correct organism context

        self.mock_sparql_client.query.return_value = [  # Simulate some MmusDv children
            {
                "term": {"value": "http://purl.obolibrary.org/obo/MmusDv_child1"},
                "label": {"value": "Mouse Dev Child 1"},
            }
        ]
        expected = [{"ID": "MmusDv:child1", "Label": "Mouse Dev Child 1"}]

        # Act
        actual = self.extractor.get_subclasses(term_id, category, organism)

        # Assert
        self.assertEqual(actual, expected)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(
            f'obo:{term_id.replace(":", "_")}', called_query
        )  # Uses MmusDv ID for parent
        self.assertIn("obo/MmusDv_", called_query)  # Filters children for MmusDv

    # Test Case: Developmental stage ID input with mismatched organism (e.g., "MmusDv:..." with organism="Homo sapiens")
    def test_get_subclasses_dev_stage_id_input_mismatched_organism(self):
        print("\nRunning: test_get_subclasses_dev_stage_id_input_mismatched_organism")
        # Test the fix for development stage IDs with mismatched organism
        # Expects no children because filter will be for Homo Sapiens children of a Mus Musculus term
        term_id = "MmusDv:0000001"  # Mouse dev stage ID
        category = "development_stage"
        organism = "Homo sapiens"  # Mismatched organism context

        self.mock_sparql_client.query.return_value = (
            []
        )  # Ubergraph would return nothing for MmusDv parent and HsapDv children filter

        # Act
        actual = self.extractor.get_subclasses(term_id, category, organism)

        # Assert
        self.assertEqual(actual, [])
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(
            f'obo:{term_id.replace(":", "_")}', called_query
        )  # Query is for MmusDv ID
        self.assertIn(
            "obo/HsapDv_", called_query
        )  # But children are filtered by HsapDv


if __name__ == "__main__":
    unittest.main()
