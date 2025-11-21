import unittest
from unittest.mock import patch, MagicMock
from cxg_query_enhancer import OntologyExtractor, SPARQLClient


class TestOntologyExtractor(unittest.TestCase):
    def setUp(self):
        """Executed before each test method."""
        self.mock_sparql_client = MagicMock(spec=SPARQLClient)
        self.extractor = OntologyExtractor(self.mock_sparql_client)

    # --- Tests for get_ontology_id_from_label ---
    def test_get_id_from_label_valid_cell_type(self):
        print("\nRunning: test_get_id_from_label_valid_cell_type")
        label = "neuron"
        category = "cell_type"
        expected_id = "CL:0000540"
        self.mock_sparql_client.query.return_value = [
            {"term": {"value": "http://purl.obolibrary.org/obo/CL_0000540"}}
        ]

        actual_id = self.extractor.get_ontology_id_from_label(label, category)

        self.assertEqual(actual_id, expected_id)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(f'rdfs:label "{label}"', called_query)
        self.assertIn("cl.owl", called_query)

    def test_get_id_from_label_development_stage_mus_musculus(self):
        print("\nRunning: test_get_id_from_label_development_stage_mus_musculus")
        label = "embryonic stage"
        category = "development_stage"
        organism = "Mus musculus"
        expected_id = "MmusDv:0000002"
        self.mock_sparql_client.query.return_value = [
            {"term": {"value": "http://purl.obolibrary.org/obo/MmusDv_0000002"}}
        ]

        actual_id = self.extractor.get_ontology_id_from_label(label, category, organism)

        self.assertEqual(actual_id, expected_id)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn("mmusdv.owl", called_query)

    def test_get_id_from_label_not_found(self):
        print("\nRunning: test_get_id_from_label_not_found")
        self.mock_sparql_client.query.return_value = []
        actual_id = self.extractor.get_ontology_id_from_label(
            "non_existent_label", "cell_type"
        )
        self.assertIsNone(actual_id)

    def test_get_id_from_label_unsupported_category(self):
        print("\nRunning: test_get_id_from_label_unsupported_category")
        with self.assertRaisesRegex(ValueError, "Unsupported category 'faké_category'"):
            self.extractor.get_ontology_id_from_label("some_label", "faké_category")

    def test_get_id_from_label_dev_stage_no_organism(self):
        print("\nRunning: test_get_id_from_label_dev_stage_no_organism")
        with self.assertRaisesRegex(
            ValueError, "The 'organism' parameter is required for 'development_stage'"
        ):
            self.extractor.get_ontology_id_from_label(
                "some_stage", "development_stage", organism=None
            )

    # --- Tests for get_subclasses (which calls _get_ontology_expansion) ---
    def test_get_subclasses_for_id_input_success(self):
        print("\nRunning: test_get_subclasses_for_id_input_success")
        term_id = "CL:0000540"
        category = "cell_type"
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

        actual_subclasses = self.extractor.get_subclasses(term_id, category)

        self.assertEqual(actual_subclasses, expected_subclasses)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(f"obo:{term_id.replace(':', '_')}", called_query)
        self.assertIn("cl.owl", called_query)

    def test_get_subclasses_for_label_input_success(self):
        print("\nRunning: test_get_subclasses_for_label_input_success")
        term_label = "neuron"
        category = "cell_type"
        self.mock_sparql_client.query.return_value = [
            {
                "term": {"value": "http://purl.obolibrary.org/obo/CL_child1"},
                "label": {"value": "Child Neuron 1"},
            },
        ]
        expected_subclasses = [{"ID": "CL:child1", "Label": "Child Neuron 1"}]

        actual_subclasses = self.extractor.get_subclasses(term_label, category)

        self.assertEqual(actual_subclasses, expected_subclasses)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(
            f'LCASE(STR(?inputTermLabel)) = LCASE("{term_label}")', called_query
        )

    def test_get_subclasses_no_subclasses_found(self):
        print("\nRunning: test_get_subclasses_no_subclasses_found")
        term_id = "CL:0000540"
        category = "cell_type"
        self.mock_sparql_client.query.return_value = []
        actual_subclasses = self.extractor.get_subclasses(term_id, category)
        self.assertEqual(actual_subclasses, [])

    def test_get_subclasses_dev_stage_id_input_correct_prefix_handling(self):
        print(
            "\nRunning: test_get_subclasses_dev_stage_id_input_correct_prefix_handling"
        )
        term_id = "MmusDv:0000001"
        category = "development_stage"
        organism = "Mus musculus"
        self.mock_sparql_client.query.return_value = [
            {
                "term": {"value": "http://purl.obolibrary.org/obo/MmusDv_child1"},
                "label": {"value": "Mouse Dev Child 1"},
            }
        ]
        expected = [{"ID": "MmusDv:child1", "Label": "Mouse Dev Child 1"}]

        actual = self.extractor.get_subclasses(term_id, category, organism)

        self.assertEqual(actual, expected)
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(f"obo:{term_id.replace(':', '_')}", called_query)
        self.assertIn("mmusdv.owl", called_query)

    def test_get_subclasses_dev_stage_id_input_mismatched_organism(self):
        print("\nRunning: test_get_subclasses_dev_stage_id_input_mismatched_organism")
        term_id = "MmusDv:0000001"
        category = "development_stage"
        organism = "Homo sapiens"
        self.mock_sparql_client.query.return_value = []

        actual = self.extractor.get_subclasses(term_id, category, organism)

        self.assertEqual(actual, [])
        self.mock_sparql_client.query.assert_called_once()
        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn(f"obo:{term_id.replace(':', '_')}", called_query)
        self.assertIn("hsapdv.owl", called_query)


if __name__ == "__main__":
    unittest.main()
