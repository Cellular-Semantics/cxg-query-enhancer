import unittest
import logging
from unittest.mock import patch, MagicMock
from cxg_query_enhancer import OntologyExtractor, SPARQLClient
from cxg_query_enhancer.enhancer import _sparql_string

# Configure logging
logger = logging.getLogger(__name__)


class TestOntologyExtractor(unittest.TestCase):
    def setUp(self):
        """Executed before each test method."""
        self.mock_sparql_client = MagicMock(spec=SPARQLClient)
        self.extractor = OntologyExtractor(self.mock_sparql_client)

    # --- Tests for get_ontology_id_from_label ---
    def test_get_id_from_label_valid_cell_type(self):
        logger.info("Running: test_get_id_from_label_valid_cell_type")
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
        # Ubergraph labels are xsd:string-typed; a plain literal never matches
        self.assertIn(f'rdfs:label "{label}"^^xsd:string', called_query)
        self.assertIn("cl.owl", called_query)

    def test_get_id_from_label_development_stage_mus_musculus(self):
        logger.info("Running: test_get_id_from_label_development_stage_mus_musculus")
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
        logger.info("Running: test_get_id_from_label_not_found")
        self.mock_sparql_client.query.return_value = []
        actual_id = self.extractor.get_ontology_id_from_label(
            "non_existent_label", "cell_type"
        )
        self.assertIsNone(actual_id)

    def test_get_id_from_label_unsupported_category(self):
        logger.info("Running: test_get_id_from_label_unsupported_category")
        with self.assertRaisesRegex(ValueError, "Unsupported category 'faké_category'"):
            self.extractor.get_ontology_id_from_label("some_label", "faké_category")

    def test_get_id_from_label_dev_stage_no_organism(self):
        logger.info("Running: test_get_id_from_label_dev_stage_no_organism")
        with self.assertRaisesRegex(
            ValueError, "The 'organism' parameter is required for 'development_stage'"
        ):
            self.extractor.get_ontology_id_from_label(
                "some_stage", "development_stage", organism=None
            )

    def test_get_id_from_label_falls_back_to_case_insensitive_match(self):
        self.mock_sparql_client.query.side_effect = [
            [],
            [{"term": {"value": "http://purl.obolibrary.org/obo/CL_0000127"}}],
        ]

        actual_id = self.extractor.get_ontology_id_from_label("Astrocyte", "cell_type")

        self.assertEqual(actual_id, "CL:0000127")
        self.assertEqual(self.mock_sparql_client.query.call_count, 2)
        exact_query, fallback_query = [
            c[0][0] for c in self.mock_sparql_client.query.call_args_list
        ]
        self.assertIn('"Astrocyte"^^xsd:string', exact_query)
        self.assertNotIn("LCASE", exact_query)
        self.assertIn('LCASE(STR(?matched)) = LCASE("Astrocyte")', fallback_query)

    def test_get_id_from_label_prefers_label_over_synonym(self):
        self.mock_sparql_client.query.return_value = [
            {"term": {"value": "http://purl.obolibrary.org/obo/CL_0000127"}}
        ]

        self.extractor.get_ontology_id_from_label("astrocyte", "cell_type")

        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn("BIND(0 AS ?rank)", called_query)
        self.assertIn("BIND(1 AS ?rank)", called_query)
        self.assertIn("ORDER BY ?rank", called_query)

    def test_get_id_from_label_escapes_label(self):
        self.mock_sparql_client.query.return_value = []
        label = 'say "hi" \\ bye'

        self.assertIsNone(self.extractor.get_ontology_id_from_label(label, "cell_type"))

        for c in self.mock_sparql_client.query.call_args_list:
            self.assertIn('"say \\"hi\\" \\\\ bye"', c[0][0])

    # --- Tests for SPARQL escaping and ID validation ---
    def test_sparql_string_escaping(self):
        cases = {
            "neuron": '"neuron"',
            "Peyer's patch": '"Peyer\'s patch"',
            'a "quoted" term': '"a \\"quoted\\" term"',
            "back\\slash": '"back\\\\slash"',
            "line\nbreak\ttab\r": '"line\\nbreak\\ttab\\r"',
            "Schwann-Zelle für µ": '"Schwann-Zelle für µ"',
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(_sparql_string(value), expected)

    def test_get_subclasses_escapes_label(self):
        self.mock_sparql_client.query.return_value = []

        self.extractor.get_subclasses('x") } ; DROP ALL ; { ("', "cell_type")

        called_query = self.mock_sparql_client.query.call_args[0][0]
        self.assertIn('LCASE("x\\") } ; DROP ALL ; { (\\"")', called_query)

    def test_get_subclasses_rejects_malformed_ids(self):
        for term in [
            "CL:0000540 } UNION { ?s ?p ?o",
            "CL:abc",
            "UBERON:",
            "CL:0000540\n",
        ]:
            with self.subTest(term=term):
                with self.assertRaisesRegex(ValueError, "Invalid ontology ID"):
                    self.extractor.get_subclasses(term, "cell_type")
        self.mock_sparql_client.query.assert_not_called()

    # --- Tests for get_subclasses (which calls _get_ontology_expansion) ---
    def test_get_subclasses_for_id_input_success(self):
        logger.info("Running: test_get_subclasses_for_id_input_success")
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
        logger.info("Running: test_get_subclasses_for_label_input_success")
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
        # Exact (typed-literal) match is tried first; the LCASE scan is only a fallback
        self.assertIn(f'"{term_label}"^^xsd:string', called_query)
        self.assertNotIn("LCASE", called_query)

    def test_get_subclasses_no_subclasses_found(self):
        logger.info("Running: test_get_subclasses_no_subclasses_found")
        term_id = "CL:0000540"
        category = "cell_type"
        self.mock_sparql_client.query.return_value = []
        actual_subclasses = self.extractor.get_subclasses(term_id, category)
        self.assertEqual(actual_subclasses, [])

    def test_get_subclasses_dev_stage_id_input_correct_prefix_handling(self):
        logger.info(
            "Running: test_get_subclasses_dev_stage_id_input_correct_prefix_handling"
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
        logger.info(
            "Running: test_get_subclasses_dev_stage_id_input_mismatched_organism"
        )
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
    # Configure logging level from command line or default to WARNING
    import sys

    log_level = logging.WARNING
    if "--verbose" in sys.argv or "-v" in sys.argv:
        log_level = logging.INFO
        sys.argv = [arg for arg in sys.argv if arg not in ["--verbose", "-v"]]
    elif "--debug" in sys.argv:
        log_level = logging.DEBUG
        sys.argv = [arg for arg in sys.argv if arg != "--debug"]

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    unittest.main()
