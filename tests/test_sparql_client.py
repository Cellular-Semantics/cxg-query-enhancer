import unittest
from unittest.mock import patch, MagicMock
from ontology_closure.onto_closure import SPARQLClient
import logging

# Configure logging for the test suite
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TestSPARQLClient(unittest.TestCase):
    @patch("ontology_closure.onto_closure.SPARQLWrapper.query")
    def test_query_success(self, mock_query):
        """
        Test SPARQLClient.query with a successful SPARQL query.
        """
        logging.info("Running: test_query_success")
        # Arrange
        # Mock SPARQLWrapper query result
        mock_query.return_value.convert.return_value = {
            "results": {
                "bindings": [
                    {"term": {"value": "http://purl.obolibrary.org/obo/CL_0000540"}}
                ]
            }
        }

        # Initialize SPARQLClient
        client = SPARQLClient()

        # Act
        logging.info("Executing SPARQL query...")
        results = client.query("SELECT ?term WHERE { ?term a owl:Class }")

        # Assert
        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["term"]["value"], "http://purl.obolibrary.org/obo/CL_0000540"
        )
        logging.info(f"Query executed successfully. Results: {results}")
        mock_query.assert_called_once()

    @patch("ontology_closure.onto_closure.SPARQLWrapper.query")
    def test_query_failure(self, mock_query):
        """
        Test SPARQLClient.query with a failed SPARQL query.
        """
        logging.info("Running: test_query_failure")
        # Arrange
        # Mock SPARQLWrapper query to raise an exception
        mock_query.side_effect = Exception("SPARQL query failed")

        # Initialize SPARQLClient
        client = SPARQLClient()

        # Act & Assert
        logging.info("Executing SPARQL query (expected to fail)...")
        with self.assertRaises(RuntimeError) as context:
            client.query("SELECT ?term WHERE { ?term a owl:Class }")

        self.assertIn("SPARQL query failed", str(context.exception))
        logging.error(f"Query failed as expected. Error: {context.exception}")
        mock_query.assert_called_once()


if __name__ == "__main__":
    unittest.main()
