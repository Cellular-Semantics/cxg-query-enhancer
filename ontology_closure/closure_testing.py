import logging
from onto_closure import OntologyExtractor, SPARQLClient, obs_close

# Configure logging for the test script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("ontology_closure_testing.log", mode="w"),  # Log to a file
    ],
)


def test_obs_close():
    """
    Test the obs_close function to ensure it rewrites the query filter correctly.
    """
    # Example query filter
    query_filter = (
        "cell_type in ['neuron', 'medium spiny neuron'] and tissue in ['kidney']"
    )

    # Categories to apply closure
    categories = ["cell_type", "tissue"]

    # Rewrite the query filter using obs_close
    logging.info("Starting test for obs_close...")
    rewritten_filter = obs_close(query_filter, categories)

    # Log the results
    logging.info("Original Query Filter:")
    logging.info(query_filter)
    logging.info("Rewritten Query Filter:")
    logging.info(rewritten_filter)


if __name__ == "__main__":
    test_obs_close()
