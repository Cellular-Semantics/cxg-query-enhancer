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
    organism = "Mus musculus"  # Example organism
    query_filter = "cell_type in ['neuron', 'medium spiny neuron'] and tissue in ['kidney'] and disease in ['diabetes mellitus'] and developmental_stage in ['10-day-old stage']"

    # Categories to apply closure
    categories = ["cell_type", "tissue", "disease", "developmental_stage"]

    # Rewrite the query filter using obs_close-
    logging.info("Starting test for obs_close...")
    rewritten_filter = obs_close(query_filter, categories, organism=organism)

    # Log the results
    logging.info("Original Query Filter:")
    logging.info(query_filter)
    logging.info("Rewritten Query Filter:")
    logging.info(rewritten_filter)


def test_obs_close():
    """
    Test the obs_close function to ensure it rewrites the query filter correctly.
    """
    # Example query filter
    organism = "Mus musculus"  # Example organism
    query_filter = (
        "cell_type in ['neuron', 'medium spiny neuron'] and "
        "tissue in ['kidney'] and "
        "disease in ['diabetes mellitus'] and "
        "developmental_stage in ['10-day-old stage']"
    )

    # Categories to apply closure
    categories = ["cell_type", "tissue", "disease", "developmental_stage"]

    # Rewrite the query filter using obs_close
    logging.info("Starting test for obs_close...")
    rewritten_filter = obs_close(query_filter, categories, organism=organism)

    # Log the results
    logging.info("Original Query Filter:")
    logging.info(query_filter)
    logging.info("Rewritten Query Filter:")
    logging.info(rewritten_filter)


def test_obs_close_with_ids():
    """
    Test the obs_close function to ensure it handles ontology IDs correctly.
    """
    # Example query filter with ontology IDs
    query_filter = (
        "cell_type_ontology_term_id in ['CL:0000540', 'CL:0000566'] and "
        "tissue_ontology_term_id in ['UBERON:0002107']"
    )

    # Categories to apply closure
    categories = ["cell_type", "tissue"]

    # Rewrite the query filter using obs_close
    logging.info("Starting test for obs_close with ontology IDs...")
    rewritten_filter = obs_close(query_filter, categories)

    # Log the results
    logging.info("Original Query Filter:")
    logging.info(query_filter)
    logging.info("Rewritten Query Filter:")
    logging.info(rewritten_filter)


def test_obs_close_with_synonyms():
    """
    Test the obs_close function to ensure it handles synonyms correctly.
    """
    # Example query filter with a synonym
    query_filter = "cell_type in ['epitheliocyte']"

    # Categories to apply closure
    categories = ["cell_type"]

    # Rewrite the query filter using obs_close
    logging.info("Starting test for obs_close with synonyms...")
    rewritten_filter = obs_close(query_filter, categories)

    # Log the results
    logging.info("Original Query Filter:")
    logging.info(query_filter)
    logging.info("Rewritten Query Filter:")
    logging.info(rewritten_filter)


if __name__ == "__main__":
    # Run both tests
    test_obs_close()
    test_obs_close_with_ids()
    test_obs_close_with_synonyms()
