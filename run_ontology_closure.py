import logging
from ontology_closure.onto_closure import obs_close, SPARQLClient, OntologyExtractor

# --- CONFIGURE LOGGING ---
logging.basicConfig(
    level=logging.INFO,  # Set to INFO or DEBUG to see output from your library
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
    ],
)
logger = logging.getLogger(__name__)  # Logger for this script itself


def run_end_to_end_test():
    """
    Runs a simple end-to-end test of the obs_close pipeline.
    """
    logger.info(" Starting end-to-end pipeline test...")

    # --- 1. Define Inputs ---
    organism = "Homo Sapiens"
    input_query_filter = (
        "cell_type in ['medium spiny neuron'] and "
        "tissue in ['kidney'] and "
        "disease in ['diabetes mellitus'] and "
        "development_stage in ['10-month-old stage']"
    )
    categories = ["cell_type", "tissue", "disease", "development_stage"]
    census_version = "latest"

    logger.info(f"Input Organism: {organism}")
    logger.info(f"Input Query Filter:\n{input_query_filter}")
    logger.info(f"Categories to process: {categories}")
    logger.info(f"Census Version for filtering: {census_version}")

    # --- 2. Execute the obs_close function (the core of the pipeline) ---
    # No need to manually initialize SPARQLClient or OntologyExtractor here,
    # as obs_close handles its own internal instantiation of OntologyExtractor.
    try:
        rewritten_filter = obs_close(
            input_query_filter,
            categories,
            organism=organism,
            census_version=census_version,
        )

        logger.info("✅ End-to-end pipeline test executed.")
        logger.info("Original Query Filter:")
        print(f"Original: {input_query_filter}")  # Also print to stdout for quick view
        logger.info("Rewritten Query Filter:")
        print(f"Rewritten: {rewritten_filter}")
    except Exception as e:
        logger.error(f"❌ End-to-end pipeline test FAILED: {e}", exc_info=True)
        print(f"❌ End-to-end pipeline test FAILED: {e}")


if __name__ == "__main__":
    run_end_to_end_test()
