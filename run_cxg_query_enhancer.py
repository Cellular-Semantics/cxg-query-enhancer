import logging
import time
from cxg_query_enhancer import enhance, OntologyExtractor, SPARQLClient

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
    Runs a simple end-to-end test of the enhance pipeline.
    """
    logger.info(" Starting end-to-end pipeline test...")

    # --- 1. Define Inputs ---
    input_query_filter = (
        "sex == 'Female' and "
        "cell_type == 'medium spiny neuron' and "
        "tissue == 'kidney' and "
        "disease in ['diabetes mellitus'] and "
        "development_stage == '10-month-old stage'"
    )

    logger.info(f"Input Query Filter:\n{input_query_filter}")

    # --- 2. Execute the enhance function (the core of the pipeline) ---
    # No need to manually initialize SPARQLClient or OntologyExtractor here,
    # as enhance handles its own internal instantiation of OntologyExtractor.
    try:
        start = time.perf_counter()
        rewritten_filter = enhance(input_query_filter, organism="Mus musculus")
        end = time.perf_counter()
        elapsed = end - start

        logger.info(f"Enhance execution time: {elapsed:.4f} seconds")

        logger.info("✅ End-to-end pipeline test executed.")
        logger.info("Original Query Filter:")
        print(f"Original: {input_query_filter}")  # Also print to stdout for quick view
        logger.info("Rewritten Query Filter:")
        print(f"Rewritten: {rewritten_filter}")
        print(f"Time to run enhance(): {elapsed:.4f} seconds")
    except Exception as e:
        logger.error(f"❌ End-to-end pipeline test FAILED: {e}", exc_info=True)
        print(f"❌ End-to-end pipeline test FAILED: {e}")


if __name__ == "__main__":
    run_end_to_end_test()
