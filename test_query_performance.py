import logging
import time
from src.cxg_query_enhancer import enhance

# --- CONFIGURE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def test_query_performance():
    """
    Tests the performance of the enhance function.
    """
    logger.info("Starting query performance test...")

    # --- 1. Define Inputs ---
    input_query_filter = "cell_type == 'neuron'"

    logger.info(f"Input Query Filter:\n{input_query_filter}")

    # --- 2. Execute the enhance function and measure time ---
    try:
        start_time = time.time()
        rewritten_filter = enhance(
            input_query_filter,
        )
        end_time = time.time()

        execution_time = end_time - start_time
        logger.info(f"✅ Query performance test executed successfully.")
        logger.info(f"Time taken: {execution_time:.4f} seconds")

        logger.info("Original Query Filter:")
        print(f"Original: {input_query_filter}")
        logger.info("Rewritten Query Filter:")
        print(f"Rewritten: {rewritten_filter}")

    except Exception as e:
        logger.error(f"❌ Query performance test FAILED: {e}", exc_info=True)
        print(f"❌ Query performance test FAILED: {e}")


if __name__ == "__main__":
    test_query_performance()
