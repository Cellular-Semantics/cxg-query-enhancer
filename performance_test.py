#!/usr/bin/env python3
"""
Performance testing script to identify bottlenecks in OntologyExtractor operations.
Focus on testing the 'astrocyte' label resolution and subclass queries.
"""

import time
import logging
from cxg_query_enhancer import OntologyExtractor, SPARQLClient

# Configure logging to see timing information
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def time_function(func_name, func, *args, **kwargs):
    """Time a function and log results."""
    logger.info(f"🔍 Starting {func_name}...")
    start_time = time.time()

    try:
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time

        logger.info(f"✅ {func_name} completed in {duration:.2f} seconds")
        return result, duration
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        logger.error(f"❌ {func_name} failed after {duration:.2f} seconds: {e}")
        return None, duration


def test_label_resolution_bottleneck():
    """Test the specific bottleneck for 'astrocyte' label resolution."""
    logger.info("=" * 60)
    logger.info("PERFORMANCE TEST: Label Resolution Bottleneck Analysis")
    logger.info("=" * 60)

    # Initialize components
    sparql_client = SPARQLClient()
    extractor = OntologyExtractor(sparql_client)

    # Test case: 'astrocyte' label
    test_label = "astrocyte"
    test_category = "cell_type"

    logger.info(f"Testing with label: '{test_label}', category: '{test_category}'")

    # Step 1: Test get_ontology_id_from_label (suspected bottleneck)
    resolved_id, duration1 = time_function(
        "get_ontology_id_from_label",
        extractor.get_ontology_id_from_label,
        test_label, test_category
    )

    if resolved_id:
        logger.info(f"📍 Resolved '{test_label}' to ID: {resolved_id}")

        # Step 2: Test get_subclasses with the resolved ID
        subclasses, duration2 = time_function(
            "get_subclasses (with resolved ID)",
            extractor.get_subclasses,
            resolved_id, test_category
        )

        if subclasses:
            logger.info(f"📊 Found {len(subclasses)} subclasses")
            logger.info("First 3 subclasses:")
            for i, subclass in enumerate(subclasses[:3]):
                logger.info(f"  {i+1}. {subclass['ID']} - {subclass['Label']}")

        # Step 3: Compare with direct ID input (should be faster)
        logger.info("\n" + "=" * 40)
        logger.info("COMPARISON: Direct ID vs Label Input")
        logger.info("=" * 40)

        direct_subclasses, duration3 = time_function(
            "get_subclasses (direct ID input)",
            extractor.get_subclasses,
            resolved_id, test_category
        )

        # Summary
        logger.info("\n" + "=" * 40)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("=" * 40)
        logger.info(f"1. Label Resolution:     {duration1:.2f}s")
        logger.info(f"2. Subclasses (label):   {duration2:.2f}s")
        logger.info(f"3. Subclasses (direct):  {duration3:.2f}s")
        logger.info(f"Total (label workflow):  {duration1 + duration2:.2f}s")
        logger.info(f"Direct ID speedup:       {(duration2/duration3):.1f}x faster")

        # Identify bottleneck
        if duration1 > duration2:
            logger.warning("🐛 BOTTLENECK IDENTIFIED: Label resolution is the slowest step!")
        else:
            logger.warning("🐛 BOTTLENECK IDENTIFIED: Subclass retrieval is the slowest step!")

    else:
        logger.error("❌ Could not resolve label - cannot continue with subclass testing")


def test_sparql_query_components():
    """Test individual SPARQL query components to identify specific bottlenecks."""
    logger.info("\n" + "=" * 60)
    logger.info("DETAILED SPARQL QUERY ANALYSIS")
    logger.info("=" * 60)

    sparql_client = SPARQLClient()

    # Test 1: Simple label resolution query (what get_ontology_id_from_label does)
    label_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

    SELECT DISTINCT ?term
    WHERE {
        # Match main label
        {
            ?term rdfs:label ?label .
            FILTER(LCASE(?label) = LCASE("astrocyte"))
        }
        UNION
        # Match exact synonyms
        {
            ?term oboInOwl:hasExactSynonym ?synonym .
            FILTER(LCASE(?synonym) = LCASE("astrocyte"))
        }
        UNION
        # Match related synonyms
        {
            ?term oboInOwl:hasRelatedSynonym ?synonym .
            FILTER(LCASE(?synonym) = LCASE("astrocyte"))
        }
        UNION
        # Match broad synonyms
        {
            ?term oboInOwl:hasBroadSynonym ?synonym .
            FILTER(LCASE(?synonym) = LCASE("astrocyte"))
        }
        UNION
        # Match narrow synonyms
        {
            ?term oboInOwl:hasNarrowSynonym ?synonym .
            FILTER(LCASE(?synonym) = LCASE("astrocyte"))
        }
        FILTER(STRSTARTS(STR(?term), "http://purl.obolibrary.org/obo/CL_"))
    }
    LIMIT 1
    """

    result1, duration1 = time_function(
        "Direct label resolution SPARQL",
        sparql_client.query,
        label_query
    )

    # Test 2: Subclasses query (assumes astrocyte is CL:0000127)
    subclass_query = """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?term (STR(?term_label) as ?label)
    WHERE {
    VALUES ?inputTerm { obo:CL_0000127 }

    {
        ?term rdfs:subClassOf ?inputTerm .
    }
    UNION
    {
        ?term obo:BFO_0000050 ?inputTerm .
    }

    ?term rdfs:label ?term_label .
    FILTER(STRSTARTS(STR(?term), "http://purl.obolibrary.org/obo/CL_"))
    }
    LIMIT 1000
    """

    result2, duration2 = time_function(
        "Direct subclasses SPARQL",
        sparql_client.query,
        subclass_query
    )

    logger.info("\n" + "=" * 40)
    logger.info("SPARQL QUERY TIMING COMPARISON")
    logger.info("=" * 40)
    logger.info(f"Label resolution query:  {duration1:.2f}s")
    logger.info(f"Subclasses query:        {duration2:.2f}s")

    if duration1 > duration2:
        logger.warning("🐛 CONFIRMED: Label resolution SPARQL is the bottleneck!")
        logger.info("💡 RECOMMENDATION: Cache label-to-ID mappings or use direct IDs when possible")
    else:
        logger.warning("🐛 UNEXPECTED: Subclasses query is slower than label resolution")


if __name__ == "__main__":
    logger.info("🚀 Starting Performance Analysis for 'astrocyte' label...")

    try:
        # Test the full workflow
        test_label_resolution_bottleneck()

        # Test individual SPARQL components
        test_sparql_query_components()

        logger.info("\n🎯 Performance testing completed!")

    except Exception as e:
        logger.error(f"💥 Performance test failed: {e}", exc_info=True)