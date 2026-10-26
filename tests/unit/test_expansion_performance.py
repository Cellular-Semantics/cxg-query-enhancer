"""
Tests for the expansion performance features:
- Exact (typed-literal) label match first, case-insensitive LCASE scan only as a fallback
- Memoization of expansion results
- Concurrent processing of categories on a shared expansion pool
"""

import concurrent.futures
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from cxg_query_enhancer import OntologyExtractor, SPARQLClient, enhance
from cxg_query_enhancer.enhancer import (
    _cached_expansion,
    _census_terms_lock,
    _thread_safe_expansion,
    process_category,
)

RESULT = [
    {
        "term": {"value": "http://purl.obolibrary.org/obo/UBERON_0002048"},
        "label": {"value": "lung"},
    }
]


class TestLabelMatchStrategy(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock(spec=SPARQLClient)
        self.extractor = OntologyExtractor(self.client)

    def queries(self):
        return [c[0][0] for c in self.client.query.call_args_list]

    def test_exact_match_hit_skips_fallback(self):
        self.client.query.return_value = RESULT

        result = self.extractor.get_subclasses("lung", "tissue")

        self.assertEqual(result, [{"ID": "UBERON:0002048", "Label": "lung"}])
        (query,) = self.queries()
        self.assertIn('"lung"^^xsd:string', query)
        self.assertNotIn("LCASE", query)

    def test_exact_match_also_tries_lowercased_term(self):
        self.client.query.return_value = RESULT

        self.extractor.get_subclasses("Lung", "tissue")

        (query,) = self.queries()
        self.assertIn(
            'VALUES ?inputTermLabel { "Lung"^^xsd:string "lung"^^xsd:string }', query
        )

    def test_falls_back_to_case_insensitive_match(self):
        self.client.query.side_effect = [[], RESULT]

        result = self.extractor.get_subclasses("t cell", "cell_type")

        self.assertEqual(result, [{"ID": "UBERON:0002048", "Label": "lung"}])
        exact_query, fallback_query = self.queries()
        self.assertNotIn("LCASE", exact_query)
        self.assertIn('LCASE(STR(?inputTermLabel)) = LCASE("t cell")', fallback_query)

    def test_no_match_returns_empty_after_both_queries(self):
        self.client.query.return_value = []

        self.assertEqual(self.extractor.get_subclasses("no such term", "tissue"), [])
        self.assertEqual(self.client.query.call_count, 2)

    def test_id_input_uses_single_query(self):
        self.client.query.return_value = []

        self.extractor.get_subclasses("UBERON:0002048", "tissue")

        (query,) = self.queries()
        self.assertIn("VALUES ?inputTerm { obo:UBERON_0002048 }", query)
        self.assertNotIn("LCASE", query)


@patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
class TestExpansionCache(unittest.TestCase):
    def setUp(self):
        _cached_expansion.cache_clear()
        self.addCleanup(_cached_expansion.cache_clear)

    def test_repeated_expansion_is_cached(self, mock_expansion):
        mock_expansion.return_value = [{"ID": "CL:0000540", "Label": "neuron"}]

        first = _thread_safe_expansion("neuron", "cell_type", "homo_sapiens")
        second = _thread_safe_expansion("neuron", "cell_type", "homo_sapiens")

        self.assertEqual(first, second)
        mock_expansion.assert_called_once()

    def test_organism_only_keys_development_stage(self, mock_expansion):
        mock_expansion.return_value = [{"ID": "X:1", "Label": "x"}]

        _thread_safe_expansion("neuron", "cell_type", "homo_sapiens")
        _thread_safe_expansion("neuron", "cell_type", "mus_musculus")
        self.assertEqual(mock_expansion.call_count, 1)

        _thread_safe_expansion("adult", "development_stage", "homo_sapiens")
        _thread_safe_expansion("adult", "development_stage", "mus_musculus")
        self.assertEqual(mock_expansion.call_count, 3)

    def test_failures_are_not_cached(self, mock_expansion):
        mock_expansion.side_effect = [
            RuntimeError("SPARQL query failed: timeout"),
            [{"ID": "CL:0000540", "Label": "neuron"}],
        ]

        with self.assertRaises(RuntimeError):
            _thread_safe_expansion("neuron", "cell_type", None)
        result = _thread_safe_expansion("neuron", "cell_type", None)

        self.assertEqual(result, [{"ID": "CL:0000540", "Label": "neuron"}])

    def test_callers_cannot_mutate_cached_result(self, mock_expansion):
        mock_expansion.return_value = [{"ID": "CL:0000540", "Label": "neuron"}]

        _thread_safe_expansion("neuron", "cell_type", None).append({"ID": "junk"})

        self.assertEqual(
            _thread_safe_expansion("neuron", "cell_type", None),
            [{"ID": "CL:0000540", "Label": "neuron"}],
        )


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        _cached_expansion.cache_clear()
        self.addCleanup(_cached_expansion.cache_clear)

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    def test_categories_are_expanded_concurrently(self, mock_expansion):
        # Each expansion waits until the other category's expansion has started,
        # which only succeeds if the categories are processed at the same time.
        barrier = threading.Barrier(2, timeout=5)

        def expansion(term, category, organism=None):
            barrier.wait()
            return [{"ID": f"ID:{term}", "Label": term}]

        mock_expansion.side_effect = expansion

        result = enhance(
            "cell_type == 'neuron' and tissue == 'lung'", census_version=None
        )

        self.assertEqual(result, "cell_type in ['neuron'] and tissue in ['lung']")

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    def test_max_workers_caps_concurrent_expansions(self, mock_expansion):
        lock = threading.Lock()
        state = {"running": 0, "peak": 0}

        def expansion(term, category, organism=None):
            with lock:
                state["running"] += 1
                state["peak"] = max(state["peak"], state["running"])
            time.sleep(0.05)
            with lock:
                state["running"] -= 1
            return [{"ID": f"ID:{term}", "Label": term}]

        mock_expansion.side_effect = expansion

        enhance(
            "cell_type in ['a', 'b', 'c'] and tissue in ['d', 'e']",
            census_version=None,
            max_workers=1,
        )

        self.assertEqual(mock_expansion.call_count, 5)
        self.assertEqual(state["peak"], 1)

    @patch("cxg_query_enhancer.enhancer.OntologyExtractor._get_ontology_expansion")
    @patch("cxg_query_enhancer.enhancer.SPARQLClient")
    def test_sparql_timeout_is_passed_to_client(self, mock_client, mock_expansion):
        mock_expansion.return_value = [{"ID": "CL:0000540", "Label": "neuron"}]

        enhance("cell_type == 'neuron'", census_version=None, sparql_timeout=7)

        mock_client.assert_called_with(timeout=7)

    def test_process_category_uses_given_executor_without_shutting_it_down(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            result = process_category(
                ["CL:1"],
                category="cell_type",
                organism="homo_sapiens",
                census_version=None,
                is_label_based=False,
                expansion_fn=lambda t, c, o: [{"ID": t, "Label": t}],
                executor=executor,
            )
            # The shared executor is still usable afterwards
            self.assertEqual(executor.submit(lambda: 42).result(), 42)

        self.assertEqual(result, ["CL:1"])

    def test_census_terms_lock_is_per_column(self):
        a = _census_terms_lock("latest", "homo_sapiens", "cell_type_ontology_term_id")
        b = _census_terms_lock("latest", "homo_sapiens", "cell_type_ontology_term_id")
        c = _census_terms_lock("latest", "homo_sapiens", "tissue_ontology_term_id")

        self.assertIs(a, b)
        self.assertIsNot(a, c)


if __name__ == "__main__":
    unittest.main()
