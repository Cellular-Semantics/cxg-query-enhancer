import unittest
from unittest.mock import MagicMock

from cxg_query_enhancer.enhancer import EnhancementError, process_category


class TestProcessCategory(unittest.TestCase):
    def test_returns_labels_when_label_based(self):
        expansion_fn = MagicMock(
            return_value=[
                {"ID": "CL:0001", "Label": "Neuron"},
                {"ID": "CL:0002", "Label": "Neuron Child"},
            ]
        )
        census_filter_fn = MagicMock(
            return_value=[{"ID": "CL:0002", "Label": "Neuron Child"}]
        )

        result = process_category(
            ["neuron"],
            category="cell_type",
            organism="homo_sapiens",
            census_version="latest",
            is_label_based=True,
            expansion_fn=expansion_fn,
            census_filter_fn=census_filter_fn,
        )

        self.assertEqual(result, ["Neuron Child"])
        expansion_fn.assert_called_once_with("neuron", "cell_type", "homo_sapiens")
        census_filter_fn.assert_called_once_with(
            ids_to_filter=["CL:0001", "CL:0002"],
            organism="homo_sapiens",
            census_version="latest",
            ontology_column_name="cell_type_ontology_term_id",
            strict=False,
        )

    def test_returns_ids_when_id_based(self):
        expansion_fn = MagicMock(
            return_value=[
                {"ID": "CL:0001", "Label": "Neuron"},
                {"ID": "CL:0002", "Label": "Neuron Child"},
            ]
        )
        census_filter_fn = MagicMock(
            return_value=[
                {"ID": "CL:0001", "Label": "Neuron"},
            ]
        )

        result = process_category(
            ["CL:0001"],
            category="cell_type",
            organism="homo_sapiens",
            census_version="latest",
            is_label_based=False,
            expansion_fn=expansion_fn,
            census_filter_fn=census_filter_fn,
        )

        self.assertEqual(result, ["CL:0001"])
        census_filter_fn.assert_called_once()

    def test_skips_census_when_version_missing(self):
        expansion_fn = MagicMock(
            return_value=[
                {"ID": "CL:0001", "Label": "Neuron"},
                {"ID": "CL:0002", "Label": "Neuron Child"},
            ]
        )
        census_filter_fn = MagicMock()

        result = process_category(
            ["CL:0001"],
            category="cell_type",
            organism="homo_sapiens",
            census_version=None,
            is_label_based=False,
            expansion_fn=expansion_fn,
            census_filter_fn=census_filter_fn,
        )

        self.assertEqual(result, ["CL:0001", "CL:0002"])
        census_filter_fn.assert_not_called()


    def test_keeps_terms_that_fail_or_do_not_expand(self):
        def expansion_fn(term, category, organism):
            if term == "neuron":
                return [
                    {"ID": "CL:0001", "Label": "neuron"},
                    {"ID": "CL:0002", "Label": "Neuron Child"},
                ]
            if term == "astrocyte":
                raise RuntimeError("SPARQL query failed: timeout")
            return []  # e.g. "normal" or a typo: no ontology match

        result = process_category(
            ["neuron", "astrocyte", "normal"],
            category="cell_type",
            organism="homo_sapiens",
            census_version=None,
            is_label_based=True,
            expansion_fn=expansion_fn,
        )

        # Failed and unmatched terms are kept verbatim instead of being dropped
        self.assertEqual(result, ["Neuron Child", "astrocyte", "neuron", "normal"])

    def test_keeps_failed_terms_after_census_filtering(self):
        def expansion_fn(term, category, organism):
            if term == "CL:0001":
                return [{"ID": "CL:0001", "Label": "Neuron"}]
            raise RuntimeError("SPARQL query failed: timeout")

        census_filter_fn = MagicMock(return_value=[{"ID": "CL:0001", "Label": "Neuron"}])

        result = process_category(
            ["CL:0001", "CL:0002"],
            category="cell_type",
            organism="homo_sapiens",
            census_version="latest",
            is_label_based=False,
            expansion_fn=expansion_fn,
            census_filter_fn=census_filter_fn,
        )

        self.assertEqual(result, ["CL:0001", "CL:0002"])

    def test_strict_raises_on_expansion_failure(self):
        def expansion_fn(term, category, organism):
            if term == "astrocyte":
                raise RuntimeError("SPARQL query failed: timeout")
            return [{"ID": "CL:0001", "Label": "neuron"}]

        census_filter_fn = MagicMock()

        with self.assertRaisesRegex(
            EnhancementError, "cell_type term\\(s\\): 'astrocyte': SPARQL query failed"
        ) as ctx:
            process_category(
                ["neuron", "astrocyte"],
                category="cell_type",
                organism="homo_sapiens",
                census_version="latest",
                is_label_based=True,
                expansion_fn=expansion_fn,
                census_filter_fn=census_filter_fn,
                strict=True,
            )

        self.assertIsInstance(ctx.exception.__cause__, RuntimeError)
        census_filter_fn.assert_not_called()

    def test_strict_does_not_raise_for_unmatched_terms(self):
        result = process_category(
            ["normal"],
            category="disease",
            organism="homo_sapiens",
            census_version=None,
            is_label_based=True,
            expansion_fn=MagicMock(return_value=[]),
            strict=True,
        )

        self.assertEqual(result, ["normal"])


if __name__ == "__main__":
    unittest.main()
