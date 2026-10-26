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
        census_filter_fn = MagicMock(return_value=["CL:0002"])

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
        census_filter_fn = MagicMock(return_value=["CL:0001"])

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

        census_filter_fn = MagicMock(return_value=["CL:0001"])

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

    # --- Special schema values (e.g. "normal" in disease) are never expanded ---
    def run_category(self, terms, category, is_label_based=True, **kwargs):
        return process_category(
            terms,
            category=category,
            organism="homo_sapiens",
            census_version=kwargs.pop("census_version", None),
            is_label_based=is_label_based,
            **kwargs,
        )

    def test_normal_disease_is_passed_through_without_lookup_or_warning(self):
        expansion_fn = MagicMock()

        with self.assertNoLogs(level="WARNING"):
            result = self.run_category(["normal"], "disease", expansion_fn=expansion_fn)

        self.assertEqual(result, ["normal"])
        expansion_fn.assert_not_called()

    def test_normal_label_is_canonicalized(self):
        for term in ["Normal", "NORMAL", "normal"]:
            with self.subTest(term=term):
                result = self.run_category([term], "disease", expansion_fn=MagicMock())
                self.assertEqual(result, ["normal"])

    def test_normal_disease_id_is_passed_through(self):
        expansion_fn = MagicMock()

        result = self.run_category(
            ["PATO:0000461"], "disease", is_label_based=False, expansion_fn=expansion_fn
        )

        self.assertEqual(result, ["PATO:0000461"])
        expansion_fn.assert_not_called()

    def test_normal_survives_census_filtering_alongside_expanded_terms(self):
        expansion_fn = MagicMock(
            return_value=[{"ID": "MONDO:0005148", "Label": "type 2 diabetes mellitus"}]
        )
        census_filter_fn = MagicMock(return_value=["MONDO:0005148"])

        result = self.run_category(
            ["normal", "type 2 diabetes mellitus"],
            "disease",
            census_version="latest",
            expansion_fn=expansion_fn,
            census_filter_fn=census_filter_fn,
        )

        self.assertEqual(result, ["normal", "type 2 diabetes mellitus"])
        expansion_fn.assert_called_once_with(
            "type 2 diabetes mellitus", "disease", "homo_sapiens"
        )
        census_filter_fn.assert_called_once()
        self.assertNotIn("normal", census_filter_fn.call_args.kwargs["ids_to_filter"])

    def test_normal_kept_when_other_term_does_not_expand(self):
        result = self.run_category(
            ["normal", "no such disease"],
            "disease",
            expansion_fn=MagicMock(return_value=[]),
        )

        self.assertEqual(result, ["no such disease", "normal"])

    def test_unknown_is_passed_through_for_other_categories(self):
        for category in ["cell_type", "development_stage", "tissue", "tissue_general"]:
            for is_label_based in [True, False]:
                with self.subTest(category=category, is_label_based=is_label_based):
                    expansion_fn = MagicMock()
                    result = self.run_category(
                        ["unknown"],
                        category,
                        is_label_based=is_label_based,
                        expansion_fn=expansion_fn,
                    )
                    self.assertEqual(result, ["unknown"])
                    expansion_fn.assert_not_called()

    def test_special_values_are_per_category(self):
        expansion_fn = MagicMock(return_value=[{"ID": "CL:1", "Label": "normal"}])

        self.run_category(["normal"], "cell_type", expansion_fn=expansion_fn)
        self.run_category(["unknown"], "disease", expansion_fn=expansion_fn)

        self.assertEqual(expansion_fn.call_count, 2)


if __name__ == "__main__":
    unittest.main()
