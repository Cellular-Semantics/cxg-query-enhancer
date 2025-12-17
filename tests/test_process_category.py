import unittest
from unittest.mock import MagicMock

from cxg_query_enhancer.enhancer import process_category


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


if __name__ == "__main__":
    unittest.main()
