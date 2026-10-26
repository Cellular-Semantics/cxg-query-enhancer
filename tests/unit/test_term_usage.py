"""
Offline tests for term_usage(), using a fake Census object (no network access).
"""

import json
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa

from cxg_query_enhancer import TermUsageReport, term_usage


def reader(table):
    """A fake SOMA DataFrame whose read() returns the given pyarrow table."""
    df = MagicMock()
    df.read.return_value.concat.return_value = pa.table(table)
    return df


def fake_census(obs_by_organism, summary_rows):
    """Builds a fake Census with the given obs tables and summary_cell_counts rows."""
    summary_cell_counts = {
        key: [row[i] for row in summary_rows]
        for i, key in enumerate(
            [
                "organism",
                "category",
                "label",
                "ontology_term_id",
                "total_cell_count",
                "unique_cell_count",
            ]
        )
    }
    return {
        "census_info": {
            "summary": reader(
                {"label": ["census_build_date"], "value": ["2025-11-17"]}
            ),
            "summary_cell_counts": reader(summary_cell_counts),
            "datasets": reader(
                {
                    "dataset_id": ["d1", "d2", "d3"],
                    "dataset_title": ["Dataset 1", "Dataset 2", "Dataset 3"],
                    "citation": ["cite 1", "cite 2", "cite 3"],
                    "collection_id": ["c1", "c1", "c2"],
                    "collection_name": ["Paper A", "Paper A", "Paper B"],
                    "collection_doi": ["10.1/a", "10.1/a", ""],
                }
            ),
        },
        "census_data": {
            organism: MagicMock(obs=reader(obs))
            for organism, obs in obs_by_organism.items()
        },
    }


def obs_table(rows):
    """rows: (term, dataset_id, tissue, development_stage, is_primary_data)"""
    return {
        "cell_type_ontology_term_id": [r[0] for r in rows],
        "dataset_id": [r[1] for r in rows],
        "tissue": [r[2] for r in rows],
        "tissue_type": ["tissue"] * len(rows),
        "development_stage": [r[3] for r in rows],
        "disease": ["normal"] * len(rows),
        "assay": ["10x 3' v3"] * len(rows),
        "is_primary_data": [r[4] for r in rows],
    }


HUMAN_OBS = obs_table(
    [
        ("CL:0000127", "d1", "cerebellum", "adult stage", True),
        ("CL:0000127", "d1", "cerebellum", "adult stage", True),
        ("CL:0000127", "d2", "cerebellum", "adult stage", False),
        ("CL:0000127", "d3", "cortex", "fetal stage", True),
    ]
)
SUMMARY = [
    ("homo_sapiens", "cell_type", "astrocyte", "CL:0000127", 4, 3),
    ("mus_musculus", "cell_type", "astrocyte", "CL:0000127", 0, 0),
    ("homo_sapiens", "tissue", "cortex", "UBERON:0001851", 99, 99),
]


@patch("cxg_query_enhancer.term_usage.SPARQLClient")
class TestTermUsage(unittest.TestCase):
    def mock_term_metadata(
        self, mock_client, label="astrocyte", definition="A glial cell."
    ):
        mock_client.return_value.query.return_value = [
            {"label": {"value": label}, "definition": {"value": definition}}
        ]

    def test_report_counts_context_datasets_and_papers(self, mock_client):
        self.mock_term_metadata(mock_client)
        census = fake_census(
            {"homo_sapiens": HUMAN_OBS, "mus_musculus": obs_table([])}, SUMMARY
        )

        report = term_usage("CL:0000127", census=census)

        self.assertIsInstance(report, TermUsageReport)
        self.assertEqual(
            (report.term_id, report.label, report.definition, report.census_version),
            ("CL:0000127", "astrocyte", "A glial cell.", "2025-11-17"),
        )
        # Only the cell_type row for organisms with cells is counted
        self.assertEqual(
            report.cell_counts.to_dict("records"),
            [
                {
                    "organism": "homo_sapiens",
                    "term_id": "CL:0000127",
                    "label": "astrocyte",
                    "total_cells": 4,
                    "primary_cells": 3,
                }
            ],
        )
        self.assertEqual(
            report.context[
                [
                    "tissue",
                    "development_stage",
                    "total_cells",
                    "primary_cells",
                    "n_datasets",
                ]
            ].to_dict("records"),
            [
                {
                    "tissue": "cerebellum",
                    "development_stage": "adult stage",
                    "total_cells": 3,
                    "primary_cells": 2,
                    "n_datasets": 2,
                },
                {
                    "tissue": "cortex",
                    "development_stage": "fetal stage",
                    "total_cells": 1,
                    "primary_cells": 1,
                    "n_datasets": 1,
                },
            ],
        )
        self.assertEqual(
            report.datasets.set_index("dataset_id")["collection_name"].to_dict(),
            {"d1": "Paper A", "d2": "Paper A", "d3": "Paper B"},
        )
        # Datasets of the same collection are combined into one paper
        self.assertEqual(
            report.papers[
                ["collection_name", "collection_doi", "n_datasets", "total_cells"]
            ].to_dict("records"),
            [
                {
                    "collection_name": "Paper A",
                    "collection_doi": "10.1/a",
                    "n_datasets": 2,
                    "total_cells": 3,
                },
                {
                    "collection_name": "Paper B",
                    "collection_doi": "",
                    "n_datasets": 1,
                    "total_cells": 1,
                },
            ],
        )

    def test_only_organisms_with_cells_are_scanned(self, mock_client):
        self.mock_term_metadata(mock_client)
        census = fake_census(
            {"homo_sapiens": HUMAN_OBS, "mus_musculus": obs_table([])}, SUMMARY
        )

        term_usage("CL:0000127", census=census)

        human_obs = census["census_data"]["homo_sapiens"].obs
        human_obs.read.assert_called_once()
        self.assertEqual(
            human_obs.read.call_args.kwargs["value_filter"],
            "cell_type_ontology_term_id == 'CL:0000127'",
        )
        census["census_data"]["mus_musculus"].obs.read.assert_not_called()

    def test_organisms_argument_limits_the_report(self, mock_client):
        self.mock_term_metadata(mock_client)
        census = fake_census({"homo_sapiens": HUMAN_OBS}, SUMMARY)

        report = term_usage("CL:0000127", organisms=["mus_musculus"], census=census)

        self.assertTrue(report.cell_counts.empty)
        census["census_data"]["homo_sapiens"].obs.read.assert_not_called()

    @patch(
        "cxg_query_enhancer.term_usage.OntologyExtractor.get_ontology_id_from_label",
        return_value="CL:0000127",
    )
    def test_label_input_is_resolved(self, mock_lookup, mock_client):
        self.mock_term_metadata(mock_client)
        census = fake_census({"homo_sapiens": HUMAN_OBS}, SUMMARY)

        report = term_usage("Astrocyte", census=census)

        mock_lookup.assert_called_once_with("Astrocyte", "cell_type")
        self.assertEqual(report.term_id, "CL:0000127")

    @patch(
        "cxg_query_enhancer.term_usage.OntologyExtractor.get_ontology_id_from_label",
        return_value=None,
    )
    def test_invalid_terms_raise(self, _mock_lookup, mock_client):
        census = fake_census({}, SUMMARY)
        for term in ["no such cell", "UBERON:0002048", "CL:abc"]:
            with self.subTest(term=term):
                with self.assertRaises(ValueError):
                    term_usage(term, census=census)

    @patch("cxg_query_enhancer.term_usage.OntologyExtractor._get_ontology_expansion")
    def test_include_descendants_counts_each_term(self, mock_expansion, mock_client):
        self.mock_term_metadata(mock_client)
        mock_expansion.return_value = [
            {"ID": "CL:0000127", "Label": "astrocyte"},
            {"ID": "CL:0002605", "Label": "astrocyte of the cerebral cortex"},
        ]
        summary = SUMMARY + [
            (
                "homo_sapiens",
                "cell_type",
                "astrocyte of the cerebral cortex",
                "CL:0002605",
                2,
                2,
            )
        ]
        census = fake_census({"homo_sapiens": HUMAN_OBS}, summary)

        report = term_usage("CL:0000127", include_descendants=True, census=census)

        self.assertEqual(report.terms, ["CL:0000127", "CL:0002605"])
        self.assertEqual(
            report.cell_counts.set_index("term_id")["total_cells"].to_dict(),
            {"CL:0000127": 4, "CL:0002605": 2},
        )
        self.assertEqual(
            census["census_data"]["homo_sapiens"].obs.read.call_args.kwargs[
                "value_filter"
            ],
            "cell_type_ontology_term_id in ['CL:0000127', 'CL:0002605']",
        )

    def test_term_without_cells_gives_empty_report(self, mock_client):
        self.mock_term_metadata(mock_client, label="rare cell")
        census = fake_census({"homo_sapiens": HUMAN_OBS}, SUMMARY)

        report = term_usage("CL:9999999", census=census)

        self.assertTrue(report.cell_counts.empty)
        self.assertTrue(report.papers.empty)
        self.assertIn("No cells in Census", report.to_markdown())
        json.dumps(report.to_dict())

    def test_to_dict_and_to_markdown(self, mock_client):
        self.mock_term_metadata(mock_client)
        census = fake_census({"homo_sapiens": HUMAN_OBS}, SUMMARY)

        report = term_usage("CL:0000127", census=census)

        data = json.loads(json.dumps(report.to_dict()))
        self.assertEqual(data["term_id"], "CL:0000127")
        self.assertEqual(data["papers"][0]["collection_doi"], "10.1/a")
        self.assertEqual(data["cell_counts"][0]["primary_cells"], 3)

        markdown = report.to_markdown()
        self.assertIn("## CL:0000127 astrocyte", markdown)
        self.assertIn(
            "**4 cells** (3 primary) in 3 datasets from 2 collections", markdown
        )
        self.assertIn("| Paper A | 10.1/a | 2 | 3 |", markdown)
        self.assertIn("| Paper B | (no DOI) | 1 | 1 |", markdown)


if __name__ == "__main__":
    unittest.main()
