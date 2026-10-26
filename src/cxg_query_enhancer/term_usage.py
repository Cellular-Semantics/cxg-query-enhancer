"""
Reports how a Cell Ontology (CL) term is used in CELLxGENE Census.

Unlike enhance(), which expands a term to its closure, this counts **direct annotations**:
cells whose cell_type_ontology_term_id is exactly the term. This supports curation decisions
such as obsoleting a term or changing its name/definition in a way that broadens or narrows
its meaning. Descendants can optionally be included, with counts still reported per term.
"""

import json
import logging
import numbers
from dataclasses import dataclass, field
from typing import List, Optional

import cellxgene_census
import pandas as pd

from .enhancer import (
    ONTOLOGY_ID_PATTERN,
    OntologyExtractor,
    SPARQLClient,
    _resolve_census_version,
)

logger = logging.getLogger(__name__)

TERM_COLUMN = "cell_type_ontology_term_id"
CONTEXT_COLUMNS = ["tissue", "tissue_type", "development_stage", "disease"]
OBS_COLUMNS = [TERM_COLUMN, "dataset_id", *CONTEXT_COLUMNS, "assay", "is_primary_data"]
COUNT_COLUMNS = ["total_cells", "primary_cells"]


@dataclass
class TermUsageReport:
    """
    Usage of a CL term in CELLxGENE Census.

    Counts: total_cells counts every cell; primary_cells counts only is_primary_data cells,
    i.e. unique cells, as the same cells may appear in several datasets.

    Attributes:
    - term_id, label, definition: the queried term (label/definition from Ubergraph).
    - census_version: the concrete Census release that was queried.
    - terms: the term IDs counted (the term, plus its descendants if requested).
    - cell_counts: organism x term counts.
    - context: organism x tissue x tissue_type x development_stage x disease counts, with n_datasets.
    - datasets: per dataset counts with dataset title, citation and collection (paper) details.
    - papers: per collection (publication) counts with name and DOI.
    """

    term_id: str
    label: Optional[str]
    definition: Optional[str]
    census_version: str
    terms: List[str]
    cell_counts: pd.DataFrame = field(repr=False)
    context: pd.DataFrame = field(repr=False)
    datasets: pd.DataFrame = field(repr=False)
    papers: pd.DataFrame = field(repr=False)

    def to_dict(self):
        """Returns the report as JSON-serializable dicts and lists, e.g. for downstream agents."""

        def records(df):
            return json.loads(df.to_json(orient="records"))

        return {
            "term_id": self.term_id,
            "label": self.label,
            "definition": self.definition,
            "census_version": self.census_version,
            "terms": list(self.terms),
            "cell_counts": records(self.cell_counts),
            "context": records(self.context),
            "datasets": records(self.datasets),
            "papers": records(self.papers),
        }

    def to_markdown(self, top_n=10):
        """Returns a short human-readable summary, listing the top_n context rows and papers."""
        lines = [
            f"## {self.term_id} {self.label or '(label not found)'}",
            "",
            f"_{self.definition}_" if self.definition else "_No definition found._",
            "",
            f"CELLxGENE Census release {self.census_version}.",
            "",
        ]
        if self.cell_counts.empty:
            lines.append("No cells in Census are annotated with this term.")
            return "\n".join(lines)

        total = self.cell_counts[COUNT_COLUMNS].sum()
        lines += [
            f"**{int(total.total_cells):,} cells** ({int(total.primary_cells):,} primary) "
            f"in {len(self.datasets)} datasets from {len(self.papers)} collections.",
            "",
            "### Cell counts",
            _markdown_table(self.cell_counts),
            "",
            f"### Top {top_n} contexts",
            _markdown_table(self.context.head(top_n)),
            "",
            f"### Top {top_n} papers",
            _markdown_table(
                self.papers.head(top_n)[
                    ["collection_name", "collection_doi", "n_datasets", *COUNT_COLUMNS]
                ].replace({"collection_doi": {"": "(no DOI)"}})
            ),
        ]
        return "\n".join(lines)


def term_usage(
    term,
    organisms=None,
    census_version="latest",
    include_descendants=False,
    census=None,
):
    """
    Reports the direct usage of a Cell Ontology term in CELLxGENE Census.

    Parameters:
    - term (str): A CL ID (e.g. "CL:0000127") or a label/exact synonym (e.g. "astrocyte").
    - organisms (list[str] or None): Census organisms to include (e.g. ["homo_sapiens"]).
      Default: all organisms in the Census.
    - census_version (str): Census version; aliases such as "latest" are resolved to a release.
    - include_descendants (bool): Also count the term's subclasses and parts, reported per term
      (each with its own direct annotations), e.g. to see where cells would go on obsoletion.
    - census: An already open Census handle (cellxgene_census.open_soma()) to reuse across calls.

    Returns:
    - TermUsageReport

    Raises:
    - ValueError: If term is not a CL ID and cannot be resolved as a CL label.

    Runtime: counts come from the Census summary table (~1s); context requires scanning the
    obs table of each organism that has annotated cells (~20s per organism for common terms).
    """
    extractor = OntologyExtractor(SPARQLClient())
    term_id = _resolve_cl_term(term, extractor)
    label, definition = _get_label_and_definition(term_id, extractor.sparql_client)

    terms = [term_id]
    if include_descendants:
        descendants = {
            item["ID"]
            for item in extractor._get_ontology_expansion(term_id, "cell_type")
        }
        terms += sorted(descendants - {term_id})

    if census is not None:
        return _build_report(census, term_id, label, definition, terms, organisms)

    resolved_version = _resolve_census_version(census_version)
    with cellxgene_census.open_soma(census_version=resolved_version) as census:
        return _build_report(census, term_id, label, definition, terms, organisms)


def _resolve_cl_term(term, extractor):
    """Returns the CL ID for a CL ID or label, raising ValueError if it cannot be resolved."""
    if ONTOLOGY_ID_PATTERN.fullmatch(term):
        if not term.startswith("CL:"):
            raise ValueError(f"'{term}' is not a Cell Ontology (CL) term.")
        return term
    term_id = extractor.get_ontology_id_from_label(term, "cell_type")
    if term_id is None:
        raise ValueError(f"Could not resolve '{term}' to a Cell Ontology (CL) term.")
    return term_id


def _get_label_and_definition(term_id, sparql_client):
    """Fetches the term's label and definition (IAO:0000115) from Ubergraph."""
    results = sparql_client.query(
        f"""
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?label ?definition
        WHERE {{
            OPTIONAL {{ obo:{term_id.replace(':', '_')} rdfs:label ?label }}
            OPTIONAL {{ obo:{term_id.replace(':', '_')} obo:IAO_0000115 ?definition }}
        }}
        LIMIT 1
        """
    )
    row = results[0] if results else {}
    label = row.get("label", {}).get("value")
    if label is None:
        logger.warning(f"No label found in Ubergraph for {term_id}.")
    return label, row.get("definition", {}).get("value")


def _build_report(census, term_id, label, definition, terms, organisms):
    """Builds the TermUsageReport from an open Census handle."""
    census_info = census["census_info"]
    organisms = organisms or list(census["census_data"].keys())

    # The release actually queried, also when the caller passed in their own handle
    build = census_info["summary"].read().concat().to_pandas()
    census_version = build.set_index("label")["value"].get(
        "census_build_date", "unknown"
    )

    # --- 1. Fast counts from the summary table, which also tells us where to look ---
    summary = census_info["summary_cell_counts"].read().concat().to_pandas()
    summary = summary[
        (summary["category"] == "cell_type")
        & summary["ontology_term_id"].isin(terms)
        & summary["organism"].isin(organisms)
        & (summary["total_cell_count"] > 0)
    ]
    cell_counts = (
        summary.rename(
            columns={
                "ontology_term_id": "term_id",
                "total_cell_count": "total_cells",
                "unique_cell_count": "primary_cells",
            }
        )[["organism", "term_id", "label", *COUNT_COLUMNS]]
        .sort_values(["organism", "total_cells"], ascending=[True, False])
        .reset_index(drop=True)
    )

    # --- 2. Context and datasets from obs, only for organisms that have cells ---
    # Aggregated per organism, so only the (small) counts are kept in memory.
    context_frames, dataset_frames = [], []
    for organism in sorted(cell_counts["organism"].unique()):
        logger.info(f"Reading {organism} cells annotated with {len(terms)} term(s)...")
        obs = (
            census["census_data"][organism]
            .obs.read(value_filter=_term_filter(terms), column_names=OBS_COLUMNS)
            .concat()
            .to_pandas()
        )
        for frames, by, n_datasets in [
            (context_frames, CONTEXT_COLUMNS, True),
            (dataset_frames, ["dataset_id"], False),
        ]:
            counts = _count_cells(obs, by, n_datasets=n_datasets)
            counts.insert(0, "organism", organism)
            frames.append(counts)

    if not context_frames:
        logger.info(f"No cells in Census {census_version} are annotated with {terms}.")
        empty = pd.DataFrame()
        return TermUsageReport(
            term_id,
            label,
            definition,
            census_version,
            terms,
            cell_counts,
            empty,
            empty,
            empty,
        )

    context = _sorted_by_cells(pd.concat(context_frames, ignore_index=True))
    datasets = _sorted_by_cells(pd.concat(dataset_frames, ignore_index=True))

    # --- 3. Papers: join datasets with their collections ---
    dataset_info = (
        census_info["datasets"]
        .read()
        .concat()
        .to_pandas()[
            [
                "dataset_id",
                "dataset_title",
                "citation",
                "collection_id",
                "collection_name",
                "collection_doi",
            ]
        ]
    )
    datasets = datasets.merge(dataset_info, on="dataset_id", how="left")
    papers = (
        datasets.groupby(
            ["collection_id", "collection_name", "collection_doi"], dropna=False
        )
        .agg(
            n_datasets=("dataset_id", "nunique"),
            total_cells=("total_cells", "sum"),
            primary_cells=("primary_cells", "sum"),
        )
        .reset_index()
        .sort_values("total_cells", ascending=False, ignore_index=True)
    )

    return TermUsageReport(
        term_id,
        label,
        definition,
        census_version,
        terms,
        cell_counts,
        context,
        datasets,
        papers,
    )


def _term_filter(terms):
    """Builds the SOMA value_filter selecting cells annotated with any of the terms."""
    if len(terms) == 1:
        return f"{TERM_COLUMN} == '{terms[0]}'"
    return f"{TERM_COLUMN} in {list(terms)}"


def _count_cells(obs, by, n_datasets=False):
    """Counts total and primary cells (and optionally datasets) per group."""
    aggregations = {
        "total_cells": ("is_primary_data", "size"),
        "primary_cells": ("is_primary_data", "sum"),
    }
    if n_datasets:
        aggregations["n_datasets"] = ("dataset_id", "nunique")
    counts = obs.groupby(by, observed=True).agg(**aggregations).reset_index()
    counts["primary_cells"] = counts["primary_cells"].astype(int)
    # Categorical columns from different organisms would not concatenate cleanly
    for column in by:
        counts[column] = counts[column].astype(str)
    return counts


def _sorted_by_cells(df):
    """Sorts counts largest first."""
    return df.sort_values("total_cells", ascending=False, ignore_index=True)


def _markdown_table(df):
    """Formats a DataFrame as a Markdown table (without requiring tabulate)."""

    def cell(value):
        if isinstance(value, numbers.Integral) and not isinstance(value, bool):
            return f"{int(value):,}"
        return str(value).replace("|", "\\|")

    header = "| " + " | ".join(df.columns) + " |"
    separator = "|" + "|".join("---" for _ in df.columns) + "|"
    rows = [
        "| " + " | ".join(cell(v) for v in row) + " |"
        for row in df.itertuples(index=False)
    ]
    return "\n".join([header, separator, *rows])
