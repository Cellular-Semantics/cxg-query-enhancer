# cxg-query-enhancer [![PyPI Downloads](https://static.pepy.tech/badge/cxg-query-enhancer)](https://pepy.tech/projects/cxg-query-enhancer)

The [cellxgene-census](https://pypi.org/project/cellxgene-census/) library supports access to abitrary slices of the CELLxGENE corpus via filters that include cell type, tissue, developmental stage and disease. 

If you use query cellxgene_census for "T cells in lung" you get 71,000 cells. This might look like a reasonable result, but it misses 630,000 cells annotated with terms for types of T-cell or parts of lung.  When you filter for "macrophage," you don't automatically get "alveolar macrophage" or "Kupffer cell." Filter for "kidney" and you miss "renal cortex" and "nephron." The data is there, annotated with precise ontology terms, but simple queries can't reach it.

**cxg-query-enhancer** fixes this. Wrap your query in `enhance()` and the library automatically expands your query to include all subtypes and parts, using the [Ubergraph](https://github.com/INCATools/ubergraph) knowledge graph built from biomedical ontologies.

## Quick Example

```python
from cxg_query_enhancer import enhance

# Your normal query—now enhanced
obs_value_filter = enhance(
    "cell_type in ['T cell'] and tissue in ['lung']",
    organism="homo_sapiens"
)
# Expands filter to include 76 T-cell type terms and 15 lung part terms used in annotation in the CxG corpus
# If used in a cellxgene_census query, returns ~700,000 cells instead of ~71,000
```

The `enhance()` function expands "T cell" to include all its subtypes (CD4+, CD8+, regulatory T cells, etc.) and "lung" to include its anatomical parts—then filters against terms actually present in CELLxGENE Census.

## Complete Working Example

This example runs in under a minute and demonstrates the core value—subtypes you'd otherwise miss:

```python
import cellxgene_census
from cxg_query_enhancer import enhance

with cellxgene_census.open_soma(census_version="latest") as census:
    adata = cellxgene_census.get_anndata(
        census=census,
        organism="Homo sapiens",
        var_value_filter="feature_id in ['ENSG00000161798', 'ENSG00000188229']",
        obs_value_filter=enhance(
            "sex == 'female' and cell_type in ['medium spiny neuron']",
            organism="Homo sapiens",
        ),
        obs_column_names=[
            "assay",
            "cell_type",
            "tissue",
            "tissue_general",
            "suspension_type",
            "disease",
        ],
    )

print(adata.obs)
```

**Output:** ~5,400 cells across three cell types—the parent term plus both subtypes:

| assay | cell_type | tissue | disease |
|-------|-----------|--------|---------|
| 10x 3' v3 | indirect pathway medium spiny neuron | caudate nucleus | normal |
| 10x 3' v3 | direct pathway medium spiny neuron | caudate nucleus | normal |
| 10x 3' v3 | medium spiny neuron | cerebral cortex | normal |

Without `enhance()`, a query for just "medium spiny neuron" misses the pathway-specific subtypes entirely.

## What It Expands

| Category | Example | Expands To Include |
|----------|---------|-------------------|
| Cell types | `macrophage` | alveolar macrophage, Kupffer cell, microglial cell... |
| Tissues | `kidney` | renal cortex, nephron, kidney blood vessel... |
| Diseases | `diabetes mellitus` | type 1 diabetes, type 2 diabetes... |
| Dev stages | `adult` | 25-year-old, 40-year-old... |

Supported ontologies:
- [Cell Ontology (CL)](https://github.com/obophenotype/cell-ontology) for cell types
- [Uberon](https://github.com/obophenotype/uberon) for anatomy
- [MONDO](https://github.com/monarch-initiative/mondo) for diseases
- [HsapDv](https://github.com/obophenotype/developmental-stage-ontologies) / [MmusDv](https://github.com/obophenotype/developmental-stage-ontologies) for developmental stages

## Installation

```bash
pip install cxg-query-enhancer
```

Requires Python 3.10 or 3.11.

## Usage

### Basic: Wrap Your Existing Query

```python
import cellxgene_census
from cxg_query_enhancer import enhance

with cellxgene_census.open_soma(census_version="latest") as census:
    adata = cellxgene_census.get_anndata(
        census=census,
        organism="Homo sapiens",
        obs_value_filter=enhance(
            "sex == 'female' and cell_type in ['medium spiny neuron']",
            organism="Homo sapiens",
        ),
    )
```

### Flexible Input

The library accepts terms as:
- Labels: `'neuron'`, `'kidney'`
- Ontology IDs: `'CL:0000540'`, `'UBERON:0002113'`
- Synonyms

### Control Census Filtering

By default, expanded terms are filtered against the latest CELLxGENE Census (only terms actually in the data are included).

```python
# Use a specific Census version for reproducibility
enhance(query, organism="homo_sapiens", census_version="2024-12-01")

# Disable Census filtering (pure ontology expansion)
enhance(query, census_version=None)
```

### Multiple Categories

```python
query = """
    cell_type in ['medium spiny neuron']
    and tissue in ['kidney']
    and disease in ['diabetes mellitus']
"""

enhanced = enhance(query, organism="homo_sapiens")

# Expands all three categories simultaneously
```

### A Note on Organism and Development Stage

The `organism` parameter is critical when querying developmental stages for non-human data.

**Why:** Human and mouse use different stage ontologies (HsapDv vs MmusDv). A query for "adult" in human expands to "25-year-old human stage," "40-year-old human stage," etc. The same query in mouse expands to "8-week-old stage," "6-month-old stage," and so on.

**The default:** If you don't specify `organism`, the library assumes `homo_sapiens` and logs a warning when expanding developmental stages. This prevents silent mismatches—but if you're querying mouse data, you'll get the wrong stages unless you specify:

```python
# Critical for non-human developmental stage queries
enhance(
    "development_stage in ['adult'] and cell_type in ['neuron']",
    organism="mus_musculus"  # Without this, you get human stages
)
```

**For cell types and tissues**, the organism parameter is used for Census filtering (ensuring expanded terms exist in your target species), but the ontology expansion itself is species-agnostic.

## Function Reference

### `enhance(query_filter, categories=None, organism=None, census_version="latest")`

| Parameter | Type | Description |
|-----------|------|-------------|
| `query_filter` | str | Your original query string |
| `categories` | list or None | Categories to expand. Default: auto-detect from query. Options: `"cell_type"`, `"tissue"`, `"tissue_general"`, `"disease"`, `"development_stage"` |
| `organism` | str | `"homo_sapiens"` or `"mus_musculus"`. Required for Census filtering. |
| `census_version` | str or None | Census version for filtering. Default: `"latest"`. Set to `None` to disable. |

**Returns:** Enhanced query string with expanded terms.

## How It Works

1. **Parse**: Identifies terms in your query that can be expanded
2. **Expand**: Queries Ubergraph for all subclasses and part-of relationships
3. **Filter**: Keeps only terms present in CELLxGENE Census (unless disabled)
4. **Rewrite**: Returns your query with expanded term lists

## Acknowledgments

- [Ubergraph](https://github.com/INCATools/ubergraph) for the ontology knowledge graph
- [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/) for single-cell reference data
- Built by the [Cellular Semantics](https://github.com/Cellular-Semantics) team at the Wellcome Sanger Institute
