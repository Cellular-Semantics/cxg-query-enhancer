# SubtypesQuery

A Python library that enhances biological queries by automatically expanding ontology terms (like cell types, tissues, etc.) to include all subtypes and part-of relationships based on underlying ontologies. This ensures that queries for general terms (e.g., 'macrophage', 'kidney') also capture annotations to more specific entities (like 'alveolar macrophage', 'renal cortex'), making your [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/) queries more comprehensive.

## The Challenge: Incomplete Queries in Biological Data

When exploring biological datasets, querying for a general term like "macrophage" often misses data annotated with more specific subtypes (e.g., "alveolar macrophage," "Kupffer cell"). Similarly, a search for "kidney" might not automatically include its constituent parts like "renal cortex" or "nephron." Manually identifying and including all these related terms for every query is time-consuming and prone to omissions, leading to incomplete results.

**How SubtypesQuery Solves This:**

SubtypesQuery tackles this by automatically enhancing your queries in the [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/). It uses the [Ubergraph](https://github.com/INCATools/ubergraph) knowledge graph to find all relevant subtypes and parts for your search terms and automatically expand your query.  It can also optionally filter these expanded terms against the [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/), ensuring results are both comprehensive and relevant to your dataset.

## Key Features

`SubtypesQuery` provides:

1. **Automated query expansion**: Rewrites query filters to include all subtypes and parts of specified terms.
2. **Multiple ontology support**: 
   - Cell Ontology (CL) for cell types
   - Uberon for anatomical structures
   - MONDO for diseases
   - Developmental stage ontologies 
        - Human Developmental Stages (HsapDv)
        - Mouse Developmental Stages (MmusDv)
3. **Flexible Term Input**: Accepts input terms as:
  - Labels (e.g., 'neuron', 'kidney')
  - Ontology IDs (e.g., 'CL:0000540', 'UBERON:0002113')
  - Ontology synonyms
4. **CellxGene Census Filtering**: Filters expanded Ubergraph terms against those present in the [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/) by providing the `census_version` (e.g., `"latest"`) and `organism` parameters to the `obs_close` function when running your query.

## Prerequisites

Ensure you have the following installed:

- [Poetry](https://python-poetry.org/docs/#installing-with-pipx) (for managing dependencies)
- Python >=3.10,<3.12

## Installation

You can install SubtypesQuery pypy package using `pip`

```bash
# Using pip
pip install SubtypesQuery

```

## Usage Examples

### Example 1: Basic Query Expansion (Ubergraph Only and No Census Filtering)

```python
from SubtypesQuery import obs_close

# Original query filter
original_query = "cell_type in ['neuron']"

# Expand to include all subtypes of neurons
expanded_query = obs_close(
    original_query,
)

print(expanded_query)

# Output: cell_type in ['neuron', 'Purkinje neuron', 'motor neuron', ...]
```

### Example 2: Multiple Categories with Census Filtering

```python
from SubtypesQuery import obs_close

# Expand cell types, tissues, and diseases, but filter against terms in the Census

original_query = cell_type in ['medium spiny neuron'] and tissue in ['kidney'] and disease in ['diabetes mellitus'] and development_stage in ['10-month-old stage']

expanded_query = obs_close(
    original_query,   
    organism="homo_sapiens",                              
    census_version="latest"   
    )

print(expanded_query)

# Output: cell_type in ['direct pathway medium spiny neuron', 'indirect pathway medium spiny neuron', 'medium spiny neuron'] and tissue in ['cortex of kidney', 'kidney', 'kidney blood vessel', 'renal medulla', 'renal papilla', 'renal pelvis'] and disease in ['type 1 diabetes mellitus', 'type 2 diabetes mellitus'] and development_stage in ['10-month-old stage']

```

## Function Reference
### Main Function

### `obs_close(query_filter, categories=None, organism=None, census_version=None)`

Rewrites a query filter to include the subtypes and part-of relationships of specified terms.

#### Parameters:

- **query_filter** (str): The original query filter string.
- **categories** (`list` or `None`): Categories to expand.
  - **If `None` (default):** Categories are auto-detected by scanning the query for patterns like `cell_type in [...]` or `tissue_ontology_term_id in [...]`.
  - **If a list (e.g., `["cell_type", "disease"]`):** Only the specified categories are expanded.
  - Supported categories: `"cell_type"`, `"tissue"`, `"disease"`, `"development_stage"`. 
- **organism** (str): The organism to query in the census (e.g., `"homo_sapiens"`, `"mus_musculus"`).
  - Required when `census_version` is provided or when using `"development_stage"`.
- **census_version** (str): Version of the CellXGene Census to use for filtering terms    against the census.
  - Use `"latest"` for the most recent version or specify a date like `"2024-12-01"`.
  - If `None` (default), only ontology expansion is performed (no filtering against Census data).

#### Returns:

- **str**: The rewritten query filter with expanded terms.

### Additional Classes

These classes are used internally and don't need to be called directly:

#### `SPARQLClient`

A client for interacting with Ubergraph endpoint using SPARQL queries.

#### `OntologyExtractor`

Extracts subclasses and part-of relationships from Ubergraph for ontology terms.

## How It Works

1. **Parse Query:** The library identify terms in your query that need expansion
2. **Resolve Terms:** For each term, it:
   - Resolves labels to ontology IDs (if necessary)
   - Queries Ubergraph to find all subclasses and part-of relationships
   - Optionally filters the expanded terms against the CellXGene Census
3. **Rewrite Query:** The expanded terms are rewritten into the original query format

## Acknowledgments

- [Ubergraph](https://github.com/INCATools/ubergraph) for providing the ontology knowledge graph
- [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/) for single-cell reference data