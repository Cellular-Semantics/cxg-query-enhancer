# SubtypesQuery

A Python library that enhances biological queries by automatically expanding ontology terms (like cell types, tissues, etc.) to include all subtypes and part-of relationships. based on underlying ontologies. This ensures that queries for general terms (e.g., 'macrophage', 'kidney') also capture annotations to more specific entities (like 'alveolar macrophage', 'renal cortex').

SubtypesQuery leverages the [Ubergraph](https://github.com/INCATools/ubergraph) knowledge graph to automatically expand ontology terms in queries. It also filters terms against the [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/) to ensure that only relevant terms present in the data are included in the query.

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
4. **CellxGene Census Filtering**: Filters expanded Ubergraph terms against those present in the  [CellXGene Census](https://chanzuckerberg.github.io/cellxgene-census/). This is enabled by providing the `census_version` (e.g., `"latest"`) and `organism` parameters to the `obs_close` function when running your query. If `census_version` is not provided (or is `None`), only Ubergraph expansion is performed.

## Prerequisites

Ensure you have the following installed:

- [Poetry](https://python-poetry.org/docs/#installing-with-pipx) (for managing dependencies)
- Python >=3.10,<3.12

## Installation

This project uses Poetry for dependency management and packaging.

```bash
# Using pip
pip install SubtypesQuery

# Using Poetry
poetry add SubtypesQuery
```

## Usage Examples

### Example 1: Basic Query Expansion

```python
from SubtypesQuery import obs_close

# Original query filter
original_query = "cell_type in ['neuron']"

# Expand to include all subtypes of neurons
expanded_query = obs_close(
    original_query,
    categories=["cell_type"]
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
    original_filter,
    categories=["cell_type", "tissue", "disease", "development_stage"],                     
    organism="homo_sapiens",                              
    census_version="latest"   
    )

print(expanded_query)

# Output: cell_type in ['direct pathway medium spiny neuron', 'indirect pathway medium spiny neuron', 'medium spiny neuron'] and tissue in ['cortex of kidney', 'kidney', 'kidney blood vessel', 'renal medulla', 'renal papilla', 'renal pelvis'] and disease in ['type 1 diabetes mellitus', 'type 2 diabetes mellitus'] and development_stage in ['10-month-old stage']

```