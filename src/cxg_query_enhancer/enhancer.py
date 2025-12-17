from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging

logger = logging.getLogger(__name__)
import cellxgene_census
from collections import defaultdict
from functools import lru_cache
import pickle
import concurrent.futures
import pyarrow.compute as pc
import ast
import threading
from typing import Callable, Dict, List, Optional, Sequence


# ==========================================
# NEW AST HELPER CLASSES
# ==========================================
class QueryTermExtractor(ast.NodeVisitor):
    """
    Walks the AST to find terms and IDs associated with specific categories.
    Replaces the old Regex extraction logic.
    """

    def __init__(self, target_categories):
        self.target_categories = target_categories
        self.terms = defaultdict(list)  # {category: [terms]}
        self.ids = defaultdict(list)  # {category: [ids]}

    def visit_Compare(self, node):
        # We look for: column == value OR column in [values]
        left = node.left

        # Ensure the left side is a variable name (e.g., 'cell_type')
        if not isinstance(left, ast.Name):
            return self.generic_visit(node)

        col_name = left.id

        # Check 1: Is it a Label column? (e.g., "cell_type")
        if col_name in self.target_categories:
            self._extract_values(node, self.terms, col_name)

        # Check 2: Is it an ID column? (e.g., "cell_type_ontology_term_id")
        elif col_name.endswith("_ontology_term_id"):
            base_cat = col_name.replace("_ontology_term_id", "")
            if base_cat in self.target_categories:
                self._extract_values(node, self.ids, base_cat)

        self.generic_visit(node)

    def _extract_values(self, node, storage_dict, category):
        # We only handle simple comparisons
        if not node.comparators:
            return

        op = node.ops[0]
        val_node = node.comparators[0]

        extracted = []

        # Handle '==' (Eq)
        if isinstance(op, ast.Eq):
            if isinstance(val_node, ast.Constant):
                extracted.append(val_node.value)

        # Handle 'in' (In)
        elif isinstance(op, ast.In):
            if isinstance(val_node, ast.List):
                for elt in val_node.elts:
                    if isinstance(elt, ast.Constant):
                        extracted.append(elt.value)

        # Store results
        if extracted:
            storage_dict[category].extend(extracted)


class QueryRewriter(ast.NodeTransformer):
    """
    Rewrites the AST, replacing original terms with expanded lists.
    Replaces the old Regex substitution logic.
    """

    def __init__(self, expanded_labels, expanded_ids):
        self.expanded_labels = expanded_labels
        self.expanded_ids = expanded_ids

    def visit_Compare(self, node):
        left = node.left
        if not isinstance(left, ast.Name):
            return node

        col_name = left.id
        new_values = None

        # Check if we have expanded labels for this column
        if col_name in self.expanded_labels:
            new_values = self.expanded_labels[col_name]

        # Check if we have expanded IDs for this column
        elif col_name.endswith("_ontology_term_id"):
            base_cat = col_name.replace("_ontology_term_id", "")
            if base_cat in self.expanded_ids:
                new_values = self.expanded_ids[base_cat]

        # If we have a replacement list, rewrite the node
        if new_values:
            # Sort values for deterministic output (helps testing)
            sorted_values = sorted(list(set(new_values)))

            # Create a new list node: ['A', 'B', 'C']
            list_node = ast.List(
                elts=[ast.Constant(value=v) for v in sorted_values], ctx=ast.Load()
            )

            # Return new node: col_name in ['A', 'B', 'C']
            # We enforce the 'In' operator regardless of whether it was '==' originally
            return ast.Compare(left=left, ops=[ast.In()], comparators=[list_node])

        return node


ExpansionResult = List[Dict[str, str]]
ExpansionFn = Callable[[str, str, Optional[str]], ExpansionResult]
CensusFilterFn = Callable[
    [List[str], str, Optional[str], Optional[str]], ExpansionResult
]

_thread_local_resources = threading.local()


@lru_cache(maxsize=None)
def _get_census_terms(census_version, organism, ontology_column_name):
    """
    Fetches and caches the unique ontology terms present in a specific CellXGene Census version for a given organism and column.
    A local file-based cache is used to speed up repeated queries.

    Parameters:
    - census_version (str): The version of the CellXGene Census to use.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - ontology_column_name (str): The column name containing ontology IDs (e.g., "cell_type_ontology_term_id").

    Returns:
    - set[str]: A set of unique ontology terms, or None if an error occurs.
    """
    # --- Local Cache Setup ---
    cache_dir = ".cache"
    os.makedirs(cache_dir, exist_ok=True)
    # Sanitize filename to be safe for all OS
    safe_organism = re.sub(r"[\W_]+", "", organism)
    cache_filename = f"{census_version}_{safe_organism}_{ontology_column_name}.pkl"
    cache_path = os.path.join(cache_dir, cache_filename)

    # --- 1. Try to load from local cache ---
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                logger.info(f"Loading cached census terms from {cache_path}")
                return pickle.load(f)
        except (pickle.UnpicklingError, EOFError) as e:
            logger.warning(
                f"Cache file {cache_path} is corrupted. Refetching. Error: {e}"
            )

    # --- 2. If not cached, fetch from CellXGene Census ---
    logger.info(
        f"Fetching census terms for '{ontology_column_name}' from CellXGene Census..."
    )
    # Normalize organism name to lowercase with underscores
    census_organism = organism.replace(" ", "_").lower()
    try:
        with cellxgene_census.open_soma(census_version=census_version) as census:
            organism_data = census["census_data"].get(census_organism)
            if not organism_data:
                logger.warning(f"Organism '{census_organism}' not found in census.")
                return None

            obs_reader = organism_data.obs
            if ontology_column_name not in obs_reader.keys():
                logger.warning(f"Column '{ontology_column_name}' not found in census.")
                return None

            logger.info(f"Streaming unique terms for {ontology_column_name}...")
            terms = set()

            # Iterate over the data in chunks (SOMA slices)
            query = obs_reader.read(column_names=[ontology_column_name])
            for chunk in query:
                # Convert the chunk to a PyArrow Table (lightweight)
                tbl = chunk

                # --- THE CHANGE ---
                # 1. Find unique values in C++ (Fast, Low RAM)
                unique_vals = pc.unique(tbl.column(ontology_column_name))

                # 2. Only convert the small list of unique values to Python
                terms.update(unique_vals.to_pylist())

            # clean up
            terms.discard("unknown")
            terms.discard(None)  # Handle potential nulls

            # --- 3. Save to local cache for future use ---
            with open(cache_path, "wb") as f:
                pickle.dump(terms, f)
            logger.info(f"Saved census terms to cache: {cache_path}")

            return terms

    except Exception as e:
        logger.error(f"Error accessing CellXGene Census: {e}")
        return None


def _filter_ids_against_census(
    ids_to_filter, organism, census_version="latest", ontology_column_name=None
):
    """
    Filters a list of ontology IDs against those present in a specific CellXGene Census version.

    Parameters:
    - ids_to_filter (list[str]): List of ontology IDs to filter.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - census_version (str): The version of the CellXGene Census to use.
    - ontology_column_name (str): The column name containing ontology IDs.

    Returns:
    - list[dict]: A list of dictionaries, where each dictionary contains the 'ID' and 'Label' of a term present in the census.
    """
    if not ids_to_filter:
        logger.info("No IDs provided to filter; returning empty list.")
        return []

    census_terms = _get_census_terms(census_version, organism, ontology_column_name)

    if census_terms is None:
        logger.warning(
            "Census terms could not be retrieved. Returning original IDs unfiltered."
        )
        # To maintain a consistent return type, we'll format the original IDs as a list of dicts
        return [{"ID": id_, "Label": "Unknown Label"} for id_ in ids_to_filter]

    # In the future, we might want to fetch labels from the census as well.
    # For now, we'll just return the ID and a placeholder for the label.
    filtered_results = [
        {"ID": id_, "Label": f"Label for {id_}"}
        for id_ in ids_to_filter
        if id_ in census_terms
    ]

    logger.info(
        f"{len(filtered_results)} of {len(set(ids_to_filter))} IDs matched in census."
    )
    return filtered_results


def process_category(
    terms: Sequence[str],
    *,
    category: str,
    organism: str,
    census_version: Optional[str],
    is_label_based: bool,
    expansion_fn: ExpansionFn,
    census_filter_fn: CensusFilterFn = _filter_ids_against_census,
) -> List[str]:
    """
    Expand the provided terms for a category and return the surviving labels or IDs.
    """
    if not terms:
        return []

    expansion_results: ExpansionResult = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_term = {
            executor.submit(expansion_fn, term, category, organism): term
            for term in terms
        }
        for future in concurrent.futures.as_completed(future_to_term):
            term_name = future_to_term[future]
            try:
                data = future.result()
                if data:
                    expansion_results.extend(data)
            except Exception as exc:
                logger.error(f"Term '{term_name}' generated an exception: {exc}")

    all_ids = {item["ID"] for item in expansion_results if "ID" in item}
    if not all_ids:
        return list(terms)

    if census_version:
        filtered_results = census_filter_fn(
            ids_to_filter=sorted(all_ids),
            organism=organism,
            census_version=census_version,
            ontology_column_name=f"{category}_ontology_term_id",
        )

        if is_label_based:
            surviving_ids = {item["ID"] for item in filtered_results}
            final_labels = {
                item["Label"]
                for item in expansion_results
                if item["ID"] in surviving_ids
            }
            return sorted(final_labels)
        return sorted({item["ID"] for item in filtered_results})

    if is_label_based:
        all_labels = {item["Label"] for item in expansion_results if "Label" in item}
        return sorted(all_labels)
    return sorted(all_ids)


class SPARQLClient:
    """
    A client to interact with Ubergraph using SPARQL queries.
    """

    def __init__(self, endpoint="https://ubergraph.apps.renci.org/sparql", timeout=60):
        """
        Initializes the SPARQL client.

        Parameters:
        - endpoint (str): The SPARQL endpoint URL (default: Ubergraph).
        """

        # Initialize the SPARQLWrapper with the provided endpoint
        self.endpoint = endpoint
        self.sparql = SPARQLWrapper(self.endpoint)
        self.sparql.setTimeout(timeout)  # Add timeout

    def query(self, sparql_query):
        """
        Executes a SPARQL query using the SPARQLWrapper library and returns the results as a list of dictionaries.

        Parameters:
        - sparql_query (str): The SPARQL query string.

        Returns:
        - list: A list of dictionaries containing query results.
        """

        # Set the query and specify the return format as JSON
        self.sparql.setQuery(sparql_query)
        self.sparql.setReturnFormat(JSON)

        try:
            # Log the start of the query execution
            logger.debug("Executing SPARQL query...")
            # Execute the query and convert the results to JSON
            results = self.sparql.query().convert()
            logger.debug("SPARQL query executed successfully.")
            # Return the bindings (results) from the query
            return results["results"]["bindings"]
        except Exception as e:
            # Log any errors that occur during query execution
            logger.error(f"Error executing SPARQL query: {e}")
            raise RuntimeError(f"SPARQL query failed: {e}")


class OntologyExtractor:
    """
    Extracts subclasses and part-of relationships from Ubergraph for a given ontology ID or label.
    Supports multiple ontologies such as Cell Ontology (CL), Uberon (UBERON), etc.
    """

    def __init__(self, sparql_client, prefix_map=None):
        """
        Initializes the ontology extractor.

        Parameters:
        - sparql_client (SPARQLClient): The SPARQL client instance.
        """
        self.sparql_client = sparql_client
        self.prefix_map = prefix_map or {
            "cell_type": "CL",  # Cell Ontology
            "tissue": "UBERON",  # Uberon
            "tissue_general": "UBERON",  # Uberon (this category is supported by CxG census so users might use it instead of tissue)
            "disease": "MONDO",  # MONDO Disease Ontology
            "development_stage": None,  # Dynamically determined based on organism
        }
        self.ontology_iri_map = {
            "CL": "http://purl.obolibrary.org/obo/cl.owl",
            "UBERON": "http://purl.obolibrary.org/obo/uberon.owl",
            "MONDO": "http://purl.obolibrary.org/obo/mondo.owl",
            "HsapDv": "http://purl.obolibrary.org/obo/hsapdv.owl",
            "MmusDv": "http://purl.obolibrary.org/obo/mmusdv.owl",
        }

    def _get_ontology_expansion(self, term, category, organism=None):
        """
        Expands a given ontology term (ID or label) to include its subclasses and parts-of relations
        by constructing and executing a single, optimized SPARQL query.

        Parameters:
        - term (str): The ontology term (ID or label).
        - category (str): The category of the term (e.g., "cell_type", "tissue").
        - organism (str): The organism, required for "development_stage".

        Returns:
        - list: A list of dictionaries with subclass IDs and labels.
        """
        # --- 1. Determine IRI prefix and ontology IRI ---
        iri_prefix = self._get_iri_prefix(category, organism)
        ontology_iri = self.ontology_iri_map.get(iri_prefix)
        if not ontology_iri:
            raise ValueError(f"No ontology IRI found for prefix '{iri_prefix}'.")

        # --- 2. Determine if the term is an ID or a label ---
        is_id = ":" in term and any(
            term.startswith(p)
            for p in ["CL:", "UBERON:", "MONDO:", "HsapDv:", "MmusDv:"]
        )

        # --- 3. Define the expansion logic (will be reused) ---
        # This block finds all children AND the term itself
        expansion_logic = """
        {
            ?term rdfs:subClassOf ?inputTerm .
        } UNION {
            ?term obo:BFO_0000050 ?inputTerm .
        }
        """

        # --- 4. Construct the query body ---
        # This query is now a large UNION between two distinct ways of
        # finding the input term (ID or Label). The expansion logic
        # is duplicated inside each branch to ensure it only runs
        # after ?inputTerm is successfully bound.

        safe_term = term.replace('"', '\\"')

        if is_id:
            # If it's an ID, we only need Path A
            query_body = f"""
            {{
                # --- Path A: Input is an ID ---
                VALUES ?inputTerm {{ obo:{term.replace(':', '_')} }}
                ?inputTerm rdfs:isDefinedBy <{ontology_iri}> .
                
                # --- Expansion for Path A ---
                {expansion_logic}
            }}
            """
        else:
            # If it's a label, we only need Path B
            query_body = f"""
            {{
                # --- Path B: Input is a Label ---
                ?inputTerm rdfs:isDefinedBy <{ontology_iri}> .
                {{
                    ?inputTerm rdfs:label ?inputTermLabel .
                    FILTER(LCASE(STR(?inputTermLabel)) = LCASE("{safe_term}"))
                }} UNION {{
                    ?inputTerm oio:hasExactSynonym ?inputTermLabel .
                    FILTER(LCASE(STR(?inputTermLabel)) = LCASE("{safe_term}"))
                }}
                
                # --- Expansion for Path B ---
                {expansion_logic}
            }}
            """

        # --- 5. Construct the full SPARQL query ---
        sparql_query = f"""
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX oio: <http://www.geneontology.org/formats/oboInOwl#>

        SELECT DISTINCT ?term (STR(?term_label) as ?label)
        WHERE {{
            # --- This outer block contains EITHER Path A or Path B ---
            {query_body}

            # --- Final filter on all results from the successful path ---
            # This ensures all returned terms are valid and have labels
            ?term rdfs:isDefinedBy <{ontology_iri}> ;
                  rdfs:label ?term_label .
        }}
        LIMIT 1000
        """

        # --- 6. Execute the query and process results ---
        logger.debug(
            "Executing ontology expansion query for term '%s' in category '%s'",
            term,
            category,
        )
        logger.debug("SPARQL query body:\n%s", sparql_query)
        results = self.sparql_client.query(sparql_query)
        if results:
            logger.debug(f"Expansion for term '{term}' retrieved successfully.")
        else:
            logger.warning(f"No expansion found for term '{term}'.")

        return [
            {
                "ID": r["term"]["value"].split("/")[-1].replace("_", ":"),
                "Label": r["label"]["value"],
            }
            for r in results
        ]

    def _get_iri_prefix(self, category, organism=None):
        """
        Determines the IRI prefix for a given category and organism.
        """
        if category == "development_stage":
            if not organism:
                raise ValueError(
                    "The 'organism' parameter is required for 'development_stage'."
                )
            normalized_organism = organism.replace("_", " ").title()
            if normalized_organism == "Homo Sapiens":
                return "HsapDv"
            elif normalized_organism == "Mus Musculus":
                return "MmusDv"
            else:
                raise ValueError(
                    f"Unsupported organism '{organism}' for 'development_stage'."
                )
        else:
            prefix = self.prefix_map.get(category)
            if not prefix:
                raise ValueError(
                    f"Unsupported category '{category}'. Supported categories are: {list(self.prefix_map.keys())}"
                )
            return prefix

    def get_ontology_id_from_label(self, label, category, organism=None):
        """
        Resolves a label to a CL or UBERON ID based on category.
        This method is simplified as the main expansion logic is now in _get_ontology_expansion.
        It is primarily used to fetch the parent ID when a label is provided, so that the parent
        can be included in the final list of terms.
        """
        # This method is no longer essential for the main enhancement path but can be kept for other uses
        # or future debugging. For the core 'enhance' logic, its functionality is now integrated
        # into _get_ontology_expansion.
        iri_prefix = self._get_iri_prefix(category, organism)
        ontology_iri = self.ontology_iri_map.get(iri_prefix)

        sparql_query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX oio: <http://www.geneontology.org/formats/oboInOwl#>

        SELECT DISTINCT ?term
        WHERE {{
            ?term rdfs:isDefinedBy <{ontology_iri}> .
            {{
                ?term rdfs:label "{label}" .
            }} UNION {{
                ?term rdfs:label "{label}" .
            }} UNION {{
                ?term oio:hasExactSynonym "{label}" .
            }}
        }}
        LIMIT 1
        """
        results = self.sparql_client.query(sparql_query)
        if results:
            return results[0]["term"]["value"].split("/")[-1].replace("_", ":")
        else:
            logger.warning(
                f"No ontology ID found for label '{label}' in category '{category}'."
            )
            return None

    def get_subclasses(self, term, category="cell_type", organism=None):
        """
        Extracts subclasses and part-of relationships for the given ontology term (CL or UBERON IDs or labels).
        This method now delegates the core logic to _get_ontology_expansion.
        """
        return self._get_ontology_expansion(term, category, organism)


def _get_thread_local_extractor() -> OntologyExtractor:
    """
    Ensure each thread uses its own SPARQL client by caching the extractor thread-locally.
    """
    extractor = getattr(_thread_local_resources, "extractor", None)
    if extractor is None:
        extractor = OntologyExtractor(SPARQLClient())
        _thread_local_resources.extractor = extractor
    return extractor


def _thread_safe_expansion(
    term: str, category: str, organism: Optional[str]
) -> ExpansionResult:
    """
    Wrapper used by enhance/process_category to ensure thread-local extractors.
    """
    extractor = _get_thread_local_extractor()
    return extractor._get_ontology_expansion(term, category, organism)


def enhance(query_filter, categories=None, organism=None, census_version="latest"):
    """
    Rewrites the query filter to include ontology closure using AST parsing.
    """

    # --- 1. Basic Setup (Same as before) ---
    organism_explicitly_provided = organism is not None
    if not organism_explicitly_provided:
        organism = "homo_sapiens"
        logger.info(
            "No 'organism' provided to enhance(), defaulting to 'homo_sapiens'."
        )

    ontology_supported_categories = {
        "cell_type",
        "tissue",
        "tissue_general",
        "disease",
        "development_stage",
    }

    # --- 2. Parse Query to AST (Replaces Regex Auto-detect) ---
    try:
        # Validate syntax immediately. mode='eval' is used for expression strings.
        tree = ast.parse(query_filter, mode="eval")
    except SyntaxError as e:
        logger.error(f"Invalid query syntax: {e}")
        return query_filter

    # If categories are None, tell the extractor to look for ALL supported categories
    target_cats = categories if categories else list(ontology_supported_categories)

    # --- 3. Extract Terms using AST (Replaces Regex Loops) ---
    term_extractor = QueryTermExtractor(target_cats)
    term_extractor.visit(tree)

    terms_to_expand = term_extractor.terms
    ids_to_expand = term_extractor.ids

    # If auto-detecting, refine 'categories' based on what AST actually found
    if categories is None:
        found_cats = set(terms_to_expand.keys()) | set(ids_to_expand.keys())
        categories = sorted(list(found_cats))
        logger.info(f"Auto-detected categories via AST: {categories}")
    else:
        # Filter explicitly provided categories to supported ones
        categories = [c for c in categories if c in ontology_supported_categories]
        logger.info(f"Categories to be processed: {categories}")
    # Check for empty results
    if not categories and not terms_to_expand and not ids_to_expand:
        logger.info("No relevant categories/terms found. Returning original filter.")
        return query_filter

    # Check development_stage warning
    if "development_stage" in categories and not organism_explicitly_provided:
        logger.warning(
            "Processing 'development_stage' using default organism. "
            "It is recommended to explicitly pass 'organism'."
        )

    # --- 4. Prepare Expansion Logic ---
    expanded_label_terms = {}
    expanded_id_terms = {}

    expansion_fn = _thread_safe_expansion

    # --- 5. Expand Terms ---
    for category, terms in terms_to_expand.items():
        expanded_label_terms[category] = process_category(
            terms,
            category=category,
            organism=organism,
            census_version=census_version,
            is_label_based=True,
            expansion_fn=expansion_fn,
        )

    for category, ids in ids_to_expand.items():
        expanded_id_terms[category] = process_category(
            ids,
            category=category,
            organism=organism,
            census_version=census_version,
            is_label_based=False,
            expansion_fn=expansion_fn,
        )

    # --- 6. Rewrite Query using AST (Replaces Regex Sub) ---
    rewriter = QueryRewriter(expanded_label_terms, expanded_id_terms)
    new_tree = rewriter.visit(tree)

    ast.fix_missing_locations(new_tree)

    # Unparse AST back to string (Python 3.9+)
    rewritten_query = ast.unparse(new_tree)

    logger.info("Query filter rewritten successfully via AST.")
    return rewritten_query
