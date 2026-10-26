from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging

logger = logging.getLogger(__name__)
import cellxgene_census
from collections import defaultdict
import functools
from functools import lru_cache
import json
import tempfile
import concurrent.futures
import pyarrow.compute as pc
import ast
import threading
from typing import Callable, Dict, List, Optional, Sequence


class EnhancementError(RuntimeError):
    """
    Raised by enhance(strict=True) when term expansion or census filtering fails,
    instead of silently falling back to the original (unexpanded/unfiltered) terms.
    """


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

        # Handle '==' (Eq) and '!=' (NotEq)
        if isinstance(op, (ast.Eq, ast.NotEq)):
            if isinstance(val_node, ast.Constant):
                extracted.append(val_node.value)

        # Handle 'in' (In) and 'not in' (NotIn)
        elif isinstance(op, (ast.In, ast.NotIn)):
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

            # Determine the appropriate operator for the rewritten query
            # == becomes in, != becomes not in
            original_op = node.ops[0]
            if isinstance(original_op, (ast.Eq, ast.In)):
                new_op = ast.In()
            elif isinstance(original_op, (ast.NotEq, ast.NotIn)):
                new_op = ast.NotIn()
            else:
                # For any other operator, keep it as-is (shouldn't happen)
                new_op = original_op

            # Return new node: col_name in/not in ['A', 'B', 'C']
            return ast.Compare(left=left, ops=[new_op], comparators=[list_node])

        return node


ExpansionResult = List[Dict[str, str]]
ExpansionFn = Callable[[str, str, Optional[str]], ExpansionResult]
CensusFilterFn = Callable[..., List[str]]

_thread_local_resources = threading.local()

# Default maximum number of concurrent SPARQL expansion requests sent to Ubergraph
EXPANSION_MAX_WORKERS = 5

# Default timeout (seconds) for each SPARQL request to Ubergraph
DEFAULT_SPARQL_TIMEOUT = 60


CACHE_DIR_ENV_VAR = "CXG_QUERY_ENHANCER_CACHE_DIR"


def _get_cache_dir():
    """
    Returns the directory for the on-disk census terms cache.

    Uses $CXG_QUERY_ENHANCER_CACHE_DIR if set, otherwise
    $XDG_CACHE_HOME/cxg_query_enhancer (default: ~/.cache/cxg_query_enhancer),
    so the cache does not depend on the current working directory.
    """
    override = os.environ.get(CACHE_DIR_ENV_VAR)
    if override:
        return override
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache"
    )
    return os.path.join(base, "cxg_query_enhancer")


def _resolve_census_version(census_version):
    """
    Resolves a census version alias (e.g. "latest", "stable") to its concrete release build
    (e.g. "2025-11-17"), so cached data is keyed on the actual release.
    """
    description = cellxgene_census.get_census_version_description(census_version)
    return description.get("release_build") or census_version


def _write_cache_atomically(cache_path, terms):
    """
    Writes the terms to cache_path as a sorted JSON list via a temp file + os.replace,
    so concurrent or interrupted writes never leave a partially written cache file.
    """
    cache_dir = os.path.dirname(cache_path)
    fd, tmp_path = tempfile.mkstemp(dir=cache_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(sorted(terms), f)
        os.replace(tmp_path, cache_path)
    except BaseException:
        os.unlink(tmp_path)
        raise


@lru_cache(maxsize=None)
def _get_census_terms(census_version, organism, ontology_column_name):
    """
    Fetches and caches the unique ontology terms present in a specific CellXGene Census version for a given organism and column.
    A local file-based cache (see _get_cache_dir) is used to speed up repeated queries.

    Parameters:
    - census_version (str): The version of the CellXGene Census to use. Aliases such as "latest"
      are resolved to a concrete release, so the cache refreshes when a new release is published.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - ontology_column_name (str): The column name containing ontology IDs (e.g., "cell_type_ontology_term_id").

    Returns:
    - set[str]: A set of unique ontology terms.

    Raises:
    - ValueError: If the organism or column is not present in the census.
    - Exception: Any error raised while accessing the census. Errors are not caught here,
      so that lru_cache does not memoize a failure for the lifetime of the process.
    """
    resolved_version = _resolve_census_version(census_version)

    # --- Local Cache Setup ---
    cache_dir = _get_cache_dir()
    os.makedirs(cache_dir, exist_ok=True)
    # Sanitize filename to be safe for all OS
    safe_version = re.sub(r"[^\w.-]+", "", resolved_version)
    safe_organism = re.sub(r"[\W_]+", "", organism)
    cache_filename = f"{safe_version}_{safe_organism}_{ontology_column_name}.json"
    cache_path = os.path.join(cache_dir, cache_filename)

    # --- 1. Try to load from local cache ---
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                logger.info(f"Loading cached census terms from {cache_path}")
                return set(json.load(f))
        except (OSError, ValueError) as e:
            logger.warning(
                f"Cache file {cache_path} is unreadable or corrupted. Refetching. Error: {e}"
            )

    # --- 2. If not cached, fetch from CellXGene Census ---
    logger.info(
        f"Fetching census terms for '{ontology_column_name}' from CellXGene Census "
        f"(release {resolved_version})..."
    )
    # Normalize organism name to lowercase with underscores
    census_organism = organism.replace(" ", "_").lower()
    with cellxgene_census.open_soma(census_version=resolved_version) as census:
        organism_data = census["census_data"].get(census_organism)
        if not organism_data:
            raise ValueError(f"Organism '{census_organism}' not found in census.")

        obs_reader = organism_data.obs
        if ontology_column_name not in obs_reader.keys():
            raise ValueError(f"Column '{ontology_column_name}' not found in census.")

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

    # --- 3. Save to local cache for future use (best effort) ---
    try:
        _write_cache_atomically(cache_path, terms)
        logger.info(f"Saved census terms to cache: {cache_path}")
    except OSError as e:
        logger.warning(f"Could not write census terms cache {cache_path}: {e}")

    return terms


_census_terms_locks = defaultdict(threading.Lock)
_census_terms_locks_guard = threading.Lock()


def _census_terms_lock(*key):
    """Returns the lock serializing _get_census_terms calls for one (version, organism, column)."""
    with _census_terms_locks_guard:
        return _census_terms_locks[key]


def _filter_ids_against_census(
    ids_to_filter,
    organism,
    census_version="latest",
    ontology_column_name=None,
    strict=False,
):
    """
    Filters a list of ontology IDs against those present in a specific CellXGene Census version.

    Parameters:
    - ids_to_filter (list[str]): List of ontology IDs to filter.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - census_version (str): The version of the CellXGene Census to use.
    - ontology_column_name (str): The column name containing ontology IDs.
    - strict (bool): If True, raise EnhancementError when the census cannot be read
      instead of returning the IDs unfiltered.

    Returns:
    - list[str]: The IDs present in the census (all IDs, unfiltered, if the census
      cannot be read and strict is False).
    """
    if not ids_to_filter:
        logger.info("No IDs provided to filter; returning empty list.")
        return []

    try:
        # Categories are processed concurrently; the lock ensures the same census
        # column is only downloaded once rather than by several threads at the same time.
        with _census_terms_lock(census_version, organism, ontology_column_name):
            census_terms = _get_census_terms(
                census_version, organism, ontology_column_name
            )
    except Exception as e:
        if strict:
            raise EnhancementError(
                f"Could not filter against CellXGene Census: {e}"
            ) from e
        logger.warning(
            f"Census terms could not be retrieved ({e}). Returning original IDs unfiltered."
        )
        return list(ids_to_filter)

    filtered_ids = [id_ for id_ in ids_to_filter if id_ in census_terms]

    logger.info(
        f"{len(filtered_ids)} of {len(set(ids_to_filter))} IDs matched in census."
    )
    return filtered_ids


def process_category(
    terms: Sequence[str],
    *,
    category: str,
    organism: str,
    census_version: Optional[str],
    is_label_based: bool,
    expansion_fn: ExpansionFn,
    census_filter_fn: CensusFilterFn = _filter_ids_against_census,
    strict: bool = False,
    executor: Optional[concurrent.futures.Executor] = None,
) -> List[str]:
    """
    Expand the provided terms for a category and return the surviving labels or IDs.

    Terms whose expansion fails or returns nothing are kept verbatim in the output,
    so they are never silently dropped from the query. With strict=True, a failed
    expansion raises EnhancementError instead.

    Expansions run on the given executor (shared across categories by enhance()),
    or on a private pool of 5 threads if none is given.
    """
    if not terms:
        return []

    expansion_results: ExpansionResult = []
    unexpanded_terms = set()
    failures = {}
    owns_executor = executor is None
    if owns_executor:
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=EXPANSION_MAX_WORKERS
        )
    try:
        future_to_term = {
            executor.submit(expansion_fn, term, category, organism): term
            for term in terms
        }
        for future in concurrent.futures.as_completed(future_to_term):
            term_name = future_to_term[future]
            try:
                data = future.result()
            except Exception as exc:
                logger.error(f"Term '{term_name}' generated an exception: {exc}")
                failures[term_name] = exc
                unexpanded_terms.add(term_name)
                continue
            if data:
                expansion_results.extend(data)
            else:
                logger.warning(
                    f"Term '{term_name}' could not be expanded; keeping it unchanged."
                )
                unexpanded_terms.add(term_name)
    finally:
        if owns_executor:
            executor.shutdown()

    if strict and failures:
        details = "; ".join(f"'{t}': {e}" for t, e in sorted(failures.items()))
        raise EnhancementError(
            f"Expansion failed for {category} term(s): {details}"
        ) from next(iter(failures.values()))

    all_ids = {item["ID"] for item in expansion_results if "ID" in item}
    if not all_ids:
        return list(terms)

    if census_version:
        surviving_ids = set(
            census_filter_fn(
                ids_to_filter=sorted(all_ids),
                organism=organism,
                census_version=census_version,
                ontology_column_name=f"{category}_ontology_term_id",
                strict=strict,
            )
        )

        if is_label_based:
            final_labels = {
                item["Label"]
                for item in expansion_results
                if item["ID"] in surviving_ids
            }
            return sorted(final_labels | unexpanded_terms)
        return sorted(surviving_ids | unexpanded_terms)

    if is_label_based:
        all_labels = {item["Label"] for item in expansion_results if "Label" in item}
        return sorted(all_labels | unexpanded_terms)
    return sorted(all_ids | unexpanded_terms)


# Ontology IDs accepted as query input, e.g. "CL:0000540"
ONTOLOGY_ID_PATTERN = re.compile(r"(CL|UBERON|MONDO|HsapDv|MmusDv):\d+")


def _sparql_string(value):
    """
    Returns value as a double-quoted SPARQL string literal, escaping backslashes,
    double quotes and line breaks so user input cannot break out of the literal.
    """
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


class SPARQLClient:
    """
    A client to interact with Ubergraph using SPARQL queries.
    """

    def __init__(
        self,
        endpoint="https://ubergraph.apps.renci.org/sparql",
        timeout=DEFAULT_SPARQL_TIMEOUT,
    ):
        """
        Initializes the SPARQL client.

        Parameters:
        - endpoint (str): The SPARQL endpoint URL (default: Ubergraph).
        - timeout (int): Timeout in seconds for each query.
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
        if is_id and not ONTOLOGY_ID_PATTERN.fullmatch(term):
            raise ValueError(f"Invalid ontology ID '{term}'.")

        # --- 3. Build the clause that binds ?inputTerm ---
        if is_id:
            input_clauses = [
                f"VALUES ?inputTerm {{ obo:{term.replace(':', '_')} }}"
            ]
        else:
            term_literal = _sparql_string(term)
            # Exact match first: Ubergraph labels are xsd:string-typed literals, so a typed
            # literal can use the index, whereas LCASE() has to scan every label in the
            # ontology (~0.5s vs ~5s for UBERON). Fall back to it only if nothing matched.
            # The lowercased term is also tried exactly, as most labels are lowercase.
            exact_literals = " ".join(
                f"{_sparql_string(variant)}^^xsd:string"
                for variant in dict.fromkeys([term, term.lower()])
            )
            input_clauses = [
                f"""
                VALUES ?inputTermLabel {{ {exact_literals} }}
                {{ ?inputTerm rdfs:label ?inputTermLabel }}
                UNION
                {{ ?inputTerm oio:hasExactSynonym ?inputTermLabel }}
                """,
                f"""
                {{ ?inputTerm rdfs:label ?inputTermLabel }}
                UNION
                {{ ?inputTerm oio:hasExactSynonym ?inputTermLabel }}
                FILTER(LCASE(STR(?inputTermLabel)) = LCASE({term_literal}))
                """,
            ]

        # --- 4. Execute the query and process results ---
        logger.debug(
            "Executing ontology expansion query for term '%s' in category '%s'",
            term,
            category,
        )
        for input_clause in input_clauses:
            sparql_query = self._build_expansion_query(ontology_iri, input_clause)
            logger.debug("SPARQL query body:\n%s", sparql_query)
            results = self.sparql_client.query(sparql_query)
            if results:
                break

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

    @staticmethod
    def _build_expansion_query(ontology_iri, input_clause):
        """
        Builds the SPARQL query returning the input term, its subclasses and its parts
        (with labels), where input_clause binds ?inputTerm.
        """
        return f"""
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX oio: <http://www.geneontology.org/formats/oboInOwl#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT DISTINCT ?term (STR(?term_label) as ?label)
        WHERE {{
            # --- Bind the input term (by ID or label) within the target ontology ---
            ?inputTerm rdfs:isDefinedBy <{ontology_iri}> .
            {input_clause}

            # --- Expansion: subclasses and parts (both include the term itself) ---
            {{
                ?term rdfs:subClassOf ?inputTerm .
            }} UNION {{
                ?term obo:BFO_0000050 ?inputTerm .
            }}

            # --- Ensure all returned terms are in the ontology and have labels ---
            ?term rdfs:isDefinedBy <{ontology_iri}> ;
                  rdfs:label ?term_label .
        }}
        LIMIT 1000
        """

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
        Resolves a label (or exact synonym) to an ontology ID, e.g. "neuron" -> "CL:0000540".

        Tries an exact match first, then falls back to a case-insensitive match.
        A primary label match is preferred over an exact synonym match.

        Parameters:
        - label (str): The term label or exact synonym.
        - category (str): The category of the term (e.g., "cell_type", "tissue").
        - organism (str): The organism, required for "development_stage".

        Returns:
        - str or None: The ontology ID, or None if no term matches.
        """
        iri_prefix = self._get_iri_prefix(category, organism)
        ontology_iri = self.ontology_iri_map.get(iri_prefix)
        label_literal = _sparql_string(label)

        # Ubergraph stores labels as xsd:string-typed literals, which a plain "label"
        # literal does not match, so the exact match must use the typed form.
        exact_match = f"""
            {{ ?term rdfs:label {label_literal}^^xsd:string . BIND(0 AS ?rank) }}
            UNION
            {{ ?term oio:hasExactSynonym {label_literal}^^xsd:string . BIND(1 AS ?rank) }}
        """
        case_insensitive_match = f"""
            {{ ?term rdfs:label ?matched . BIND(0 AS ?rank) }}
            UNION
            {{ ?term oio:hasExactSynonym ?matched . BIND(1 AS ?rank) }}
            FILTER(LCASE(STR(?matched)) = LCASE({label_literal}))
        """

        for match in (exact_match, case_insensitive_match):
            sparql_query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX oio: <http://www.geneontology.org/formats/oboInOwl#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT ?term
            WHERE {{
                ?term rdfs:isDefinedBy <{ontology_iri}> .
                {match}
            }}
            ORDER BY ?rank ?term
            LIMIT 1
            """
            results = self.sparql_client.query(sparql_query)
            if results:
                return results[0]["term"]["value"].split("/")[-1].replace("_", ":")

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


def _get_thread_local_extractor(
    sparql_timeout: int = DEFAULT_SPARQL_TIMEOUT,
) -> OntologyExtractor:
    """
    Ensure each thread uses its own SPARQL client (one per timeout value)
    by caching the extractor thread-locally.
    """
    extractors = getattr(_thread_local_resources, "extractors", None)
    if extractors is None:
        extractors = _thread_local_resources.extractors = {}
    if sparql_timeout not in extractors:
        extractors[sparql_timeout] = OntologyExtractor(
            SPARQLClient(timeout=sparql_timeout)
        )
    return extractors[sparql_timeout]


@lru_cache(maxsize=1024)
def _cached_expansion(
    term: str, category: str, organism: Optional[str], sparql_timeout: int
):
    """
    Memoizes expansion results per (term, category, organism) for the lifetime of the process.
    Failures raise and are therefore not cached.
    """
    extractor = _get_thread_local_extractor(sparql_timeout)
    return tuple(extractor._get_ontology_expansion(term, category, organism))


def _thread_safe_expansion(
    term: str,
    category: str,
    organism: Optional[str],
    sparql_timeout: int = DEFAULT_SPARQL_TIMEOUT,
) -> ExpansionResult:
    """
    Wrapper used by enhance/process_category to ensure thread-local extractors
    and to reuse cached expansion results.
    """
    # Only development_stage expansion depends on the organism; dropping it from the
    # cache key for other categories lets e.g. human and mouse queries share results.
    if category != "development_stage":
        organism = None
    return list(_cached_expansion(term, category, organism, sparql_timeout))


def enhance(
    query_filter,
    categories=None,
    organism=None,
    census_version="latest",
    strict=False,
    max_workers=EXPANSION_MAX_WORKERS,
    sparql_timeout=DEFAULT_SPARQL_TIMEOUT,
):
    """
    Rewrites the query filter to include ontology closure using AST parsing.

    By default, failures degrade gracefully: invalid syntax returns the query unchanged,
    terms that fail to expand are kept as-is, and census filtering is skipped if the
    census cannot be read. With strict=True these cases raise instead
    (ValueError for invalid syntax, EnhancementError for expansion/census failures).

    max_workers caps the number of concurrent requests sent to Ubergraph, and
    sparql_timeout is the timeout in seconds for each of those requests.
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
        if strict:
            raise ValueError(f"Invalid query filter syntax: {e}") from e
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

    expansion_fn = functools.partial(
        _thread_safe_expansion, sparql_timeout=sparql_timeout
    )

    # --- 5. Expand Terms ---
    # Categories are processed concurrently; their term expansions share one pool,
    # which caps the number of parallel requests sent to Ubergraph.
    jobs = [(category, terms, True) for category, terms in terms_to_expand.items()]
    jobs += [(category, ids, False) for category, ids in ids_to_expand.items()]

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as expansion_executor, concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, len(jobs))
    ) as category_executor:
        futures = [
            (
                category,
                is_label_based,
                category_executor.submit(
                    process_category,
                    terms,
                    category=category,
                    organism=organism,
                    census_version=census_version,
                    is_label_based=is_label_based,
                    expansion_fn=expansion_fn,
                    strict=strict,
                    executor=expansion_executor,
                ),
            )
            for category, terms, is_label_based in jobs
        ]
        for category, is_label_based, future in futures:
            if is_label_based:
                expanded_label_terms[category] = future.result()
            else:
                expanded_id_terms[category] = future.result()

    # --- 6. Rewrite Query using AST (Replaces Regex Sub) ---
    rewriter = QueryRewriter(expanded_label_terms, expanded_id_terms)
    new_tree = rewriter.visit(tree)

    ast.fix_missing_locations(new_tree)

    # Unparse AST back to string (Python 3.9+)
    rewritten_query = ast.unparse(new_tree)

    logger.info("Query filter rewritten successfully via AST.")
    return rewritten_query
