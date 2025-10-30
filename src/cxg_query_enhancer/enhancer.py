from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging
import cellxgene_census
from functools import lru_cache
import pickle


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
                logging.info(f"Loading cached census terms from {cache_path}")
                return pickle.load(f)
        except (pickle.UnpicklingError, EOFError) as e:
            logging.warning(
                f"Cache file {cache_path} is corrupted. Refetching. Error: {e}"
            )

    # --- 2. If not cached, fetch from CellXGene Census ---
    logging.info(
        f"Fetching census terms for '{ontology_column_name}' from CellXGene Census..."
    )
    # Normalize organism name to lowercase with underscores
    census_organism = organism.replace(" ", "_").lower()
    try:
        with cellxgene_census.open_soma(census_version=census_version) as census:
            organism_data = census["census_data"].get(census_organism)
            if not organism_data:
                logging.warning(f"Organism '{census_organism}' not found in census.")
                return None

            obs_reader = organism_data.obs
            if ontology_column_name not in obs_reader.keys():
                logging.warning(f"Column '{ontology_column_name}' not found in census.")
                return None

            df = (
                obs_reader.read(column_names=[ontology_column_name])
                .concat()
                .to_pandas()
            )
            terms = set(df[ontology_column_name].dropna().unique()) - {"unknown"}

            # --- 3. Save to local cache for future use ---
            with open(cache_path, "wb") as f:
                pickle.dump(terms, f)
            logging.info(f"Saved census terms to cache: {cache_path}")

            return terms

    except Exception as e:
        logging.error(f"Error accessing CellXGene Census: {e}")
        return None


def _filter_ids_against_census(
    ids_to_filter, census_version, organism, ontology_column_name
):
    """
    Filters a list of ontology IDs against those present in a specific CellXGene Census version.

    Parameters:
    - ids_to_filter (list[str]): List of ontology IDs to filter.
    - census_version (str): The version of the CellXGene Census to use.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - ontology_column_name (str): The column name containing ontology IDs (e.g., "cell_type_ontology_term_id").

    Returns:
    - list[str]: Sorted list of ontology IDs present in the census, or the original list if no matches were found.
    """

    if not ids_to_filter:
        logging.info("No IDs provided to filter; returning empty list.")
        return []

    # Call the cached function to get the set of valid terms from the census (using the helper function).
    census_terms = _get_census_terms(census_version, organism, ontology_column_name)

    # return original ids unfiltered if census terms cannot be retrieved
    if census_terms is None:
        logging.warning(
            "Census terms could not be retrieved. Returning original IDs unfiltered."
        )
        return ids_to_filter

    # Filter input IDs based on presence in the census terms
    # Perform the intersection between the input IDs (ids_to_filter) and the census terms
    # sorts filtered IDs for consistent output
    filtered_ids = sorted([id_ for id_ in ids_to_filter if id_ in census_terms])
    logging.info(
        f"{len(filtered_ids)} of {len(set(ids_to_filter))} IDs matched in census."
    )
    return filtered_ids


class SPARQLClient:
    """
    A client to interact with Ubergraph using SPARQL queries.
    """

    def __init__(self, endpoint="https://ubergraph.apps.renci.org/sparql"):
        """
        Initializes the SPARQL client.

        Parameters:
        - endpoint (str): The SPARQL endpoint URL (default: Ubergraph).
        """

        # Initialize the SPARQLWrapper with the provided endpoint
        self.endpoint = endpoint
        self.sparql = SPARQLWrapper(self.endpoint)

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
            logging.info("Executing SPARQL query...")
            # Execute the query and convert the results to JSON
            results = self.sparql.query().convert()
            logging.info("SPARQL query executed successfully.")
            # Return the bindings (results) from the query
            return results["results"]["bindings"]
        except Exception as e:
            # Log any errors that occur during query execution
            logging.error(f"Error executing SPARQL query: {e}")
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
        logging.info(f"Executing query: {sparql_query}")
        results = self.sparql_client.query(sparql_query)
        if results:
            logging.info(f"Expansion for term '{term}' retrieved successfully.")
        else:
            logging.warning(f"No expansion found for term '{term}'.")

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
            logging.warning(
                f"No ontology ID found for label '{label}' in category '{category}'."
            )
            return None

    def get_subclasses(self, term, category="cell_type", organism=None):
        """
        Extracts subclasses and part-of relationships for the given ontology term (CL or UBERON IDs or labels).
        This method now delegates the core logic to _get_ontology_expansion.
        """
        return self._get_ontology_expansion(term, category, organism)


def enhance(query_filter, categories=None, organism=None, census_version="latest"):
    """
    Rewrites the query filter to include ontology closure and filters IDs against the CellxGene Census.

    Parameters:
    - query_filter (str): The original query filter string.
    - categories (list): List of categories to apply closure to (default: ["cell_type"]).
    - organism (str): The organism to query in the census (e.g., "homo_sapiens"). If not provided, defaults to "homo_sapiens". A warning is logged if 'development_stage' is processed without an explicitly provided organism.
    - census_version (str): Version of the CellxGene Census to use for filtering IDs.

    Returns:
    - str: The rewritten query filter with expanded terms based on ontology closure.
    """

    # --- Determine whether the organism was explicitly provided ---
    organism_explicitly_provided = organism is not None

    # --- Set default organism if not provided ---
    if not organism_explicitly_provided:
        organism = "homo_sapiens"
        logging.info(
            "No 'organism' provided to enhance(), defaulting to 'homo_sapiens'."
        )

    # Auto-detect categories if not explicitly provided
    if categories is None:
        matches = re.findall(
            r"(\b\w+?\b)(?:_ontology_term_id)?\s*(?:==|in)\s+",
            query_filter,
            re.IGNORECASE,
        )
        # Normalize to lowercase for consistency, and remove _ontology_term_id suffix
        auto_detected_categories = sorted(
            list(set(m.lower().replace("_ontology_term_id", "") for m in matches))
        )
        logging.info(f"Auto-detected categories: {auto_detected_categories}")
        categories_to_filter = auto_detected_categories
    else:
        # Normalize explicitly provided categories to lowercase and remove _ontology_term_id suffix
        categories_to_filter = sorted(
            list(set(c.lower().replace("_ontology_term_id", "") for c in categories))
        )
        logging.info(
            f"Explicitly provided categories (normalized): {categories_to_filter}"
        )

    # --- Filter categories to only those supported by ontology expansion ---
    ontology_supported_categories = {
        "cell_type",
        "tissue",
        "tissue_general",
        "disease",
        "development_stage",
    }

    # 'categories' will now hold only the ones that should be processed for ontology expansion
    categories = [
        cat for cat in categories_to_filter if cat in ontology_supported_categories
    ]
    logging.info(f"Categories to be processed for ontology expansion: {categories}")

    # A check to ensure 'categories' (now filtered) is not empty, return the original query if no relevant categories are found
    if not categories:
        logging.info(
            "No ontology-supported categories to process. Returning original filter."
        )
        return query_filter

    # Check if organism is required for development_stage category
    if "development_stage" in categories and not organism_explicitly_provided:
        logging.warning(
            "Processing 'development_stage' using the default organism "
            f"'{organism}'. If your final CELLxGENE Census query targets a different "
            "organism, the development stage expansion may be incorrect. "
            "It is recommended to explicitly pass the 'organism' parameter to enhance() "
            "when 'development_stage' is involved."
        )

    # Dictionaries to store terms and IDs to expand for each category
    terms_to_expand = {}  # {category: [terms]}
    ids_to_expand = {}  # {category: [ontology IDs]}

    # Extract terms and IDs for each category from the query filter
    for category in categories:
        terms = []
        ids = []

        # Regexes for label-based matches
        # Detect label-based queries with "== 'term'" (e.g., "cell_type == 'neuron'")
        match_eq_label = re.search(
            rf"\b{category}\b\s*==\s*['\"](.*?)['\"]", query_filter, re.IGNORECASE
        )
        # Detect label-based queries with "in ['term1', 'term2']" (e.g., "cell_type in ['neuron', 'microglial cell']")
        match_in_label = re.search(
            rf"\b{category}\b\s+in\s+\[(.*?)\]", query_filter, re.IGNORECASE
        )
        if match_eq_label:
            terms.append(match_eq_label.group(1).strip().strip("'\""))
        elif match_in_label:
            terms = [
                term.strip().strip("'\"")
                for term in match_in_label.group(1).split(",")
                if term.strip()
            ]

        # Regexes for ID-based matches
        # Match ontology IDs (e.g., "cell_type_ontology_term_id == 'CL:0000540'")
        match_eq_id = re.search(
            rf"\b{category}_ontology_term_id\b\s*==\s*['\"](.*?)['\"]",
            query_filter,
            re.IGNORECASE,
        )
        # Match ontology IDs (e.g., "cell_type_ontology_term_id in ['CL:0000540']")
        match_in_id = re.search(
            rf"\b{category}_ontology_term_id\b\s+in\s+\[(.*?)\]",
            query_filter,
            re.IGNORECASE,
        )
        if match_eq_id:
            ids.append(match_eq_id.group(1).strip().strip("'\""))
        elif match_in_id:
            ids = [
                id_.strip().strip("'\"")
                for id_ in match_in_id.group(1).split(",")
                if id_.strip()
            ]

        # Store extracted terms and IDs
        if terms:
            terms_to_expand[category] = terms
        if ids:
            ids_to_expand[category] = ids

    # Initialize the OntologyExtractor if there are terms or IDs to expand
    if terms_to_expand or ids_to_expand:
        extractor = OntologyExtractor(SPARQLClient())

    # Dictionaries to store expanded terms for labels and IDs
    expanded_label_terms = {}  # label expansions
    expanded_id_terms = {}  # ID expansions

    # Process label-based queries: fetch subclasses, resolve labels to IDs, and filter against the census
    for category, terms in terms_to_expand.items():
        expanded_label_terms[category] = []
        for term in terms:
            # Fetch subclasses for the term (label)
            subclasses = extractor.get_subclasses(term, category, organism)
            if not subclasses:
                logging.warning(
                    f"Could not resolve label '{term}' to any ontology terms."
                )
                expanded_label_terms[category].append(term)  # Keep the original label
                continue

            # Extract IDs and labels from the subclasses
            all_ids = [sub["ID"] for sub in subclasses]
            all_labels = [sub["Label"] for sub in subclasses]

            # Filter IDs against the census if applicable
            if census_version:
                logging.info(
                    f"Filtering subclasses for label '{term}' based on CellxGene Census..."
                )
                filtered_ids = _filter_ids_against_census(
                    ids_to_filter=all_ids,
                    census_version=census_version,
                    organism=organism,
                    ontology_column_name=f"{category}_ontology_term_id",
                )
                # Keep only labels corresponding to filtered IDs
                filtered_labels = [
                    sub["Label"] for sub in subclasses if sub["ID"] in filtered_ids
                ]
                all_labels = filtered_labels

            # store the expanded and filtered labels
            if all_labels:
                expanded_label_terms[category].extend(sorted(set(all_labels)))

    # Process ID-based queries: fetch subclasses and filter against the census
    for category, ids in ids_to_expand.items():
        if category not in expanded_id_terms:
            expanded_id_terms[category] = []
        for ontology_id in ids:
            # Fetch subclasses for the ontology ID
            subclasses = extractor.get_subclasses(ontology_id, category, organism)

            # Extract IDs from the subclasses
            child_ids = [sub["ID"] for sub in subclasses]

            # Filter IDs against the census if applicable
            if census_version:
                logging.info(
                    f"Filtering subclasses for ontology ID '{ontology_id}' based on CellxGene Census..."
                )
                filtered_ids = _filter_ids_against_census(
                    ids_to_filter=child_ids,
                    census_version=census_version,
                    organism=organism,
                    ontology_column_name=f"{category}_ontology_term_id",
                )
                child_ids = filtered_ids

            # Add filtered IDs to expanded terms
            if child_ids:
                expanded_id_terms[category].extend(child_ids)

    # Rewrite the query filter with the expanded label based terms
    for category, terms in expanded_label_terms.items():
        # Remove duplicates and sort the terms in alphabetical order for consistency
        unique_terms = sorted(set(terms))
        # Convert the terms back into the format: ['term1', 'term2', ...]
        expanded_terms_str = ", ".join(f"'{t}'" for t in unique_terms)

        # Replace label-based expressions
        # replace "category in [...]" with expanded terms
        query_filter = re.sub(
            rf"{category}\s+in\s+\[.*?\]",
            f"{category} in [{expanded_terms_str}]",
            query_filter,
            flags=re.IGNORECASE,
        )
        # Replace "category == '...'" with expanded terms
        query_filter = re.sub(
            rf"{category}\s*==\s*['\"].*?['\"]",
            f"{category} in [{expanded_terms_str}]",
            query_filter,
            flags=re.IGNORECASE,
        )

    # Rewrite the query filter with the expanded ID-based terms
    for category, ids in expanded_id_terms.items():
        query_type = f"{category}_ontology_term_id"
        unique_ids = sorted(set(ids))
        expanded_ids_str = ", ".join(f"'{t}'" for t in unique_ids)

        # Replace "category_ontology_term_id in [...]" with expanded IDs
        query_filter = re.sub(
            rf"{query_type}\s+in\s+\[.*?\]",
            f"{query_type} in [{expanded_ids_str}]",
            query_filter,
            flags=re.IGNORECASE,
        )
        # Replace "category_ontology_term_id == '...'" with expanded IDs
        query_filter = re.sub(
            rf"{query_type}\s*==\s*['\"].*?['\"]",
            f"{query_type} in [{expanded_ids_str}]",
            query_filter,
            flags=re.IGNORECASE,
        )

    logging.info("Query filter rewritten successfully.")
    return query_filter
