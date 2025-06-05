from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging
import cellxgene_census
from functools import lru_cache


@lru_cache(maxsize=None)
def _get_census_terms(census_version, organism, ontology_column_name):
    """
    Fetches and caches the unique ontology terms present in a specific CellXGene Census version for a given organism and column.

    Parameters:
    - census_version (str): The version of the CellXGene Census to use.
    - organism (str): The organism to query (e.g., "homo_sapiens").
    - ontology_column_name (str): The column name containing ontology IDs (e.g., "cell_type_ontology_term_id", "tissue_ontology_term_id").

    Returns:
    - set[str]: A set of unique ontology terms present in the census, or an empty set if no data is found.
    """
    # Normalize organism name to lowercase with underscores
    census_organism = organism.replace(" ", "_").lower()
    try:
        with cellxgene_census.open_soma(census_version=census_version) as census:
            # Retrieve the organism data safely
            # Use .get() for the organism dictionary access to provide a default if the organism key is missing
            organism_data = census["census_data"].get(census_organism)
            if not organism_data:
                logging.warning(f"Organism '{census_organism}' not found in census.")
                return None

            obs_reader = organism_data.obs

            # Check if the ontology column exists using keys()
            if ontology_column_name not in obs_reader.keys():
                logging.warning(f"Column '{ontology_column_name}' not found in census.")
                return None

            # Read the specified column, concatenate chunks, and convert to a pandas DataFrame
            df = (
                obs_reader.read(column_names=[ontology_column_name])
                .concat()
                .to_pandas()
            )

            # Return the unique, non-null terms (excluding "unknown")
            return set(df[ontology_column_name].dropna().unique()) - {"unknown"}

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
            "cell_type": "CL_",  # Cell Ontology
            "tissue": "UBERON_",  # Uberon
            "tissue_general": "UBERON_",  # Uberon (this category is supported by CxG census so users might use it instead of tissue)
            "disease": "MONDO_",  # MONDO Disease Ontology
            "development_stage": None,  # Dynamically determined based on organism
        }

    def get_ontology_id_from_label(self, label, category, organism=None):
        """
        Resolves a label to a CL or UBERON ID based on category.

        Parameters:
        - label (str): The label to resolve (e.g., "neuron").
        - category (str): The category of the label (e.g., "cell_type" or "tissue").
        - organism (str): The organism (e.g., "Homo sapiens", "Mus musculus") for development_stage.

        Returns:
        - str: The corresponding ontology ID (e.g., "CL:0000540") or None if not found.
        """

        # Normalize the organism parameter
        normalized_organism = None
        if organism:
            normalized_organism = organism.replace(
                "_", " "
            ).title()  # "homo_sapiens" -> "Homo Sapiens"

        # Determine the prefix for the given category
        if category == "development_stage":
            if not normalized_organism:  # Check the normalized version
                raise ValueError(
                    "The 'organism' parameter is required for 'development_stage'."
                )
            if normalized_organism == "Homo Sapiens":  # Comparison with space
                prefix = "HsapDv_"
            elif normalized_organism == "Mus Musculus":
                prefix = "MmusDv_"
            else:
                raise ValueError(
                    f"Unsupported organism '{normalized_organism}' for development_stage."
                )
        else:
            prefix = self.prefix_map.get(category)

        if not prefix:
            raise ValueError(
                f"Unsupported category '{category}'. Supported categories are: {list(self.prefix_map.keys())}"
            )

        # Construct the SPARQL query to resolve the label to an ontology ID. This sparql query takes into account synonyms
        sparql_query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

        SELECT DISTINCT ?term
        WHERE {{
            # Match main label
            {{
                ?term rdfs:label ?label .
                FILTER(LCASE(?label) = LCASE("{label}"))
            }}
            UNION
            # Match exact synonyms
            {{
                ?term oboInOwl:hasExactSynonym ?synonym .
                FILTER(LCASE(?synonym) = LCASE("{label}"))
            }}
            UNION
            # Match related synonyms
            {{
                ?term oboInOwl:hasRelatedSynonym ?synonym .
                FILTER(LCASE(?synonym) = LCASE("{label}"))
            }}
            UNION
            # Match broad synonyms
            {{
                ?term oboInOwl:hasBroadSynonym ?synonym .
                FILTER(LCASE(?synonym) = LCASE("{label}"))
            }}
            UNION
            # Match narrow synonyms
            {{
                ?term oboInOwl:hasNarrowSynonym ?synonym .
                FILTER(LCASE(?synonym) = LCASE("{label}"))
            }}
            FILTER(STRSTARTS(STR(?term), "http://purl.obolibrary.org/obo/{prefix}"))
        }}
        LIMIT 1
        """

        # Execute the query and process the results
        results = self.sparql_client.query(sparql_query)
        if results:
            logging.info(
                f"Ontology ID for label '{label}' found: {results[0]['term']['value']}"
            )
            # Extract and return the ontology ID in the desired format (ie., CL:0000540)
            return results[0]["term"]["value"].split("/")[-1].replace("_", ":")
        else:
            logging.warning(
                f"No ontology ID found for label '{label}' in category '{category}'."
            )
            return None

    def get_subclasses(self, term, category="cell_type", organism=None):
        """
        Extracts subclasses and part-of relationships for the given ontology term (CL or UBERON IDs or labels).

        Parameters:
        - term (str): The ontology term (label or ID).
        - category (str): The category of the term (e.g., "cell_type" or "tissue", "development_stage").

        Returns:
        - list: A list of dictionaries with subclass IDs and labels for ontology terms.
        """

        # Normalize the organism parameter
        normalized_organism = None
        if organism:
            normalized_organism = organism.replace(
                "_", " "
            ).title()  # "homo_sapiens" -> "Homo Sapiens"

        if category == "development_stage":
            if not normalized_organism:
                raise ValueError(
                    "The 'organism' parameter is required for 'development_stage'."
                )
            if normalized_organism == "Homo Sapiens":  # Comparison with space
                iri_prefix = "HsapDv"
            elif normalized_organism == "Mus Musculus":
                iri_prefix = "MmusDv"
            else:
                raise ValueError(
                    f"Unsupported organism '{normalized_organism}' for 'development_stage'."
                )
        else:
            iri_prefix = self.prefix_map.get(category)
            if not iri_prefix:
                raise ValueError(
                    f"Unsupported category '{category}'. Supported categories are: {list(self.prefix_map.keys())}"
                )
            iri_prefix = iri_prefix.rstrip("_")

        # Convert label to ontology ID if needed
        if not term.startswith(f"{iri_prefix}:"):
            # If category is development_stage and term already looks like a dev stage ID, don't try to resolve it as a label.
            if category == "development_stage" and (
                term.startswith("MmusDv:") or term.startswith("HsapDv:")
            ):
                pass
            else:
                term = self.get_ontology_id_from_label(
                    term, category, organism=organism
                )
            if not term:
                return []

        # Construct the SPARQL query to find subclasses and part-of relationships for a given term
        sparql_query = f"""
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?term (STR(?term_label) as ?label)
        WHERE {{
        VALUES ?inputTerm {{ obo:{term.replace(":", "_")} }}

        {{
            ?term rdfs:subClassOf ?inputTerm .
        }}
        UNION
        {{
            ?term obo:BFO_0000050 ?inputTerm .
        }}

        ?term rdfs:label ?term_label .
        FILTER(STRSTARTS(STR(?term), "http://purl.obolibrary.org/obo/{iri_prefix}_"))
        }}
        LIMIT 1000
        """

        # Execute the query and process the results
        results = self.sparql_client.query(sparql_query)
        if results:
            logging.info(f"Subclasses for term '{term}' retrieved successfully.")
        else:
            logging.warning(f"No subclasses found for term '{term}'.")
        return (
            [
                {
                    "ID": r["term"]["value"].split("/")[-1].replace("_", ":"),
                    "Label": r["label"]["value"],
                }
                for r in results
            ]
            if results
            else []
        )


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
            # Resolve the label to its ontology ID
            parent_id = extractor.get_ontology_id_from_label(term, category, organism)
            if not parent_id:
                logging.warning(f"Could not resolve label '{term}' to an ontology ID.")
                expanded_label_terms[category].append(term)  # Keep the original label
                continue

            # Fetch subclasses for the parent ID
            subclasses = extractor.get_subclasses(parent_id, category, organism)

            # Extract IDs and labels from the subclasses
            child_ids = [sub["ID"] for sub in subclasses]
            child_labels = [sub["Label"] for sub in subclasses]

            # Filter IDs against the census if applicable
            if census_version:
                logging.info(
                    f"Filtering subclasses for label '{term}' based on CellxGene Census..."
                )
                filtered_ids = _filter_ids_against_census(
                    ids_to_filter=[parent_id] + child_ids,  # Include the parent ID
                    census_version=census_version,
                    organism=organism,
                    ontology_column_name=f"{category}_ontology_term_id",
                )
                # Keep only labels corresponding to filtered IDs
                filtered_labels = [
                    sub["Label"] for sub in subclasses if sub["ID"] in filtered_ids
                ]
                if parent_id in filtered_ids:
                    filtered_labels.append(
                        term
                    )  # Add the original label if the parent ID survived
                child_labels = filtered_labels

            # store the expanded and filtered labels
            if child_labels:
                expanded_label_terms[category].extend(sorted(set(child_labels)))

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
                    ids_to_filter=[ontology_id] + child_ids,  # Include the parent ID
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
