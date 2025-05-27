from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging
import cellxgene_census


def _filter_ids_against_census(
    ids_to_filter: list[str],
    census_version: str,
    organism: str,
    ontology_column_name: str = "cell_type_ontology_term_id",
) -> list[str]:
    """
    Filters a list of ontology IDs against those present in a specific CellXGene Census version.

    Parameters:
    - ids_to_filter (list[str]): List of ontology IDs to filter.
    - census_version (str): Version of the CellXGene Census to use.
    - organism (str): Organism to query in the census (e.g., "homo_sapiens").
    - ontology_column_name (str): Column name for ontology IDs in the census (e.g.cell_type_ontology_term_id, tissue_type_ontology_term_id).

    Returns:
    - list[str]: Filtered list of IDs present in the Census, or the original list if filtering fails.
    """
    if not ids_to_filter:
        logging.info("No IDs provided to filter; returning empty list.")
        return []

    census_organism = organism.replace(" ", "_").lower()

    # It's good practice to add a general logging statement here about what's being attempted.
    logging.info(
        f"Attempting to filter IDs against census version '{census_version}' for organism "
        f"'{census_organism}', column '{ontology_column_name}'."
    )

    try:
        with cellxgene_census.open_soma(census_version=census_version) as census:
            # Use .get() for the organism dictionary access to provide a default if the organism key is missing
            # and chain .get('obs') to handle if the organism itself is missing or doesn't have 'obs'.
            organism_data = census["census_data"].get(census_organism)
            if not organism_data:
                logging.warning(
                    f"Organism data for '{census_organism}' not found in census version '{census_version}'. "
                    "Returning original IDs."
                )
                return ids_to_filter

            obs_reader = organism_data.obs

            # Use keys() to check for column names
            if ontology_column_name not in obs_reader.keys():
                logging.warning(
                    f"Column '{ontology_column_name}' not found in census for organism '{census_organism}'. Returning original IDs."
                )
                return ids_to_filter

            # Fetch the specific column as a pandas DataFrame
            census_terms = (
                obs_reader.read(column_names=[ontology_column_name])
                .concat()
                .to_pandas()
            )

            # Check if the DataFrame is empty
            if census_terms.empty:
                logging.warning(
                    f"No terms found in census for '{census_organism}', column '{ontology_column_name}'. "
                )
                return []

            # Extract the specific column as a Series and get unique terms
            census_terms = census_terms[ontology_column_name]
            census_ontology_terms = set(census_terms.dropna().unique()) - {"unknown"}
            # Perform the intersection between the input IDs (ids_to_filter) and the census terms
            # sorts filtered IDs for consistent output
            filtered_ids = sorted(list(set(ids_to_filter) & census_ontology_terms))

            logging.info(
                f"{len(filtered_ids)} of {len(set(ids_to_filter))} unique input IDs matched in census."
            )  # Use set for accurate count of unique inputs
            return filtered_ids

    except Exception as e:
        logging.error(
            f"Error accessing CellXGene Census or processing data: {e}. Returning original IDs."
        )
        return ids_to_filter


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

    def __init__(
        self, sparql_client, root_ids, output_dir="ontology_results", prefix_map=None
    ):
        """
        Initializes the ontology extractor.

        Parameters:
        - sparql_client (SPARQLClient): The SPARQL client instance.
        - root_ids (list): List of root ontology IDs to extract subclasses from.
        - output_dir (str): Directory to store extracted results.
        """
        self.sparql_client = sparql_client
        self.root_ids = root_ids
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.prefix_map = prefix_map or {
            "cell_type": "CL_",  # Cell Ontology
            "tissue": "UBERON_",  # Uberon
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

    def extract_and_save_hierarchy(self):
        """
        Extracts hierarchical levels separately and saves them in separate CSV files.
        """
        for root_id in self.root_ids:
            logging.info(f"Extracting subclasses for {root_id}...")
            subclasses = self.get_subclasses(root_id)

            if not subclasses:
                logging.warning(f"No subclasses found for {root_id}. Skipping...")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(subclasses)

            # Save to CSV
            output_file = os.path.join(
                self.output_dir, f"{root_id.replace(':', '_')}_hierarchy.csv"
            )
            df.to_csv(output_file, index=False)

            logging.info(f"Saved hierarchy for {root_id} to {output_file}")


def enhance(query_filter, categories=None, organism=None, census_version="latest"):
    """
    Rewrites the query filter to include ontology closure and filters IDs against the CellxGene Census.

    Parameters:
    - query_filter (str): The original query filter string.
    - categories (list): List of categories to apply closure to (default: ["cell_type"]).
    - organism (str): The organism to query in the census (e.g., "homo_sapiens").
    - census_version (str): Version of the CellxGene Census to use for filtering IDs.

    Returns:
    - str: The rewritten query filter with expanded terms based on ontology closure.
    """

    if categories is None:
        matches = re.findall(r"(\w+?)(?:_ontology_term_id)?\s+in\s+\[", query_filter)
        categories = list(set(matches))
        logging.info(f"Auto-detected categories: {categories}")

    # Add a check here if you want to ensure 'categories' is not empty
    # or if you want to filter it against 'known_ontology_fields'
    if not categories:
        logging.info("No categories to process. Returning original filter.")
        return query_filter

    if "development_stage" in categories and not organism:
        raise ValueError(
            "The 'organism' parameter is required for the 'development_stage' category."
        )

    # Ensure organism is valid for filtering
    if not organism:
        organism = "homo_sapiens"  # Default to "homo_sapiens" if not provided

    # Dictionaries to store terms and IDs to expand for each category
    terms_to_expand = {}  # {category: [terms]}
    ids_to_expand = {}  # {category: [ontology IDs]}

    # Extract terms and IDs for each category from the query filter
    for category in categories:
        # Match terms (e.g., "cell_type in ['neuron', 'microglial cell']")
        match_labels = re.search(rf"{category} in \[(.*?)\]", query_filter)
        if match_labels:
            terms = [
                term.strip().strip("'\"") for term in match_labels.group(1).split(",")
            ]
            terms_to_expand[category] = terms

        # Match ontology IDs (e.g., "cell_type_ontology_term_id in ['CL:0000540']")
        match_ids = re.search(
            rf"{category}_ontology_term_id in \[(.*?)\]", query_filter
        )
        if match_ids:
            ids = [term.strip().strip("'\"") for term in match_ids.group(1).split(",")]
            ids_to_expand[category] = ids

    # Initialize the OntologyExtractor only if needed
    if terms_to_expand or ids_to_expand:
        extractor = OntologyExtractor(SPARQLClient(), [])

    # Dictionary to store expanded terms for each category
    expanded_terms = {}

    # Process label-based queries
    for category, terms in terms_to_expand.items():
        expanded_terms[category] = []
        for term in terms:
            # Resolve the label to its ontology ID
            parent_id = extractor.get_ontology_id_from_label(term, category, organism)
            if not parent_id:
                logging.warning(f"Could not resolve label '{term}' to an ontology ID.")
                expanded_terms[category].append(term)  # Keep the original label
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

            # Add filtered labels to expanded terms
            if child_labels:
                expanded_terms[category].extend(list(set(child_labels)))

    # Process ID-based queries
    for category, ids in ids_to_expand.items():
        if category not in expanded_terms:
            expanded_terms[category] = []
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
                expanded_terms[category].extend(list(set(child_ids)))

    # Rewrite the query filter with the expanded terms
    for category, terms in expanded_terms.items():
        # Remove duplicates and sort the terms in alphabetical order for consistency
        unique_terms = sorted(set(terms))

        # Determine if the original query used labels or IDs
        if category in terms_to_expand:
            query_type = category  # Label-based query
        else:
            query_type = f"{category}_ontology_term_id"  # ID-based query

        # Convert the terms back into the format: ['term1', 'term2', ...]
        expanded_terms_str = ", ".join(f"'{t}'" for t in unique_terms)

        # Replace the original terms in the query filter with the expanded terms
        query_filter = re.sub(
            rf"{query_type} in \[.*?\]",
            f"{query_type} in [{expanded_terms_str}]",
            query_filter,
        )

    logging.info("Query filter rewritten successfully.")
    return query_filter
