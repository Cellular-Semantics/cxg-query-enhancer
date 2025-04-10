from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os
import re
import logging

# Configure logging
# logging.basicConfig(
# level=logging.INFO,
# format="%(asctime)s - %(levelname)s - %(message)s",
# handlers=[logging.StreamHandler()],
# )


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

    def __init__(self, sparql_client, root_ids, output_dir="ontology_results"):
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

        # Map of supported categories to their ontology prefixes
        self.prefix_map = {
            "cell_type": "CL_",  # Cell Ontology
            "tissue": "UBERON_",  # Uberon
            "disease": "MONDO_",  # MONDO Disease Ontology
            "developmental_stage": None,  # Dynamically determined based on organism
        }

    def get_ontology_id_from_label(self, label, category, organism=None):
        """
        Resolves a label to a CL or UBERON ID based on category.

        Parameters:
        - label (str): The label to resolve (e.g., "neuron").
        - category (str): The category of the label (e.g., "cell_type" or "tissue").
        - organism (str): The organism (e.g., "Homo sapiens", "Mus musculus") for developmental_stage.

        Returns:
        - str: The corresponding ontology ID (e.g., "CL:0000540") or None if not found.
        """

        # Normalize the organism parameter
        if organism:
            organism = organism.title()

        # Determine the prefix for the given category
        if category == "developmental_stage":
            if organism == "Homo Sapiens":
                prefix = "HsapDv_"
            elif organism == "Mus Musculus":
                prefix = "MmusDv_"
            else:
                raise ValueError(
                    f"Unsupported organism '{organism}' for developmental_stage."
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
        - category (str): The category of the term (e.g., "cell_type" or "tissue", "developmental_stage").

        Returns:
        - list: A list of dictionaries with subclass IDs and labels for ontology terms.
        """

        # Normalize the organism parameter
        if organism:
            organism = organism.title()

        # Determine the prefix for the given category
        if category == "developmental_stage":
            if not organism:
                raise ValueError(
                    "The 'organism' parameter is required for 'developmental_stage'."
                )
            if organism == "Homo Sapiens":
                iri_prefix = "HsapDv"
            elif organism == "Mus Musculus":
                iri_prefix = "MmusDv"
            else:
                raise ValueError(
                    f"Unsupported organism '{organism}' for 'developmental_stage'."
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
            term = self.get_ontology_id_from_label(term, category, organism=organism)
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


def obs_close(query_filter, categories=["cell_type"], organism=None):
    """
    Rewrites the query filter to include ontology closure.

    Parameters:
    - query_filter (str): The original query filter string.
    - categories (list): List of categories to apply closure to (default: ["cell_type"]).

    Returns:
    - str: The rewritten query filter with expanded terms based on ontology closure.
    Example: "cell_type in ['neuron', 'pyramidal neuron', 'microglial cell'] and tissue in ['kidney', 'renal cortex']"
    """

    if "developmental_stage" in categories and not organism:
        raise ValueError(
            "The 'organism' parameter is required for the 'developmental_stage' category."
        )

    # Dictionary to store terms to expand for each category
    # Example: {"cell_type": ["neuron", "microglial cell"], "tissue": ["kidney"]}
    terms_to_expand = {}  # {category: [terms]}
    ids_to_expand = {}  # {category: [ontology IDs]}

    # Extract terms for each category from the query filter
    for category in categories:
        # Use regex to find terms in the format: category in [<terms>]
        match_labels = re.search(rf"{category} in \[(.*?)\]", query_filter)
        if match_labels:
            # Split the matched terms and clean up quotes and whitespace
            terms = [
                term.strip().strip("'\"") for term in match_labels.group(1).split(",")
            ]
            terms_to_expand[category] = terms

        # Match ontology IDs (e.g., cell_type_ontology_term_id in [...])
        match_ids = re.search(
            rf"{category}_ontology_term_id in \[(.*?)\]", query_filter
        )
        if match_ids:
            # Split the matched IDs and clean up quotes and whitespace
            ids = [term.strip().strip("'\"") for term in match_ids.group(1).split(",")]
            ids_to_expand[category] = ids

    # Initialize the OntologyExtractor only if needed
    if terms_to_expand or ids_to_expand:
        extractor = OntologyExtractor(SPARQLClient(), [])

    # Dictionary to store expanded terms for each category
    expanded_terms = {}

    # Iterate over each category and expand terms for ontology labels
    for category, terms in terms_to_expand.items():
        expanded_terms[category] = []
        for term in terms:
            # Fetch subclasses for the term using the OntologyExtractor
            if category == "developmental_stage":
                # Pass the organism parameter for developmental_stage
                subclasses = extractor.get_subclasses(term, category, organism=organism)
            else:
                subclasses = extractor.get_subclasses(term, category)

            # Extract labels from the subclasses
            labels = [sub["Label"] for sub in subclasses]
            if labels:
                expanded_terms[category].extend(labels)
            else:
                expanded_terms[category].append(term)

    # Expand terms for ontology IDs
    for category, ids in ids_to_expand.items():
        if category not in expanded_terms:
            expanded_terms[category] = []
        for ontology_id in ids:
            # Fetch subclasses for the ontology ID using the OntologyExtractor
            subclasses = extractor.get_subclasses(ontology_id, category)

            # Extract IDs from the subclasses
            subclass_ids = [sub["ID"] for sub in subclasses]
            if subclass_ids:
                expanded_terms[category].extend(subclass_ids)
            else:
                expanded_terms[category].append(ontology_id)

    # Rewrite the query filter with the expanded terms
    for category, terms in expanded_terms.items():
        # Remove duplicates and sort the terms in alphabetical order for consistency
        unique_terms = sorted(set(terms))
        # Convert the terms back into the format: ['term1', 'term2', ...]
        expanded_terms_str = ", ".join(f"'{t}'" for t in unique_terms)
        # Replace the original terms in the query filter with the expanded terms
        query_filter = re.sub(
            rf"{category}(_ontology_term_id)? in \[.*?\]",
            f"{category} in [{expanded_terms_str}]",
            query_filter,
        )

    # Log the successful rewriting of the query filter
    logging.info("Query filter rewritten successfully.")
    return query_filter
