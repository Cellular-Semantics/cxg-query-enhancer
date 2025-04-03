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
            return None


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

    def get_ontology_id_from_label(self, label, category):
        """
        Resolves a label to a CL or UBERON ID based on category.

        Parameters:
        - label (str): The label to resolve (e.g., "neuron").
        - category (str): The category of the label (e.g., "cell_type" or "tissue").

        Returns:
        - str: The corresponding ontology ID (e.g., "CL:0000540") or None if not found.
        """
        prefix_map = {"cell_type": "CL_", "tissue": "UBERON_"}
        prefix = prefix_map.get(category)
        if not prefix:
            raise ValueError(f"Unsupported category '{category}'")

        # Construct the SPARQL query to resolve the label to an ontology ID
        sparql_query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX obo: <http://purl.obolibrary.org/obo/>

        SELECT DISTINCT ?term
        WHERE {{
            ?term rdfs:label ?label .
            FILTER(LCASE(?label) = LCASE("{label}"))
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

    def get_subclasses(self, term, category="cell_type"):
        """
        Extracts subclasses and part-of relationships for the given ontology term (CL or UBERON IDs or labels).

        Parameters:
        - term (str): The ontology term (label or ID).
        - category (str): The category of the term (e.g., "cell_type" or "tissue").

        Returns:
        - list: A list of dictionaries with subclass IDs and labels for ontology terms.
        """
        iri_prefix_map = {"cell_type": "CL", "tissue": "UBERON"}
        iri_prefix = iri_prefix_map.get(category)
        if not iri_prefix:
            raise ValueError(f"Unsupported category '{category}'")

        # Convert label to ontology ID if needed
        if not term.startswith(f"{iri_prefix}:"):
            term = self.get_ontology_id_from_label(term, category)
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


def obs_close(query_filter, categories=["cell_type"]):
    """
    Rewrites the query filter to include ontology closure.

    Parameters:
    - query_filter (str): The original query filter string.
    - categories (list): List of categories to apply closure to (default: ["cell_type"]).

    Returns:
    - str: The rewritten query filter with expanded terms based on ontology closure.
    Example: "cell_type in ['neuron', 'pyramidal neuron', 'microglial cell'] and tissue in ['kidney', 'renal cortex']"
    """

    # Dictionary to store terms to expand for each category
    # Example: {"cell_type": ["neuron", "microglial cell"], "tissue": ["kidney"]}
    terms_to_expand = {}  # {category: [terms]}

    # Extract terms for each category from the query filter
    for category in categories:
        # Use regex to find terms in the format: category in [<terms>]
        match = re.search(rf"{category} in \[(.*?)\]", query_filter)
        if match:
            # Split the matched terms and clean up quotes and whitespace
            terms = [term.strip().strip("'\"") for term in match.group(1).split(",")]
            terms_to_expand[category] = terms

    extractor = OntologyExtractor(SPARQLClient(), [])
    expanded_terms = {}

    # Iterate over each category and its terms to expand them
    for category, terms in terms_to_expand.items():
        expanded_terms[category] = []
        for term in terms:
            # Fetch subclasses for the term using the OntologyExtractor
            subclasses = extractor.get_subclasses(term, category)
            # Extract labels from the subclasses
            labels = [sub["Label"] for sub in subclasses]
            if labels:
                expanded_terms[category].extend(labels)
            else:
                expanded_terms[category].append(term)

    # Rewrite the query filter with the expanded terms
    for category, terms in expanded_terms.items():
        # Remove duplicates and sort the terms in alphabetical order for consistency
        unique_terms = sorted(set(terms))
        # Convert the terms back into the format: ['term1', 'term2', ...]
        expanded_terms_str = ", ".join(f"'{t}'" for t in unique_terms)
        # Replace the original terms in the query filter with the expanded terms
        query_filter = re.sub(
            rf"{category} in \[.*?\]",
            f"{category} in [{expanded_terms_str}]",
            query_filter,
        )

    # Log the successful rewriting of the query filter
    logging.info("Query filter rewritten successfully.")
    return query_filter
