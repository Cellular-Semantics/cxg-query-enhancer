from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import os


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
        self.sparql.setQuery(sparql_query)
        self.sparql.setReturnFormat(JSON)

        try:
            results = self.sparql.query().convert()
            return results["results"]["bindings"]
        except Exception as e:
            print(f"❌ Error executing SPARQL query: {e}")
            return None


class OntologyExtractor:
    """
    Extracts subclasses and part-of relationships from Ubergraph for a given CL ID.
    """

    def __init__(self, sparql_client, root_cl_ids, output_dir="ontology_results"):
        """
        Initializes the ontology extractor.

        Parameters:
        - sparql_client (SPARQLClient): The SPARQL client instance.
        - root_cl_ids (list): List of root CL IDs to extract subclasses from.
        - output_dir (str): Directory to store extracted results.
        """
        self.sparql_client = sparql_client
        self.root_cl_ids = root_cl_ids
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def get_subclasses(self, cl_id):
        """
        Extracts only the direct subclasses and part-of relationships for the given CL ID.

        Parameters:
        - cl_id (str): The CL ID (e.g., "CL_0000000").

        Returns:
        - list: A list of dictionaries with subclass CL IDs and labels.
        """
        sparql_query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?term (STR(?term_label) as ?cell_label)
        WHERE {{
        VALUES ?inputTerm {{ obo:{cl_id.replace(":", "_")} }}  
        
        {{
            ?term rdfs:subClassOf ?inputTerm .
        }}
        UNION
        {{
            ?term obo:BFO_0000050 ?inputTerm . # part_of relationship
        }}

        ?term rdfs:label ?term_label .
        FILTER(STRSTARTS(STR(?term), "http://purl.obolibrary.org/obo/CL_"))
        }}
        LIMIT 1000
        """

        results = self.sparql_client.query(sparql_query)
        subclasses = (
            [
                {
                    "CL_ID": r["term"]["value"].split("/")[-1].replace("_", ":"),
                    "CL_Label": r["cell_label"]["value"],
                }
                for r in results
            ]
            if results
            else []
        )

        return subclasses

    def extract_and_save_hierarchy(self):
        """
        Extracts hierarchical levels separately and saves them in separate CSV files.
        """
        for cl_id in self.root_cl_ids:
            print(f"🔍 Extracting subclasses for {cl_id}...")
            subclasses = self.get_subclasses(cl_id)

            if not subclasses:
                print(f"⚠️ No subclasses found for {cl_id}. Skipping...")
                continue

            # Convert to DataFrame
            df = pd.DataFrame(subclasses)

            # Save to CSV
            output_file = os.path.join(
                self.output_dir, f"{cl_id.replace(':', '_')}_hierarchy.csv"
            )
            df.to_csv(output_file, index=False)

            print(f"✅ Saved hierarchy for {cl_id} to {output_file}")
