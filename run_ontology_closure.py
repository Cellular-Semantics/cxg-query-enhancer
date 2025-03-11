# run_closure.py
from ontology_closure import OntologyExtractor, SPARQLClient

if __name__ == "__main__":
    # Define the list of root CL IDs for different hierarchical levels
    root_cl_ids = [
        "CL:0000066",  # Epithelial cell
        "CL:0002076",  # Endo-epithelial cell
    ]

    # Initialize SPARQL Client
    sparql_client = SPARQLClient()

    # Initialize Ontology Extractor
    extractor = OntologyExtractor(sparql_client, root_cl_ids)

    # Extract and save results
    extractor.extract_and_save_hierarchy()

    print("✅ Ontology closure extraction completed successfully!")
