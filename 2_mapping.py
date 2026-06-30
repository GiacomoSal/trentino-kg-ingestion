import pickle
import re
import yaml
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD

# Namespaces
OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
OSM_KG = Namespace("http://osm.kg/")
ETYPE = Namespace("http://teleology.kg/etype#")

def load_mapping_rules(filepath="mapping.yaml"):
    """Loads semantic mapping rules from YAML configuration."""
    with open(filepath, "r") as f:
        config = yaml.safe_load(f)
    return config.get("mappings", [])

def main():
    print("Loading raw data from cache...")
    try:
        with open("raw_osm_data.pkl", "rb") as f:
            raw_data = pickle.load(f)
    except FileNotFoundError:
        print("[ERROR] raw_osm_data.pkl not found. Run 1_extraction.py first.")
        return

    # Load dynamic mapping rules
    mapping_rules = load_mapping_rules()

    kg = Graph()
    kg.bind("osm_ont", OSM_ONT)
    kg.bind("osm", OSM_KG)
    kg.bind("etype", ETYPE)

    print("Generating Source KG with dynamic mapping...")

    # Handle both DataFrame and dict structures
    if hasattr(raw_data, "iterrows"):
        element_list = [row.to_dict() | {"_index_id": index} for index, row in raw_data.iterrows()]
    elif isinstance(raw_data, dict):
        element_list = list(raw_data.values())
    else:
        element_list = raw_data

    for element in element_list:
        raw_id_str = str(element.get("osmid", element.get("id", element.get("_index_id", ""))))
        
        # Extract numerical ID
        numeri = re.findall(r'\d+', raw_id_str)
        if not numeri:
            continue
        clean_osm_id = numeri[-1] 

        tags = element.get("tags", element)
        node_uri = URIRef(f"http://osm.kg/{clean_osm_id}")
        
        # Base entity typing
        kg.add((node_uri, RDF.type, OSM_ONT.openstreetmap_place))
        
        # Dynamic POI mapping based on mapping.yaml
        for rule in mapping_rules:
            target_class_full = rule.get("target_class", "")
            filters = rule.get("osm_filter", {})

            # Check if any OSM tag matches the defined filters for this target class
            rule_matched = False
            for osm_key, allowed_values in filters.items():
                if tags.get(osm_key) in allowed_values:
                    rule_matched = True
                    break
            
            if rule_matched:
                # Extract class name (e.g., 'etype:restaurant' -> 'restaurant')
                class_name = target_class_full.split(":")[-1]
                kg.add((node_uri, RDF.type, ETYPE[class_name]))
                break # Avoid multiple assignments if one rule perfectly matches
        
        # Casting basic properties
        kg.add((node_uri, OSM_ONT.osm_id, Literal(clean_osm_id, datatype=XSD.integer)))
        
        if "name" in tags and isinstance(tags["name"], str):
            kg.add((node_uri, OSM_ONT.name, Literal(tags["name"], datatype=XSD.string)))

    out_file = "source_kg.nt"
    kg.serialize(destination=out_file, format="nt", encoding="utf-8")
    print(f"Process completed. Graph saved to {out_file}")

if __name__ == "__main__":
    main()