import pickle
import re
import yaml
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD

# Official KnowDive namespaces
OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
OSM_KG = Namespace("http://osm.kg/")
ETYPE = Namespace("http://knowdive.disi.unitn.it/etype#")

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

    mapping_rules = load_mapping_rules()

    kg = Graph()
    kg.bind("osm_ont", OSM_ONT)
    kg.bind("osm", OSM_KG)
    kg.bind("etype", ETYPE)

    print("Generating Source KG with dynamic mapping and properties...")

    if hasattr(raw_data, "iterrows"):
        element_list = [row.to_dict() | {"_index_id": index} for index, row in raw_data.iterrows()]
    elif isinstance(raw_data, dict):
        element_list = list(raw_data.values())
    else:
        element_list = raw_data

    for element in element_list:
        tags = element.get("tags", {})
        if not tags:
            tags = element

        # LA TUA LOGICA ORIGINALE PER GLI ID (Estrarre solo i numeri!)
        raw_id_str = str(element.get("osmid", element.get("id", element.get("_index_id", ""))))
        numeri = re.findall(r'\d+', raw_id_str)
        if not numeri:
            continue
        clean_osm_id = numeri[-1]
            
        node_uri = URIRef(f"http://osm.kg/{clean_osm_id}")
        kg.add((node_uri, RDF.type, OSM_ONT.openstreetmap_place))
        
        # 1. Type Mapping (Classes)
        rule_matched = False
        for rule in mapping_rules:
            target_class_full = rule.get("target_class", "")
            filters = rule.get("osm_filter", {})

            for osm_key, allowed_values in filters.items():
                if tags.get(osm_key) in allowed_values:
                    rule_matched = True
                    break
            
            if rule_matched:
                class_name = target_class_full.split(":")[-1]
                kg.add((node_uri, RDF.type, ETYPE[class_name]))
                break 
        
        # 2. Property Mapping (Extracting physical properties required for evaluation)
        kg.add((node_uri, OSM_ONT.osm_id, Literal(clean_osm_id, datatype=XSD.integer)))
        
        # Name
        if "name" in tags and isinstance(tags["name"], str):
            kg.add((node_uri, OSM_ONT.name, Literal(tags["name"], datatype=XSD.string)))
            
        # City
        if "addr:city" in tags:
            kg.add((node_uri, ETYPE.has_city, Literal(tags["addr:city"], datatype=XSD.string)))
            
        # Full address (Street + Housenumber)
        street = tags.get("addr:street", "")
        housenumber = tags.get("addr:housenumber", "")
        if street:
            full_address = f"{street} {housenumber}".strip()
            kg.add((node_uri, ETYPE.has_address, Literal(full_address, datatype=XSD.string)))
            
        # Phone number
        phone = tags.get("phone", tags.get("contact:phone", ""))
        if phone:
            kg.add((node_uri, ETYPE.has_phone, Literal(phone, datatype=XSD.string)))

        # Spatial coordinates (if available from the dataframe)
        if "lat" in element and "lon" in element:
            kg.add((node_uri, ETYPE.has_latitude, Literal(element["lat"], datatype=XSD.float)))
            kg.add((node_uri, ETYPE.has_longitude, Literal(element["lon"], datatype=XSD.float)))

    out_file = "source_kg.nt"
    kg.serialize(destination=out_file, format="nt", encoding="utf-8")
    print(f"Source KG successfully generated. Saved to {out_file}")

if __name__ == "__main__":
    main()