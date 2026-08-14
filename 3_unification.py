import os
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, OWL

# Project namespaces (UPDATED TO OFFICIAL DISI NAMESPACE)
APP = Namespace("http://knowdive.disi.unitn.it/trentino-app#")
OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
ETYPE = Namespace("http://knowdive.disi.unitn.it/etype#")

def main():
    print("Loading Knowledge Graphs...")
    personal_kg = Graph()
    reference_kg = Graph()
    
    # Load input (Personal Context) and OSM graph (Reference Context)
    if os.path.exists("tourist_profile.ttl") and os.path.exists("source_kg.nt"):
        personal_kg.parse("tourist_profile.ttl", format="turtle")
        reference_kg.parse("source_kg.nt", format="nt")
    else:
        print("[ERROR] Input graphs missing. Check tourist_profile.ttl and source_kg.nt")
        return

    # Initialize unified final graph
    unified_kg = Graph()
    unified_kg += personal_kg
    unified_kg += reference_kg

    # Bind prefixes for clean output serialization
    unified_kg.bind("app", APP)
    unified_kg.bind("owl", OWL)
    unified_kg.bind("osm_ont", OSM_ONT)
    unified_kg.bind("etype", ETYPE)

    print("Executing Entity Resolution (Identifying Set: Name + Type)...")
    match_count = 0

    # 1. Iterate over all typed nodes in Personal Context
    for personal_node, _, p_type in personal_kg.triples((None, RDF.type, None)):
        
        # Filter for target domain types only (e.g., etype:Restaurant)
        if not str(p_type).startswith(str(ETYPE)):
            continue
            
        # 2. Extract personal node name
        personal_name = None
        for _, p, o in personal_kg.triples((personal_node, None, None)):
            # Flexible search for 'name' property (e.g., schema:name)
            if "name" in str(p).lower():
                personal_name = str(o).lower().strip()
                break
        
        if not personal_name:
            continue

        # 3. IDENTIFYING SET LOGIC (Case-insensitive Type + Elastic Name)
        # Match Reference Context (OSM) node with SAME TYPE
        for osm_node, _, osm_type in reference_kg.triples((None, RDF.type, None)):
            
            # Robust type checking (case-insensitive to prevent mismatch errors)
            if str(osm_type).lower() != str(p_type).lower():
                continue
            
            # ... AND MATCHING NAME
            for _, _, osm_name_literal in reference_kg.triples((osm_node, OSM_ONT.name, None)):
                osm_name = str(osm_name_literal).lower().strip()
                
                # Elastic Match: True if one string is fully contained within the other
                if personal_name in osm_name or osm_name in personal_name:
                    # IDENTIFYING SET FULL MATCH! Generate owl:sameAs bridge
                    unified_kg.add((personal_node, OWL.sameAs, osm_node))
                    match_count += 1
                    print(f"[+] Bridge created: {personal_node.split('#')[-1]} <sameAs> {osm_node.split('/')[-1]}")
                    break

    out_file = "unified_kg.ttl"
    unified_kg.serialize(destination=out_file, format="turtle")
    print(f"\nUnification completed. {match_count} owl:sameAs bridges generated. Saved to {out_file}")

if __name__ == "__main__":
    main()