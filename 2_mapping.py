import yaml
import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF

print("Caricamento dati grezzi e mapping.yaml...")
gdf = pd.read_pickle("raw_osm_data.pkl")
with open("mapping.yaml", "r") as file:
    config = yaml.safe_load(file)

namespaces = {prefix: Namespace(uri) for prefix, uri in config['namespaces'].items()}
ETYPE, OSM = namespaces['etype'], namespaces['osm']

kg = Graph()
for prefix, ns in namespaces.items():
    kg.bind(prefix, ns)

for index, row in gdf.iterrows():
    osmid = index[1] 
    entity_uri = ETYPE[f"node/{osmid}"]
    classe_uri = OSM.Feature 
    
    # Mapping dinamico (il codice che avevi già)
    for mapping in config['mappings']:
        match_trovato = False
        for osm_key, osm_values in mapping['osm_filter'].items():
            if osm_key in row and row.get(osm_key) in osm_values:
                match_trovato = True
                break
        if match_trovato:
            prefix, class_name = mapping['target_class'].split(':')
            classe_uri = namespaces[prefix][class_name]
            break
            
    kg.add((entity_uri, RDF.type, classe_uri))
    kg.add((entity_uri, OSM.name, Literal(row['name'])))

# Salva il grafo mappato (senza unificazioni turistiche)
kg.serialize(destination="mapped_kg.nt", format="nt", encoding="utf-8")
print("Grafo RDF mappato e salvato in mapped_kg.nt")