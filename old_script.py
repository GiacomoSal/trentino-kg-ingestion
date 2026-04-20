import yaml
import osm2kg as og
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF
import requests

# Configurazione IP e lettura del mapping YAML
GRAPHDB_URL = "http://192.168.178.33:7200/repositories/Mio_Reference_KG/statements"

print("Lettura del mapping.yaml...")
with open("mapping.yaml", "r") as file:
    config = yaml.safe_load(file)

namespaces = {prefix: Namespace(uri) for prefix, uri in config['namespaces'].items()}
ETYPE = namespaces['etype']
OSM = namespaces['osm']
APP = namespaces['app']

tags_to_download = {}
for mapping in config['mappings']:
    for osm_key, osm_values in mapping['osm_filter'].items():
        if osm_key not in tags_to_download:
            tags_to_download[osm_key] = []
        tags_to_download[osm_key].extend(osm_values)

# Estrazione dati da OSM via osm2kg (raggio 2km da Trento centro)
centro_trento = (46.0678, 11.1211)
raggio_metri = 2000

print(f"Scaricamento dati OSM per i tag: {list(tags_to_download.keys())}...")
gdf = og.feature.features_from_point(centro_trento, tags=tags_to_download, dist=raggio_metri)
gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})

# Generazione Entity Graph (EG)
print(f"Generazione RDF per {len(gdf)} entita' trovate...")
kg = Graph()
for prefix, ns in namespaces.items():
    kg.bind(prefix, ns)

for index, row in gdf.iterrows():
    osmid = index[1] 
    entity_uri = ETYPE[f"node/{osmid}"]
    classe_uri = OSM.Feature # Fallback base
    
    # Mapping dinamico basato sul file YAML
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
            
    # Aggiunta triple all'Entity Graph
    kg.add((entity_uri, RDF.type, classe_uri))
    kg.add((entity_uri, OSM.name, Literal(row['name'])))

# Inserimento Personal Context di test
turista_uri = APP["Tourist/Giacomo"]
kg.add((turista_uri, RDF.type, APP.Tourist))
primo_food = next((ETYPE[f"node/{idx[1]}"] for idx, r in gdf.iterrows() if r.get('amenity') in ['restaurant', 'pub', 'bar']), None)
if primo_food:
    kg.add((turista_uri, APP.eatsAt, primo_food))

# Caricamento su GraphDB locale
print("Invio Entity Graph a GraphDB...")
response = requests.post(GRAPHDB_URL, data=kg.serialize(format="nt"), headers={'Content-Type': 'application/n-triples'})

if response.status_code == 204:
    print("Successo: Dati caricati correttamente su GraphDB.")
else:
    print(f"Errore HTTP {response.status_code} durante il caricamento.")