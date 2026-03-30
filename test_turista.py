import osm2kg as og
import pandas as pd

# Configurazione IP e puntamento al server GraphDB locale
IP_WINDOWS = "192.168.1.XX" # Sostituire con l'IP effettivo
NOME_REPO_GRAPHDB = "Mio_Reference_KG"

# Sovrascrittura impostazioni di default della libreria base
og.settings.RDF4J_SERVER = f"http://{IP_WINDOWS}:7200/repositories"

# Definizione area di ricerca (Trento centro)
centro = (46.0678, 11.1211) 
raggio_metri = 2000
tags_osm = {"amenity": ["bar", "pub", "restaurant", "cafe"]}

print(f"Scaricamento dati OSM nel raggio di {raggio_metri}m...")
gdf = og.feature.features_from_point(centro, tags=tags_osm, dist=raggio_metri)
gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})

# Test di mapping hardcoded (approccio iniziale, successivamente sostituito da YAML)
def applica_mapping_ontologico(row):
    amenity = row.get('amenity')
    if amenity in ['bar', 'pub']:
        return 'etype:bar_pub'
    elif amenity in ['restaurant', 'cafe']:
        return 'etype:restaurant'
    return 'osm:catering'

gdf['personal_class'] = gdf.apply(applica_mapping_ontologico, axis=1)

print(f"Estrazione completata: {len(gdf)} POI trovati.")
print(gdf[['name', 'amenity', 'personal_class']].head(10))

# N.B. L'invio tramite la funzione originale di osm2kg è commentato
# in quanto genera namespace misti e non separa i livelli iTelos.
# print("Avvio caricamento su GraphDB tramite modulo kg originale...")
# og.kg.process_gdf(gdf, columns=["personal_class"], TARGET_REPO=NOME_REPO_GRAPHDB)