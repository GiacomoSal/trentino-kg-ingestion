import osm2kg as og
import pandas as pd

# Estrazione dati da OSM via osm2kg
centro_trento = (46.0678, 11.1211)
raggio_metri = 2000

# Per semplicità, mettiamo i tag hardcoded o li leggiamo da un file config semplice
tags_to_download = {'amenity': ['bar', 'pub', 'restaurant', 'cafe', 'fast_food', 'fuel', 'bicycle_rental'], 
                    'tourism': ['hotel', 'motel', 'guest_house', 'alpine_hut', 'museum', 'gallery'], 
                    'historic': ['castle', 'ruins'], 
                    'railway': ['station', 'halt'], 
                    'highway': ['bus_stop']}

print("Scaricamento dati OSM...")
gdf = og.feature.features_from_point(centro_trento, tags=tags_to_download, dist=raggio_metri)
gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})

# Salva i dati grezzi estratti (es. in un file pickle o geojson) per lo script successivo
gdf.to_pickle("raw_osm_data.pkl")
print("Dati estratti e salvati in raw_osm_data.pkl")