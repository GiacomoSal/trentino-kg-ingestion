import osm2kg as og
import pandas as pd

# centro di trento
centro_trento = (46.0678, 11.1211)
raggio_metri = 2000

# TODO: per ora ho hardcodato i tag qua per fare veloce, l'idea è spostarli nel mapping.yaml
tags_to_download = {'amenity': ['bar', 'pub', 'restaurant', 'cafe', 'fast_food', 'fuel', 'bicycle_rental'], 
                    'tourism': ['hotel', 'motel', 'guest_house', 'alpine_hut', 'museum', 'gallery'], 
                    'historic': ['castle', 'ruins'], 
                    'railway': ['station', 'halt'], 
                    'highway': ['bus_stop']}

print("Scaricamento dati OSM...")
gdf = og.feature.features_from_point(centro_trento, tags=tags_to_download, dist=raggio_metri)
gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})

# mi salvo i raw data in locale così non sfondo di richieste le API di OSM ogni volta che runno un test
gdf.to_pickle("raw_osm_data.pkl")
print("Dati estratti e salvati in raw_osm_data.pkl")