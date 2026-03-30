import networkx as nx
import geopandas as gpd
from shapely.geometry import Polygon, Point, LineString
import matplotlib.pyplot as plt
from shapely.ops import nearest_points
import osmnx as ox
ox.settings.log_console = True
# ox.settings.max_query_area_size = 250000000
import osm2kg as og
og.settings.log_console = True
import pandas as pd
import time

place = "Riva del Garda, Trento, Italy"
tags={"amenity":["restaurant", "fast_food", "bar", "pub"], "highway":"bus_stop", "historic":"castle"}

gdf = og.feature.features_from_place(place, tags=tags)
name_gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})
street_graph = og.graph.graph_from_place(place, network_type="walk", simplify=False)
street_graph_with_gdf = og.graph.add_feature_on_graph(name_gdf, street_graph, simplify=True)
street_graph_with_gdf_with_time = og.routing.add_time_to_edge(street_graph_with_gdf)
departure = og.distance.nearest_nodes(street_graph_with_gdf, 10.841414,45.884508) #bus station riva del garda
start_time = int(time.time() )% (24 * 3600)  # seconds since midnight
set_of_nodes = og.feature.extract_nodes_from_feature_gdf(gdf=gdf)

k_feat, five_nearest_path, arrivals_time = og.isochrone.k_nearest_features(street_graph_with_gdf, departure, 15,set_of_nodes ,start_time=123)
subset = gdf.loc[[idx for (idx, _) in k_feat]]

subset.to_file("data/osm_points.geojson", driver="GeoJSON")
# og.crea_repository("osm2kg_subset")
# og.final_kg.process_gdf(subset)


