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

place = "Riva del Garda, Trento, Italy"
tags={"amenity":["restaurant", "fast_food", "bar", "pub"]}

gdf = og.feature.features_from_place(place, tags=tags)
name_gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})
street_graph = og.graph.graph_from_place(place, network_type="walk", simplify=False)
street_graph_with_gdf = og.graph.add_feature_on_graph(name_gdf, street_graph, simplfy=True)
departure = og.distance.nearest_nodes(street_graph_with_gdf, 10.845161,45.891972) #bus station riva del garda
a = name_gdf.index.to_list()
a = set(a)
street_graph_with_gdf_with_time = og.routing.add_time_to_edge(street_graph_with_gdf)
reach_feat = og.isochrone.find_reachable_features_in_time_from_node(G=street_graph_with_gdf_with_time, start_node=departure,trip_times=[60*5])
reach_gdf = og.isochrone.extract_reachable_feature_from_gdf(gdf, reach_feat)
subset = gdf.loc[[idx for idx in reach_feat[60*5]]]


og.kg.process_gdf(subset)

# # street_graph_with_gdf_with_time = og.routing.add_time_to_edge(street_graph_with_gdf)
# shortest_route_gdf = [og.routing.simplyfied_path_gdf(og.routing.route_to_gdf(street_graph_with_gdf,r_gdf)) for r_gdf in five_nearest_path]

# import final_kg
# for i in shortest_route_gdf:
#     final_kg.process_move_action_gdf(i)
# create a GeoDataFrame with as key the destionation and as value a geomtryfor the path and a distance and time columns
# route_gdf = gpd.GeoDataFrame({
#     "departure": [("node",0), ("node",1)],
#     "destination": [("node",123), ("node",456)],
#     "mouvement": ["yes", "yes"],
#     "geometry": [LineString(12.445,43.54, 12.446,43.541) , LineString(12.445,43.54, 12.446,43.541)],
#     "distance": [111,222],
#     "time": [123,456]
# }, crs="EPSG:4326") 
# import final_kg
# final_kg.process_gdf(route_gdf)