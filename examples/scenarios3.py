import ast
import os
from nxtransit.converters import parse_seconds_to_time, parse_time_to_seconds
import networkx as nx
import pandas as pd
import geopandas as gpd
from osm2kg.gtfs import feed_to_graph, shortest_path_gtfs
import osmnx as ox
import osm2kg as og

og.settings.log_console = True  # type: ignore
ox.settings.log_console = True  # type: ignore

GTFSpath = r"Knowdive-OSM2KG/data/urbano_tte"
departure_time_input = "06:00:00" # Departure time in HH:MM:SS format
day = '30/05/2025' # Day of the week
# nt.validate_feed(GTFSpath)
# Build the combined graph (transit + street)


# stops_df = pd.read_csv(
#     os.path.join(GTFSpath, "stops.txt"), usecols=["stop_id","stop_name", "stop_lat", "stop_lon"],
#     # dtype=str
#     )
# stops_gdf = gpd.GeoDataFrame(
#         stops_df, geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat)
#     )
# boundary = stops_gdf.unary_union.convex_hull

# G_city = og.graph.graph_from_polygon(boundary, network_type="walk", simplify=False, truncate_by_edge=True)
# feat = og.feature.features_from_polygon(boundary, tags={"highway":"bus_stop"})
# G_city = og.graph.add_feature_on_graph(feat, G_city, simplfy=True)
# G_city = ox.convert.to_digraph(G_city)
# ox.io.save_graphml(G_city, "tte.graphml")
# ----
# G = ox.io.load_graphml("tte.graphml",node_dtypes={"osmid": str})
# mapping = {}
# for node in G.nodes:
#     try:
#         mapping[node] = ast.literal_eval(node)
#     except Exception:
#         mapping[node] = node
# G = nx.relabel_nodes(G, mapping)
# G = ox.convert.to_digraph(G)
# #-----

G = og.graph.graph_from_place(
    {"city":"Trento"},
    network_type="walk",
    simplify=False,
    truncate_by_edge=True,
    retain_all=False,
)

G = og.routing.add_time_to_edge(G, weight="time") # preserve the digraph structure
# print(type(G))
G = og.convert.to_digraph(G)
G_combined = feed_to_graph(
    GTFSpath=GTFSpath,
    departure_time_input="16:00:00",
    start_date=day,
    input_graph_path="data/graph.graphml",
    duration_seconds=3600*3,
    read_shapes=False,
    multiprocessing=False,
    # load_graphml=True,
    city_graph=G,
)
# print(type(G_combined))
G_combined = ox.convert.to_digraph(G_combined)
# print(type(G_combined))
while True:
    input_source = input("Enter source node ID (or press Enter for random): ")
    s, i = input_source.split(',')
    source = (s.strip(), int(i)) if input_source else ("node", 6946050331)  # Default source node ID
    input_target = input("Enter target node ID (or press Enter for random): ")
    s, i = input_target.split(',')
    target = (s.strip(), int(i)) if input_target else ("node",10734724245)  # Default target node ID
    departure_time_input = input("Enter departure time (HH:MM:SS, default is 6:00:00): ") or "16:53:00"
    time = parse_time_to_seconds(departure_time_input)

    time_dependent_path, arrival_time, arrivals_time, used_routes = shortest_path_gtfs(
        graph=G_combined,
        source=source,
        target=target,
        start_time=time,
        track_used_routes=True,
        wheelchair=False
    )
    print("Path:", time_dependent_path)
    print('Used routes:', used_routes)
    print(f"Arrival time at destination: {parse_seconds_to_time(arrival_time)} in {parse_seconds_to_time(arrivals_time[target]-time)}")
    # print(arrival_time, travel_time)
    G_path = nx.subgraph(G_combined, time_dependent_path)
    graph = nx.MultiDiGraph(G_path)
    G_nodes, G_edges = og.convert.graph_to_gdfs(graph)
    m = G_nodes.explore(column = 'type',
                    cmap = 'rainbow',
                    tiles = 'Cartodb dark_matter',
                    )

    G_edges.explore(m=m, column = 'length',
                    cmap = 'rainbow',
                    tiles = 'Cartodb dark_matter',
                    )
    m.show_in_browser()
# time_dependent_path = [('node', 6946050331), ('node', 2799568029), (2833, 60900), (150, 61020), (148, 61080), (146, 61080), (175, 61140), (453, 61200), (451, 61260), (176, 61380), (276, 61440), (403, 61560), (2189, 61620), ('node', 885670005), ('node', 1762021752), ('node', 11080589350), ('node', 1299369691), ('node', 977894491), ('node', 1150667552), ('node', 1150667546), ('node', 1156723779), ('node', 1045044814), ('node', 558628495), (163, 61800), (2680, 61800), (76, 61920), (294, 61980), (297, 62040), (293, 62160), (300, 62280), (292, 62340), (290, 62400), (2438, 62460), (52, 62460), (54, 62520), (50, 62580), (62, 62640), (494, 62760), ('node', 1644893951), ('node', 1644893874), ('node', 2263360531), ('node', 10734724245)]
# G_path = nx.subgraph(G_combined, time_dependent_path)
# graph = nx.MultiDiGraph(G_path)
# G_nodes, G_edges = og.convert.graph_to_gdfs(graph)
# print(G_nodes.head())
# print("--"*20)
# print(G_edges.head())
# print("--"*20)

# m = G_nodes.explore(
#     # column = 'ctype',
#                 cmap = 'rainbow',
#                 tiles = 'Cartodb dark_matter',
#                 )

# G_edges.explore(m=m, 
#                 # column = 'length',
#                 cmap = 'rainbow',
#                 tiles = 'Cartodb dark_matter',
#                 )
# m.show_in_browser()

# print("Graph visualization opened in browser.")