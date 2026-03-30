"""Load GTFS data and add it in a time-expanded graph. Optionally, load it in an existing OSM (osm2kg) graph."""

import ast
from heapq import heappop, heappush
import itertools
import multiprocessing as mp
import os
from functools import partial
import time
from typing import List, Optional, Tuple
import warnings

import geopandas as gpd
import networkx as nx
import osmnx as ox
from scipy.spatial import KDTree
import osm2kg as og
import pandas as pd
import shapely


from . import utils
from . import convert


def validate_feed(gtfs_path: str) -> bool:
    """
    Validates the GTFS feed located at the specified path.

    Parameters
    ----------
    gtfs_path : str
        Path to the GTFS dataset directory.

    Returns
    -------
    bool
        True if the GTFS feed is valid, False otherwise.
    """
    if not os.path.isdir(gtfs_path):
        warnings.warn("Invalid GTFS path.")
        return False

    # List of required GTFS files
    required_files = [
        "agency.txt", "stops.txt", "routes.txt",
        "trips.txt", "stop_times.txt", "calendar.txt"
    ]

    # Check for the existence of required GTFS files
    for file in required_files:
        if not os.path.isfile(os.path.join(gtfs_path, file)):
            warnings.warn(f"Missing required file: {file}")
            return False

    try:
        # Load GTFS files
        agency_df = pd.read_csv(os.path.join(gtfs_path, "agency.txt"))
        stops_df = pd.read_csv(os.path.join(gtfs_path, "stops.txt"))
        routes_df = pd.read_csv(os.path.join(gtfs_path, "routes.txt"))
        trips_df = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
        stop_times_df = pd.read_csv(os.path.join(gtfs_path, "stop_times.txt"), low_memory=False)
        calendar_df = pd.read_csv(os.path.join(gtfs_path, "calendar.txt"))
        
        critical_errors = False

        # Validate agency.txt
        if agency_df.empty or 'agency_id' not in agency_df.columns:
            print("agency.txt is invalid or missing required 'agency_id' column.")

        # Validate stops.txt
        if stops_df.empty or 'stop_id' not in stops_df.columns:
            print("stops.txt is invalid or missing required 'stop_id' column.")
            critical_errors = True

        # Validate routes.txt
        if routes_df.empty or 'route_id' not in routes_df.columns or 'route_id' not in routes_df.columns:
            print("routes.txt is invalid or missing required columns (agency_id, route_id).")
            critical_errors = True
            
        if not set(routes_df['agency_id']).issubset(set(agency_df['agency_id'])):
            print("Mismatch in agency IDs between routes and agency files.")
            critical_errors = True
            
        # Validate trips.txt
        if trips_df.empty or 'trip_id' not in trips_df.columns or 'route_id' not in trips_df.columns:
            print("trips.txt is invalid or missing required columns.")
            critical_errors = True

        if not set(trips_df['route_id']).issubset(set(routes_df['route_id'])):
            print("Mismatch in route IDs between trips and routes files.")
            critical_errors = True
            
        # Validate stop_times.txt
        if stop_times_df.empty or 'trip_id' not in stop_times_df.columns or 'stop_id' not in stop_times_df.columns:
            print("stop_times.txt is invalid or missing required columns.")
            critical_errors = True

        if not set(stop_times_df['trip_id']).issubset(set(trips_df['trip_id'])):
            print("Mismatch in trip IDs between stop_times and trips files.")
            critical_errors = True

        if not set(stop_times_df['stop_id']).issubset(set(stops_df['stop_id'])):
            print("Mismatch in stop IDs between stop_times and stops files.")
            critical_errors = True

        # Validate calendar.txt
        if calendar_df.empty:
            print("calendar.txt is invalid or empty.")
            critical_errors = True

        # Validate stop_times.txt for blank times and format of times
        if 'departure_time' not in stop_times_df.columns or 'arrival_time' not in stop_times_df.columns:
            print("stop_times.txt is missing required time columns.")
            critical_errors = True

        # Check for blank times
        if stop_times_df['departure_time'].isnull().any() or stop_times_df['arrival_time'].isnull().any():
            print("Blank departure or arrival times found in stop_times.txt.")

        # Validate time format (HH:MM:SS)
        time_format_regex = r'^(\d{2}):([0-5]\d):([0-5]\d)$'  # check for HH:MM:SS format
        invalid_departure_times = stop_times_df[~stop_times_df['departure_time'].str.match(time_format_regex)]
        invalid_arrival_times = stop_times_df[~stop_times_df['arrival_time'].str.match(time_format_regex)]

        if not invalid_departure_times.empty or not invalid_arrival_times.empty:
            print("Invalid time format found in departure or arrival times in stop_times.txt.")
            print(f"Invalid departure times: {invalid_departure_times['departure_time'].values}")
            print(f"Invalid arrival times: {invalid_arrival_times['arrival_time'].values}")
        
        # Additional format and consistency checks= will be added
    
    except Exception as e:
        print(f"Error during validation: {e}")
        return False

    if critical_errors:
        print("GTFS feed contains critical errors.")
        return False
    else:
        print("GTFS feed is valid.")
        return True

def connect_stops_to_streets(graph: nx.DiGraph, stops: pd.DataFrame) -> nx.DiGraph:
    """Connects GTFS stops to the nearest street node in the graph
    using projected coordinates in EPSG:4087.
    """
    # Create a list of street node tuples (x, y, node_id)
    # node_data = [
    #     (data["metric_X"], data["metric_Y"], idx, data["x"], data["y"])
    #     for idx, data in graph.nodes(data=True)
    #     if data["type"] == "street"
    # ]
    # create a gdf from strops using as geometry the point based on the x and y column of the df
    # gdf = geopandas.GeoDataFrame(
    #     df, geometry=geopandas.points_from_xy(df.stop_lon, df.stop_lat), crs="EPSG:4326"
    # )

    graph = og.projection.project_graph(graph)
    node_data = [
        (idx, data["x"], data["y"])
        for idx, data in graph.nodes(data=True)
        if data["type"] == "osm"
    ]



    # Create a KD-tree for nearest neighbor search
    # The tree is created from a list of street node tuples (node_id, x, y)
    tree = KDTree([(x, y) for _, x, y in node_data])

    for stop, data in graph.nodes(data=True):
        if data["type"] == "stop":
        # stop_wgs = (stop["stop_lon"], stop["stop_lat"])
        # x, y = (
        #     graph.nodes[stop["stop_id"]]["metric_X"],
        #     graph.nodes[stop["stop_id"]]["metric_Y"],
        # )
            stop, time = stop
            stop_coords = (data["x"], data["y"])

            # query returns the distance to the nearest neighbor and its index in the tree
            distance, idx = tree.query(stop_coords)
            nearest_street_node = node_data[idx][0]

        # Add a connector edge to the graph
        # Create a LineString geometry for the connector edge
        # stop_geom = shapely.geometry.Point(stop_wgs)
        # street_geom = shapely.geometry.Point((node_data[idx][3], node_data[idx][4]))
        # linestring = shapely.geometry.LineString([stop_geom, street_geom])

            walk_time = distance / 1.39  # walk speed in m/s

            graph.add_edge(
                (stop, time),
                nearest_street_node,
                time=walk_time,
                type="connector",
                # geometry=linestring,
            )
            graph.add_edge(
                nearest_street_node,
                (stop, time),
                time=walk_time,
                type="connector",
                # geometry=linestring,
            )
    return og.projection.project_graph(graph, to_crs="EPSG:4326")


def _add_edge_with_geometry(graph: nx.DiGraph, start_stop, end_stop, schedule_info, geometry):
    """
    Adds or updates an edge in the graph with schedule information and geometry.
    """
    (departure, arrival, route_id, wheelchair_accessible) = schedule_info
    # edge = (start_stop["stop_id"], end_stop["stop_id"])
    # if graph.has_edge(*edge):
    #     graph[edge[0]][edge[1]]["schedules"].append(schedule_info)
    #     if "geometry" not in graph[edge[0]][edge[1]]:
    #         graph[edge[0]][edge[1]]["geometry"] = geometry
    # else:
    #     graph.add_edge(
    #         *edge, schedules=[schedule_info], type="transit", geometry=geometry
    #     )
    graph.add_edge((start_stop["stop_id"],departure), (end_stop["stop_id"], arrival), 
                route_id=route_id,
                wheelchair_accessible=wheelchair_accessible,
                type="gtfs_transfer",
                time= (arrival - departure),
    )


def _process_trip_group(
    group, graph, trips_df, shapes, trip_to_shape_map, stops_df, read_shapes
):
    """
    Processes a group of sorted stops for a single trip, adding edges between them to the graph.

    Parameters
    ----------
    group : pd.DataFrame
        A group of sorted stops for a single trip.
    graph : networkx.DiGraph
        The graph to which the edges will be added.
    trips_df : pd.DataFrame
        DataFrame containing trip information.
    shapes : dict
        Dictionary mapping shape IDs to shape geometries.
    trip_to_shape_map : dict
        Dictionary mapping trip IDs to shape IDs.
    stops_df : pd.DataFrame
        DataFrame containing stop information.
    read_shapes : bool
        Flag indicating whether to read shape geometries from shapes.txt.

    Returns
    -------
    None
    """
    # Mapping stop_id to coordinates for faster lookup
    stop_coords_mapping = stops_df.set_index("stop_id")[
        ["stop_lat", "stop_lon"]
    ].to_dict("index")
    trip_route_mapping = trips_df.set_index("trip_id")["route_id"].to_dict()

    # Some GTFS feeds do not have wheelchair_accessible information
    if "wheelchair_accessible" in trips_df.columns:
        trip_wheelchair_mapping = trips_df.set_index("trip_id")[
            "wheelchair_accessible"
        ].to_dict()
    else:
        trip_wheelchair_mapping = {}

    # For each pair of consecutive stops in the group, add an edge to the graph
    for i in range(len(group) - 1):
        start_stop, end_stop = group.iloc[i], group.iloc[i + 1]
        departure, arrival = (
            convert.parse_time_to_seconds(start_stop["departure_time"]),
            convert.parse_time_to_seconds(end_stop["arrival_time"]),
        )
        if departure > arrival:
            raise ValueError(
                f"Departure time {departure} is greater than arrival time {arrival} for edge {start_stop['stop_id']} -> {end_stop['stop_id']}\n"
                "Negative travel time not allowed\n"
                "Check the GTFS feed for errors in stop_times.txt or calendar.txt, or adjust the departure time\n"
            )

        trip_id = start_stop["trip_id"]
        route_id = trip_route_mapping.get(trip_id)
        wheelchair_accessible = trip_wheelchair_mapping.get(trip_id, None)
        schedule_info = (departure, arrival, route_id, wheelchair_accessible)

        # If read_shapes is True, use the shape geometry from shapes.txt
        if read_shapes:
            shape_id = trip_to_shape_map.get(trip_id)
            geometry = shapes.get(shape_id)
        # Otherwise, use the stop coordinates to create a simple LineString geometry
        else:
            start_coords, end_coords = (
                stop_coords_mapping.get(start_stop["stop_id"]),
                stop_coords_mapping.get(end_stop["stop_id"]),
            )
            geometry = shapely.geometry.LineString(
                [
                    (start_coords["stop_lon"], start_coords["stop_lat"]),
                    (end_coords["stop_lon"], end_coords["stop_lat"]),
                ]
            )

        _add_edge_with_geometry(
            graph=graph,
            start_stop=start_stop,
            end_stop=end_stop,
            schedule_info=schedule_info,
            geometry=geometry,
        )


def _add_edges_parallel(
    trips_chunks, graph, trips_df, shapes, read_shapes, trip_to_shape_map, stops_df
):
    """
    Adds edges to the graph for chunks of trips in parallel.
    """
    local_graph = graph.copy()
    for _, group in trips_chunks.groupby(["trip_id"]):
        sorted_group = group.sort_values("stop_sequence")
        _process_trip_group(
            group=sorted_group,
            graph=local_graph,
            trips_df=trips_df,
            shapes=shapes,
            trip_to_shape_map=trip_to_shape_map,
            stops_df=stops_df,
            read_shapes=read_shapes,
        )
    return local_graph


def add_waiting_edges(G_time_expanded: nx.MultiDiGraph, stop_gdf: gpd.GeoDataFrame) -> nx.DiGraph:
    """
    For each stop, add waiting edges between consecutive time nodes.
    Edge from (stop_id, t) to (stop_id, t_next) with weight = t_next - t.
    """
    from collections import defaultdict

    # Collect all timestamps for each stop_id
    stop_times = defaultdict(list)
    for stop_id, t in G_time_expanded.nodes:
        stop_times[stop_id].append(t)
    # For each stop, sort times and add waiting edges
    for stop_id, times in stop_times.items():
        times_sorted = sorted(times)
        for i in range(len(times_sorted) - 1):
            t = times_sorted[i]
            t_next = times_sorted[i + 1]
            G_time_expanded.add_edge(
                (stop_id, t),
                (stop_id, t_next),
                type="wait",
                time=t_next - t,
            )
            G_time_expanded.add_nodes_from(
                [(stop_id, t), (stop_id, t_next)],
                x=stop_gdf.loc[stop_gdf["stop_id"] == stop_id, "stop_lon"].values[0],
                y=stop_gdf.loc[stop_gdf["stop_id"] == stop_id, "stop_lat"].values[0],
                type="stop"
            )
        if (len(times_sorted)==1):
            G_time_expanded.add_node(
                (stop_id, times_sorted[0]),
                x=stop_gdf.loc[stop_gdf["stop_id"] == stop_id, "stop_lon"].values[0],
                y=stop_gdf.loc[stop_gdf["stop_id"] == stop_id, "stop_lat"].values[0],
                type="stop"
            )
    return G_time_expanded


def _filter_stop_times_by_time(
    stop_times: pd.DataFrame, departure_time: int, duration_seconds: int
):
    """Filters stop_times to only include trips that occur within a specified time window."""

    stop_times["departure_time_seconds"] = stop_times["departure_time"].apply(
        convert.parse_time_to_seconds
    )
    return stop_times[
        (stop_times["departure_time_seconds"] >= departure_time)
        & (stop_times["departure_time_seconds"] <= departure_time + duration_seconds)
    ]



def _split_dataframe(df: pd.DataFrame, n_splits: int) -> list[pd.DataFrame]:
    """
    Splits a DataFrame into n equal parts by rows.
    This function replaces np.split_array which will be deprecated soon.

    Parameters
    ----------
    df : pandas DataFrame
        The DataFrame to be split.
    n_splits : int
        The number of parts to split the DataFrame into.

    Returns
    -------
    list of pandas DataFrames
        A list of DataFrame parts.
    """
    total_rows = len(df)
    base_size = total_rows // n_splits
    remainder = total_rows % n_splits

    # Determine the number of rows each split will have
    split_sizes = [
        base_size + 1 if i < remainder else base_size for i in range(n_splits)
    ]
    # Calculate the start indices for each split
    start_indices = [sum(split_sizes[:i]) for i in range(n_splits)]

    return [
        df.iloc[start : start + size] for start, size in zip(start_indices, split_sizes)
    ]


def _load_GTFS(
    GTFSpath: str,
    departure_time_input: str,
    start_date: str,
    duration_seconds,
    read_shapes=False,
    multiprocessing=False,
    geography_boundary: Optional[shapely.geometry.MultiPolygon] = None,
) -> tuple[nx.DiGraph, pd.DataFrame]:
    """
    Loads GTFS data from the specified directory path and returns a graph and a dataframe of stops.
    The function uses parallel processing to speed up data loading.

    Parameters
    ----------
    GTFSpath : str
        Path to the directory containing GTFS data files.
    departure_time_input : str
        The departure time in 'HH:MM:SS' format.
    date : str
        Day of the departure in dd/mm/YYYY format.
    duration_seconds : int
        Duration of the time window to load in seconds.
    read_shapes : bool
        Geometry reading flag, passed from feed_to_graph.

    Returns
    -------
    tuple
        A tuple containing:
            - nx.DiGraph: Graph representing GTFS data.
            - pd.DataFrame: DataFrame containing stop information.
    """
    # Initializing empty graph and read data files.
    G = nx.MultiDiGraph()
    stops_df = pd.read_csv(
    os.path.join(GTFSpath, "stops.txt"), usecols=["stop_id","stop_name", "stop_lat", "stop_lon"],
    # dtype=str
    )
    stop_times_df = pd.read_csv(
        os.path.join(GTFSpath, "stop_times.txt"),
        usecols=[
            "departure_time",
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
        ],
        # dtype=str
    )
    routes = pd.read_csv(
        os.path.join(GTFSpath, "routes.txt"), usecols=["route_id", "route_short_name"],
        # dtype=str
    )
    trips_df = pd.read_csv(os.path.join(GTFSpath, "trips.txt"),
    # dtype=str
                        )
    calendar_df = pd.read_csv(os.path.join(GTFSpath, "calendar.txt"),
                            # dtype=str
                            )

    # Join route information to trips
    trips_df = trips_df.merge(routes, on="route_id")
    # stop_times_df = stop_times_df.merge(trips_df)
    # Filter trips by day of the week
    start_date = time.strptime(start_date, '%d/%m/%Y')
    target_date = time.strftime('%Y%m%d', start_date)
    target_day = time.strftime('%A', start_date).lower()
    service_ids = set(calendar_df[(calendar_df[target_day] == 1) & (calendar_df["start_date"] <= int(target_date)) &
        (calendar_df["end_date"] >= int(target_date))]["service_id"])
    # #]---
    # Find exceptions for the target date
    calendar_dates_df = pd.read_csv(
        os.path.join(GTFSpath, "calendar_dates.txt"),
        #  dtype=str

    )
    exceptions = calendar_dates_df[(calendar_dates_df["date"] == int(target_date))]
    # Remove service_ids with exception_type == 2 (service removed)
    removed = set(exceptions[exceptions["exception_type"] == 2]["service_id"])
    # print(removed)
    # Add service_ids with exception_type == 1 (service added)
    added = set(exceptions[exceptions["exception_type"] == 1]["service_id"])
    # Update the set of active service_ids
    service_ids = (service_ids - removed) | added
    # #-----
    trips_df = trips_df[trips_df["service_id"].isin(service_ids)]

    # Filter out stops and so trips that are outside the geographic boundary if provided
    if geography_boundary is not None:
        # Convert stops_df to GeoDataFrame
        stops_gdf = gpd.GeoDataFrame(
            stops_df, geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat), crs="EPSG:4326"
        )
        # Filter stops within the geographic boundary
        stops_gdf = stops_gdf[stops_gdf.geometry.within(geography_boundary)]
        # Filter stop_times by the filtered stops
        stop_times_df = stop_times_df[stop_times_df["stop_id"].isin(stops_gdf["stop_id"])]
        # Update stops_df to only include filtered stops
        stops_df = stops_gdf.drop(columns="geometry")
    # Filter stop_times by valid trips
    valid_trips = stop_times_df["trip_id"].isin(trips_df["trip_id"])
    stop_times_df = stop_times_df[valid_trips].dropna()

    # Convert departure_time from HH:MM:SS o seconds
    departure_time_seconds = convert.parse_time_to_seconds(departure_time_input)
    # Filtering stop_times by time window
    filtered_stops = _filter_stop_times_by_time(
        stop_times_df, departure_time_seconds, duration_seconds
    )

    print(f"{len(filtered_stops)} of {len(stop_times_df)} trips retained")

    # # Adding stops as nodes to the graph
    # for _, stop in stops_df.iterrows():
    #     G.add_node(
    #         stop["stop_id"],
    #         type="transit",
    #         pos=(stop["stop_lon"], stop["stop_lat"]),
    #         x=stop["stop_lon"],
    #         y=stop["stop_lat"],
    #     )
    if read_shapes:
        shapes = None
        trip_to_shape_map = None

    else:
        shapes = None
        trip_to_shape_map = None

    if multiprocessing:
        # !! for now is not working !!
        multiprocessing = False
        pass
        
        # print("Building graph in parallel")
        # # Divide filtered_stops into chunks for parallel processing
        # # Use half of the available CPU logical cores
        # # (likely equal to the number of physical cores)
        # num_cores = int(mp.cpu_count() / 2) if mp.cpu_count() > 1 else 1
        # chunks = _split_dataframe(filtered_stops, num_cores)

        # # Create a pool of processes
        # with mp.Pool(processes=num_cores) as pool:
        #     # Create a subgraph in each process
        #     # Each will return a graph with edges for a subset of trips
        #     # The results will be combined into a single graph
        #     add_edges_partial = partial(
        #         _add_edges_parallel,
        #         graph=G,
        #         trips_df=trips_df,
        #         shapes=shapes,
        #         read_shapes=read_shapes,
        #         trip_to_shape_map=trip_to_shape_map,
        #         stops_df=stops_df,
        #     )
        #     results = pool.map(add_edges_partial, chunks)

        # # Merge results from all processes
        # merged_graph = nx.DiGraph()

        # for graph in results:
        #     merged_graph.add_nodes_from(graph.nodes(data=True))
        # # Add edges from subgraphs to the merged graph
        # for graph in results:
        #     for u, v, data in graph.edges(data=True):
        #         # # If edge already exists, merge schedules
        #         # if merged_graph.has_edge(u, v):
        #         #     # Merge sorted_schedules attribute
        #         #     existing_schedules = merged_graph[u][v]["schedules"]
        #         #     new_schedules = data["schedules"]
        #         #     merged_graph[u][v]["schedules"] = existing_schedules + new_schedules
        #         # # If edge does not exist, add it
        #         # else:
        #         #     # Add new edge with data
        #         merged_graph.add_edge(u, v, **data)
        # # Add edge for waiting at stops
        # merged_graph = add_waiting_edges(merged_graph, stops_df)
        # # Sorting schedules for faster lookup using binary search
        # # _preprocess_schedules(merged_graph)
        # utils.log("Transit graph created")

        # return merged_graph, stops_df

    else:
        for trip_id, group in filtered_stops.groupby("trip_id"):
            sorted_group = group.sort_values("stop_sequence")
            _process_trip_group(
                group=sorted_group,
                graph=G,
                trips_df=trips_df,
                shapes=shapes,
                trip_to_shape_map=trip_to_shape_map,
                stops_df=stops_df,
                read_shapes=read_shapes,
            )   
        G = add_waiting_edges(G, stops_df)
        # Sorting schedules for faster lookup using binary search
        # _preprocess_schedules(graph=G)
        utils.log("Transit graph created")

        return G, stops_df


def _load_osm(stops: pd.DataFrame, save_graphml: bool, path) -> nx.DiGraph:
    """
    Loads OpenStreetMap data within a convex hull of stops in GTFS feed,
    creates a street network graph, and adds walking times as edge weights.

    Parameters
    ----------
    stops : pandas.DataFrame
        DataFrame containing the stops information from the GTFS feed.
    save_graphml : bool
        Flag indicating whether to save the resulting graph as a GraphML file.
    path : str
        The file path to save the GraphML file (if save_graphml is True).

    Returns
    -------
    G_city : networkx.DiGraph
        A street network graph with walking times as edge weights.
    """
    # Building a convex hull from stop coordinates for OSM loading
    stops_gdf = gpd.GeoDataFrame(
        stops, geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat)
    )
    boundary = stops_gdf.unary_union.convex_hull

    utils.log("Loading OSM graph via OSMNX")
    # Loading OSM data within the convex hull
    G_city = ox.graph_from_polygon(boundary, network_type="walk", simplify=True)

    attributes_to_keep = {"length", "highway", "name"}
    for u, v, key, data in G_city.edges(keys=True, data=True):
        # Clean extra attributes
        for attribute in list(data):
            if attribute not in attributes_to_keep:
                del data[attribute]

        # Calculate walking time in seconds
        data["time"] = data["length"] / 1.39
        # data["static_weight"] = data["weight"]  # temp
        data["type"] = "street"

        # Add geometry to the edge
        u_geom = shapely.geometry.Point(G_city.nodes[u]["x"], G_city.nodes[u]["y"])
        v_geom = shapely.geometry.Point(G_city.nodes[v]["x"], G_city.nodes[v]["y"])
        data["geometry"] = shapely.geometry.LineString([u_geom, v_geom])

    nx.set_node_attributes(G_city, "street", "type")

    if save_graphml:
        ox.save_graphml(G_city, path)

    utils.log("Street network graph created")

    return nx.DiGraph(G_city)


def feed_to_graph(
    GTFSpath: str,
    departure_time_input: str,
    start_date: str,
    duration_seconds: int,
    read_shapes: bool = False,
    multiprocessing: bool = True,
    input_graph_path: str = None,
    output_graph_path: str = None,
    save_graphml: bool = False,
    load_graphml: bool = False,
    city_graph: Optional[nx.DiGraph] = None,
    geography_boundary: Optional[shapely.geometry.MultiPolygon] = None,
) -> nx.DiGraph:
    """
    Creates a directed graph (DiGraph) based on General Transit Feed Specification (GTFS) and OpenStreetMap (OSM) data.

    Parameters
    ----------
    GTFSpath : str
        Path to the GTFS files.
    departure_time_input : str
        Departure time in 'HH:MM:SS' format.
    start_date : str
        Start date in 'dd/mm/YYYY' format. eg '31/01/2025'.
    duration_seconds : int
        Time period from departure for which the graph will be loaded.
    read_shapes : bool, optional
        Flag for reading geometry from shapes.txt file. Default is False. This parameter is currently not working as intended.
    multiprocessing : bool, optional
        Flag for using multiprocessing. Default is False.
    input_graph_path : str, optional
        Path to the OSM graph file in GraphML format. Default is None.
    output_graph_path : str, optional
        Path for saving the OSM graph in GraphML format. Default is None.
    save_graphml : bool, optional
        Flag for saving the OSM graph in GraphML format. Default is False.
    load_graphml : bool, optional
        Flag for loading the OSM graph from a GraphML file. Default is False.
    city_graph : Optional[nx.DiGraph], optional
        Pre-loaded OSM graph. If provided, it will be used instead of loading from a GraphML file. Default is None.

    Returns
    -------
    G_combined : nx.DiGraph
        Combined multimodal graph representing transit network.
    """
    # Validate the GTFS feed
    bool_feed_valid = validate_feed(GTFSpath)
    if not bool_feed_valid:
        raise ValueError("The GTFS feed is not valid")

    G_transit, stops = _load_GTFS(
        GTFSpath,
        departure_time_input,
        start_date,
        duration_seconds,
        read_shapes=read_shapes,
        multiprocessing=multiprocessing,
        geography_boundary=geography_boundary,
    )
    if city_graph is not None:
        G_combined = nx.compose(G_transit, city_graph)
        # Filling EPSG:4087 coordinates for graph nodes
        # _fill_coordinates(G_combined)
        # Connecting stops to OSM streets
        G_combined = connect_stops_to_streets(G_combined, stops)

        utils.log(
            f"Nodes: {G_combined.number_of_nodes()}, Edges: {G_combined.number_of_edges()}"
        )

        return G_combined
    if load_graphml:
        print("Loading OSM graph from GraphML file")
        # Dictionary with data types for edges
        # edge_dtypes = {"weight": float, "length": float}
        # G_city = ox.load_graphml(input_graph_path, edge_dtypes=edge_dtypes)
        
        G_city = ox.load_graphml(input_graph_path, node_dtypes={"osmid": str})
        mapping = {}
        for node in G.nodes:
            try:
                mapping[node] = ast.literal_eval(node)
            except Exception:
                mapping[node] = node
        G = nx.relabel_nodes(G, mapping)

        # G_city = nx.DiGraph(G_city) # use DiGraph
    else:
        # Import OSM data
        G_city = _load_osm(stops, save_graphml, output_graph_path)

    # Combining OSM and GTFS data
    G_combined = nx.compose(G_transit, G_city)
    # Filling EPSG:4087 coordinates for graph nodes
    # _fill_coordinates(G_combined)
    # Connecting stops to OSM streets
    connect_stops_to_streets(G_combined, stops)

    utils.log(
        f"Nodes: {G_combined.number_of_nodes()}, Edges: {G_combined.number_of_edges()}"
    )

    return G_combined


def load_stops_gdf(path) -> gpd.GeoDataFrame:
    """
    Load stops data from a specified path and return a GeoDataFrame.

    Parameters
    ----------
    path: str
        The path to the directory containing the stops data.

    Returns
    -------
    stops_gdf: gpd.GeoDataFrame
        GeoDataFrame containing the stops data with geometry information.

    """
    stops_df = pd.read_csv(os.path.join(path, "stops.txt"))
    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat),
        crs="epsg:4326",
    )
    return stops_gdf



# ------------------------------------------------------------------------------
def _reconstruct_path(target, predecessors):
    """
    Reconstruct path from predecessors dictionary
    """

    path = []
    current_node = target

    while current_node is not None:
        path.insert(0, current_node)

        current_node = predecessors.get(current_node)

    return path


def time_dependent_dijkstra(
    graph: nx.DiGraph,
    source: tuple,
    target: tuple,
    start_time: float,
    track_used_routes: bool = False,
    allowed_routes: set = None,
    wheelchair: bool = False,
) -> Tuple[List[tuple[tuple, str]], float, list[float], Optional[list]]:
    """
    Finds the shortest path between two nodes in a time-dependent graph using Dijkstra's algorithm.

    Parameters
    ----------
    graph : networkx.Graph
        The graph to search for the shortest path.
    source
        The starting node.
    target
        The target node.
    start_time : float
        The starting time.
    track_used_routes : bool, optional
        If set to True, the algorithm will track the used routes in the path.
    allowed_routes : set, optional
        A set of allowed route IDs. If provided, the algorithm will only consider 
        edges with these route numbers for pubblic transport.
    wheelchair : bool, optional
        If set to True, the algorithm will only use wheelchair accessible routes.

    Returns
    -------
    tuple
        A tuple containing the following elements:
            - list: The shortest path from the source to the target node as a tuple of (node, used_route).
            - float: The arrival time at the target node.
            - list: The arrival times for each node in the graph.
            - list: The set of used routes in order.
    """
    # abort immediately if the source or target node does not exist in the graph
    if source not in graph or target not in graph:
        raise ValueError("The source or target node does not exist in the graph.")
    
    # Initialize arrival times and predecessors for the current node in the queue
    arrival_times = {node: float("inf") for node in graph.nodes}
    predecessors = {node: None for node in graph.nodes}
    arrival_times[source] = start_time
    queue = [(0, start_time, source )]
    visited = set()
    # Track used routes
    routes = {}
    # while the queue is not empty and the target node has not been visited
    while queue:
        # Extract the node with the smallest arrival time from the queue
        _, current_time, u = heappop(queue)
        # If the node is the target, stop the execution
        # if u == target:
        #     break
        # If the node has already been visited with a better result, skip it
        if u in visited and current_time > arrival_times[u]:
            continue
        # Add the node to the visited set to avoid visiting it again
        visited.add(u)
        # Iterate over all neighbors of the node
        for v in graph.neighbors(u):
            # If the neighbor has not been visited yet
            if v not in visited:
                extra = 0
                if graph.nodes[u].get("type",None) == "osm" and graph.nodes[v].get("type",None) == "stop" :
                    if v[1] < current_time:
                        # If the neighbor is in the past, skip it
                        continue
                    else:
                        extra = True

                # If the new arrival time is better, update the arrival time and predecessor
                last_route = routes.get(u)
                route_id = graph[u][v][0].get("route_id", None)
                # If the route_id is not None, check if the route is allowed
                if (allowed_routes!=None and route_id!=None and route_id not in allowed_routes):
                    continue
                    
                weight = graph[u][v][0]["time"]
                if extra:
                    new_arrival_time = v[1]
                else:
                    new_arrival_time = current_time + weight

                
                if new_arrival_time < arrival_times[v]:
                    arrival_times[v] = new_arrival_time
                    routes[v] = graph[u][v][0].get("route_id", None)

                    # Assign the current node U as the predecessor of the neighbor V (in the loop)
                    predecessors[v] = u
                    # Add the neighbor to the queue with the new arrival time
                    if route_id is None and graph.nodes[v].get("type", None) == "osm":
                        heappush(queue, (3000*new_arrival_time, new_arrival_time, v))
                    else:
                        heappush(queue, (new_arrival_time, new_arrival_time, v))

    path = _reconstruct_path(target=target, predecessors=predecessors)

    if path[0] == source:
        # Empty set to track used routes
        used_routes = list()
        # Iterate over all nodes in the path
        for i in range(len(path) - 1):
            v = path[i + 1]
            # Add route, used to go from node U to node V
            path[i] = (path[i], routes[v])
            if routes[v] is not None:
                if len(used_routes) == 0 or used_routes[-1] != routes[v]:
                    used_routes.append(routes[v])
        used_routes = list(dict.fromkeys(used_routes)) # works only in Python 3.7+
        return path, arrival_times[target], arrival_times, used_routes
    else:
        # If the path does not start with the source node, something went wrong, the path was not found
        return [], float("inf"), -float("inf"), set()


def shortest_path_gtfs(
    graph: nx.DiGraph,
    source: tuple,
    target: tuple,
    start_time: float,
    track_used_routes: bool = False,
    wheelchair: bool = False,
) -> Tuple[List[tuple[tuple,str]], float, float, Optional[list]]:
    """
    Finds the shortest path between two nodes in a graph with added GTFS information using Dijkstra's algorithm.
    It search for the shortest path with the minimum change possible.

    Parameters
    ----------
    graph : networkx.Graph
        The graph to search for the shortest path.
    source
        The starting node.
    target
        The target node.
    start_time : float
        The starting time.
    wheelchair : bool, optional
        If set to True, the algorithm will only use wheelchair accessible routes.

    Returns
    -------
    tuple
        A tuple containing the following elements:
            - list: The shortest path from the source to the target node as a tuple of node, used_route.
            - float: The arrival time at the target node.
            - list: The arrival times for each node in the graph.
            - list: The set of used routes in order.
    """
    
    path, arrival_time, arrivals_time, used_routes = time_dependent_dijkstra(
        graph=graph,
        source=source,
        target=target,
        start_time=start_time,
        track_used_routes=track_used_routes,
        allowed_routes=None,
        wheelchair=wheelchair,
    )
    path_len = len(path)
    if path_len == 0:
        # If the path is empty, return empty values
        return [], float("inf"), [float("inf")], []
        
    elif len(used_routes) <= 1:
        return path, arrival_time, arrivals_time, used_routes
    elif len(used_routes) == 2:
        # If there are only two used routes, check if only the first is sufficient
        allowed_routes = set()
        allowed_routes.add(used_routes[0])
        new_path, new_arrival_time, new_arrivals_time, new_used_routes = time_dependent_dijkstra(
            graph=graph,
            source=source,
            target=target,
            start_time=start_time,
            track_used_routes=track_used_routes,
            allowed_routes=allowed_routes,
            wheelchair=wheelchair,
        )
        if new_arrival_time <= arrival_time:
            return new_path, new_arrival_time, new_arrivals_time, new_used_routes
        # check if only the second is sufficient
        else:
            allowed_routes = set()
            allowed_routes.add(used_routes[1])
            new_path, new_arrival_time, new_arrivals_time, new_used_routes = time_dependent_dijkstra(
                graph=graph,
                source=source,
                target=target,
                start_time=start_time,
                track_used_routes=track_used_routes,
                allowed_routes=allowed_routes,
                wheelchair=wheelchair,
            )
            if new_arrival_time <= arrival_time:
                return new_path, new_arrival_time, new_arrivals_time, new_used_routes
            else:
                return path, arrival_time, arrivals_time, used_routes
    elif len(used_routes) > 2:
        # If there are more than two used routes, check if removing any of them is sufficient
        allowed_routes = set()
        allowed_routes.add(used_routes[0])
        allowed_routes.add(used_routes[-1])
        new_path, new_arrival_time, new_arrivals_time, new_used_routes = time_dependent_dijkstra(
                    graph=graph,
                    source=source,
                    target=target,
                    start_time=start_time,
                    track_used_routes=track_used_routes,
                    allowed_routes=allowed_routes,
                    wheelchair=wheelchair,
                )
        if new_arrival_time <= arrival_time:
            return new_path, new_arrival_time, new_arrivals_time, new_used_routes
        inner = used_routes[1:-1]
        for r in range(1, len(inner) + 1):
            for combo in itertools.combinations(inner, r):
                allowed_routes = set(combo)
                allowed_routes.add(used_routes[0])
                allowed_routes.add(used_routes[-1])
                new_path, new_arrival_time, new_arrivals_time, new_used_routes = time_dependent_dijkstra(
                    graph=graph,
                    source=source,
                    target=target,
                    start_time=start_time,
                    track_used_routes=track_used_routes,
                    allowed_routes=allowed_routes,
                    wheelchair=wheelchair,
                )
                if new_arrival_time <= arrival_time:
                    return new_path, new_arrival_time, new_arrivals_time, new_used_routes
                else:
                    continue
        return path, arrival_time, arrivals_time, used_routes