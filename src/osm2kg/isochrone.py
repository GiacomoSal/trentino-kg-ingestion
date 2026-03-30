"""Function to find reachable features in time from a point, place or gdf."""
from typing import overload
import networkx as nx
import osmnx as ox
import osm2kg as og
import geopandas as gpd
from shapely.geometry import Point
from . import utils
from . import feature
from .routing import add_time_to_edge, add_speed_and_time
import heapq

def find_reachable_features_in_time_from_gdf(
        start: gpd.GeoDataFrame,
        G: nx.MultiDiGraph,
        trip_times: int,
        velocity: float = None,
        custom_column: str = None,
) -> gpd.GeoDataFrame:
    """
    Find reachable features in time from every row of the gdf.
    The gdf will be used to get the coordinates, and if 
    it is a polygon, the centroid will be used as the start point or you can
    pass a column that you want to use as start point.
    The trip time is in seconds.
    The velocity is in km/h, if None the minimum time given by max_velocity attribute is used.
    The function will return a gdf with two new columns: 'reachable_feature' and 'reachable_nodes'.

    Parameters
    ----------
    start : GeoDataFrame
        The gdf to start from.
    G : nx.MultiDiGraph
        The graph to search in.
    trip_times : int or list[int]
        The trip time in seconds.
    velocity : float, optional
        The velocity in km/h. The default is None.
    custom_column : str, optional
        The column to use as start point. The default is None.
        If None, the first geometry of the gdf will be used.
        
    Returns
    -------
    geopandas.GeoDataFrame
        A gdf with the reachable features and nodes in time from every row of the gdf as new columns.
    """
    gdf = start.copy()
    msg = f"Starting to find reachable features in time: {trip_times} from gdf"
    utils.log(msg)
    # Set geometry column if custom column provided
    if custom_column is not None:
        gdf = gdf.set_geometry(custom_column)
        centroids = ox.projection.project_gdf(gdf).geometry.centroid.to_crs("epsg:4326")
        xs = centroids.x.values
        ys = centroids.y.values
        # Find all nearest nodes at once
        start_nodes = og.distance.nearest_nodes(G, xs, ys)
        msg = f"Found {len(start_nodes)} nearest nodes"
        utils.log(msg)
    else:
        start_nodes = gdf.index.values
    if velocity == None:
        add_speed_and_time(G)
        distance = "travel_time"
    else:
        G = add_time_to_edge(G, velocity=velocity)
        distance = "time"
    
    reachable_features = []

    for trip_time in trip_times:
        msg = f"Finding reachable features in {trip_time} second(s) from gdf"
        utils.log(msg)
        for start_node in start_nodes:
                # if a node is in the gdf but not in the graph, skip it
                # this could happen if the feature in the gdf is too far from a valid edge of the graph
            if G.nodes.get(start_node) == None:
                reachable_features.append(None)
                continue
            subgraph : nx.MultiDiGraph = nx.ego_graph(G, start_node, radius=trip_time, distance=distance)
            reachable_feature = list()
            seen = set()
            for node, data in subgraph.nodes(data=True):
                if data.get("feature") != None:
                    if node not in seen:
                        seen.add(node)
                        reachable_feature.append(node)
            if len(reachable_feature) == 0:
                reachable_features.append(None)
            else:
                reachable_features.append(reachable_feature)
        gdf[f"reachable_in_{trip_time/60}"] = reachable_features
        reachable_features = []
    
    return gdf


@overload
def find_reachable_features_in_time_from_point(
        start: Point,
        G: nx.MultiDiGraph,
        trip_times: list[int],
        velocity: float = None,
) -> tuple[dict[int, list[tuple[str, int]]], dict[int, list[int]]]: ...
    

def find_reachable_features_in_time_from_point(
        start: Point,
        G: nx.MultiDiGraph,
        trip_times: int,
        velocity: float = None,
) -> dict[int, list[tuple[str, int]]]:
    """
    Find reachable features in time from a point. 
    The trip time is in seconds.
    The velocity is in km/h, if None the minimum time given by max_velocity attribute is used.
    The function will return a dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    The feature is the name of the feature and the osmId is the id of the feature in the graph.

    Parameters
    ----------
    start : Point
        The point to start from.
    G : nx.MultiDiGraph
        The graph to search in.
    trip_times : int or list[int]
        The trip time in seconds.
    velocity : float, optional
        The velocity in km/h. The default is None.

    Returns
    -------
    dict[int, list[tuple[str, int]]]
        A dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    """
    msg = "Starting to find reachable features from point"
    utils.log(msg)
    start_node = og.distance.nearest_nodes(G, start.x, start.y)
    return find_reachable_features_in_time_from_node(start_node, G, trip_times, velocity=velocity)


@overload
def find_reachable_features_in_time_from_place(
        start: str,
        G: nx.MultiDiGraph,
        trip_times: list[int],
        velocity: float = None,
) -> tuple[dict[int, list[tuple[str, int]]], dict[int, list[int]]]: ...


def find_reachable_features_in_time_from_place(
        start: str,
        G: nx.MultiDiGraph,
        trip_times: list[int],
        velocity: float = True,
) -> dict[int, list[tuple[str, int]]]:
    """
    Find reachable features in time from a place.
    The place will be geocoded to get the coordinates, and if 
    it is a polygon, the centroid will be used as the start point.
    The trip time is in seconds.
    The velocity is in km/h, if None the minimum time given by max_velocity attribute is used.
    The function will return a dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    The feature is the name of the feature and the osmId is the id of the feature in the graph.

    Parameters
    ----------
    start : str
        The place to start from.
    G : nx.MultiDiGraph
        The graph to search in.
    trip_times : int or list[int]
        The trip time in seconds.
    velocity : float, optional
        The velocity in km/h. The default is None.

    Returns
    -------
    dict[int, list[tuple[str, int]]]
        A dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    """
    msg = "Starting to find reachable features from place"
    utils.log(msg)
    start_geom = ox.geocode(start)
    start_node = og.distance.nearest_nodes(G, start_geom[1], start_geom[0])
    return find_reachable_features_in_time_from_node(start_node, G, trip_times, velocity=velocity)


@overload
def find_reachable_features_in_time_from_node(
        start_node: int,
        G: nx.MultiDiGraph,
        trip_times: list[int],
        velocity: float = None,
) ->tuple[dict[int, list[tuple[str, int]]], dict[int, list[int]]]: ...

def find_reachable_features_in_time_from_node(
        start_node: int,
        G: nx.MultiDiGraph,
        trip_times: list[int],
        velocity: float = None,
    ) -> dict[int, list[tuple[str, int]]]:
    """
    Find reachable features in time from a node.
    The node should be in the graph.
    The trip time is in seconds.
    The velocity is in km/h, if None the minimum time given by max_velocity attribute is used.
    The function will return a dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    The feature is the name of the feature and the osmId is the id of the feature in the graph.

    Parameters
    ----------
    start_node : int
        The node to start from.
    G : nx.MultiDiGraph
        The graph to search in.
    trip_times : int or list[int]
        The trip time in seconds.
    velocity : float, optional
        The velocity in km/h. The default is None.

    Returns
    -------
    dict[int, list[tuple[str, int]]]
        A dictionary with the trip time as key and a set of tuples (feature, osmId) as value.
    """
    msg = "Starting to find reachable features from node"
    utils.log(msg)
    if velocity == None:
        add_speed_and_time(G)
        distance = "travel_time"
    else:
        G = add_time_to_edge(G, velocity=velocity)
        distance = "time"
    
    reachable_feature = {}

    for trip_time in trip_times:
        msg = f"Finding reachable features in {trip_time} second(s) from node {start_node}"
        utils.log(msg)
        subgraph : nx.MultiDiGraph = nx.ego_graph(G, start_node, radius=trip_time, distance=distance)
        reachable_feature[trip_time] = list()
        seen = set()
        for node, data in subgraph.nodes(data=True):
            if data.get("feature") != None:
                if node not in seen:
                    seen.add(node)
                    reachable_feature[trip_time].append(node)
    
    return reachable_feature


def extract_reachable_feature_from_gdf(
    gdf: gpd.GeoDataFrame,
    reachable_feature: dict[int, list[tuple[str, int]]],
    including_filters: dict[str, bool] = None,
    excluding_filters: dict[str, bool] = None,
    ) -> dict[int, gpd.GeoDataFrame]:
    """
    This function extract the set of feature form the result of find_reachable_features_in_time_from_*.
    You need to pass the gdf that contains the features you previously downloaded from OSM, added to the graph;
    you need also to pass the result of the function find_reachable_features_in_time_from_*.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        The gdf that contains the features you previously downloaded from OSM, added to the graph.
    reachable_feature : dict[int, list[tuple[str, int]]]
        The result of the function find_reachable_features_in_time_from_*.
        The key is the trip time in seconds, and the value is a list of tuples (feature, osmId).
    including_filters : dict[str, bool], optional
        A dictionary of filters to include in the gdf. The key is the column name and the value is a boolean.
        If True, the column will be included in the gdf, if False it will be excluded. The default is None.
    excluding_filters : dict[str, bool], optional
        A dictionary of filters to exclude from the gdf. The key is the column name and the value is a boolean.
        If True, the column will be excluded from the gdf, if False it will be included. The default is None.

    Returns
    -------
    dict[int, gpd.GeoDataFrame]
        A dictionary with the trip time as key and a gdf as value.
        The gdf contains the features that are reachable in the given trip time.
        The gdf is filtered according to the including_filters and excluding_filters parameters.
    """
    list_of_gdf = {}
    msg = "Starting to extract reachable features from gdf"
    utils.log(msg)
    for trip_time, features in reachable_feature.items():
        list_of_gdf[trip_time] = feature.filter_gdf(gdf.loc[features],
            including_filters=including_filters,
            excluding_filters=excluding_filters,
        ).copy()

    return list_of_gdf



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


def k_nearest_features(
    graph: nx.DiGraph | nx.MultiDiGraph,
    source: tuple,
    k: int,
    targets: set,
    start_time: float = None,
    allowed_routes: set = None,
):
    """
    Find the k nearest features from a source node in the graph.
    The features are given as a set of nodes to look for.

    Parameters
    ----------
    graph : nx.DiGraph
        The graph to search in.
    source : tuple
        The source node to start from, as a tuple (feature, osmId).
    k : int
        The number of nearest features to return.
    looking_for : set
        A set of nodes, which are the features to look for in the graph.
    start_time : float
        The departure time in seconds from the source node.
    allowed_routes : set, optional
        A set of allowed gtfs route short names. If provided, only edges with these route short name will be considered.

    Returns
    -------
    list
        A list containing the k nearest features in the graph, ordered by arrival time.
    list
        A list of paths, where each path is a list of nodes (tuples) with their corresponding route short name or 'None' for foot.
    list
        A list of arrival times for each nodes in the graph.
    
    """
    # abort immediately if the source or target node does not exist in the graph
    if source not in graph:
        raise ValueError("The source or target node does not exist in the graph.")
    if start_time == None:
        import time
        start_time = int(time.time()) % (24 * 3600) 
    # Initialize arrival times and predecessors for the current node in the queue
    arrival_times = {node: float("inf") for node in graph.nodes}
    predecessors = {node: None for node in graph.nodes}
    arrival_times[source] = start_time
    queue = [(0, start_time, source )]
    visited = set()
    founded = dict()
    # Track used routes
    routes = {}
    # footh_length = {node: float("inf") for node in graph.nodes}
    # current_footh_length = 0
    # while the queue is not empty and the target node has not been visited
    while queue:
        # Extract the node with the smallest arrival time from the queue
        _, current_time, u = heapq.heappop(queue)

        # If the node has already been visited with a better result, skip it
        if u in visited and current_time > arrival_times[u]:
            continue

        visited.add(u)
        if u in targets:
            if u in founded:
                if current_time < founded[u]:
                    founded[u] = current_time
            else:
                founded[u] = current_time

        # Iterate over all neighbors of the node
        for v in graph.neighbors(u):
            if v not in visited:
                waiting = False
                if graph.nodes[u].get("type",None) == "osm" and graph.nodes[v].get("type",None) == "stop" :
                    if v[1] < current_time:
                        # If the neighbor is in the past, skip it
                        continue
                    else:
                        waiting = True

                last_route = routes.get(u)
                route_id = graph[u][v][0].get("route_id", None)
                if (allowed_routes!=None and route_id!=None and route_id not in allowed_routes):
                    continue
                    
                time = graph[u][v][0]["time"]
                if waiting:
                    new_arrival_time = v[1]
                else:
                    new_arrival_time = current_time + time

                
                if new_arrival_time < arrival_times[v]:
                    arrival_times[v] = new_arrival_time
                    # footh_length[v] = current_footh_length
                    routes[v] = graph[u][v][0].get("route_id", None)

                    # Assign the current node U as the predecessor of the neighbor V (in the loop)
                    predecessors[v] = u
                    # Add the neighbor to the queue with the new arrival time and a penalty if the edge is not a public transport edge
                    if route_id is None and graph.nodes[v].get("type", None) == "osm":
                        heapq.heappush(queue, (3000*new_arrival_time, new_arrival_time, v))
                    else:
                        heapq.heappush(queue, (new_arrival_time, new_arrival_time, v))


    # now, get the first k features with lowest arrival times
    targets = sorted(founded.items(), key=lambda x: x[1])[:k]

    # reconstruct the path for every k target node
    paths = []
    for target, arrival_time in targets:
        path = _reconstruct_path(target=target, predecessors=predecessors)
        if path[0] == source:
            # Iterate over all nodes in the path
            for i in range(len(path) - 1):
                v = path[i + 1]
                path[i] = (path[i], routes[v])
        paths.append(path)

    if len(paths) != 0:
        return targets, paths, arrival_times
    else:
        # If the path does not start with the source node, something went wrong, the path was not found
        return [], [], []