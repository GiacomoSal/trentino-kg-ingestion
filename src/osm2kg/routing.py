"""
Functions for routing: add speed, time and shortest/circular path calculations.
"""
from __future__ import annotations
from typing import overload
import numpy as np
import osmnx as ox
import networkx as nx
import pandas as pd
import geopandas as gpd
import osm2kg as og
from . import convert
from . import utils
from shapely.geometry import Point, LineString
from shapely.affinity import rotate, scale, translate
from shapely.ops import linemerge
#--


import itertools
import logging as lg
import multiprocessing as mp
import re
from collections.abc import Iterable
from collections.abc import Iterator
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from warnings import warn

if TYPE_CHECKING:
    import geopandas as gpd


hwy_speeds = {
    "motorway": 130,
    "motorway_link": 40,
    "trunk": 110,
    "trunk_link": 40,
    "primary": 90,
    "primary_link": 50,
    "secondary": 90,
    "secondary_link": 50,
    "tertiary": 90,
    "tertiary_link": 50,
    "unclassified": 50,
    "residential": 50,
    "living_street": 30,
    "service": 50,
    "pedestrian": 30,
}

def add_speed_and_time(
    G: nx.MultiDiGraph,
    hwy_speeds=hwy_speeds,
) -> nx.MultiDiGraph:
    """
    Add speed and time to each edge in the graph based on edge max_speed.
    If no max_speed tag is found, it will use hwy_speeds to set a probably correct speed.
    The velocity is in km/h.
    The time is calculated in seconds.

    Parameters
    ----------
    G : nx.MultiDiGraph
        The graph to add speed and time to.
    hwy_speeds : dict, optional
        The dictionary of highway speeds. The default is hwy_speeds.

    Returns
    -------
    nx.MultiDiGraph
        The graph with speed and time added to each edge.
    """
    G = add_edge_speeds(G, hwy_speeds=hwy_speeds)
    return add_edge_travel_times(G)


def add_time_to_edge(
    G: nx.MultiDiGraph,
    velocity: float = 4.5,
    weight: str = "time",
) -> nx.MultiDiGraph:
    """
    Add time to each edge in the graph based on the given velocity.
    The velocity is in km/h.
    The time is calculated in seconds.

    Parameters
    ----------
    G : nx.MultiDiGraph
        The graph to add time to.
    velocity : float, optional
        The velocity in km/h. The default is 4.5.

    Returns
    -------
    nx.MultiDiGraph
        The graph with time added to each edge.
    """
    if type(G) is nx.DiGraph :
        meters_per_second = velocity / 3.6  # km per hour to m per second
        for _, _, data in G.edges(data=True):
            data[weight] = data["length"] / meters_per_second
    else:
        meters_per_second = velocity / 3.6  # km per hour to m per second
        for _, _, _, data in G.edges(data=True, keys=True):
            data[weight] = data["length"] / meters_per_second
    return G


def shortest_path_gdf(
        G: nx.MultiDiGraph,
        origin: int,
        destination: int,
        weight: str = "length",
        velocity: float = 4.5,
)-> gpd.GeoDataFrame:
    """
    Find the shortest path between two nodes in the graph and return it as a GeoDataFrame.
    The path is calculated using the velocity if provided, otherwise the max_speed attribute is used.

    Parameters
    ----------
    G : nx.MultiDiGraph
        The graph to find the shortest path in.
    origin : int
        The origin node id.
    destination : int
        The destination node id.
    velocity : float, optional
        The velocity in km/h. The default is 4.5.
        If None, the max_speed attribute will be used.

    Returns
    -------
    GeoDataFrame
        A GeoDataFrame with the shortest path.
    """
    path = shortest_path(G, origin, destination, weight=weight)
    return route_to_gdf(G, path)


def simplyfied_path_gdf(
    path_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame of the path simplyfied.

    Parameters
    ----------
    path_gdf
        The gdf rappresenti the path

    Returns
    -------
    GeoDataFrame
        The simplyfied one
    """
    # 1. Get start and end node
    start = path_gdf.index[0][0]  # first 'u'
    end = path_gdf.index[-1][1]   # last 'v'

    # 2. Merge all geometries into a single LineString
    lines = list(path_gdf['geometry'])
    merged_line = linemerge(lines)

    # 3. Sum length and time
    total_length = path_gdf['length'].sum()
    total_time = path_gdf['time'].sum()

    # 4. Create a new GeoDataFrame
    route_gdf = gpd.GeoDataFrame([{
        "departure": start,
        "destination": end,
        "geometry": merged_line,
        "distance": total_length,
        "time": total_time
    }], crs=path_gdf.crs)
    return route_gdf


def calculate_time_and_distance(
        path_gdf: gpd.GeoDataFrame
) -> tuple[float, float]:
    """
    Calculate the time and distance of a path coverted in a GeoDataFrame
    by the '.routing.route_to_gdf()' or by the result of shortest_path_gdf().
    The time is calculated in seconds and the distance in meters.

    Parameters
    ----------
    path_gdf : GeoDataFrame
        The GeoDataFrame with the path.

    Returns
    -------
    tuple
        A tuple with the time in seconds and the distance in meters.
    """
    distance = path_gdf["length"].sum()
    time = path_gdf["time"].sum()
    return time, distance

@overload # if nodes is True, return a tuple of two GeoDataFrames, nodes and egdes
def route_to_gdf(
    G: nx.MultiDiGraph,
    route: list[int],
    nodes: bool = True,
    *,
    weight: str = "length",
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: ...
    

def route_to_gdf(
    G: nx.MultiDiGraph,
    route: list[int],
    nodes: bool = False,
    *,
    weight: str = "length",
) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame of the edges in a path, in order.

    Parameters
    ----------
    G
        Input graph.
    route : list[int]
        Node IDs constituting the path.
    weight : str, optional
        Attribute value to minimize when choosing between parallel edges.
    nodes : bool, optional
        If True, return a tuple of two GeoDataFrames, one with the nodes and one with the edges.

    Returns
    -------
    gdf_edges
        The ordered edges in the path.
        or
    gdf_nodes, gdf_edges
        If nodes is True, return a tuple of two GeoDataFrames, one with the nodes and one with the edges.
    """
    pairs = zip(route[:-1], route[1:])
    if type(G) is nx.DiGraph :
        uv = ((u, v) for u, v in pairs)
    else:
        uvk = ((u, v, min(G[u][v].items(), key=lambda i: i[1][weight])[0]) for u, v in pairs)
    if G.subgraph(route).edges==0:
        print("No edges in the subgraph, returning empty GeoDataFrame")
    if nodes:
        if type(G) is nx.DiGraph :
            n, e = convert.digraph_to_gdfs(G.subgraph(route), nodes=True)
            mask = e.index.isin(list(uv))
            return n, e.loc[mask]
        else:
            n, e = convert.graph_to_gdfs(G.subgraph(route), nodes=True)
            return n, e.loc[uvk]
    else:
        if type(G) is nx.DiGraph :
            e = convert.digraph_to_gdfs(G.subgraph(route), nodes=False)
            mask = e.index.isin(list(uv))
            return e.loc[mask]
        else:
            e = convert.graph_to_gdfs(G.subgraph(route), nodes=False)
            return e.loc[uvk]


def _penalized_weight_function_counter(G, edge_use_counts, base_weight="length", penalty=1.25):
    """
    Create a weight function that penalizes edges based on their usage count."""
    def weight(u, v, data):
        base = data[0].get(base_weight)
        count = edge_use_counts.get((v, u), 0)
        return base * (1 + penalty * count)
    return weight



def _remove_cul_de_sacs(graph, path:list):
    """Remove cul-de-sacs from a path
    
    Parameters
    ----------
    graph : nx.Graph
        The graph from which the path is extracted.
    path : list
        The path as a list of node IDs.
    
    Returns
    -------
    list
        The path with cul-de-sacs removed.
    """
    if len(path) <= 2:
        return path
    
    view = nx.subgraph(graph, path).copy()
    if not view.graph.get("simplified"): 
        view = ox.simplification.simplify_graph(view)
    cul_de_sacs = set()
    cul_de_sacs.update([n for n in view.nodes if (view.degree(n) == 2 and (view[n][(next(iter(view.neighbors(n))))][0].get("length") < 550))])
    i = -1
    while i < len(path) - 1:
        i+=1
        if path[i] in cul_de_sacs:
            if i == 0 or i == len(path) - 1:
                continue  # Skip the first node
            x = 1
            y = 1
            path[i] = -1  # Mark as removed
            last_node = path[i+y]
            while i-x >= 0 and i+y < len(path) and path[i-x] == path[i+y]  :
                last_node = path[i+y]
                if i-x < 0 or i+y >= len(path):
                    break  # Prevent index out of range
                path[i-x] = -1
                path[i+y] = -1
                x += 1
                y += 1
            yy = y-1
            path[i+yy] = last_node  # Keep the last node of the cul-de-sac to maintain connection
            i += yy
    paths = [p for p in path if p!=-1]
    
    return paths
    

def track_from_node(
    G: nx.Graph,
    start_node: int,
    desired_length: float,
    length_margin: float = 1500,
    perimeter_ratio: float = 4.6,
    a_b_ratio: float = 0.4,
    num_points: int = 4,
    max_grade: float = None,
    min_altitude: float = None,
    max_altitude: float = None,
    rotation_step: float = 15,  # degrees between each attempt
    max_rotations: int = 24,    # 360/15 = 24
) -> list[tuple[float,list[int]]]:
    """
    Generate some possible cycles from a starting point.
    The cycles could have lengths, altitudes and grades constraints.
    
    Parameters
    ----------
    G : nx.Graph
        The graph to generate the cycle from.
    start_node : int
        The starting node ID.
    desired_length : float
        The desired length of the cycle in meters.
    length_margin : float, optional
        The margin of error for the length of the cycle in meters. Default is 1500.
    perimeter_ratio : float, optional
        This parameter is used to scale the value of a of the ellipse.
        The default is 0.46 and works well only if a_b_ratio is 0.4.
        Also the desired_length is divided by 2 before is used as the perimeter of the ellipse.
    a_b_ratio : float, optional
        The ratio between the semi-major axis (a) and the semi-minor axis (b) of the ellipse.
    num_points : int, optional
        The number of points to sample along the perimeter of the ellipse.
        Default is 4, if you increse it a lot, the track will probably have more cul-de-sacs.
    max_grade : float, optional
        The maximum grade of the cycle. If None, no constraint is applied.
    min_altitude : float, optional
        The minimum altitude of the cycle. If None, no constraint is applied.
    max_altitude : float, optional
        The maximum altitude of the cycle. If None, no constraint is applied.
    rotation_step : float, optional
        The angle in degrees between each rotation of the ellipse to generate different cycles.
        Default is 15 degrees.
    max_rotations : int, optional
        The maximum number of rotations to generate different cycles.
        Default is 24, which means a full rotation of 360 degrees.

    Returns
    -------
    list[tuple[float,list[int]]]
        A list of tuples, each containing the length of the cycle as the first element of the tuple and the list of node IDs in the cycle as the second one.
        The length is in meters and the node IDs are in the order they appear in the cycle.
    """
    if G.graph["crs"] == "epsg:4326":
        if type(G) is nx.DiGraph:
            G = og.projection.project_digraph(G)
        else:
            G = og.project_graph(G)
    # Get coordinates of start
    x0, y0 = G.nodes[start_node]["x"], G.nodes[start_node]["y"]

    # Generate base shape
    msg = "Generating ellipse shape"
    og.utils.log(msg)
    a = (desired_length/2)/perimeter_ratio  # Scale factor for x-axis
    b = a * a_b_ratio  # Scale factor for y-axis (10% of a)
    base_shape = Point(x0, y0).buffer(1.0)
    shape_geom = scale(base_shape, xfact=a, yfact=b)
    dx = a
    translated = translate(shape_geom, xoff=dx, yoff=0).exterior

    # filter the graph by altitude and grade
    # Filter the graph based on provided constraints
    node_filter = None
    edge_filter = no_filter = lambda *args: True
    msg = f"Filtering graph with constraints: min_altitude={min_altitude}, max_altitude={max_altitude}, max_grade={max_grade}, perimeter={desired_length}"
    og.utils.log(msg)
    # Build node filters based on altitude constraint
    if min_altitude is not None and max_altitude is not None:
        node_filter = lambda n: min_altitude <= G.nodes[n].get("elevation") <= max_altitude
    elif min_altitude is None and max_altitude is None:
        node_filter = no_filter = lambda *args: True
    elif min_altitude is not None:
        node_filter = lambda n: G.nodes[n].get("elevation", 0) >= min_altitude
    elif max_altitude is not None:
        node_filter = lambda n: G.nodes[n].get("elevation", 0) <= max_altitude
        
    # Build edge filter for grade constraint
    if max_grade is not None:
        edge_filter = lambda u, v, k: G[u][v][k].get("grade", 0) <= max_grade
        
    # Create subgraph view with appropriate filters
    G_view = nx.subgraph_view(G, filter_node=node_filter, filter_edge=edge_filter)
    
    # deleting all the nodes that are not reachable from the start node
    for component in nx.strongly_connected_components(G_view):
        if start_node in component:
            subgraph = G_view.subgraph(component).copy()
            break
    if subgraph is None or G_view is None:
        raise nx.NetworkXNoPath(f"Start node {start_node} not in track with the following constraints: min_altitude={min_altitude}, max_altitude={max_altitude}, max_grade={max_grade}, length={desired_length}")


    possible_paths = []
    geomtries = []
    # rotate the shape to obtain more than one possible cycle
    msg = f"Generating cycles with {max_rotations} rotations and {num_points} points per rotation"
    og.utils.log(msg)
    for rot_idx in range(max_rotations):
        angle = rot_idx * rotation_step
        shape_geom = rotate(translated, angle, origin=(x0,y0))


        # Sample points using as starting point the start_node
        start_distance = shape_geom.project(Point(x0, y0), normalized=True)
        step = 1 / num_points
        perimeter_points = [
            shape_geom.interpolate((start_distance + i * step) % 1, normalized=True)
            for i in range(num_points)
        ]
        # Extract coordinates as NumPy arrays
        sampled_coords = np.array([(pt.x, pt.y) for pt in perimeter_points])
        x_coords = sampled_coords[:, 0]
        y_coords = sampled_coords[:, 1]

        # Map coordinates to nearest nodes
        sampled_nodes = og.distance.nearest_nodes(
            subgraph, x_coords, y_coords
            )

        # Ensure uniqueness
        node_sequence = list(dict.fromkeys(sampled_nodes))

        
        # Build cycle
        full_path = []
        # generate a penalized weight function to avoid using the same edge multiple times
        used_edges = {}
        weight_fn = _penalized_weight_function_counter(subgraph, used_edges)
        for i in range(len(node_sequence)):
            u = node_sequence[i]
            v = node_sequence[(i + 1) % len(node_sequence)]
            try:
                segment = nx.shortest_path(subgraph, u, v, weight=weight_fn)
                full_path.extend(segment[:-1])
                for seg_start, seg_end in zip(segment[:-1], segment[1:]):
                    edge = (seg_start, seg_end)
                    used_edges[edge] = used_edges.get(edge, 0) + 1
            except nx.NetworkXNoPath:
                # msg = f"No path find betwenn {u} and {v}, so no possible cycle"
                # og.utils.log(msg)
                break
            
        if len(full_path) == 0:
            continue  # Skip to the next rotation if no path is found
        full_path.append(full_path[0])  # close the cycle
        full_path = _remove_cul_de_sacs(subgraph, full_path)
        if len(full_path) < 2:
            continue
        if len(subgraph.edges) == 0:
            print("No nodes in the graph, skipping cycle generation")
        e = route_to_gdf(subgraph, full_path)
        length = e["length"].sum()
        if length >= (desired_length - length_margin) and length <= (desired_length + length_margin):
            possible_paths.append((length,full_path))

    if len(possible_paths) == 0:
        raise nx.NetworkXNoPath("No path found for the given constraints.")
    else:
        msg = f"Found {len(possible_paths)} possible cycles with the given constraints."
        og.utils.log(msg)

    return possible_paths

# ----------------------------------------------from __future__ import annotations

"""Calculate edge speeds, travel times, and weighted shortest paths."""



# Dict that is used by `add_edge_speeds` to convert implicit values
# to numbers, based on https://wiki.openstreetmap.org/wiki/Key:maxspeed
_IMPLICIT_MAXSPEEDS: dict[str, float] = {
    "AR:rural": 110.0,
    "AR:urban": 40.0,
    "AR:urban:primary": 60.0,
    "AR:urban:secondary": 60.0,
    "AT:bicycle_road": 30.0,
    "AT:motorway": 130.0,
    "AT:rural": 100.0,
    "AT:trunk": 100.0,
    "AT:urban": 50.0,
    "BE-BRU:rural": 70.0,
    "BE-BRU:urban": 30.0,
    "BE-VLG:rural": 70.0,
    "BE-VLG:urban": 50.0,
    "BE-WAL:rural": 90.0,
    "BE-WAL:urban": 50.0,
    "BE:cyclestreet": 30.0,
    "BE:living_street": 20.0,
    "BE:motorway": 120.0,
    "BE:trunk": 120.0,
    "BE:zone30": 30.0,
    "BG:living_street": 20.0,
    "BG:motorway": 140.0,
    "BG:rural": 90.0,
    "BG:trunk": 120.0,
    "BG:urban": 50.0,
    "BY:living_street": 20.0,
    "BY:motorway": 110.0,
    "BY:rural": 90.0,
    "BY:urban": 60.0,
    "CA-AB:rural": 90.0,
    "CA-AB:urban": 65.0,
    "CA-BC:rural": 80.0,
    "CA-BC:urban": 50.0,
    "CA-MB:rural": 90.0,
    "CA-MB:urban": 50.0,
    "CA-ON:rural": 80.0,
    "CA-ON:urban": 50.0,
    "CA-QC:motorway": 100.0,
    "CA-QC:rural": 75.0,
    "CA-QC:urban": 50.0,
    "CA-SK:nsl": 80.0,
    "CH:motorway": 120.0,
    "CH:rural": 80.0,
    "CH:trunk": 100.0,
    "CH:urban": 50.0,
    "CZ:living_street": 20.0,
    "CZ:motorway": 130.0,
    "CZ:pedestrian_zone": 20.0,
    "CZ:rural": 90.0,
    "CZ:trunk": 110.0,
    "CZ:urban": 50.0,
    "CZ:urban_motorway": 80.0,
    "CZ:urban_trunk": 80.0,
    "DE:bicycle_road": 30.0,
    "DE:living_street": 15.0,
    "DE:motorway": 120.0,
    "DE:rural": 80.0,
    "DE:urban": 50.0,
    "DK:motorway": 130.0,
    "DK:rural": 80.0,
    "DK:urban": 50.0,
    "EE:rural": 90.0,
    "EE:urban": 50.0,
    "ES:living_street": 20.0,
    "ES:motorway": 120.0,
    "ES:rural": 90.0,
    "ES:trunk": 90.0,
    "ES:urban": 50.0,
    "ES:zone30": 30.0,
    "FI:motorway": 120.0,
    "FI:rural": 80.0,
    "FI:trunk": 100.0,
    "FI:urban": 50.0,
    "FR:motorway": 120.0,
    "FR:rural": 80.0,
    "FR:urban": 50.0,
    "FR:zone30": 30.0,
    "GB:nsl_restricted": 48.28,
    "GR:motorway": 130.0,
    "GR:rural": 90.0,
    "GR:trunk": 110.0,
    "GR:urban": 50.0,
    "HU:living_street": 20.0,
    "HU:motorway": 130.0,
    "HU:rural": 90.0,
    "HU:trunk": 110.0,
    "HU:urban": 50.0,
    "IT:motorway": 130.0,
    "IT:rural": 90.0,
    "IT:trunk": 110.0,
    "IT:urban": 50.0,
    "JP:express": 100.0,
    "JP:nsl": 60.0,
    "LT:rural": 90.0,
    "LT:urban": 50.0,
    "NO:rural": 80.0,
    "NO:urban": 50.0,
    "PH:express": 100.0,
    "PH:rural": 80.0,
    "PH:urban": 30.0,
    "PT:motorway": 120.0,
    "PT:rural": 90.0,
    "PT:trunk": 100.0,
    "PT:urban": 50.0,
    "RO:motorway": 130.0,
    "RO:rural": 90.0,
    "RO:trunk": 100.0,
    "RO:urban": 50.0,
    "RS:living_street": 10.0,
    "RS:motorway": 130.0,
    "RS:rural": 80.0,
    "RS:trunk": 100.0,
    "RS:urban": 50.0,
    "RU:living_street": 20.0,
    "RU:motorway": 110.0,
    "RU:rural": 90.0,
    "RU:urban": 60.0,
    "SE:rural": 70.0,
    "SE:urban": 50.0,
    "SI:motorway": 130.0,
    "SI:rural": 90.0,
    "SI:trunk": 110.0,
    "SI:urban": 50.0,
    "SK:living_street": 20.0,
    "SK:motorway": 130.0,
    "SK:motorway_urban": 90.0,
    "SK:rural": 90.0,
    "SK:trunk": 90.0,
    "SK:urban": 50.0,
    "TR:living_street": 20.0,
    "TR:motorway": 130.0,
    "TR:rural": 90.0,
    "TR:trunk": 110.0,
    "TR:urban": 50.0,
    "TR:zone30": 30.0,
    "UA:living_street": 20.0,
    "UA:motorway": 130.0,
    "UA:rural": 90.0,
    "UA:trunk": 110.0,
    "UA:urban": 50.0,
    "UK:motorway": 112.65,
    "UK:nsl_dual": 112.65,
    "UK:nsl_single": 96.56,
    "UZ:living_street": 30.0,
    "UZ:motorway": 110.0,
    "UZ:rural": 100.0,
    "UZ:urban": 70.0,
}


# orig/dest int, weight present, cpus present
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
    *,
    weight: str,
    cpus: int | None,
) -> list[tuple] | None: ...


# orig/dest int, weight missing, cpus present
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
    *,
    cpus: int | None,
) -> list[tuple] | None: ...


# orig/dest int, weight present, cpus missing
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
    *,
    weight: str,
) -> list[tuple] | None: ...


# orig/dest int, weight missing, cpus missing
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
) -> list[tuple] | None: ...


# orig/dest Iterable, weight present, cpus present
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: Iterable[tuple],
    dest: Iterable[tuple],
    *,
    weight: str,
    cpus: int | None,
) -> list[list[tuple] | None]: ...


# orig/dest Iterable, weight missing, cpus present
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: Iterable[tuple],
    dest: Iterable[tuple],
    *,
    cpus: int | None,
) -> list[list[tuple] | None]: ...


# orig/dest Iterable, weight present, cpus missing
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: Iterable[tuple],
    dest: Iterable[tuple],
    *,
    weight: str,
) -> list[list[tuple] | None]: ...


# orig/dest Iterable, weight missing, cpus missing
@overload
def shortest_path(
    G: nx.MultiDiGraph,
    orig: Iterable[tuple],
    dest: Iterable[tuple],
) -> list[list[tuple] | None]: ...


def shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple | Iterable[tuple],
    dest: tuple | Iterable[tuple],
    *,
    weight: str = "length",
    cpus: int | None = 1,
) -> list[tuple] | None | list[list[tuple] | None]:
    """
    Solve shortest path from origin node(s) to destination node(s).

    Uses Dijkstra's algorithm. If `orig` and `dest` are single node IDs, this
    will return a list of the nodes constituting the shortest path between
    them. If `orig` and `dest` are lists of node IDs, this will return a list
    of lists of the nodes constituting the shortest path between each
    origin-destination pair. If a path cannot be solved, this will return None
    for that path. You can parallelize solving multiple paths with the `cpus`
    parameter, but be careful to not exceed your available RAM.

    See also `k_shortest_paths` to solve multiple shortest paths between a
    single origin and destination. For additional functionality or different
    solver algorithms, use NetworkX directly.

    Parameters
    ----------
    G
        Input graph.
    orig
        Origin node ID(s).
    dest
        Destination node ID(s).
    weight
        Edge attribute to minimize when solving shortest path.
    cpus
        How many CPU cores to use if multiprocessing. If None, use all
        available. If you are multiprocessing, make sure you protect your
        entry point: see the Python docs for details.

    Returns
    -------
    path
        The node IDs constituting the shortest path, or, if `orig` and `dest`
        are both iterable, then a list of such paths.
    """
    _verify_edge_attribute(G, weight)

    # if both are a single tuple so a single node, just return the shortest path
    if (isinstance(orig, tuple) and isinstance(dest, tuple)):
        return _single_shortest_path(G, orig, dest, weight)

    # if only 1 of orig or dest is iterable and the other is not, raise error
    if not (isinstance(orig, Iterable) and isinstance(dest, Iterable)):
        msg = "`orig` and `dest` must either both be iterable or neither must be iterable."
        raise TypeError(msg)

    # if both orig and dest are iterable, make them lists (so we're guaranteed
    # to be able to get their sizes) then ensure they have same lengths
    orig = list(orig)
    dest = list(dest)
    if len(orig) != len(dest):  # pragma: no cover
        msg = "`orig` and `dest` must be of equal length."
        raise ValueError(msg)

    # determine how many cpu cores to use
    if cpus is None:
        cpus = mp.cpu_count()
    cpus = min(cpus, mp.cpu_count())

    msg = f"Solving {len(orig)} paths with {cpus} CPUs..."
    utils.log(msg, level=lg.INFO)

    # if single-threading, calculate each shortest path one at a time
    if cpus == 1:
        paths = [_single_shortest_path(G, o, d, weight) for o, d in zip(orig, dest)]

    # if multi-threading, calculate shortest paths in parallel
    else:
        args = ((G, o, d, weight) for o, d in zip(orig, dest))
        with mp.get_context().Pool(cpus) as pool:
            paths = pool.starmap_async(_single_shortest_path, args).get()

    return paths


def k_shortest_paths(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
    k: int,
    *,
    weight: str = "length",
) -> Iterator[list[int]]:
    """
    Solve `k` shortest paths from an origin node to a destination node.

    Uses Yen's algorithm. See also `shortest_path` to solve just the one
    shortest path.

    Parameters
    ----------
    G
        Input graph.
    orig
        Origin node ID.
    dest
        Destination node ID.
    k
        Number of shortest paths to solve.
    weight
        Edge attribute to minimize when solving shortest paths.

    Yields
    ------
    path
        The node IDs constituting the next-shortest path.
    """
    _verify_edge_attribute(G, weight)
    paths_gen = nx.shortest_simple_paths(
        G=convert.to_digraph(G, weight=weight),
        source=orig,
        target=dest,
        weight=weight,
    )
    yield from itertools.islice(paths_gen, 0, k)


def _single_shortest_path(
    G: nx.MultiDiGraph,
    orig: tuple,
    dest: tuple,
    weight: str,
) -> list[int] | None:
    """
    Solve the shortest path from an origin node to a destination node.

    This function uses Dijkstra's algorithm. It is a convenience wrapper
    around `networkx.shortest_path`, with exception handling for unsolvable
    paths. If the path is unsolvable, it returns None.

    Parameters
    ----------
    G
        Input graph.
    orig
        Origin node ID.
    dest
        Destination node ID.
    weight
        Edge attribute to minimize when solving shortest path.

    Returns
    -------
    path
        The node IDs constituting the shortest path.
    """
    try:
        return list(nx.shortest_path(G, orig, dest, weight=weight, method="dijkstra"))
    except nx.exception.NetworkXNoPath:  # pragma: no cover
        msg = f"Cannot solve path from {orig} to {dest}"
        utils.log(msg, level=lg.WARNING)
        return None


def _verify_edge_attribute(G: nx.MultiDiGraph, attr: str) -> None:
    """
    Verify attribute values are numeric and non-null across graph edges.

    Raises a ValueError if this attribute contains non-numeric values, and
    issues a UserWarning if this attribute is missing or null on any edges.

    Parameters
    ----------
    G
        Input graph.
    attr
        Name of the edge attribute to verify.
    """
    try:
        values_float = np.array([data for _, _, data in G.edges(data=attr)]).astype(float)
        if np.isnan(values_float).any():
            msg = f"The attribute {attr!r} is missing or null on some edges."
            warn(msg, category=UserWarning, stacklevel=2)
    except ValueError as e:
        msg = f"The edge attribute {attr!r} contains non-numeric values."
        raise ValueError(msg) from e


def add_edge_speeds(
    G: nx.MultiDiGraph,
    *,
    hwy_speeds: dict[str, float] | None = None,
    fallback: float | None = None,
    agg: Callable[[Any], Any] = np.mean,
) -> nx.MultiDiGraph:
    """
    Add edge speeds (km per hour) to graph as new `speed_kph` edge attributes.

    By default, this imputes free-flow travel speeds for all edges via the
    mean `maxspeed` value of the edges of each highway type. For highway types
    in the graph that have no `maxspeed` value on any edge, it assigns the
    mean of all `maxspeed` values in graph.

    This default mean-imputation can obviously be imprecise, and the user can
    override it by passing in `hwy_speeds` and/or `fallback` arguments that
    correspond to local speed limit standards. The user can also specify a
    different aggregation function (such as the median) to impute missing
    values from the observed values.

    If edge `maxspeed` attribute has "mph" in it, value will automatically be
    converted from miles per hour to km per hour. Any other speed units should
    be manually converted to km per hour prior to running this function,
    otherwise there could be unexpected results. If "mph" does not appear in
    the edge's maxspeed attribute string, then function assumes kph, per OSM
    guidelines: https://wiki.openstreetmap.org/wiki/Map_Features/Units

    If you wish to set all edge speeds to a single constant value (such as for
    a walking network), use `nx.set_edge_attributes` to set the `speed_kph`
    attribute value directly, rather than using this function.

    Parameters
    ----------
    G
        Input graph.
    hwy_speeds
        Dict keys are OSM highway types and values are typical speeds (km per
        hour) to assign to edges of that highway type for any edges missing
        speed data. Any edges with highway type not in `hwy_speeds` will be
        assigned the mean pre-existing speed value of all edges of that
        highway type.
    fallback
        Default speed value (km per hour) to assign to edges whose highway
        type did not appear in `hwy_speeds` and had no pre-existing speed
        attribute values on any edge.
    agg
        Aggregation function to impute missing values from observed values.
        The default is `numpy.mean`, but you might also consider for example
        `numpy.median`, `numpy.nanmedian`, or your own custom function.

    Returns
    -------
    G
        Graph with `speed_kph` attributes on all edges.
    """
    if fallback is None:
        fallback = np.nan

    edges = convert.graph_to_gdfs(G, nodes=False, fill_edge_geometry=False)

    # collapse any highway lists (can happen during graph simplification)
    # into string values simply by keeping just the first element of the list
    edges["highway"] = edges["highway"].map(lambda x: x[0] if isinstance(x, list) else x)

    if "maxspeed" in edges.columns:
        # collapse any maxspeed lists (can happen during graph simplification)
        # into a single value
        edges["maxspeed"] = edges["maxspeed"].apply(_collapse_multiple_maxspeed_values, agg=agg)

        # create speed_kph by cleaning maxspeed strings and converting mph to
        # kph if necessary
        edges["speed_kph"] = edges["maxspeed"].astype(str).map(_clean_maxspeed).astype(float)
    else:
        # if no edges in graph had a maxspeed attribute
        edges["speed_kph"] = None

    # if user provided hwy_speeds, use them as default values, otherwise
    # initialize an empty series to populate with values
    hwy_speed_avg = pd.Series(dtype=float) if hwy_speeds is None else pd.Series(hwy_speeds).dropna()

    # for each highway type that caller did not provide in hwy_speeds, impute
    # speed of type by taking the mean of the preexisting speed values of that
    # highway type
    for hwy, group in edges.groupby("highway"):
        if hwy not in hwy_speed_avg:
            hwy_speed_avg.loc[hwy] = agg(group["speed_kph"])

    # if any highway types had no preexisting speed values, impute their speed
    # with fallback value provided by caller. if fallback=np.nan, impute speed
    # as the mean speed of all highway types that did have preexisting values
    hwy_speed_avg = hwy_speed_avg.fillna(fallback).fillna(agg(hwy_speed_avg))

    # for each edge missing speed data, assign it the imputed value for its
    # highway type
    speed_kph = (
        edges[["highway", "speed_kph"]].set_index("highway").iloc[:, 0].fillna(hwy_speed_avg)
    )

    # all speeds will be null if edges had no preexisting maxspeed data and
    # caller did not pass in hwy_speeds or fallback arguments
    if pd.isna(speed_kph).all():
        msg = (
            "This graph's edges have no preexisting 'maxspeed' attribute "
            "values so you must pass `hwy_speeds` or `fallback` arguments."
        )
        raise ValueError(msg)

    # add speed kph attribute to graph edges
    edges["speed_kph"] = speed_kph.to_numpy()
    nx.set_edge_attributes(G, values=edges["speed_kph"], name="speed_kph")

    return G


def add_edge_travel_times(G: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """
    Add edge travel time (seconds) to graph as new `travel_time` edge attributes.

    Calculates free-flow travel time along each edge, based on `length` and
    `speed_kph` attributes. Note: run `add_edge_speeds` first to generate the
    `speed_kph` attribute. All edges must have `length` and `speed_kph`
    attributes and all their values must be non-null.

    Parameters
    ----------
    G
        Input graph.

    Returns
    -------
    G
        Graph with `travel_time` attributes on all edges.
    """
    edges = convert.graph_to_gdfs(G, nodes=False)

    # verify edge length and speed_kph attributes exist
    if not ("length" in edges.columns and "speed_kph" in edges.columns):  # pragma: no cover
        msg = "All edges must have 'length' and 'speed_kph' attributes."
        raise KeyError(msg)

    # verify edge length and speed_kph attributes contain no nulls
    if pd.isna(edges["length"]).any() or pd.isna(edges["speed_kph"]).any():  # pragma: no cover
        msg = "Edge 'length' and 'speed_kph' values must be non-null."
        raise ValueError(msg)

    # convert distance meters to km, and speed km per hour to km per second
    distance_km = edges["length"] / 1000
    speed_km_sec = edges["speed_kph"] / (60 * 60)

    # calculate edge travel time in seconds
    travel_time = distance_km / speed_km_sec

    # add travel time attribute to graph edges
    edges["travel_time"] = travel_time.to_numpy()
    nx.set_edge_attributes(G, values=edges["travel_time"], name="travel_time")

    return G


def _clean_maxspeed(
    maxspeed: str | float,
    *,
    agg: Callable[[Any], Any] = np.mean,
    convert_mph: bool = True,
) -> float | None:
    """
    Clean a maxspeed string and convert mph to kph if necessary.

    If present, splits maxspeed on "|" (which denotes that the value contains
    different speeds per lane) then aggregates the resulting values. If given
    string is not a valid numeric string, tries to look up its value in
    implicit maxspeed values mapping. Invalid inputs return None. See
    https://wiki.openstreetmap.org/wiki/Key:maxspeed for details on values and
    formats.

    Parameters
    ----------
    maxspeed
        An OSM way "maxspeed" attribute value. Null values are expected to be
        of type float (`numpy.nan`), and non-null values are strings.
    agg
        Aggregation function if `maxspeed` contains multiple values (default
        is `numpy.mean`).
    convert_mph
        If True, convert miles per hour to kilometers per hour.

    Returns
    -------
    clean_value
        Clean value resulting from `agg` function.
    """
    MILES_TO_KM = 1.60934
    if not isinstance(maxspeed, str):
        return None

    # regex adapted from OSM wiki
    pattern = "^([0-9][\\.,0-9]+?)(?:[ ]?(?:km/h|kmh|kph|mph|knots))?$"
    values = re.split(r"\|", maxspeed)  # creates a list even if it's a single value
    try:
        clean_values = []
        for value in values:
            match = re.match(pattern, value)
            clean_value = float(match.group(1).replace(",", "."))  # type: ignore[union-attr]
            if convert_mph and "mph" in maxspeed.lower():
                clean_value = clean_value * MILES_TO_KM
            clean_values.append(clean_value)
        return float(agg(clean_values))

    except (ValueError, AttributeError):
        # if not valid numeric string, try looking it up as implicit value
        return _IMPLICIT_MAXSPEEDS.get(maxspeed)


def _collapse_multiple_maxspeed_values(
    value: str | float | list[str | float],
    agg: Callable[[Any], Any],
) -> float | str | None:
    """
    Collapse a list of maxspeed values to a single value.

    Returns None if a ValueError is encountered.

    Parameters
    ----------
    value
        An OSM way "maxspeed" attribute value. Null values are expected to be
        of type float (`numpy.nan`), and non-null values are strings.
    agg
        The aggregation function to reduce the list to a single value.

    Returns
    -------
    collapsed
        If `value` was a string or null, it is just returned directly.
        Otherwise, the return is a float representation of the aggregated
        value in the list (converted to kph if original value was in mph).
    """
    # if this isn't a list, just return it right back to the caller
    if not isinstance(value, list):
        return value

    # otherwise, it is a list, so process it
    try:
        # clean/convert each value in list as needed then aggregate
        values = [_clean_maxspeed(x) for x in value]
        collapsed: float | None = float(agg(pd.Series(values).dropna()))
    except ValueError:
        return None
    else:
        # return that single aggregated value if it's non-null, otherwise None
        if not pd.isna(collapsed):
            return collapsed
        return None
