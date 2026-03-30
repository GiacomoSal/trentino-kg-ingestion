"""
Download and create graphs from OpenStreetMap data, merging them with OSM features.
"""

from __future__ import annotations

import numpy as np
from shapely import Point, LineString
import geopandas as gpd
import osmnx as ox
import networkx as nx
import shapely

import logging as lg
from collections.abc import Iterable
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import networkx as nx
from shapely import MultiPolygon
from shapely import Polygon

import osmnx as ox
from . import truncate
from . import utils
from . import projection
from . import settings
from . import distance
from ._errors import CacheOnlyInterruptError
from ._errors import InsufficientResponseError
# from ._version import __version__

if TYPE_CHECKING:
    from collections.abc import Iterable


def graph_from_bbox(
    bbox: tuple[float, float, float, float],
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within a lat-lon bounding box.

    This function uses filters to query the Overpass API: you can either
    specify a pre-defined `network_type` or provide your own `custom_filter`
    with Overpass QL.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph. You can also use the `settings` module to retrieve a
    snapshot of historical OSM data as of a certain date, or to configure the
    Overpass server timeout, memory allocation, and other customizations.

    Parameters
    ----------
    bbox
        Bounding box as `(left, bottom, right, top)`. Coordinates should be in
        unprojected latitude-longitude degrees (EPSG:4326).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology via the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # convert bounding box to a polygon
    polygon = ox.utils_geo.bbox_to_poly(bbox)

    # create graph using this polygon geometry
    G = graph_from_polygon(
        polygon,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_bbox returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_point(
    center_point: tuple[float, float],
    dist: float,
    *,
    dist_type: str = "bbox",
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within some distance of a lat-lon point.

    This function uses filters to query the Overpass API: you can either
    specify a pre-defined `network_type` or provide your own `custom_filter`
    with Overpass QL.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph. You can also use the `settings` module to retrieve a
    snapshot of historical OSM data as of a certain date, or to configure the
    Overpass server timeout, memory allocation, and other customizations.

    Parameters
    ----------
    center_point
        The `(lat, lon)` center point around which to construct the graph.
        Coordinates should be in unprojected latitude-longitude degrees
        (EPSG:4326).
    dist
        Retain only those nodes within this many meters of `center_point`,
        measuring distance according to `dist_type`.
    dist_type
        {"bbox", "network"}
        If "bbox", retain only those nodes within a bounding box of `dist`
        length/width. If "network", retain only those nodes within `dist`
        network distance of the nearest node to `center_point`.
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    if dist_type not in {"bbox", "network"}:  # pragma: no cover
        msg = "`dist_type` must be 'bbox' or 'network'."
        raise ValueError(msg)

    # create bounding box from center point and distance in each direction
    bbox = ox.utils_geo.bbox_from_point(center_point, dist)

    # create a graph from the bounding box
    G = graph_from_bbox(
        bbox,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    if dist_type == "network":
        # find node nearest to center then truncate graph by dist from it
        node = distance.nearest_nodes(G, X=center_point[1], Y=center_point[0])
        G = truncate.truncate_graph_dist(G, node, dist)

    msg = f"graph_from_point returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_address(
    address: str,
    dist: float,
    *,
    dist_type: str = "bbox",
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within some distance of an address.

    This function uses filters to query the Overpass API: you can either
    specify a pre-defined `network_type` or provide your own `custom_filter`
    with Overpass QL.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph. You can also use the `settings` module to retrieve a
    snapshot of historical OSM data as of a certain date, or to configure the
    Overpass server timeout, memory allocation, and other customizations.

    Parameters
    ----------
    address
        The address to geocode and use as the central point around which to
        construct the graph.
    dist
        Retain only those nodes within this many meters of `center_point`,
        measuring distance according to `dist_type`.
    dist_type
        {"network", "bbox"}
        If "bbox", retain only those nodes within a bounding box of `dist`. If
        "network", retain only those nodes within `dist` network distance from
        the centermost node.
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # geocode the address string to a (lat, lon) point
    point = ox.geocoder.geocode(address)

    # then create a graph from this point
    G = graph_from_point(
        point,
        dist,
        dist_type=dist_type,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_address returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_place(
    query: str | dict[str, str] | list[str | dict[str, str]],
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    which_result: int | None | list[int | None] = None,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within the boundaries of some place(s).

    The query must be geocodable and OSM must have polygon boundaries for the
    geocode result. If OSM does not have a polygon for this place, you can
    instead get its street network using the `graph_from_address` function,
    which geocodes the place name to a point and gets the network within some
    distance of that point.

    If OSM does have polygon boundaries for this place but you're not finding
    it, try to vary the query string, pass in a structured query dict, or vary
    the `which_result` argument to use a different geocode result. If you know
    the OSM ID of the place, you can retrieve its boundary polygon using the
    `geocode_to_gdf` function, then pass it to the `features_from_polygon`
    function.

    This function uses filters to query the Overpass API: you can either
    specify a pre-defined `network_type` or provide your own `custom_filter`
    with Overpass QL.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph. You can also use the `settings` module to retrieve a
    snapshot of historical OSM data as of a certain date, or to configure the
    Overpass server timeout, memory allocation, and other customizations.

    Parameters
    ----------
    query
        The query or queries to geocode to retrieve place boundary polygon(s).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes outside the place boundary polygon(s) if at
        least one of the node's neighbors lies within the polygon(s).
    which_result
        Which geocoding result to use. if None, auto-select the first
        (Multi)Polygon or raise an error if OSM doesn't return one.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # extract the geometry from the GeoDataFrame to use in query
    polygon = ox.geocoder.geocode_to_gdf(query, which_result=which_result).union_all()
    msg = "Constructed place geometry polygon(s) to query Overpass"
    utils.log(msg, level=lg.INFO)

    # create graph using this polygon(s) geometry
    G = graph_from_polygon(
        polygon,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_place returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_polygon(
    polygon: Polygon | MultiPolygon,
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within the boundaries of a (Multi)Polygon.

    This function uses filters to query the Overpass API: you can either
    specify a pre-defined `network_type` or provide your own `custom_filter`
    with Overpass QL.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph. You can also use the `settings` module to retrieve a
    snapshot of historical OSM data as of a certain date, or to configure the
    Overpass server timeout, memory allocation, and other customizations.

    Parameters
    ----------
    polygon
        The geometry within which to construct the graph. Coordinates should
        be in unprojected latitude-longitude degrees (EPSG:4326).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes outside `polygon` if at least one of the node's
        neighbors lies within `polygon`.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # verify that the geometry is valid and is a shapely Polygon/MultiPolygon
    # before proceeding
    if not polygon.is_valid:  # pragma: no cover
        msg = "The geometry of `polygon` is invalid."
        raise ValueError(msg)
    if not isinstance(polygon, (Polygon, MultiPolygon)):  # pragma: no cover
        msg = (
            "Geometry must be a shapely Polygon or MultiPolygon. If you "
            "requested graph from place name, make sure your query resolves "
            "to a Polygon or MultiPolygon, and not some other geometry, like "
            "a Point. See OSMnx documentation for details."
        )
        raise TypeError(msg)

    # create a new buffered polygon 0.5km around the desired one
    poly_proj, crs_utm = projection.project_geometry(polygon)
    poly_proj_buff = poly_proj.buffer(500)
    poly_buff, _ = projection.project_geometry(poly_proj_buff, crs=crs_utm, to_latlong=True)

    # download the network data from OSM within buffered polygon
    response_jsons = ox._overpass._download_overpass_network(poly_buff, network_type, custom_filter)

    # create buffered graph from the downloaded data
    bidirectional = network_type in settings.bidirectional_network_types
    G_buff = _create_graph(response_jsons, bidirectional)

    # truncate buffered graph to the buffered polygon and retain_all for
    # now. needed because overpass returns entire ways that also include
    # nodes outside the poly if the way (that is, a way with a single OSM
    # ID) has a node inside the poly at some point.
    G_buff = truncate.truncate_graph_polygon(G_buff, poly_buff, truncate_by_edge=truncate_by_edge)

    # keep only the largest weakly connected component if retain_all is False
    if not retain_all:
        G_buff = truncate.largest_component(G_buff, strongly=False)

    # simplify the graph topology
    if simplify:
        G_buff = ox.simplification.simplify_graph(G_buff)

    # truncate graph by original polygon to return graph within polygon
    # caller wants. don't simplify again: this allows us to retain
    # intersections along the street that may now only connect 2 street
    # segments in the network, but in reality also connect to an
    # intersection just outside the polygon
    G = truncate.truncate_graph_polygon(G_buff, polygon, truncate_by_edge=truncate_by_edge)

    # keep only the largest weakly connected component if retain_all is False
    # we're doing this again in case the last truncate disconnected anything
    # on the periphery
    if not retain_all:
        G = truncate.largest_component(G, strongly=False)

    # count how many physical streets in buffered graph connect to each
    # intersection in un-buffered graph, to retain true counts for each
    # intersection, even if some of its neighbors are outside the polygon
    spn = ox.stats.count_streets_per_node(G_buff, nodes=G.nodes)
    nx.set_node_attributes(G, values=spn, name="street_count")

    msg = f"graph_from_polygon returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_xml(
    filepath: str | Path,
    *,
    bidirectional: bool = False,
    simplify: bool = True,
    retain_all: bool = False,
    encoding: str = "utf-8",
) -> nx.MultiDiGraph:
    """
    Create a graph from data in an OSM XML file.

    Do not load an XML file previously generated by OSMnx: this use case is
    not supported and may not behave as expected. To save/load graphs to/from
    disk for later use in OSMnx, use the `io.save_graphml` and
    `io.load_graphml` functions instead.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes.

    Parameters
    ----------
    filepath
        Path to file containing OSM XML data.
    bidirectional
        If True, create bidirectional edges for one-way streets.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    encoding
        The OSM XML file's character encoding.

    Returns
    -------
    G
        The resulting MultiDiGraph.
    """
    # transmogrify file of OSM XML data into JSON
    response_jsons = [ox._osm_xml._overpass_json_from_xml(Path(filepath), encoding)]

    # create graph using this response JSON
    G = _create_graph(response_jsons, bidirectional)

    # keep only the largest weakly connected component if retain_all is False
    if not retain_all:
        G = truncate.largest_component(G, strongly=False)

    # simplify the graph topology as the last step
    if simplify:
        G = ox.simplification.simplify_graph(G)

    msg = f"graph_from_xml returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def _create_graph(
    response_jsons: Iterable[dict[str, Any]],
    bidirectional: bool,  # noqa: FBT001
) -> nx.MultiDiGraph:
    """
    Create a NetworkX MultiDiGraph from Overpass API responses.

    Adds length attributes in meters (great-circle distance between endpoints)
    to all of the graph's (pre-simplified, straight-line) edges via the
    `distance.add_edge_lengths` function.

    Parameters
    ----------
    response_jsons
        Iterable of JSON responses from the Overpass API.
    bidirectional
        If True, create bidirectional edges for one-way streets.

    Returns
    -------
    G
        The resulting MultiDiGraph.
    """
    # each dict's keys are OSM IDs and values are dicts of attributes
    nodes: dict[tuple[str, int], dict[str, Any]] = {}
    paths: dict[tuple[str, int], dict[str, Any]] = {}

    # consume response_jsons generator to download data from server. if
    # cache_only_mode, just consume response_jsons then continue next loop.
    # otherwise, extract nodes and paths from the downloaded OSM data.
    response_count = 0
    for response_json in response_jsons:
        response_count += 1
        if not settings.cache_only_mode:
            nodes_temp, paths_temp = _parse_nodes_paths(response_json)
            nodes.update(nodes_temp)
            paths.update(paths_temp)

    msg = f"Retrieved all data from API in {response_count} request(s)"
    utils.log(msg, level=lg.INFO)
    if settings.cache_only_mode:  # pragma: no cover
        # after consuming all response_jsons in loop, raise exception to catch
        msg = "Interrupted because `settings.cache_only_mode=True`."
        raise CacheOnlyInterruptError(msg)

    # ensure we got some node/way data back from the server request(s)
    if (len(nodes) == 0) and (len(paths) == 0):  # pragma: no cover
        msg = "No data elements in server response. Check query location/filters and log."
        raise InsufficientResponseError(msg)

    # create the MultiDiGraph and set its graph-level attributes
    metadata = {
        "created_date": utils.ts(),
        # "created_with": f"OSMnx {__version__}", # TO DO: add version
        "crs": settings.default_crs,
    }
    G = nx.MultiDiGraph(**metadata)

    # add each OSM node and way (a path of edges) to the graph
    msg = f"Creating graph from {len(nodes):,} OSM nodes and {len(paths):,} OSM ways..."
    utils.log(msg, level=lg.INFO)
    G.add_nodes_from(nodes.items())
    _add_paths(G, paths.values(), bidirectional)

    msg = f"Created graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)

    # add length (great-circle distance between nodes) attribute to each edge
    if len(G.edges) > 0:
        G = distance.add_edge_lengths(G)

    return G


def _convert_node(element: dict[str, Any]) -> dict[str, Any]:
    """
    Convert an OSM node element into the format for a NetworkX node.

    Parameters
    ----------
    element
        OSM element of type "node".

    Returns
    -------
    node
        The converted node.
    """
    node = {"y": element["lat"], "x": element["lon"],"type":"osm"}
    if "tags" in element:
        for useful_tag in settings.useful_tags_node:
            if useful_tag in element["tags"]:
                node[useful_tag] = element["tags"][useful_tag]
    return node


def _convert_path(element: dict[str, Any]) -> dict[str, Any]:
    """
    Convert an OSM way element into the format for a NetworkX path.

    Parameters
    ----------
    element
        OSM element of type "way".

    Returns
    -------
    path
        The converted path.
    """
    path = {"osmid": element["id"]}

    # remove any consecutive duplicate elements in the list of nodes
    path["nodes"] = [group[0] for group in groupby(element["nodes"])]

    if "tags" in element:
        for useful_tag in settings.useful_tags_way:
            if useful_tag in element["tags"]:
                path[useful_tag] = element["tags"][useful_tag]
    return path


def _parse_nodes_paths(
    response_json: dict[str, Any],
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]]:
    """
    Construct dicts of nodes and paths from an Overpass response.

    Parameters
    ----------
    response_json
        JSON response from the Overpass API.

    Returns
    -------
    nodes, paths
        Each dict's keys are OSM IDs and values are dicts of attributes.
    """
    nodes = {}
    paths = {}
    for element in response_json["elements"]:
        if element["type"] == "node":
            nodes[("node",element["id"])] = _convert_node(element)
        elif element["type"] == "way":
            paths[("way",element["id"])] = _convert_path(element)

    return nodes, paths


def _is_path_one_way(attrs: dict[str, Any], bidirectional: bool, oneway_values: set[str]) -> bool:  # noqa: FBT001
    """
    Determine if a path of nodes allows travel in only one direction.

    Parameters
    ----------
    attrs
        A path's `tag:value` attribute data.
    bidirectional
        Whether this is a bidirectional network type.
    oneway_values
        The values OSM uses in its "oneway" tag to denote True.

    Returns
    -------
    is_one_way
        True if path allows travel in only one direction, otherwise False.
    """
    # rule 1
    if settings.all_oneway:
        # if globally configured to set every edge one-way, then it's one-way
        return True

    # rule 2
    if bidirectional:
        # if this is a bidirectional network type, then nothing in it is
        # considered one-way. eg, if this is a walking network, this may very
        # well be a one-way street (as cars/bikes go), but in a walking-only
        # network it is a bidirectional edge (you can walk both directions on
        # a one-way street). so we will add this path (in both directions) to
        # the graph and set its oneway attribute to False.
        return False

    # rule 3
    if "oneway" in attrs and attrs["oneway"] in oneway_values:
        # if this path is tagged as one-way and if it is not a bidirectional
        # network type then we'll add the path in one direction only
        return True

    # rule 4
    if "junction" in attrs and attrs["junction"] == "roundabout":  # noqa: SIM103
        # roundabouts are also one-way but are not explicitly tagged as such
        return True

    # otherwise, if no rule passed then this path is not tagged as a one-way
    return False


def _is_path_reversed(attrs: dict[str, Any], reversed_values: set[str]) -> bool:
    """
    Determine if the order of nodes in a path should be reversed.

    Parameters
    ----------
    attrs
        A path's `tag:value` attribute data.
    reversed_values
        The values OSM uses in its 'oneway' tag to denote travel can only
        occur in the opposite direction of the node order.

    Returns
    -------
    is_reversed
        True if nodes' order should be reversed, otherwise False.
    """
    return "oneway" in attrs and attrs["oneway"] in reversed_values


def _add_paths(
    G: nx.MultiDiGraph,
    paths: Iterable[dict[str, Any]],
    bidirectional: bool,  # noqa: FBT001
) -> None:
    """
    Add OSM paths to the graph as edges.

    Parameters
    ----------
    G
        The graph to add paths to.
    paths
        Iterable of paths' `tag:value` attribute data dicts.
    bidirectional
        If True, create bidirectional edges for one-way streets.
    """
    # the values OSM uses in its 'oneway' tag to denote True, and to denote
    # travel can only occur in the opposite direction of the node order. see:
    # https://wiki.openstreetmap.org/wiki/Key:oneway
    # https://www.geofabrik.de/de/data/geofabrik-osm-gis-standard-0.7.pdf
    oneway_values = {"yes", "true", "1", "-1", "reverse", "T", "F"}
    reversed_values = {"-1", "reverse", "T"}

    for path in paths:
        # extract/remove the ordered list of nodes from this path element so
        # we don't add it as a superfluous attribute to the edge later
        nodes = path.pop("nodes")

        # reverse the order of nodes in the path if this path is both one-way
        # and only allows travel in the opposite direction of nodes' order
        is_one_way = _is_path_one_way(path, bidirectional, oneway_values)
        if is_one_way and _is_path_reversed(path, reversed_values):
            nodes.reverse()

        # set the oneway attribute, but only if when not forcing all edges to
        # oneway with the all_oneway setting. With the all_oneway setting, you
        # want to preserve the original OSM oneway attribute for later clarity
        if not settings.all_oneway:
            path["oneway"] = is_one_way

        # zip path nodes to get (u, v) tuples like [(0,1), (1,2), (2,3)].
        # edges = list(zip(nodes[:-1], nodes[1:]))
        edges = [ (("node", u), ("node", v)) for u, v in zip(nodes[:-1], nodes[1:]) ]

        # add all the edge tuples and give them the path's tag:value attrs
        path["reversed"] = False
        G.add_edges_from(edges, **path)

        # if the path is NOT one-way, reverse direction of each edge and add
        # this path going the opposite direction too
        if not is_one_way:
            path["reversed"] = True
            G.add_edges_from([(v, u) for u, v in edges], **path)


def _create_tuple_from_gdf(u, v, datas, dist1, dist2) -> tuple:
    """
    Create a tuple (u,v, **data) with the lenght changed, used
    to add feature edge on the graph if they shere the same near edge.
    Use the distance from u to calculate the length from node to node.
    
    Parameters
    ----------
    u
        The first node of the edge.
    v
        The second node of the edge.
    datas
        The data of the edge.
    dist1
        The distance from the current node of gdf to the u of the near edge.
    dist2
        The distance from the next node of the gdf to the u of the near edge.
    which_result
        Which search result to return. If None, auto-select the first
        (Multi)Polygon or raise an error if OSM doesn't return one.

    Returns
    -------
        A tuple (u, v, datas) with the length changed that can bu used by NetworkX graph.
    """
    datas["length"] = dist2-dist1
    return (u, v, datas)


def invert_reversed_path(row) -> gpd.GeoDataFrame:
    """
    Invert the reversed path if the edge is not one way and the reversed is True.
    
    Parameters
    ----------
    row : gpd.GeoDataFrame
        The row of the GeoDataFrame to invert.
    
    Returns
    -------
    row : gpd.GeoDataFrame
        The row of the GeoDataFrame with the reversed path inverted.
        """
    d = row['edge_info']
    c = row['edge']
    if d.get('oneway')==False and d.get('reversed') == True:
        new_row = row.copy() 
        new_row['edge'] = (c[1], c[0], c[2])
        dd = d.copy()
        dd['reversed'] = False
        new_row['edge_info'] = dd
        return new_row
    return row


def _point_on_line(p1:Point, p2:Point, p3:Point, max_distance=100) -> Point | None:
    """
    Calculate the point on line p1-p2, nearest p3 but with a maximum distance from it.
    If the distance is greater than max_distance, return None.
    
    Parameters
    ----------
    p1
        The first point of the line.
    p2
        The second point of the line.
    p3
        The point to find the nearest point on the line.
    max_distance
        The maximum distance from the point p3 to the line p1-p2.
    
    eturns
    -------
    p4
        The point on the line p1-p2 nearest to p3, or None if the distance is greater than max_distance.
    """
    line = LineString([p1, p2])
    p4 : Point = line.interpolate(line.project(p3))
    if (ox.distance.great_circle(p3.y, p3.x, p4.y, p4.x)) <= max_distance:
        return p4
    else:
        return None

def _add_feature_on_edge(
        single_rows: gpd.GeoDataFrame,
        G: nx.MultiDiGraph
        ) -> nx.MultiDiGraph:
    """
    This function add the the edge to conncet the new rappresentative point to the graph.

    parameters
    ----------
    single_rows : gpd.GeoDataFrame
        The custom GeoDataFrame containing the features to add to the graph.
    G : nx.MultiDiGraph
        The graph to add the features to.
    Returns
    -------
    G : nx.MultiDiGraph
        The graph with the features added to it.
    """
    for name, u, v, dist_u, dist_v, infos in zip(single_rows["name"].to_numpy(), single_rows.u.to_numpy(),
        single_rows.v.to_numpy(), single_rows.dist_u.to_numpy(), single_rows.dist_v.to_numpy(), single_rows.edge_info.to_numpy()):
        if (infos['oneway']==True):
            infos['length'] = dist_u
            G.add_edge(u, name, **infos)
            infos['length'] = dist_v
            G.add_edge(name, v, **infos)
            G.remove_edge(u, v)
        else:
            infos['length'] = dist_u
            G.add_edge(u, name, **infos)
            infos['reversed'] = not infos['reversed']
            G.add_edge(name, u, **infos)
            infos['length'] = dist_v
            G.add_edge(v, name, **infos)
            infos['reversed'] = not infos['reversed']
            G.add_edge(name, v, **infos)
            G.remove_edge(u, v)
            G.remove_edge(v,u)



def _add_group_feature_on_edge(
        multi_rows: gpd.GeoDataFrame,
        G: nx.MultiDiGraph
        ) -> nx.MultiDiGraph:
    """
    This function add the the edge to conncet the new rappresentative point to the graph.
    It is used when group of feature share the same edge and should be used with graph obtained
    using groupby on the edge. The feature on the gdf should be ordered by distance from the first node of the edge.
    
    Parameters
    ----------
    multi_rows : gpd.GeoDataFrame
        The custom GeoDataFrame containing the features to add to the graph.
    G : nx.MultiDiGraph
        The graph to add the features to.
    
    Returns
    -------
    G : nx.MultiDiGraph
        The graph with the features added to it.
    """
    for ne, group in multi_rows.groupby("edge"):
        list_of_tuple = [_create_tuple_from_gdf(u, v , datas, dist1, dist2) for u,v,datas, dist1, dist2 in zip(group["name"].to_numpy()[:-1], group["name"].to_numpy()[1:], group.edge_info.to_numpy(),group.dist_u.to_numpy(),group.dist_u.to_numpy()[1:])]
        if ((group.edge_info.iloc[0])["oneway"]==True):
            G.add_edges_from(list_of_tuple)
            first_node = group.iloc[0]
            data = first_node.edge_info
            data["length"] = first_node.dist_u
            G.add_edge(first_node.u, first_node["name"], **data)
            last_node = group.iloc[-1]
            data = last_node.edge_info
            data["length"] = last_node.dist_v
            G.add_edge(last_node["name"], last_node.v, **data)
            G.remove_edge(last_node.u, last_node.v)
        else:
            G.add_edges_from(list_of_tuple)
            G.add_edges_from([(v,u, {**data, "reversed": not data["reversed"]}) for u,v,data in list_of_tuple ])
            first_node = group.iloc[0]
            data = first_node.edge_info
            data["length"] = first_node.dist_u
            G.add_edge(first_node.u, first_node["name"], **data)
            data["reversed"] = not data["reversed"]
            G.add_edge(first_node["name"], first_node.u, **data)
            last_node = group.iloc[-1]
            data = last_node.edge_info
            data["length"] = last_node.dist_v
            G.add_edge(last_node["name"], last_node.v, **data)
            data["reversed"] = not data["reversed"]
            G.add_edge(last_node.v, last_node["name"], **data)
            G.remove_edge(last_node.u, last_node.v)
            G.remove_edge(last_node.v, last_node.u)


def add_feature_on_graph(
        feature_gdf: gpd.GeoDataFrame, 
        G: nx.MultiDiGraph, 
        max_distance=100, 
        simplify=False,
        excluding_feature={"tunnel":{"yes", "building_passage",	"avalanche_protector"}, 
                            "bridge":{"yes", " aqueduct", "boardwalk", "cantilever", "covered", "movable", "trestle", "viaduct"}}, 
        custom_column=None
        ) -> nx.MultiDiGraph:
    """
    This function take a GeoDataFrame with a multi level Index (osmid, type(node, way, relation)) containg feature and a street NOT SIMPLFIED graph, 
    and add node and edge to the returned graph.
    The feature are taken from the gdf, and their centroid (or a custom geometry point) is used as rappresentative point for them,
    then the nearest point on the nearest edge is calculated and this point is used as the new node.
    
    This function preserve all the previus edge information  and try to avoid adding to the wrong edge using a set of tags
    as filter for street that can't have associate features (like tunnel, bridge, etc..).
    Each added node has a new attribute called "feature" that contains the type of feature (way, point, relation) and
    are identified by the Additive inverse of the osmid of the feature. This is done to avoid collision given that osmid node, way and relation
    share the same id space and so it colud be possible that an id for a feature is already used for a node of the graph.

    Parameters
    ----------
    feature_gdf : gpd.GeoDataFrame
        The GeoDataFrame containing the features to add to the graph.
    G : nx.MultiDiGraph
        The graph to add the features to.
    max_distance : int, optional
        The maximum distance from the point rappresentative point to the nearest edge. The default is 100.
        If the distance is greater than this value, the point is not added to the graph.
    simplfy : bool, optional
        If True, the graph is simplified using the simplify_graph function from osmnx.
        The default is False.
    excluding_feature : dict, optional
        A dictionary containing the tags to exclude from the graph. The default is None.
        And the default one is used.
        The dictionary should have the following format:
        {
            "tunnel": {"yes", "building_passage", "avalanche_protector"},
            "bridge": {"yes", " aqueduct", "boardwalk", "cantilever", "covered", "movable", "trestle", "viaduct"}
        }
        The keys are the tags to exclude and the values are the values of the tags to exclude.
    custom_column : str, optional
        The name of the column to useto obtain the point to add to the graph. The default is None.
        If None, the centroid of the active geometry is used as the point to add to the graph.

    Returns
    -------
    G : nx.MultiDiGraph
        The graph with the features added to it. The node added have a new attribute called "feature" that
        contains the type of feature (way, point, relation)
    """
    
    if custom_column is not None:
        if isinstance(feature_gdf[custom_column].iloc[0],Point):
            rapp_point = feature_gdf[custom_column]
        elif isinstance(feature_gdf[custom_column].iloc[0] ,shapely.geometry):
            feature_gdf = feature_gdf.set_geometry(custom_column)
            rapp_point: Point = ox.projection.project_gdf(feature_gdf).geometry.centroid.to_crs("epsg:4326")
        else:
            raise ValueError("The custom column should be a Point or a shapely geometry.")
    else:
        rapp_point: Point = ox.projection.project_gdf(feature_gdf).geometry.centroid.to_crs("epsg:4326")
    
    xs = rapp_point.x.to_numpy()
    ys = rapp_point.y.to_numpy()
    # edges = ox.distance.nearest_edges(G,xs, ys)
    edge_gdf = ox.convert.graph_to_gdfs(G, nodes=False)
    edges = distance.nearest_edges(
        gdf=edge_gdf,
        X=xs,
        Y=ys,
        return_dist=False,
        excluding_filter=excluding_feature,
        including_filter=None,
        
    )
    df = gpd.GeoDataFrame({
        "osmid": feature_gdf.index.get_level_values('id'),
        "feature":feature_gdf.index.get_level_values('element'),
        "rapp_point":rapp_point,
        "point_on_line": None,
        "edge": edges,
    })
    df = df.reset_index(drop=True)
    df['point_on_line'] = df.apply(lambda row: _point_on_line(Point(G.nodes[row['edge'][0]]["x"],
                                                                    G.nodes[row['edge'][0]]["y"]),
                                                            Point(G.nodes[row['edge'][1]]["x"],
                                                                    G.nodes[row['edge'][1]]["y"]),
                                                            row['rapp_point'], max_distance=max_distance), axis=1)
    df = df.dropna(subset=['point_on_line'])

    df['edge_info'] = [G.edges[e] for e in df["edge"]]
    # if excluding_feature==None:
    # {"tunnel":{"yes", "building_passage",	"avalanche_protector"}, 
                            # "bridge":{"yes", " aqueduct", "boardwalk", "cantilever", "covered", "movable", "trestle", "viaduct"}}
    # df = df[~df["edge_info"].apply(
    #     lambda d: any(d.get(k) in excluding_feature[k] for k in excluding_feature)
    #     )
    # ]
    index_of_inverted = (df['edge_info'].apply(lambda d: d.get('oneway') == False and d.get('reversed') == True))
    # Change the edge to the inverted one
    df.loc[index_of_inverted, 'edge'] = df.loc[index_of_inverted, 'edge'].apply(lambda c: (c[1], c[0], c[2]))
    # Change the edge info to the inverted one
    df.loc[index_of_inverted, 'edge_info'] = df.loc[index_of_inverted, 'edge_info'].apply(lambda d: {**d, 'reversed': False})
    
    df['u'] = [u for u, v, k in df['edge']]
    df['v'] = [v for u, v, k in df['edge']]
    u_list = [Point(G.nodes[u]["x"],G.nodes[u]["y"]) for u, v, k in df['edge']]
    v_list = [Point(G.nodes[v]["x"],G.nodes[v]["y"]) for u, v, k in df['edge']]
    u_list_x = [x.x for x in u_list ]
    u_list_y = [y.y for y in u_list ]
    v_list_x = [x.x for x in v_list ]
    v_list_y = [y.y for y in v_list ]
    df["dist_u"] = ox.distance.great_circle(df.point_on_line.y, df.point_on_line.x, u_list_y, u_list_x)
    df["dist_v"] = ox.distance.great_circle(df.point_on_line.y, df.point_on_line.x, v_list_y, v_list_x)
    df.set_geometry("point_on_line")
    G1=G.copy()
    id_lenght = len(G1.nodes())
    # df["name"] = np.arange(-id_lenght, -id_lenght-len(df), -1)
    df["name"] = list(zip(df["feature"], df["osmid"]))
    df.sort_values("dist_u", inplace=True)
    single_rows = df.groupby("edge").filter(lambda x: len(x) == 1)
    multi_rows = df.groupby("edge").filter(lambda x: len(x) > 1)

    
    
    
    nodes_with_attr = df.apply(
            lambda row: (
            row["name"],
            {
                "y": row["point_on_line"].y,
                "x": row["point_on_line"].x,
                "street_count": 2,
                "feature":row["feature"],
                "osmId": row["osmid"],
                "type":"osm",
            }
            ),
            axis=1
            ).tolist()
    G1.add_nodes_from(nodes_with_attr)
    _add_feature_on_edge(single_rows, G1)
    _add_group_feature_on_edge(multi_rows, G1)
    if simplify:
        G1 = ox.simplify_graph(G1, node_attrs_include=["feature"])
    return G1
