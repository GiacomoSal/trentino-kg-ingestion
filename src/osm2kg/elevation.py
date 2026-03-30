"""
Calculate and add impedance attributes to edges in a graph.
"""
import networkx as nx
import numpy as np


def impedance(lengths: np.ndarray, grades: np.ndarray) -> np.ndarray:
    """
    Vectorized calculation of impedance based on the length and grade arrays.
    The impedance is calculated as the product of the length and the square of the grade.

    Parameters
    ----------
    lengths : np.ndarray
        Array of path lengths.
    grades : np.ndarray
        Array of path grades (ratio of vertical rise to horizontal run).

    Returns
    -------
    np.ndarray
        Array of impedance values.
    """
    penalty = grades ** 2
    return lengths * penalty


def add_edge_impedance(G: nx.MultiDiGraph, impedance_function=impedance) -> nx.MultiDiGraph:
    """
    Calculate and add `impedance` attributes to all graph edges.

    Vectorized function to calculate the impedance using a custom function
    or the `impedance` function. 
    Edges must already have `grade` and `length` attributes before using this
    function.

    See also the `osmnx.elevation.add_edge_grades` function.

    Parameters
    ----------
    G : networkx.MultiDiGraph
        Graph with `elevation` node attributes.
    empedance_function : function, optional
        Function to calculate the impedance. The default is the `impedance`

    Returns
    -------
    G : networkx.MultiDiGraph
        Graph with `impedance` attribute on the edges.
    """
    u, v, k, lengths, grades =zip(*[(u, v, k, d.get("length"), d.get("grade"))
                                    for u, v, k, d in G.edges(keys=True, data=True)])
    uvk = tuple(zip(u, v, k))

    # calculate edges' impedance using the custom function
    impedances = impedance_function(np.array(lengths), np.array(grades))
    nx.set_edge_attributes(G, dict(zip(uvk, impedances)), name="impedance")

    # msg = "Added impedance attributes to all edges"
    # utils.log(msg, level=lg.INFO)
    return G