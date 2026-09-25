"""
linkanalysis/graph.py — Build directed weighted IP communication graph.

Pure domain logic: no I/O, no framework dependencies.
"""

import ipaddress
from collections.abc import Iterable

import networkx as nx

# Constants
DEFAULT_TOP_N = 50
VALID_WEIGHTS = ("packets", "bytes")


def build_graph_from_edges(edges: Iterable[dict]) -> nx.DiGraph:
    """
    Build a directed weighted graph from ip_edges records.

    Args:
        edges: Iterable of dicts with keys: src_ip, dst_ip, packets, bytes

    Returns:
        nx.DiGraph with nodes=IPs, edges weighted by packets and bytes.
    """
    G = nx.DiGraph()

    for e in edges:
        src = e.get("src_ip")
        dst = e.get("dst_ip")
        packets = int(e.get("packets", 0) or 0)
        bytes_ = int(e.get("bytes", 0) or 0)

        if not src or not dst:
            continue

        if G.has_edge(src, dst):
            G[src][dst]["packets"] += packets
            G[src][dst]["bytes"] += bytes_
        else:
            G.add_edge(src, dst, packets=packets, bytes=bytes_)

    return G


def prune_graph_top_n(
    G: nx.DiGraph,
    n: int = DEFAULT_TOP_N,
    weight: str = "packets",
) -> nx.DiGraph:
    """
    Return a subgraph with top-N nodes by weighted degree.

    Args:
        G: Input DiGraph
        n: Number of nodes to keep
        weight: Edge attribute to use for degree ('packets' or 'bytes')

    Returns:
        Subgraph with top-N nodes
    """
    if weight not in VALID_WEIGHTS:
        raise ValueError(f"weight must be one of {VALID_WEIGHTS}")

    if G.number_of_nodes() <= n:
        return G

    degrees = dict(G.degree(weight=weight))
    top_nodes = sorted(degrees, key=degrees.get, reverse=True)[:n]
    return G.subgraph(top_nodes).copy()


def classify_ip(ip: str) -> str:
    """
    Classify IP as 'private' or 'public'.

    Args:
        ip: IP address string

    Returns:
        'private' or 'public'
    """
    try:
        addr = ipaddress.ip_address(ip)
        return "private" if addr.is_private else "public"
    except ValueError:
        return "public"
