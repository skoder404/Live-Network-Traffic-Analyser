"""
linkanalysis/centrality.py — Degree and betweenness centrality.

Pure domain logic: no I/O, no framework dependencies.
"""

import networkx as nx

# Constants (per TECH_RULES §3.6)
MAX_NODES_FOR_BETWEENNESS = 300
DEFAULT_WEIGHT = "packets"


def _normalized_degree(
    G: nx.DiGraph,
    weight: str,
    degree_func,
) -> dict[str, float]:
    """
    Compute normalized degree centrality using a degree function.

    Args:
        G: Directed graph
        weight: Edge attribute to use as weight
        degree_func: Function to compute degree (G.degree, G.in_degree, G.out_degree)

    Returns:
        Dict mapping node -> normalized degree centrality
    """
    n = G.number_of_nodes()
    if n <= 1:
        return {node: 0.0 for node in G.nodes()}

    degrees = dict(degree_func(weight=weight))
    return {node: deg / (n - 1) for node, deg in degrees.items()}


def degree_centrality(G: nx.DiGraph, weight: str = DEFAULT_WEIGHT) -> dict[str, float]:
    """
    Compute weighted degree centrality (total degree).

    Args:
        G: Directed graph
        weight: Edge attribute to use as weight

    Returns:
        Dict mapping node -> weighted degree centrality (normalized by n-1)
    """
    return _normalized_degree(G, weight, G.degree)


def in_degree_centrality(G: nx.DiGraph, weight: str = DEFAULT_WEIGHT) -> dict[str, float]:
    """Weighted in-degree centrality."""
    return _normalized_degree(G, weight, G.in_degree)


def out_degree_centrality(G: nx.DiGraph, weight: str = DEFAULT_WEIGHT) -> dict[str, float]:
    """Weighted out-degree centrality."""
    return _normalized_degree(G, weight, G.out_degree)


def betweenness_centrality(
    G: nx.DiGraph,
    weight: str = DEFAULT_WEIGHT,
    k: int | None = None,
    normalized: bool = True,
) -> dict[str, float]:
    """
    Compute betweenness centrality (only for graphs with <= 300 nodes per TECH_RULES §3.6).

    Args:
        G: Directed graph
        weight: Edge attribute to use as weight (distance = 1/weight)
        k: Sample k nodes for approximation (if None, exact)
        normalized: Normalize by (n-1)(n-2)/2

    Returns:
        Dict mapping node -> betweenness centrality
    """
    if G.number_of_nodes() > MAX_NODES_FOR_BETWEENNESS:
        # Per TECH_RULES §3.6: skip and return zeros when > 300 nodes
        return {node: 0.0 for node in G.nodes()}

    # Convert weight to distance (higher weight = shorter distance)
    # NetworkX betweenness uses distance, so invert weights
    if weight:
        G_weighted = nx.DiGraph()
        for u, v, d in G.edges(data=True):
            w = d.get(weight, 1)
            G_weighted.add_edge(u, v, weight=1.0 / max(w, 1))
        return nx.betweenness_centrality(G_weighted, k=k, normalized=normalized, weight="weight")
    else:
        return nx.betweenness_centrality(G, k=k, normalized=normalized)


def top_centrality_nodes(
    centrality: dict[str, float],
    top_n: int = 10,
) -> list[tuple[str, float]]:
    """Return top-N nodes by centrality score."""
    return sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:top_n]


def centrality_summary(
    G: nx.DiGraph,
    weight: str = DEFAULT_WEIGHT,
) -> dict[str, dict[str, float]]:
    """
    Compute all centrality measures for a graph.

    Returns:
        Dict with keys: 'degree', 'in_degree', 'out_degree', 'betweenness'
    """
    n = G.number_of_nodes()
    return {
        "degree": degree_centrality(G, weight),
        "in_degree": in_degree_centrality(G, weight),
        "out_degree": out_degree_centrality(G, weight),
        "betweenness": betweenness_centrality(G, weight) if n <= MAX_NODES_FOR_BETWEENNESS else {},
    }
