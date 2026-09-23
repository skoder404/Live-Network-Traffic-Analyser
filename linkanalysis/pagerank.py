"""
linkanalysis/pagerank.py — PageRank implementations (NetworkX + from-scratch power iteration).

Pure domain logic: no I/O, no framework dependencies.
"""
from typing import Dict, Optional

import networkx as nx
import numpy as np


# Constants (per TECH_RULES §3.6)
DEFAULT_ALPHA = 0.85
DEFAULT_WEIGHT = "packets"
DEFAULT_MAX_ITER = 100
DEFAULT_TOL = 1e-6
VALIDATION_TOLERANCE = 1e-4


def pagerank_networkx(
    G: nx.DiGraph,
    alpha: float = DEFAULT_ALPHA,
    weight: str = DEFAULT_WEIGHT,
) -> Dict[str, float]:
    """
    Compute PageRank using NetworkX.

    Args:
        G: Directed graph with weighted edges
        alpha: Damping factor (default 0.85)
        weight: Edge attribute to use as weight

    Returns:
        Dict mapping node -> PageRank score
    """
    return nx.pagerank(G, alpha=alpha, weight=weight)


def pagerank_power_iteration(
    G: nx.DiGraph,
    alpha: float = DEFAULT_ALPHA,
    weight: str = DEFAULT_WEIGHT,
    max_iter: int = DEFAULT_MAX_ITER,
    tol: float = DEFAULT_TOL,
    personalization: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """
    Compute PageRank using from-scratch power iteration.

    Args:
        G: Directed graph with weighted edges
        alpha: Damping factor (default 0.85)
        weight: Edge attribute to use as weight
        max_iter: Maximum iterations
        tol: Convergence tolerance (L1 norm)
        personalization: Optional personalization vector

    Returns:
        Dict mapping node -> PageRank score
    """
    if G.number_of_nodes() == 0:
        return {}

    nodes = list(G.nodes())
    n = len(nodes)
    node_index = {node: i for i, node in enumerate(nodes)}

    # Build transition matrix (row-stochastic)
    M = np.zeros((n, n), dtype=float)

    for u in G.nodes():
        i = node_index[u]
        out_edges = list(G.out_edges(u, data=True))
        if not out_edges:
            continue
        total_weight = sum(d.get(weight, 1) for _, _, d in out_edges)
        for _, v, d in out_edges:
            j = node_index[v]
            M[i, j] = d.get(weight, 1) / total_weight

    # Handle dangling nodes (rows that sum to 0)
    dangling = np.where(M.sum(axis=1) == 0)[0]
    if len(dangling) > 0:
        M[dangling, :] = 1.0 / n

    # Personalization vector
    if personalization is None:
        p = np.ones(n) / n
    else:
        p = np.array([personalization.get(node, 0) for node in nodes])
        p_sum = p.sum()
        if p_sum > 0:
            p = p / p_sum
        else:
            p = np.ones(n) / n

    # Power iteration
    x = np.ones(n) / n
    for _ in range(max_iter):
        x_new = alpha * (x @ M) + (1 - alpha) * p
        if np.abs(x_new - x).sum() < tol:
            x = x_new
            break
        x = x_new

    return {nodes[i]: float(x[i]) for i in range(n)}


def validate_pagerank_agreement(
    G: nx.DiGraph,
    alpha: float = DEFAULT_ALPHA,
    weight: str = DEFAULT_WEIGHT,
    tolerance: float = VALIDATION_TOLERANCE,
) -> bool:
    """
    Validate that NetworkX and power iteration PageRank agree within tolerance.

    Args:
        G: Directed graph
        alpha: Damping factor
        weight: Edge weight attribute
        tolerance: Max allowed L-infinity difference

    Returns:
        True if agreement within tolerance
    """
    pr_nx = pagerank_networkx(G, alpha=alpha, weight=weight)
    pr_pi = pagerank_power_iteration(G, alpha=alpha, weight=weight)

    all_nodes = set(pr_nx.keys()) | set(pr_pi.keys())
    max_diff = max(abs(pr_nx.get(n, 0) - pr_pi.get(n, 0)) for n in all_nodes)
    return max_diff <= tolerance
