"""
linkanalysis/markov.py — Markov transition matrix and next-hop probabilities.

Pure domain logic: no I/O, no framework dependencies.
"""

import networkx as nx
import numpy as np

# Constants
DEFAULT_WEIGHT = "packets"
DEFAULT_MAX_ITER = 1000
DEFAULT_TOL = 1e-8


def _normalize_rows(matrix: np.ndarray) -> np.ndarray:
    """
    Ensure all rows sum to 1.0 by setting zero-sum rows to uniform distribution.

    Args:
        matrix: Square transition matrix

    Returns:
        Matrix with normalized rows
    """
    n = matrix.shape[0]
    if n == 0:
        return matrix

    row_sums = matrix.sum(axis=1)
    dangling = np.where(row_sums == 0)[0]
    if len(dangling) > 0:
        matrix[dangling, :] = 1.0 / n
    return matrix


def build_markov_matrix(
    G: nx.DiGraph,
    weight: str = DEFAULT_WEIGHT,
) -> tuple[dict[str, int], np.ndarray]:
    """
    Build row-stochastic Markov transition matrix from weighted graph.

    Args:
        G: Directed graph with weighted edges
        weight: Edge attribute to use as weight

    Returns:
        (node_to_index dict, transition_matrix np.ndarray)
        Rows with no out-edges become uniform (1/n) per TECH_RULES §3.6
    """
    nodes = list(G.nodes())
    n = len(nodes)
    node_to_index = {node: i for i, node in enumerate(nodes)}

    if n == 0:
        return node_to_index, np.zeros((0, 0))

    P = np.zeros((n, n), dtype=float)

    for u in G.nodes():
        i = node_to_index[u]
        out_edges = list(G.out_edges(u, data=True))
        if not out_edges:
            continue
        total_weight = sum(d.get(weight, 1) for _, _, d in out_edges)
        for _, v, d in out_edges:
            j = node_to_index[v]
            P[i, j] += d.get(weight, 1) / total_weight

    # Handle dangling nodes: row becomes uniform (1/n) — TECH_RULES §3.6
    P = _normalize_rows(P)

    return node_to_index, P


def markov_next_hop(
    G: nx.DiGraph,
    src_ip: str,
    weight: str = DEFAULT_WEIGHT,
    top_k: int = 10,
) -> list[tuple[str, float]]:
    """
    Get next-hop probabilities from a source IP.

    Args:
        G: Directed graph
        src_ip: Source IP address
        weight: Edge weight attribute
        top_k: Return top K destinations

    Returns:
        List of (dst_ip, probability) tuples sorted by probability desc
    """
    if src_ip not in G:
        return []

    out_edges = list(G.out_edges(src_ip, data=True))
    if not out_edges:
        return []

    total_weight = sum(d.get(weight, 1) for _, _, d in out_edges)
    probs = [(v, d.get(weight, 1) / total_weight) for _, v, d in out_edges]
    probs.sort(key=lambda x: x[1], reverse=True)
    return probs[:top_k]


def markov_transition_heatmap(
    G: nx.DiGraph,
    top_k: int = 20,
    weight: str = DEFAULT_WEIGHT,
) -> tuple[list[str], np.ndarray]:
    """
    Get transition matrix for top-K nodes by weighted degree for heatmap.

    Args:
        G: Directed graph
        top_k: Number of top nodes to include
        weight: Edge weight attribute

    Returns:
        (node_labels, submatrix) where submatrix is top_k x top_k
    """
    if G.number_of_nodes() == 0:
        return [], np.zeros((0, 0))

    degrees = dict(G.degree(weight=weight))
    top_nodes = sorted(degrees, key=degrees.get, reverse=True)[:top_k]

    if len(top_nodes) < 2:
        return top_nodes, np.zeros((len(top_nodes), len(top_nodes)))

    node_to_idx = {node: i for i, node in enumerate(top_nodes)}
    n = len(top_nodes)
    submatrix = np.zeros((n, n), dtype=float)

    for u in top_nodes:
        i = node_to_idx[u]
        out_edges = list(G.out_edges(u, data=True))
        if not out_edges:
            continue
        total_weight = sum(d.get(weight, 1) for _, _, d in out_edges)
        for _, v, d in out_edges:
            if v in node_to_idx:
                j = node_to_idx[v]
                submatrix[i, j] = d.get(weight, 1) / total_weight

    # Handle dangling rows
    submatrix = _normalize_rows(submatrix)

    return top_nodes, submatrix


def stationary_distribution(
    G: nx.DiGraph,
    weight: str = DEFAULT_WEIGHT,
    max_iter: int = DEFAULT_MAX_ITER,
    tol: float = DEFAULT_TOL,
) -> dict[str, float] | None:
    """
    Compute stationary distribution of the Markov chain (Nice-to-have).

    Args:
        G: Directed graph
        weight: Edge weight attribute
        max_iter: Maximum iterations
        tol: Convergence tolerance

    Returns:
        Dict mapping node -> stationary probability, or None if not converged
    """
    node_to_idx, P = build_markov_matrix(G, weight)
    if P.size == 0:
        return None

    n = P.shape[0]
    pi = np.ones(n) / n

    for _ in range(max_iter):
        pi_new = pi @ P
        if np.abs(pi_new - pi).sum() < tol:
            pi = pi_new
            break
        pi = pi_new
    else:
        return None

    idx_to_node = {i: node for node, i in node_to_idx.items()}
    return {idx_to_node[i]: float(pi[i]) for i in range(n)}
