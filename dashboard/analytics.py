"""Bridge to linkanalysis (Priyan's package): graph -> PageRank -> centrality -> Markov."""

from linkanalysis import (
    betweenness_centrality,
    build_graph_from_edges,
    build_markov_matrix,
    classify_ip,
    degree_centrality,
    pagerank_networkx,
    pagerank_power_iteration,
    prune_graph_top_n,
)


def analyze(edges, top_n=30, weight="packets"):
    G = prune_graph_top_n(build_graph_from_edges(edges), top_n, weight)
    if G.number_of_nodes() == 0:
        return {"G": G, "pr": {}, "deg": {}, "bet": {}, "agree": 0.0}
    pr = pagerank_power_iteration(G, weight=weight)
    ref = pagerank_networkx(G, weight=weight)
    return {
        "G": G,
        "pr": pr,
        "deg": degree_centrality(G, weight),
        "bet": betweenness_centrality(G, weight) if G.number_of_nodes() <= 300 else {},
        "agree": max(abs(pr[n] - ref[n]) for n in pr),
    }


def markov_top(G, weight="packets", k=12):
    top = sorted(G.degree(weight=weight), key=lambda x: x[1], reverse=True)[:k]
    nodes = [n for n, _ in top]
    idx, P = build_markov_matrix(G, weight)
    return nodes, [[float(P[idx[a], idx[b]]) for b in nodes] for a in nodes]


kind = classify_ip
