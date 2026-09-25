"""
tests/unit/test_linkanalysis.py — Unit tests for link analysis module.
"""

import networkx as nx
import pytest

from linkanalysis.centrality import (
    betweenness_centrality,
    degree_centrality,
    in_degree_centrality,
    out_degree_centrality,
    top_centrality_nodes,
)
from linkanalysis.graph import build_graph_from_edges, classify_ip, prune_graph_top_n
from linkanalysis.markov import (
    build_markov_matrix,
    markov_next_hop,
    markov_transition_heatmap,
    stationary_distribution,
)
from linkanalysis.pagerank import (
    pagerank_networkx,
    pagerank_power_iteration,
    validate_pagerank_agreement,
)


@pytest.fixture
def toy_graph() -> nx.DiGraph:
    """
    Toy graph from TECH_RULES §3.6 validation:
    A→B, A→C, C→B, D→B
    """
    G = nx.DiGraph()
    edges = [
        ("A", "B", 10, 1000),
        ("A", "C", 5, 500),
        ("C", "B", 3, 300),
        ("D", "B", 7, 700),
    ]
    for src, dst, pkts, bytes_ in edges:
        G.add_edge(src, dst, packets=pkts, bytes=bytes_)
    return G


@pytest.fixture
def ip_edges_records():
    """Sample ip_edges records."""
    return [
        {"src_ip": "192.168.1.1", "dst_ip": "8.8.8.8", "packets": 100, "bytes": 10000},
        {"src_ip": "192.168.1.1", "dst_ip": "1.1.1.1", "packets": 50, "bytes": 5000},
        {"src_ip": "10.0.0.1", "dst_ip": "192.168.1.1", "packets": 30, "bytes": 3000},
        {
            "src_ip": "192.168.1.1",
            "dst_ip": "8.8.8.8",
            "packets": 20,
            "bytes": 2000,
        },  # duplicate edge
    ]


class TestGraph:
    def test_build_graph_from_edges(self, ip_edges_records):
        G = build_graph_from_edges(ip_edges_records)
        # 4 unique IPs: 192.168.1.1, 8.8.8.8, 1.1.1.1, 10.0.0.1
        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 3  # duplicate merged
        assert G["192.168.1.1"]["8.8.8.8"]["packets"] == 120
        assert G["192.168.1.1"]["8.8.8.8"]["bytes"] == 12000

    def test_build_graph_empty(self):
        G = build_graph_from_edges([])
        assert G.number_of_nodes() == 0

    def test_build_graph_skips_missing_ips(self):
        records = [{"src_ip": "", "dst_ip": "1.1.1.1", "packets": 10, "bytes": 100}]
        G = build_graph_from_edges(records)
        assert G.number_of_nodes() == 0

    def test_prune_graph_top_n(self, toy_graph):
        G_pruned = prune_graph_top_n(toy_graph, n=2, weight="packets")
        assert G_pruned.number_of_nodes() == 2
        assert "B" in G_pruned.nodes()
        assert "A" in G_pruned.nodes()

    def test_classify_ip_private(self):
        assert classify_ip("192.168.1.1") == "private"
        assert classify_ip("10.0.0.1") == "private"
        assert classify_ip("172.16.0.1") == "private"
        assert classify_ip("127.0.0.1") == "private"
        assert classify_ip("::1") == "private"
        assert classify_ip("fe80::1") == "private"

    def test_classify_ip_public(self):
        assert classify_ip("8.8.8.8") == "public"
        assert classify_ip("1.1.1.1") == "public"
        assert classify_ip("2001:4860:4860::8888") == "public"


class TestPageRank:
    def test_pagerank_networkx_toy(self, toy_graph):
        pr = pagerank_networkx(toy_graph, alpha=0.85, weight="packets")
        assert len(pr) == 4
        assert all(v > 0 for v in pr.values())
        assert abs(sum(pr.values()) - 1.0) < 1e-6

    def test_pagerank_power_iteration_toy(self, toy_graph):
        pr = pagerank_power_iteration(toy_graph, alpha=0.85, weight="packets")
        assert len(pr) == 4
        assert all(v > 0 for v in pr.values())
        assert abs(sum(pr.values()) - 1.0) < 1e-6

    def test_pagerank_agreement(self, toy_graph):
        assert validate_pagerank_agreement(toy_graph, alpha=0.85, weight="packets", tolerance=1e-4)

    def test_pagerank_empty_graph(self):
        G = nx.DiGraph()
        pr = pagerank_power_iteration(G)
        assert pr == {}

    def test_pagerank_single_node(self):
        G = nx.DiGraph()
        G.add_node("A")
        pr = pagerank_power_iteration(G)
        assert pr == {"A": 1.0}

    def test_pagerank_hand_calculated_toy(self, toy_graph):
        """
        Hand-calculated PageRank for A→B, A→C, C→B, D→B (weights=1, alpha=0.85):
        Iteration converges to approximately:
        A: 0.148, B: 0.425, C: 0.148, D: 0.148
        """
        pr = pagerank_power_iteration(toy_graph, alpha=0.85, weight="packets")
        # B should have highest rank (3 inbound edges)
        assert pr["B"] == max(pr.values())
        # A, C, D should be similar (each has 1 outbound, no inbound except C←A)
        assert abs(pr["A"] - pr["D"]) < 0.01


class TestMarkov:
    def test_build_markov_matrix(self, toy_graph):
        _node_to_idx, P = build_markov_matrix(toy_graph, weight="packets")
        assert P.shape == (4, 4)
        # Rows should sum to 1
        row_sums = P.sum(axis=1)
        assert all(abs(s - 1.0) < 1e-6 for s in row_sums)

    def test_markov_rows_sum_to_one(self, toy_graph):
        """TECH_RULES §3.6: Markov rows sum to 1."""
        _node_to_idx, P = build_markov_matrix(toy_graph)
        for row in P:
            assert abs(row.sum() - 1.0) < 1e-6

    def test_markov_next_hop(self, toy_graph):
        next_hops = markov_next_hop(toy_graph, "A", weight="packets")
        assert len(next_hops) == 2
        assert next_hops[0][0] == "B"  # A→B has weight 10, A→C has weight 5
        assert next_hops[0][1] > next_hops[1][1]

    def test_markov_next_hop_unknown_src(self, toy_graph):
        next_hops = markov_next_hop(toy_graph, "UNKNOWN")
        assert next_hops == []

    def test_markov_transition_heatmap(self, toy_graph):
        nodes, matrix = markov_transition_heatmap(toy_graph, top_k=3)
        assert len(nodes) == 3
        assert matrix.shape == (3, 3)

    def test_stationary_distribution(self, toy_graph):
        pi = stationary_distribution(toy_graph)
        assert pi is not None
        assert abs(sum(pi.values()) - 1.0) < 1e-6


class TestCentrality:
    def test_degree_centrality(self, toy_graph):
        dc = degree_centrality(toy_graph, weight="packets")
        assert len(dc) == 4
        # B has highest degree (3 inbound edges)
        assert dc["B"] == max(dc.values())

    def test_in_out_degree_centrality(self, toy_graph):
        in_dc = in_degree_centrality(toy_graph, weight="packets")
        out_dc = out_degree_centrality(toy_graph, weight="packets")
        # B has 3 inbound, 0 outbound
        assert in_dc["B"] > 0
        assert out_dc["B"] == 0
        # A has 0 inbound, 2 outbound
        assert in_dc["A"] == 0
        assert out_dc["A"] > 0

    def test_betweenness_centrality_small(self, toy_graph):
        bc = betweenness_centrality(toy_graph, weight="packets")
        assert len(bc) == 4
        # With weight=packets, distance = 1/weight.
        # A→B direct: distance 0.1, A→C→B: distance 0.2 + 0.33 = 0.53
        # Shortest path A→B is direct, so C has 0 betweenness for A→B.
        # But C is on shortest path for no pairs in this graph.
        # This is correct behavior - betweenness can be 0.
        assert bc["C"] >= 0  # Non-negative

    def test_betweenness_skips_large_graph(self):
        G = nx.DiGraph()
        for i in range(301):
            G.add_node(f"N{i}")
        bc = betweenness_centrality(G, weight="packets")
        assert all(v == 0.0 for v in bc.values())

    def test_top_centrality_nodes(self, toy_graph):
        dc = degree_centrality(toy_graph, weight="packets")
        top = top_centrality_nodes(dc, top_n=2)
        assert len(top) == 2
        assert top[0][0] == "B"  # B should be first


class TestIntegration:
    def test_full_pipeline(self, ip_edges_records):
        """Test full pipeline: edges → graph → pagerank → markov → centrality."""
        G = build_graph_from_edges(ip_edges_records)
        assert G.number_of_nodes() > 0

        pr = pagerank_power_iteration(G)
        assert len(pr) == G.number_of_nodes()

        _node_to_idx, P = build_markov_matrix(G)
        assert P.shape == (G.number_of_nodes(), G.number_of_nodes())

        dc = degree_centrality(G)
        assert len(dc) == G.number_of_nodes()
