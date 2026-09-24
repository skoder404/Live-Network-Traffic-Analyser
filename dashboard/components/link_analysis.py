"""
dashboard/components/link_analysis.py — Link Analysis tab for LNTA dashboard.

Shows IP communication graph, PageRank rankings, Markov transitions, and centrality.
"""
import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import ServingDB, get_ip_edges, get_source_stats
from dashboard.theme import LNTA_COLORS


def render_link_analysis_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the Link Analysis tab with graph, PageRank, Markov, and centrality."""
    st.markdown("## 🕸️ Link Analysis")

    window_len = controls["window_len_s"]
    top_n = controls["top_n"]

    # Fetch data
    with st.spinner("Loading link analysis data..."):
        try:
            ip_edges_df = get_ip_edges(db, window_len, limit=5000)
            get_source_stats(db, window_len, limit=200)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load link analysis data: {e}")
            return

    if ip_edges_df.empty:
        st.info("🔍 No IP edges data available yet. Start capture or replay to see link analysis.")
        return

    # Build graph from edges
    from linkanalysis.centrality import centrality_summary
    from linkanalysis.graph import build_graph_from_edges, prune_graph_top_n
    from linkanalysis.markov import (
        build_markov_matrix,
    )
    from linkanalysis.pagerank import pagerank_power_iteration

    edges_records = ip_edges_df.to_dict("records")
    G = build_graph_from_edges(edges_records)

    if G.number_of_nodes() == 0:
        st.info("🔍 No valid IP communication edges found.")
        return

    # Prune to top-N for visualization
    G_viz = prune_graph_top_n(G, n=top_n, weight="packets")

    # Compute analytics
    pr_scores = pagerank_power_iteration(G)
    _node_to_idx, _markov_matrix = build_markov_matrix(G)
    centrality = centrality_summary(G)

    # --- Layout: Two columns ---
    col_graph, col_side = st.columns([2, 1])

    with col_graph:
        st.markdown("### Communication Graph")
        render_ip_graph(G_viz, pr_scores, centrality)

    with col_side:
        st.markdown("### PageRank Rankings")
        render_pagerank_table(pr_scores, centrality, top_n=15)

        st.markdown("### Node Classification")
        render_node_classification(G_viz)

    # --- Markov & Centrality below ---
    st.divider()

    col_markov, col_centrality = st.columns(2)

    with col_markov:
        st.markdown("### Markov Transition Heatmap")
        render_markov_heatmap(G, top_k=min(15, top_n))

        st.markdown("### Next-Hop Probabilities")
        render_next_hop_selector(G, pr_scores)

    with col_centrality:
        st.markdown("### Centrality Measures")
        render_centrality_bars(centrality, top_n=10)

    # Caption per DESIGN.md
    st.caption(
        "ℹ️ **Structural importance in the observed graph — not a measure of risk.** "
        "PageRank scores reflect communication patterns, not maliciousness."
    )


def render_ip_graph(
    G, pr_scores: dict[str, float], centrality: dict[str, dict[str, float]]
) -> None:
    """Render interactive Plotly graph of IP communication."""
    import networkx as nx

    if G.number_of_nodes() == 0:
        st.info("No graph to display")
        return

    # Layout
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

    # Node data
    node_x = []
    node_y = []
    node_text = []
    node_size = []
    node_color = []
    node_border = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

        # Size by PageRank (scaled)
        pr = pr_scores.get(node, 0)
        size = max(10, pr * 3000)  # Scale for visibility
        node_size.append(size)

        # Color by private/public
        from linkanalysis.graph import classify_ip
        ip_type = classify_ip(node)
        color = LNTA_COLORS["proto_tcp"] if ip_type == "private" else LNTA_COLORS["proto_udp"]
        node_color.append(color)
        node_border.append("white")

        # Hover text
        in_deg = centrality.get("in_degree", {}).get(node, 0)
        out_deg = centrality.get("out_degree", {}).get(node, 0)
        node_text.append(
            f"<b>{node}</b><br>"
            f"Type: {ip_type}<br>"
            f"PageRank: {pr:.4f}<br>"
            f"In-degree: {in_deg:.3f}<br>"
            f"Out-degree: {out_deg:.3f}"
        )

    # Edge data
    edge_x = []
    edge_y = []
    edge_weights = []

    for u, v, data in G.edges(data=True):
        if u in pos and v in pos:
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
            edge_weights.append(data.get("packets", 1))

    # Create figure
    fig = go.Figure()

    # Edges
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        mode="lines",
        line={"width": 0.5, "color": LNTA_COLORS["border"]},
        hoverinfo="none",
        showlegend=False,
    ))

    # Nodes
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        marker={
            "size": node_size,
            "color": node_color,
            "line": {"width": 1, "color": "white"},
            "opacity": 0.9,
        },
        text=[n[:15] for n in G.nodes()],  # Truncate long IPs
        textposition="top center",
        textfont={"size": 8, "color": LNTA_COLORS["text_secondary"]},
        hovertext=node_text,
        hoverinfo="text",
        showlegend=False,
    ))

    fig.update_layout(
        template="lnta_dark",
        showlegend=False,
        hovermode="closest",
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
        height=500,
        xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        plot_bgcolor=LNTA_COLORS["bg_base"],
        paper_bgcolor=LNTA_COLORS["bg_base"],
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_pagerank_table(
    pr_scores: dict[str, float],
    centrality: dict[str, dict[str, float]],
    top_n: int = 15,
) -> None:
    """Render PageRank rankings table."""
    # Sort by PageRank desc
    sorted_pr = sorted(pr_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

    data = []
    for rank, (ip, score) in enumerate(sorted_pr, 1):
        in_deg = centrality.get("in_degree", {}).get(ip, 0)
        out_deg = centrality.get("out_degree", {}).get(ip, 0)
        from linkanalysis.graph import classify_ip
        ip_type = classify_ip(ip)
        data.append({
            "Rank": rank,
            "IP": ip,
            "Type": ip_type,
            "PageRank": f"{score:.4f}",
            "In-Deg": f"{in_deg:.3f}",
            "Out-Deg": f"{out_deg:.3f}",
        })

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_node_classification(G) -> None:
    """Show count of private vs public IPs."""
    from linkanalysis.graph import classify_ip

    private_count = sum(1 for n in G.nodes() if classify_ip(n) == "private")
    public_count = G.number_of_nodes() - private_count

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Private IPs", private_count)
    with c2:
        st.metric("Public IPs", public_count)


def render_markov_heatmap(G, top_k: int = 15) -> None:
    """Render Markov transition matrix heatmap."""
    from linkanalysis.markov import markov_transition_heatmap

    nodes, matrix = markov_transition_heatmap(G, top_k=top_k)

    if len(nodes) == 0:
        st.info("Not enough data for transition matrix")
        return

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=nodes,
        y=nodes,
        colorscale="Blues",
        showscale=True,
        hoverongaps=False,
        hovertemplate="From: %{y}<br>To: %{x}<br>Prob: %{z:.3f}<extra></extra>",
    ))

    fig.update_layout(
        template="lnta_dark",
        height=400,
        xaxis={"title": "Destination IP", "tickangle": 45},
        yaxis={"title": "Source IP"},
        margin={"l": 80, "r": 20, "t": 40, "b": 80},
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_next_hop_selector(G, pr_scores: dict[str, float]) -> None:
    """Selector to view next-hop probabilities for a source IP."""
    # Top 10 by PageRank for selector
    top_ips = sorted(pr_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    ip_options = [f"{ip} (PR: {pr:.3f})" for ip, pr in top_ips]
    ip_map = {opt: ip for opt, ip in zip(ip_options, [ip for ip, _ in top_ips])}

    selected = st.selectbox("Select source IP", ip_options, key="markov_src_select")

    if selected:
        src_ip = ip_map[selected]
        from linkanalysis.markov import markov_next_hop
        next_hops = markov_next_hop(G, src_ip, top_k=10)

        if next_hops:
            data = [
                {"Destination": dst, "Probability": f"{prob:.2%}"}
                for dst, prob in next_hops
            ]
            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
        else:
            st.info(f"No outbound connections from {src_ip}")


def render_centrality_bars(centrality: dict[str, dict[str, float]], top_n: int = 10) -> None:
    """Render horizontal bar charts for centrality measures."""
    from linkanalysis.centrality import top_centrality_nodes

    measures = ["degree", "in_degree", "out_degree", "betweenness"]
    titles = ["Total Degree", "In-Degree", "Out-Degree", "Betweenness"]

    for measure, title in zip(measures, titles):
        scores = centrality.get(measure, {})
        if not scores:
            continue

        top = top_centrality_nodes(scores, top_n=top_n)
        if not top:
            continue

        ips, vals = zip(*top)

        fig = go.Figure(go.Bar(
            x=list(vals),
            y=list(ips),
            orientation="h",
            marker_color=LNTA_COLORS["accent"],
            hovertemplate="%{y}: %{x:.4f}<extra></extra>",
        ))

        fig.update_layout(
            template="lnta_dark",
            title=title,
            height=250,
            margin={"l": 100, "r": 20, "t": 40, "b": 20},
            xaxis={"showgrid": True, "gridcolor": LNTA_COLORS["border"]},
            yaxis={"autorange": "reversed"},
        )

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
