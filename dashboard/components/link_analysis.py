import pandas as pd, streamlit as st
from ._ui import state
from .graph3d import render_graph3d
from dashboard.analytics import analyze, markov_top, kind
from linkanalysis import markov_next_hop
def render_link_analysis(s, window, ctx):
    a, b = st.columns([1, 1])
    weight = a.radio("Edge weight", ["packets", "bytes"], horizontal=True); top_n = b.slider("Nodes shown", 8, 40, 24)
    r = analyze(s["edges"], top_n, weight); G = r["G"]
    if G.number_of_nodes() == 0: return state("empty", "No conversations in this window yet.")
    st.subheader("IP communication graph"); render_graph3d(G, r["pr"], r["deg"])
    st.caption(f"PageRank from-scratch vs NetworkX: max difference {r['agree']:.1e}. Scores show structural importance, not maliciousness.")
    df = pd.DataFrame({"IP": list(G.nodes())}); df["Type"] = df.IP.map(kind); df["PageRank"] = df.IP.map(r["pr"]); df["Degree"] = df.IP.map(r["deg"])
    if r["bet"]: df["Betweenness"] = df.IP.map(r["bet"])
    st.dataframe(df.sort_values("PageRank", ascending=False), hide_index=True, width="stretch")
    st.subheader("Markov: where does this host talk next?")
    src = st.selectbox("Source IP", sorted(G.nodes(), key=lambda n: -r["pr"][n]))
    for dst, p in markov_next_hop(G, src, weight, 6):
        st.markdown(f'<div class="mono" style="display:flex;gap:12px;align-items:center"><span style="width:150px">{dst}</span><div class="bar" style="width:{max(p, .02) * 320:.0f}px"></div><span>{p:.0%}</span></div>', unsafe_allow_html=True)
    nodes, M = markov_top(G, weight, 10)
    rows = "".join(f'<tr><th>{n}</th>' + "".join(f'<td style="background:rgba(255,46,136,{min(v * 1.6, 1):.2f})">{v:.2f}</td>' for v in row) + '</tr>' for n, row in zip(nodes, M))
    st.markdown(f'<div style="overflow-x:auto"><table class="heat" aria-label="Markov transition matrix"><tr><th></th>{"".join(f"<th>{n.split(chr(46))[-1]}</th>" for n in nodes)}</tr>{rows}</table></div>', unsafe_allow_html=True)
