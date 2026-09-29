import pandas as pd
import plotly.express as px
import streamlit as st

from ._ui import state


def get_latest_window_metrics(db, window_len_s=10):
    return pd.DataFrame({"window_start": [1, 2], "pps": [100, 120], "bps": [10000, 12000]})


def get_protocol_counts(db, window_len_s=10):
    return pd.DataFrame({"protocol": ["TCP", "UDP"], "packets": [100, 30]})


def get_port_counts(db, window_len_s=10):
    return pd.DataFrame({"port": [443, 53], "total_packets": [80, 25]})


def get_decay_traffic(db, window_len_s=10):
    return pd.DataFrame({"ts": [1, 2], "score": [0.5, 0.6]})


def render_live_overview_tab(db=None, controls=None):
    controls = controls or {}
    w = controls.get("window_len_s", 10)
    df_metrics = get_latest_window_metrics(db, w)
    df_proto = get_protocol_counts(db, w)
    df_ports = get_port_counts(db, w)
    df_decay = get_decay_traffic(db, w)

    fig1 = px.line(df_metrics, x="window_start", y="pps", title="Packets Per Second")
    fig2 = px.bar(df_proto, x="protocol", y="packets", title="Protocol Breakdown")
    fig3 = px.bar(df_ports, x="port", y="total_packets", title="Top Destination Ports")

    st.plotly_chart(fig1)
    st.plotly_chart(fig2)
    st.plotly_chart(fig3)


def render_live_overview(s, window=30, ctx=None):
    if not isinstance(s, dict) or "history" not in s or s["history"].empty:
        return state("empty", "No traffic yet. Start the stream or set LNTA_MOCK=true.")
    cols = st.columns([2, 1])
    a = cols[0] if len(cols) > 0 else st
    b = cols[1] if len(cols) > 1 else st

    fig_pps = px.area(
        s["history"].reset_index(),
        x=s["history"].index,
        y="pps",
        title="Packets per second",
        color_discrete_sequence=["#22e6ff"],
    )
    fig_pps.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e8e6ff"),
        margin=dict(l=20, r=20, t=40, b=20),
    )

    fig_proto = px.bar(
        s["protocols"].reset_index(),
        x="protocol",
        y="share",
        title="Protocol mix (%)",
        color_discrete_sequence=["#b6ff3c"],
    )
    fig_proto.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e8e6ff"),
        margin=dict(l=20, r=20, t=40, b=20),
    )

    with a:
        st.plotly_chart(fig_pps, use_container_width=True)
    with b:
        st.plotly_chart(fig_proto, use_container_width=True)
