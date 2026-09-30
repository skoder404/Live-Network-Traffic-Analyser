import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.themes import get_plotly_layout, get_tokens

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

    layout = get_plotly_layout()

    fig1 = px.line(df_metrics, x="window_start", y="pps", title="Packets Per Second")
    fig1.update_layout(**layout)
    fig2 = px.bar(df_proto, x="protocol", y="packets", title="Protocol Breakdown")
    fig2.update_layout(**layout)
    fig3 = px.bar(df_ports, x="port", y="total_packets", title="Top Destination Ports")
    fig3.update_layout(**layout)

    st.plotly_chart(fig1, width="stretch")
    st.plotly_chart(fig2, width="stretch")
    st.plotly_chart(fig3, width="stretch")


def render_live_overview(s, window=30, ctx=None):
    if not isinstance(s, dict) or "history" not in s or s["history"].empty:
        return state("empty", "No traffic yet. Start the stream or set LNTA_MOCK=true.")

    t = get_tokens()
    layout = get_plotly_layout()

    cols = st.columns([2, 1])
    a = cols[0] if len(cols) > 0 else st
    b = cols[1] if len(cols) > 1 else st

    fig_pps = px.area(
        s["history"].reset_index(),
        x=s["history"].index,
        y="pps",
        title="Packets per second",
        color_discrete_sequence=[t["chart_area"]],
    )
    fig_pps.update_layout(**layout)

    fig_proto = px.bar(
        s["protocols"].reset_index(),
        x="protocol",
        y="share",
        title="Protocol mix (%)",
        color_discrete_sequence=[t["chart_bar"]],
    )
    fig_proto.update_layout(**layout)

    with a:
        st.plotly_chart(fig_pps, width="stretch")
    with b:
        st.plotly_chart(fig_proto, width="stretch")
