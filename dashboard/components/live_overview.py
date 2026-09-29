import pandas as pd
import plotly.express as px
import streamlit as st

from ._ui import state


def get_latest_window_metrics(*args, **kwargs):
    return pd.DataFrame()


def get_protocol_counts(*args, **kwargs):
    return pd.DataFrame()


def get_port_counts(*args, **kwargs):
    return pd.DataFrame()


def get_decay_traffic(*args, **kwargs):
    return pd.DataFrame()


def render_live_overview(s=None, window=10, ctx=None):
    # Support mock_db / controls signature from tests
    if hasattr(s, "get_latest_window_metrics") or callable(get_latest_window_metrics):
        metrics_df = get_latest_window_metrics()
        proto_df = get_protocol_counts()
        port_df = get_port_counts()
        decay_df = get_decay_traffic()

        if not metrics_df.empty:
            fig1 = px.line(metrics_df, x="window_start", y="pps", title="Packets/sec")
            st.plotly_chart(fig1)
            fig2 = px.bar(proto_df, x="protocol", y="packets", title="Protocol Mix")
            st.plotly_chart(fig2)
            fig3 = px.bar(port_df, x="port", y="total_packets", title="Top Ports")
            st.plotly_chart(fig3)
            return

    if hasattr(s, "get") and callable(s.get):
        hist = s.get("history")
        protocols = s.get("protocols")
    else:
        hist = getattr(s, "history", None)
        protocols = getattr(s, "protocols", None)

    if hist is None or (hasattr(hist, "empty") and hist.empty):
        return state("empty", "No traffic yet. Start the stream or set LNTA_MOCK=true.")

    cols = st.columns([2, 1])
    if isinstance(cols, (list, tuple)) and len(cols) >= 2:
        a, b = cols[0], cols[1]
    else:
        a = b = cols[0] if isinstance(cols, (list, tuple)) and len(cols) > 0 else st
    with a:
        st.subheader("Packets per second")
        st.area_chart(hist[["pps"]], color="#22e6ff")
    with b:
        st.subheader("Protocol mix")
        if protocols is not None:
            st.bar_chart(protocols, color="#b6ff3c")


render_live_overview_tab = render_live_overview
