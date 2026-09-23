"""
dashboard/app.py — LNTA Streamlit Dashboard entry point.

Streamlit app with 6 tabs, header/KPI strip, sidebar controls.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import streamlit as st

# Ensure repo root is on path (for imports when run as script)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dashboard.components.header import render_header
from dashboard.components.history import render_history_tab
from dashboard.components.live_overview import render_live_overview_tab
from dashboard.data import check_db_health, get_db
from dashboard.theme import register_lnta_theme

# Page config (must be first Streamlit command)
st.set_page_config(
    page_title="LNTA — Live Network Traffic Analyser",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Register Plotly theme
register_lnta_theme()

# Inject custom CSS
_css_path = os.path.join(os.path.dirname(__file__), "assets", "theme.css")
with open(_css_path, encoding="utf-8") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def render_sidebar() -> dict:
    """Render sidebar controls and return control values."""
    with st.sidebar:
        st.markdown("### ⚙️ Controls")
        window_len = st.selectbox(
            "Window Length", [10, 30, 60], index=0, format_func=lambda x: f"{x}s"
        )
        refresh_interval = st.slider("Refresh Interval (s)", 1, 10, 3)
        protocol_filter = st.multiselect(
            "Protocol Filter",
            ["TCP", "UDP", "ICMP", "OTHER"],
            default=["TCP", "UDP", "ICMP", "OTHER"],
        )
        top_n = st.slider("Top-N Nodes (Graph)", 10, 200, 50, step=10)
        st.divider()
        st.caption("LNTA v0.1.0 — Big Data Analytics Project")
        return {
            "window_len_s": window_len,
            "refresh_interval": refresh_interval,
            "protocol_filter": protocol_filter,
            "top_n": top_n,
        }


# Tab renderers (M2 implementation)
def render_stream_analytics_tab(db, controls) -> None:
    """Tab 2: Stream Analytics."""
    st.markdown("## 🔬 Stream Analytics")
    st.info(
        "Stream Analytics tab — Filtering, Sampling, Count Distinct, "
        "Counting Ones, Moments, Decay, Frequent Itemsets"
    )


def render_link_analysis_tab(db, controls) -> None:
    """Tab 3: Link Analysis."""
    st.markdown("## 🕸️ Link Analysis")
    st.info("Link Analysis tab — IP Graph, PageRank, Markov Transitions, Centrality")


def render_alerts_tab(db, controls) -> None:
    """Tab 4: Alerts."""
    st.markdown("## 🚨 Alerts")
    st.info("Alerts tab — Active alerts with explanations, timeline, filters")


def render_pipeline_tab(db, controls) -> None:
    """Tab 6: Pipeline Health."""
    st.markdown("## 🔧 Pipeline Health")
    st.info("Pipeline tab — Stage status, batch metrics, end-to-end lag")


_TAB_RENDERERS = [
    ("📊 Live Overview", render_live_overview_tab),
    ("🔬 Stream Analytics", render_stream_analytics_tab),
    ("🕸️ Link Analysis", render_link_analysis_tab),
    ("🚨 Alerts", render_alerts_tab),
    ("📜 History", render_history_tab),
    ("🔧 Pipeline", render_pipeline_tab),
]


def main() -> None:
    """Main entry point for Streamlit."""
    # Get DB path from environment (set by launch script) or default
    db_path = os.environ.get("LNTA_SERVING_DB", "serving/analytics.db")
    mock_mode = os.environ.get("LNTA_MOCK", "false").lower() == "true"

    db = get_db(db_path)
    db_health = check_db_health(db)

    source_mode = "DEMO DATA" if mock_mode else "LIVE"
    if not db_health.get("connected"):
        source_mode = "STALE"

    # Header + KPI strip
    render_header(db_health, source_mode)

    # Sidebar
    controls = render_sidebar()

    # Tabs
    tabs = st.tabs([label for label, _ in _TAB_RENDERERS])

    for tab, (_, renderer) in zip(tabs, _TAB_RENDERERS):
        with tab:
            renderer(db, controls)


if __name__ == "__main__":
    main()
