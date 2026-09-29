import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import streamlit as st

from dashboard import components as C
from dashboard.data import get_db
from dashboard.themes import THEME_TOKENS, apply_theme
from linkanalysis import AlertEngine, load_alert_config

st.set_page_config(
    page_title="LNTA · Live Network Traffic Analyser",
    page_icon="📡",
    layout="wide",
)

with st.sidebar:
    st.markdown("### ⚙️ Controls")
    window_choice = st.radio("Window", ["10s", "30s", "60s"], index=1, horizontal=True)
    window = int(window_choice[:-1])
    auto = st.toggle("Auto-refresh (5s)", True)
    theme_name = st.selectbox(
        "🎨 Theme",
        list(THEME_TOKENS.keys()),
        index=0,
        key="theme_select",
    )
    st.divider()
    st.markdown("### 📡 LNTA Navigation")
    names = ["Live", "Stream concepts", "Link analysis", "Alerts", "History", "Pipeline"]
    selected_page = st.radio("Navigation", names, index=0, key="sidebar_nav")
    st.caption("LNTA_MOCK=true. Set false to read SQLite store.")

apply_theme(theme_name)
db = st.cache_resource(get_db)()


def tick(s):
    ss = st.session_state
    ss.setdefault(
        "engine", AlertEngine(load_alert_config(str(ROOT / "config" / "alert_rules.yaml")))
    )
    ss.setdefault("alert_log", [])
    if ss.get("last_bucket") == s["bucket"]:
        return
    ss.last_bucket = s["bucket"]
    new = ss.engine.evaluate_window(
        datetime.now(timezone.utc).isoformat(),
        window,
        s["pps"],
        s["port_counts"],
        s["dst_counts"],
        s["pkt_counts"],
    )
    ss.alert_log[:0] = [a.to_dict() for a in new]
    del ss.alert_log[200:]


def body():
    s = db.snapshot(window)
    tick(s)
    ctx = {"db": db}
    C.render_header(s, window, len(st.session_state.get("alert_log", [])))
    st.write("")

    fns = [
        C.render_live_overview,
        C.render_stream_analytics,
        C.render_link_analysis,
        C.render_alerts,
        C.render_history,
        C.render_pipeline,
    ]

    tabs = st.tabs(names)
    active_idx = names.index(selected_page) if selected_page in names else 0

    for i, (tab, fn) in enumerate(zip(tabs, fns, strict=False)):
        if i == active_idx:
            with tab:
                try:
                    fn(s, window, ctx)
                except Exception as e:
                    st.markdown(
                        f'<div class="state error" role="alert">Could not load this view: {e}</div>',
                        unsafe_allow_html=True,
                    )


(st.fragment(run_every=5)(body) if auto else body)()
