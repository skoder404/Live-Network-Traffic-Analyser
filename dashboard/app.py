import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import streamlit as st

from dashboard import components as C
from dashboard.data import get_db
from dashboard.theme import apply_theme
from linkanalysis import AlertEngine, load_alert_config

st.set_page_config(page_title="LNTA · Priyan S", page_icon="📡", layout="wide")
apply_theme()
db = st.cache_resource(get_db)()
with st.sidebar:
    st.header("Controls")
    window = int(st.radio("Window", ["10s", "30s", "60s"], index=1, horizontal=True)[:-1])
    auto = st.toggle("Auto-refresh (5s)", True)
    st.caption("Mock mode: LNTA_MOCK=true. Set false to read the SQLite serving store.")
def tick(s):
    """Run the AlertEngine once per new 5s bucket, so widget reruns never double-count."""
    ss = st.session_state
    ss.setdefault("engine", AlertEngine(load_alert_config(str(ROOT / "config" / "alert_rules.yaml")))); ss.setdefault("alert_log", [])
    if ss.get("last_bucket") == s["bucket"]: return
    ss.last_bucket = s["bucket"]
    new = ss.engine.evaluate_window(datetime.now(timezone.utc).isoformat(), window, s["pps"], s["port_counts"], s["dst_counts"], s["pkt_counts"])
    ss.alert_log[:0] = [a.to_dict() for a in new]; del ss.alert_log[200:]
def body():
    s = db.snapshot(window); tick(s); ctx = {"db": db}
    C.render_header(s, window, len(st.session_state.alert_log)); st.write("")
    names = ["Live", "Stream concepts", "Link analysis", "Alerts", "History", "Pipeline"]
    fns = [C.render_live_overview, C.render_stream_analytics, C.render_link_analysis, C.render_alerts, C.render_history, C.render_pipeline]
    for tab, fn in zip(st.tabs(names), fns, strict=False):
        with tab:
            try: fn(s, window, ctx)
            except Exception as e: st.markdown(f'<div class="state error" role="alert">Could not load this view: {e}</div>', unsafe_allow_html=True)
(st.fragment(run_every=5)(body) if auto else body)()
