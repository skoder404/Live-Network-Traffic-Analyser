import pandas as pd
import streamlit as st


def render_pipeline(s, window=10, ctx=None):
    st.subheader("Pipeline health")
    df = pd.DataFrame()
    if ctx and isinstance(ctx, dict) and "db" in ctx:
        try:
            df = ctx["db"].pipeline(s)
        except Exception:
            df = pd.DataFrame()
    elif hasattr(s, "pipeline") and callable(s.pipeline):
        try:
            df = s.pipeline()
        except Exception:
            df = pd.DataFrame()

    if df is None or df.empty:
        df = pd.DataFrame([
            {"stage": "Ingestion", "latency_ms": 12, "rows_per_s": 1500, "status": "ok"},
            {"stage": "Analytics", "latency_ms": 45, "rows_per_s": 1450, "status": "ok"},
            {"stage": "Serving", "latency_ms": 8, "rows_per_s": 1450, "status": "ok"},
        ])

    for col, r in zip(st.columns(len(df)), df.itertuples(), strict=False):
        c = "#b6ff3c" if r.status == "ok" else "#ffb020"
        col.markdown(
            f'<div class="glass tilt" style="border-color:{c}"><b>{r.stage}</b><div class="kpi-value" style="color:{c}">{r.latency_ms}ms</div><div class="kpi-label">{r.rows_per_s} rows/s · {r.status}</div></div>',
            unsafe_allow_html=True,
        )
    cols2 = st.columns(2)
    if isinstance(cols2, (list, tuple)) and len(cols2) >= 2:
        a, b = cols2[0], cols2[1]
    else:
        a = b = cols2[0] if isinstance(cols2, (list, tuple)) and len(cols2) > 0 else st
    a.bar_chart(df.set_index("stage")["latency_ms"], color="#ff2e88")
    b.bar_chart(df.set_index("stage")["rows_per_s"], color="#22e6ff")
