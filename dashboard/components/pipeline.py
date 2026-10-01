import streamlit as st
def render_pipeline(s, window, ctx):
    df = ctx["db"].pipeline(s); st.subheader("Pipeline health")
    for col, r in zip(st.columns(len(df)), df.itertuples()):
        c = "#b6ff3c" if r.status == "ok" else "#ffb020"
        col.markdown(f'<div class="glass tilt" style="border-color:{c}"><b>{r.stage}</b><div class="kpi-value" style="color:{c}">{r.latency_ms}ms</div><div class="kpi-label">{r.rows_per_s} rows/s · {r.status}</div></div>', unsafe_allow_html=True)
    a, b = st.columns(2); a.bar_chart(df.set_index("stage")["latency_ms"], color="#ff2e88"); b.bar_chart(df.set_index("stage")["rows_per_s"], color="#22e6ff")
