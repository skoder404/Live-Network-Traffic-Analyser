import streamlit as st

from dashboard.themes import get_tokens


def render_pipeline(s, window, ctx):
    t = get_tokens()
    df = ctx["db"].pipeline(s)
    st.subheader("Pipeline health")
    for col, r in zip(st.columns(len(df)), df.itertuples(), strict=False):
        c = t["accent1"] if r.status == "ok" else t["accent2"]
        col.markdown(
            f'<div class="glass tilt" style="border-color:{c}"><b>{r.stage}</b>'
            f'<div class="kpi-value" style="color:{c}">{r.latency_ms}ms</div>'
            f'<div class="kpi-label">{r.rows_per_s} rows/s · {r.status}</div></div>',
            unsafe_allow_html=True,
        )
    a, b = st.columns(2)
    a.bar_chart(df.set_index("stage")["latency_ms"], color=t["primary"], width="stretch")
    b.bar_chart(df.set_index("stage")["rows_per_s"], color=t["secondary"], width="stretch")
