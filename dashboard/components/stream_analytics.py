import streamlit as st

from ._ui import card


def render_stream_analytics(s, window=10, ctx=None):
    st.subheader("Streaming concepts, read from the serving store")
    cards = []
    if ctx and isinstance(ctx, dict) and "db" in ctx:
        try:
            cards = ctx["db"].concepts(s)
        except Exception:
            cards = []
    elif hasattr(s, "concepts") and callable(s.concepts):
        try:
            res = s.concepts()
            if isinstance(res, (list, tuple)):
                cards = res
        except Exception:
            cards = []

    if not isinstance(cards, (list, tuple)):
        cards = []

    for i in range(0, len(cards), 4):
        for col, (n, d, v) in zip(st.columns(4), cards[i:i + 4], strict=False):
            col.markdown(card(n, v, f'<div class="kpi-label">{d}</div>', "#ff2e88"), unsafe_allow_html=True)
