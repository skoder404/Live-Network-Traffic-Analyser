import streamlit as st

from dashboard.themes import get_tokens

from ._ui import card


def render_stream_analytics(s, window, ctx):
    t = get_tokens()
    st.subheader("Streaming concepts, read from the serving store")
    cards = ctx["db"].concepts(s)
    for i in range(0, len(cards), 4):
        for col, (n, d, v) in zip(st.columns(4), cards[i : i + 4], strict=False):
            col.markdown(
                card(n, v, f'<div class="kpi-label">{d}</div>', t["primary"]),
                unsafe_allow_html=True,
            )
