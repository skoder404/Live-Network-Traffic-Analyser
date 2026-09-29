import streamlit as st
from ._ui import state
def render_live_overview(s, window, ctx):
    if s["history"].empty: return state("empty", "No traffic yet. Start the stream or set LNTA_MOCK=true.")
    a, b = st.columns([2, 1])
    with a: st.subheader("Packets per second"); st.area_chart(s["history"][["pps"]], color="#22e6ff")
    with b: st.subheader("Protocol mix"); st.bar_chart(s["protocols"], color="#b6ff3c")
