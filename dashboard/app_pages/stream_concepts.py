import streamlit as st

from dashboard.components import render_header, render_stream_analytics

s = st.session_state.get("_lnta_snapshot", {})
window = st.session_state.get("_lnta_window", 30)
ctx = st.session_state.get("_lnta_ctx", {})

if not s:
    st.warning("No data available.")
    st.stop()

render_header(s, window, len(st.session_state.get("alert_log", [])))
st.write("")
render_stream_analytics(s, window, ctx)
