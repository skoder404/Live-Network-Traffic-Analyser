import streamlit as st
from ._ui import card, spark, human
def render_header(s, window, alert_count):
    stale = s["pps"] <= 0
    st.markdown(f'<div class="top"><div class="scene" aria-hidden="true"><div class="cube"><i></i><i></i><i></i><i></i><i></i><i></i></div></div><div><div class="hero">LNTA · Live Network Traffic Analyser</div>'
                f'<span class="pulse {"stale" if stale else ""}"></span> <span class="mono">{"stale" if stale else "streaming"} · {window}s window · {alert_count} alerts</span></div>'
                f'<div class="radar {"stale" if stale else ""}" aria-hidden="true"></div></div>', unsafe_allow_html=True)
    h = s["history"]; items = [("Packets / sec", human(s["pps"]), spark(h.pps, "#22e6ff"), "#22e6ff"), ("Bytes / sec", human(s["bps"]) + "B", spark(h.bps, "#ff2e88"), "#ff2e88"),
                               ("Hosts", str(s["hosts"]), "", "#b6ff3c"), ("Conversations", str(s["flows"]), "", "#ffb020")]
    for col, (a, b, c, d) in zip(st.columns(4), items): col.markdown(card(a, b, c, d), unsafe_allow_html=True)
