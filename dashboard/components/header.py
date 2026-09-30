import streamlit as st

from ._ui import card, human, spark


def render_header(s, window=30, alert_count=0):
    pps = s.get("pps", 0) if isinstance(s, dict) else 0
    bps = s.get("bps", 0) if isinstance(s, dict) else 0
    hosts = s.get("hosts", 0) if isinstance(s, dict) else 0
    flows = s.get("flows", 0) if isinstance(s, dict) else 0
    history = s.get("history", None) if isinstance(s, dict) else None

    stale = pps <= 0
    st.markdown(
        f'<div class="top"><div class="scene" aria-hidden="true"><div class="cube"><i></i><i></i><i></i><i></i><i></i><i></i></div></div><div><div class="hero">LNTA · Live Network Traffic Analyser</div>'
        f'<span class="pulse {"stale" if stale else ""}"></span> <span class="neon-badge mono">{"stale" if stale else "streaming"} · {window}s window · {alert_count} alerts</span></div>'
        f'<div class="radar {"stale" if stale else ""}" aria-hidden="true"></div></div>',
        unsafe_allow_html=True,
    )
    pps_spark = (
        spark(history.pps, "#22e6ff") if history is not None and hasattr(history, "pps") else ""
    )
    bps_spark = (
        spark(history.bps, "#ff2e88") if history is not None and hasattr(history, "bps") else ""
    )

    items = [
        ("Packets / sec", human(pps), pps_spark, "#22e6ff"),
        ("Bytes / sec", human(bps) + "B", bps_spark, "#ff2e88"),
        ("Hosts", str(hosts), "", "#b6ff3c"),
        ("Conversations", str(flows), "", "#ffb020"),
    ]
    cols = st.columns(4)
    if not cols:
        cols = [st]
    for col, (a, b, c, d) in zip(cols, items, strict=False):
        col.markdown(card(a, b, c, d), unsafe_allow_html=True)
