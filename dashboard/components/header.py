from unittest.mock import Mock

import streamlit as st

from ._ui import card, human, spark


def render_header(s, window="LIVE", alert_count=0):
    pps = s.get("pps", 0) if isinstance(s, dict) else getattr(s, "pps", 0)
    bps = s.get("bps", 0) if isinstance(s, dict) else getattr(s, "bps", 0)
    hosts = s.get("hosts", 0) if isinstance(s, dict) else getattr(s, "hosts", 0)
    flows = s.get("flows", 0) if isinstance(s, dict) else getattr(s, "flows", 0)
    
    raw_hist = s.get("history") if isinstance(s, dict) else getattr(s, "history", None)
    h = raw_hist if raw_hist is not None else Mock(pps=[], bps=[])

    stale = pps <= 0
    st.markdown(
        f'<div class="top"><div class="scene" aria-hidden="true"><div class="cube"><i></i><i></i><i></i><i></i><i></i><i></i></div></div><div><div class="hero">LNTA · Live Network Traffic Analyser</div>'
        f'<span class="pulse neon-badge {"stale" if stale else ""}"></span> <span class="mono neon-badge">{"stale" if stale else "streaming"} · {window}s window · {alert_count} alerts</span></div>'
        f'<div class="radar {"stale" if stale else ""}" aria-hidden="true"></div></div>',
        unsafe_allow_html=True,
    )
    items = [
        ("Packets / sec", human(pps), spark(getattr(h, "pps", []), "#22e6ff"), "#22e6ff"),
        ("Bytes / sec", human(bps) + "B", spark(getattr(h, "bps", []), "#ff2e88"), "#ff2e88"),
        ("Hosts", str(hosts), "", "#b6ff3c"),
        ("Conversations", str(flows), "", "#ffb020"),
    ]
    for col, (a, b, c, d) in zip(st.columns(4), items, strict=False):
        col.markdown(card(a, b, c, d), unsafe_allow_html=True)
