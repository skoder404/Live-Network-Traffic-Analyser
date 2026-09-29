import streamlit as st
def state(kind, text):
    icon = {"loading": "⏳", "empty": "🌌", "error": "⚠️", "stale": "🕒"}[kind]
    st.markdown(f'<div class="state {kind}" role="status">{icon} {text}</div>', unsafe_allow_html=True)
def card(label, value, extra="", color="#22e6ff"):
    return f'<div class="glass tilt"><div class="kpi-label">{label}</div><div class="kpi-value" style="color:{color}">{value}</div>{extra}</div>'
def spark(vals, color="#22e6ff", w=170, h=34):
    vals = list(vals)
    if len(vals) < 2: return ""
    lo, hi = min(vals), max(vals); rg = (hi - lo) or 1
    pts = " ".join(f"{i * w / (len(vals) - 1):.1f},{h - (v - lo) / rg * (h - 4) - 2:.1f}" for i, v in enumerate(vals))
    return f'<svg width="{w}" height="{h}" role="img" aria-label="trend"><polyline fill="none" stroke="{color}" stroke-width="2" points="{pts}"/></svg>'
def human(n):
    for u in ["", "K", "M", "G"]:
        if abs(n) < 1000: return f"{n:.1f}{u}".replace(".0", "")
        n /= 1000
    return f"{n:.1f}T"
