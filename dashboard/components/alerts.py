import streamlit as st
from ._ui import state
def render_alerts(s, window, ctx):
    log = st.session_state.get("alert_log", [])
    sev = st.multiselect("Severity", ["WARN", "CRITICAL"], default=["WARN", "CRITICAL"]); typ = sorted({a["type"] for a in log})
    kinds = st.multiselect("Type", typ, default=typ) if typ else []
    rows = [a for a in log if a["severity"] in sev and a["type"] in kinds]
    if st.button("Clear alerts"): st.session_state.alert_log = []; rows = []
    if not rows: return state("empty", "All quiet. Alerts explain themselves here when a rule fires.")
    for a in rows[:20]:
        who = f' from <b>{a["src_ip"]}</b>' if a["src_ip"] else ""
        st.markdown(f'<div class="glass alert {a["severity"]}"><span class="badge sev-{a["severity"]}">{a["severity"]}</span> <b>{a["type"].replace("_", " ").title()}</b>{who}<div>{a["reason"]}</div>'
                    f'<div class="mono kpi-label">now {a["current_value"]:.0f} · baseline {a["baseline_value"]:.0f} · {a["change_pct"]:+.0f}% · threshold {a["threshold"]:.0f} · {a["ts"][11:19]} UTC</div></div>', unsafe_allow_html=True)
