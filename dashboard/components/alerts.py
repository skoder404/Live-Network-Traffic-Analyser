import streamlit as st

from ._ui import state


def render_alerts_tab(db=None, controls=None):
    st.markdown("### Alerts")


def render_alerts(s, window=30, ctx=None):
    log = st.session_state.get("alert_log", [])
    sev = st.multiselect("Severity", ["WARN", "CRITICAL"], default=["WARN", "CRITICAL"])
    typ = sorted({a["type"] for a in log if isinstance(a, dict) and "type" in a})
    kinds = st.multiselect("Type", typ, default=typ) if typ else []
    rows = [
        a
        for a in log
        if isinstance(a, dict) and a.get("severity") in sev and a.get("type") in kinds
    ]
    if st.button("Clear alerts"):
        st.session_state.alert_log = []
        rows = []
    if not rows:
        return state("empty", "All quiet. Alerts explain themselves here when a rule fires.")
    for a in rows[:20]:
        who = f" from <b>{a.get('src_ip', '')}</b>" if a.get("src_ip") else ""
        st.markdown(
            f'<div class="glass alert {a.get("severity", "WARN")}"><span class="badge sev-{a.get("severity", "WARN")}">{a.get("severity", "WARN")}</span> <b>{str(a.get("type", "")).replace("_", " ").title()}</b>{who}<div>{a.get("reason", "")}</div>'
            f'<div class="mono kpi-label">now {a.get("current_value", 0):.0f} · baseline {a.get("baseline_value", 0):.0f} · {a.get("change_pct", 0):+.0f}% · threshold {a.get("threshold", 0):.0f} · {str(a.get("ts", ""))[11:19]} UTC</div></div>',
            unsafe_allow_html=True,
        )
