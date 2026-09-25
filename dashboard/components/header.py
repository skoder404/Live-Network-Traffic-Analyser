"""
dashboard/components/header.py — Header and KPI strip component for LNTA dashboard.
"""

import streamlit as st

# Constants
KPI_LABELS = [
    "Packets/s",
    "Bytes/s",
    "Unique IPs",
    "Active Ports",
    "Decay Score",
    "Alerts",
]

SOURCE_BADGE_MAP = {
    "LIVE": "live",
    "REPLAY": "replay",
    "DEMO": "demo",
    "STALE": "stale",
}


def render_header(db_health: dict, source_mode: str = "LIVE") -> None:
    """Render global header with logo, source badge, pulse, KPI strip."""
    col_logo, col_badge, col_pulse, col_update, *col_kpis = st.columns(
        [2, 2, 1, 2, 1, 1, 1, 1, 1, 1]
    )

    with col_logo:
        st.markdown(
            """
        <div style="display:flex;align-items:center;gap:10px;height:100%;">
            <span style="font-size:28px;font-weight:700;color:#22D3EE;">LNTA</span>
            <span style="font-size:12px;color:#8496B0;text-transform:uppercase;">Live Network Traffic Analyser</span>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col_badge:
        badge_class = SOURCE_BADGE_MAP.get(source_mode, "live")
        st.markdown(
            f'<span class="source-badge {badge_class}">{source_mode}</span>', unsafe_allow_html=True
        )

    with col_pulse:
        pulse_class = "fresh" if db_health.get("connected") else "down"
        st.markdown(
            f'<div class="live-pulse {pulse_class}" title="Pipeline status"></div>',
            unsafe_allow_html=True,
        )

    with col_update:
        if db_health.get("connected"):
            latest = db_health.get("table_status", {}).get("window_metrics")
            st.caption(f"Updated: {latest}" if latest else "Waiting for data...")
        else:
            st.caption("⚠️ Database disconnected")

    # KPI cards - placeholder values, will be filled by components
    for i, label in enumerate(KPI_LABELS):
        with col_kpis[i]:
            st.markdown(
                f"""
            <div class="kpi-card">
                <div class="kpi-value">—</div>
                <div class="kpi-label">{label}</div>
                <div class="kpi-delta"></div>
            </div>
            """,
                unsafe_allow_html=True,
            )
