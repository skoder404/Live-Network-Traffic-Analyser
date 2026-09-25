"""
dashboard/components/alerts.py — Alerts tab for LNTA dashboard.

Shows active alerts with explanations, timeline, and filters.
Per DESIGN.md §103-108 and PRD §FR-ALR.
"""

import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import ServingDB, get_active_alerts
from dashboard.theme import LNTA_COLORS, get_severity_color


def render_alerts_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the Alerts tab with active alerts, timeline, and filters."""
    st.markdown("## 🚨 Alerts")

    # Fetch alerts
    with st.spinner("Loading alerts..."):
        try:
            alerts_df = get_active_alerts(db, limit=100)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load alerts: {e}")
            return

    if alerts_df.empty:
        st.success("✅ No alerts in the recent period")
        return

    # --- Filters ---
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        severity_filter = st.multiselect(
            "Severity",
            ["CRITICAL", "WARN", "INFO"],
            default=["CRITICAL", "WARN", "INFO"],
            key="alert_severity_filter",
        )
    with col_filter2:
        type_filter = st.multiselect(
            "Type",
            alerts_df["type"].unique().tolist(),
            default=alerts_df["type"].unique().tolist(),
            key="alert_type_filter",
        )
    with col_filter3:
        time_window = st.selectbox(
            "Time Window",
            ["Last 1 hour", "Last 6 hours", "Last 24 hours", "All"],
            index=1,
            key="alert_time_window",
        )

    # Apply filters
    filtered_df = alerts_df[
        (alerts_df["severity"].isin(severity_filter)) & (alerts_df["type"].isin(type_filter))
    ].copy()

    # Time filter
    if time_window != "All":
        hours = {"Last 1 hour": 1, "Last 6 hours": 6, "Last 24 hours": 24}[time_window]
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
        filtered_df["ts_parsed"] = pd.to_datetime(filtered_df["ts"], utc=True)
        filtered_df = filtered_df[filtered_df["ts_parsed"] >= cutoff]

    if filtered_df.empty:
        st.info("No alerts match the current filters")
        return

    # --- Active Alert Cards ---
    st.markdown(f"### Active Alerts ({len(filtered_df)})")

    for _, alert in filtered_df.iterrows():
        render_alert_card(alert)

    st.divider()

    # --- Alert Timeline ---
    st.markdown("### Alert Timeline")
    render_alert_timeline(filtered_df)

    # --- Summary Stats ---
    st.divider()
    render_alert_summary(filtered_df)


def render_alert_card(alert: pd.Series) -> None:
    """Render a single alert card with full explanation."""
    severity = alert["severity"]
    get_severity_color(severity)
    severity_icon = {"CRITICAL": "🔴", "WARN": "🟡", "INFO": "🔵"}.get(severity, "⚪")

    # Parse details JSON
    try:
        details = (
            eval(alert["details_json"])
            if isinstance(alert["details_json"], str)
            else alert["details_json"]
        )
    except Exception:
        details = {}

    # Format timestamp
    try:
        ts = pd.to_datetime(alert["ts"], utc=True)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        ts_str = str(alert["ts"])

    with st.container():
        st.markdown(
            f"""
            <div class="alert-card {severity.lower()}">
                <div class="alert-header">
                    <span class="alert-severity {severity.lower()}">{severity_icon} {severity}</span>
                    <span class="alert-type">{alert["type"]}</span>
                </div>
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; color: {LNTA_COLORS["text_secondary"]};">
                    Source: {alert["src_ip"]}
                </div>
                <div class="alert-metrics">
                    <div class="alert-metric">
                        <div class="alert-metric-label">Current</div>
                        <div class="alert-metric-value">{alert["current_value"]:.2f}</div>
                    </div>
                    <div class="alert-metric">
                        <div class="alert-metric-label">Baseline</div>
                        <div class="alert-metric-value">{alert["baseline_value"]:.2f}</div>
                    </div>
                    <div class="alert-metric">
                        <div class="alert-metric-label">Change</div>
                        <div class="alert-metric-value" style="color: {"#EF4444" if alert["change_pct"] > 0 else "#22C55E"};">
                            {alert["change_pct"]:+.1f}%
                        </div>
                    </div>
                    <div class="alert-metric">
                        <div class="alert-metric-label">Threshold</div>
                        <div class="alert-metric-value">{alert["threshold"]:.2f}</div>
                    </div>
                </div>
                <div class="alert-reason">{alert["reason"]}</div>
                <div class="alert-timestamp">{ts_str} | Alert ID: {alert["alert_id"][:8]}...</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Expandable details
    with st.expander("🔍 View Details"):
        st.json(
            {
                "alert_id": alert["alert_id"],
                "type": alert["type"],
                "severity": alert["severity"],
                "src_ip": alert["src_ip"],
                "metric": alert["metric"],
                "current_value": alert["current_value"],
                "baseline_value": alert["baseline_value"],
                "change_pct": alert["change_pct"],
                "threshold": alert["threshold"],
                "reason": alert["reason"],
                "details": details,
            }
        )


def render_alert_timeline(df: pd.DataFrame) -> None:
    """Render alert timeline as a scatter plot over time."""
    if df.empty:
        return

    df = df.copy()
    df["ts_parsed"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts_parsed")

    # Map severity to color and symbol
    severity_map = {
        "CRITICAL": {"color": LNTA_COLORS["critical"], "symbol": "x", "size": 14},
        "WARN": {"color": LNTA_COLORS["warn"], "symbol": "triangle-up", "size": 12},
        "INFO": {"color": LNTA_COLORS["info"], "symbol": "circle", "size": 10},
    }

    fig = go.Figure()

    for severity in ["CRITICAL", "WARN", "INFO"]:
        sev_df = df[df["severity"] == severity]
        if sev_df.empty:
            continue
        props = severity_map[severity]
        fig.add_trace(
            go.Scatter(
                x=sev_df["ts_parsed"],
                y=sev_df["type"],
                mode="markers",
                marker={
                    "color": props["color"],
                    "symbol": props["symbol"],
                    "size": props["size"],
                    "line": {"width": 1, "color": "white"},
                },
                name=severity,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Time: %{x}<br>"
                    "Src IP: %{customdata[0]}<br>"
                    "Severity: %{customdata[1]}<br>"
                    "Change: %{customdata[2]:.1f}%<br>"
                    "Reason: %{customdata[3]}"
                    "<extra></extra>"
                ),
                customdata=list(
                    zip(
                        sev_df["src_ip"],
                        sev_df["severity"],
                        sev_df["change_pct"],
                        sev_df["reason"],
                        strict=False,
                    )
                ),
            )
        )

    fig.update_layout(
        template="lnta_dark",
        height=300,
        margin={"l": 150, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Alert Type", "autorange": "reversed"},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        hovermode="closest",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_alert_summary(df: pd.DataFrame) -> None:
    """Render summary statistics for alerts."""
    st.markdown("### Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Alerts", len(df))

    with col2:
        critical_count = len(df[df["severity"] == "CRITICAL"])
        st.metric(
            "Critical", critical_count, delta_color="inverse" if critical_count > 0 else "off"
        )

    with col3:
        warn_count = len(df[df["severity"] == "WARN"])
        st.metric("Warnings", warn_count, delta_color="inverse" if warn_count > 0 else "off")

    with col4:
        types = df["type"].nunique()
        st.metric("Alert Types", types)

    # Top offending IPs
    if not df.empty:
        st.markdown("**Top Offending Source IPs**")
        top_ips = df.groupby("src_ip").size().sort_values(ascending=False).head(5)
        ip_df = pd.DataFrame({"Source IP": top_ips.index, "Alert Count": top_ips.values})
        st.dataframe(ip_df, use_container_width=True, hide_index=True)
