"""
dashboard/components/live_overview.py — Live Overview tab for LNTA dashboard.

Shows traffic over time, protocol distribution, top ports, and decay score.
Per DESIGN.md §73-79.
"""

import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import (
    ServingDB,
    get_decay_traffic,
    get_latest_window_metrics,
    get_port_counts,
    get_protocol_counts,
)
from dashboard.theme import LNTA_COLORS, get_protocol_color


def render_live_overview_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the Live Overview tab with traffic chart, protocol donut, top ports, decay."""
    st.markdown("## 📊 Live Overview")

    window_len = controls["window_len_s"]

    # Fetch data with loading state
    with st.spinner("Loading live overview data..."):
        try:
            metrics_df = get_latest_window_metrics(db, window_len, limit=120)
            protocol_df = get_protocol_counts(db, window_len, limit=120)
            ports_df = get_port_counts(db, window_len, limit=20)
            decay_df = get_decay_traffic(db, limit=120)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load data: {e}")
            return

    # Check for stale data
    if not metrics_df.empty:
        latest_ts = metrics_df["window_start"].max()
        # Handle both tz-naive and tz-aware timestamps
        latest_ts = pd.Timestamp(latest_ts)
        now_utc = pd.Timestamp.now(tz="UTC")
        if latest_ts.tz is None:
            latest_ts = latest_ts.tz_localize("UTC")
        if latest_ts < now_utc - pd.Timedelta(seconds=15):
            st.warning("⚠️ Data may be stale (last update > 15s ago)")

    # Empty state
    if metrics_df.empty:
        st.markdown(
            """
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <div>No traffic data available yet.</div>
                <div style="font-size:12px; color:var(--text-muted); margin-top:8px;">
                    Start capture or replay to see live overview.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # --- Row 1: Traffic over time (dual-axis) ---
    st.markdown("### Traffic Over Time")
    render_traffic_chart(metrics_df)

    # --- Row 2: Protocol donut (1/3) + Top ports (2/3) ---
    col_proto, col_ports = st.columns([1, 2])

    with col_proto:
        st.markdown("### Protocol Distribution")
        render_protocol_donut(protocol_df)

    with col_ports:
        st.markdown("### Top Destination Ports")
        render_top_ports(ports_df)

    # --- Row 3: Decay score ---
    st.markdown("### Decay Score")
    render_decay_score(decay_df)


def render_traffic_chart(df: pd.DataFrame) -> None:
    """Render dual-axis line chart: packets/s (left) and bytes/s (right)."""
    if df.empty:
        st.info("No traffic data")
        return

    # Sort by time ascending for proper line chart
    df = df.sort_values("window_start")

    fig = go.Figure()

    # Packets/s (left axis)
    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["pps"],
            mode="lines",
            name="Packets/s",
            line={"color": LNTA_COLORS["proto_tcp"], "width": 2},
            hovertemplate="%{x}<br>Packets/s: %{y:.1f}<extra></extra>",
        )
    )

    # Bytes/s (right axis)
    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["bps"],
            mode="lines",
            name="Bytes/s",
            line={"color": LNTA_COLORS["proto_udp"], "width": 2},
            yaxis="y2",
            hovertemplate="%{x}<br>Bytes/s: %{y:,.0f}<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=320,
        margin={"l": 60, "r": 60, "t": 40, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Packets/s", "side": "left", "color": LNTA_COLORS["proto_tcp"]},
        yaxis2={
            "title": "Bytes/s",
            "side": "right",
            "overlaying": "y",
            "color": LNTA_COLORS["proto_udp"],
        },
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_protocol_donut(df: pd.DataFrame) -> None:
    """Render protocol distribution donut chart."""
    if df.empty:
        st.info("No protocol data")
        return

    # Aggregate across windows
    agg = df.groupby("protocol").agg({"packets": "sum"}).reset_index()

    fig = go.Figure(
        go.Pie(
            labels=agg["protocol"],
            values=agg["packets"],
            hole=0.5,
            marker={
                "colors": [get_protocol_color(p) for p in agg["protocol"]],
                "line": {"color": LNTA_COLORS["bg_base"], "width": 2},
            },
            textinfo="label+percent",
            textfont={"size": 12, "color": LNTA_COLORS["text_primary"]},
            hovertemplate="%{label}: %{value:,} packets (%{percent})<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=280,
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        showlegend=True,
        legend={"orientation": "v", "yanchor": "middle", "y": 0.5, "xanchor": "left", "x": 1.05},
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_top_ports(df: pd.DataFrame) -> None:
    """Render horizontal bar chart of top destination ports."""
    if df.empty:
        st.info("No port data")
        return

    # Service name mapping
    service_names = {
        443: "HTTPS",
        53: "DNS",
        80: "HTTP",
        22: "SSH",
        25: "SMTP",
        123: "NTP",
        161: "SNMP",
        389: "LDAP",
        445: "SMB",
        3389: "RDP",
        5432: "PostgreSQL",
        3306: "MySQL",
    }

    df = df.copy()
    df["service"] = df["port"].map(service_names).fillna("Other")
    df["label"] = df.apply(lambda r: f"{r['port']} ({r['service']})", axis=1)

    fig = go.Figure(
        go.Bar(
            x=df["total_packets"],
            y=df["label"],
            orientation="h",
            marker_color=LNTA_COLORS["accent"],
            text=df["total_packets"],
            textposition="outside",
            hovertemplate="%{y}: %{x:,} packets<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=280,
        margin={"l": 120, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Packets"},
        yaxis={"autorange": "reversed"},
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_decay_score(df: pd.DataFrame) -> None:
    """Render decay score line chart with trend arrow."""
    if df.empty:
        st.info("No decay data")
        return

    df = df.sort_values("ts")

    fig = go.Figure(
        go.Scatter(
            x=df["ts"],
            y=df["score"],
            mode="lines",
            line={"color": LNTA_COLORS["proto_udp"], "width": 2},
            fill="tozeroy",
            fillcolor="rgba(167, 139, 250, 0.1)",
            hovertemplate="%{x}<br>Decay Score: %{y:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=140,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Decay Score"},
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Trend arrow
    if len(df) >= 2:
        last = df.iloc[-1]["score"]
        prev = df.iloc[-2]["score"]
        trend = "▲" if last > prev else "▼" if last < prev else "●"
        color = (
            LNTA_COLORS["ok"]
            if last > prev
            else LNTA_COLORS["critical"]
            if last < prev
            else LNTA_COLORS["text_muted"]
        )
        st.markdown(
            f'<div style="text-align:right; color:{color}; font-family:monospace;">Trend: {trend} {last:.2f}</div>',
            unsafe_allow_html=True,
        )
