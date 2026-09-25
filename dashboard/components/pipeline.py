"""
dashboard/components/pipeline.py — Pipeline tab for LNTA dashboard.

Shows pipeline stage health, batch metrics, and end-to-end lag.
Per DESIGN.md §114-117.
"""

import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import ServingDB, get_pipeline_health
from dashboard.theme import LNTA_COLORS


def render_pipeline_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the Pipeline tab with stage tiles and health metrics."""
    st.markdown("## 🔧 Pipeline Health")

    # Fetch pipeline health data with loading state
    with st.spinner("Loading pipeline health..."):
        try:
            health_df = get_pipeline_health(db, limit=200)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load pipeline health: {e}")
            return

    # Check for stale data
    if not health_df.empty:
        latest_ts = health_df["ts"].max()
        latest_ts = pd.Timestamp(latest_ts)
        now_utc = pd.Timestamp.now(tz="UTC")
        if latest_ts.tz is None:
            latest_ts = latest_ts.tz_localize("UTC")
        if latest_ts < now_utc - pd.Timedelta(seconds=30):
            st.warning("⚠️ Pipeline data may be stale (last update > 30s ago)")

    # Empty state
    if health_df.empty:
        st.markdown(
            """
            <div class="empty-state" role="status" aria-live="polite">
                <div class="empty-state-icon">🔧</div>
                <div>No pipeline health data available yet.</div>
                <div style="font-size:12px; color:var(--text-muted); margin-top:8px;">
                    Start the pipeline to see stage health and metrics.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # --- Stage Tiles ---
    st.markdown("### Pipeline Stages")
    render_stage_tiles(health_df)

    st.divider()

    # --- Batch Duration vs Trigger Interval ---
    st.markdown("### Batch Duration vs Trigger Interval")
    render_batch_duration_chart(health_df)

    # --- Input Rows per Batch ---
    st.markdown("### Input Rows per Batch")
    render_input_rows_chart(health_df)

    # --- End-to-End Lag ---
    st.markdown("### End-to-End Lag")
    render_e2e_lag_chart(health_df)

    # --- Counters ---
    st.markdown("### Counters")
    render_counters(health_df)


def render_stage_tiles(df: pd.DataFrame) -> None:
    """Render stage status tiles: Capture → Flume → HDFS → Spark → Serving."""
    stages = [
        ("Capture", "capture", "📡"),
        ("Flume", "flume", "📥"),
        ("HDFS", "hdfs", "💾"),
        ("Spark", "spark", "⚡"),
        ("Serving", "serving", "🗄️"),
    ]

    cols = st.columns(5)

    for i, (name, component, icon) in enumerate(stages):
        with cols[i]:
            # Get latest status for this component
            comp_df = df[df["component"] == component]
            if comp_df.empty:
                status = "UNKNOWN"
                last_seen = "Never"
            else:
                latest = comp_df.sort_values("ts").iloc[-1]
                # Simple heuristic: if metric is batch_duration_ms and value < 5000, OK
                if latest["metric"] == "batch_duration_ms":
                    status = "OK" if latest["value"] < 5000 else "LAGGING"
                else:
                    status = "OK"
                last_seen = latest["ts"]

            # Status color
            status_colors = {
                "OK": LNTA_COLORS["ok"],
                "LAGGING": LNTA_COLORS["warn"],
                "DOWN": LNTA_COLORS["critical"],
                "UNKNOWN": LNTA_COLORS["text_muted"],
            }
            color = status_colors.get(status, LNTA_COLORS["text_muted"])

            # Accessible status with icon + text (not color-only)
            status_icons = {
                "OK": "✅",
                "LAGGING": "⚠️",
                "DOWN": "🔴",
                "UNKNOWN": "❓",
            }
            status_icon = status_icons.get(status, "❓")

            st.markdown(
                f"""
                <div class="panel-card" style="text-align:center; border-left: 4px solid {color};" role="status" aria-label="{name} stage: {status}">
                    <div style="font-size:24px;" aria-hidden="true">{icon}</div>
                    <div style="font-weight:600; margin:8px 0;">{name}</div>
                    <div style="color:{color}; font-weight:600;">{status_icon} {status}</div>
                    <div style="font-size:11px; color:var(--text-muted); margin-top:4px;">Last: {last_seen}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_batch_duration_chart(df: pd.DataFrame) -> None:
    """Render batch duration vs trigger interval chart."""
    batch_df = df[df["metric"] == "batch_duration_ms"].copy()
    if batch_df.empty:
        st.markdown(
            """
            <div class="empty-state" role="status" aria-live="polite">
                <div class="empty-state-icon">📊</div>
                <div>No batch duration data</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    batch_df = batch_df.sort_values("ts")

    fig = go.Figure()

    # Batch duration
    fig.add_trace(
        go.Scatter(
            x=batch_df["ts"],
            y=batch_df["value"],
            mode="lines+markers",
            name="Batch Duration (ms)",
            line={"color": LNTA_COLORS["accent"], "width": 2},
            hovertemplate="%{x}<br>Duration: %{y} ms<extra></extra>",
        )
    )

    # Trigger interval line (5 seconds = 5000 ms)
    fig.add_hline(
        y=5000,
        line_dash="dash",
        line_color=LNTA_COLORS["critical"],
        annotation_text="Trigger Interval (5s)",
        annotation_position="bottom right",
    )

    fig.update_layout(
        template="lnta_dark",
        height=300,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Duration (ms)"},
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_input_rows_chart(df: pd.DataFrame) -> None:
    """Render input rows per batch chart."""
    rows_df = df[df["metric"] == "input_rows"].copy()
    if rows_df.empty:
        st.markdown(
            """
            <div class="empty-state" role="status" aria-live="polite">
                <div class="empty-state-icon">📊</div>
                <div>No input rows data</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    rows_df = rows_df.sort_values("ts")

    fig = go.Figure(
        go.Bar(
            x=rows_df["ts"],
            y=rows_df["value"],
            marker_color=LNTA_COLORS["proto_tcp"],
            hovertemplate="%{x}<br>Rows: %{y:,}<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=250,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Input Rows"},
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_e2e_lag_chart(df: pd.DataFrame) -> None:
    """Render end-to-end lag chart."""
    lag_df = df[df["metric"] == "e2e_lag_ms"].copy()
    if lag_df.empty:
        st.markdown(
            """
            <div class="empty-state" role="status" aria-live="polite">
                <div class="empty-state-icon">📊</div>
                <div>No end-to-end lag data</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    lag_df = lag_df.sort_values("ts")

    fig = go.Figure(
        go.Scatter(
            x=lag_df["ts"],
            y=lag_df["value"],
            mode="lines",
            line={"color": LNTA_COLORS["proto_udp"], "width": 2},
            fill="tozeroy",
            fillcolor="rgba(167, 139, 250, 0.1)",
            hovertemplate="%{x}<br>E2E Lag: %{y} ms<extra></extra>",
        )
    )

    fig.update_layout(
        template="lnta_dark",
        height=250,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Time (UTC)"},
        yaxis={"title": "Lag (ms)"},
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_counters(df: pd.DataFrame) -> None:
    """Render bad records and late records counters."""
    bad_df = df[df["metric"] == "bad_records"].copy()
    late_df = df[df["metric"] == "late_records"].copy()

    cols = st.columns(2)

    with cols[0]:
        bad_count = int(bad_df["value"].sum()) if not bad_df.empty else 0
        st.metric(
            "Bad Records", f"{bad_count:,}", delta_color="inverse" if bad_count > 0 else "off"
        )

    with cols[1]:
        late_count = int(late_df["value"].sum()) if not late_df.empty else 0
        st.metric(
            "Late Records", f"{late_count:,}", delta_color="inverse" if late_count > 0 else "off"
        )
