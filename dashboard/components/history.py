"""
dashboard/components/history.py — History tab for LNTA dashboard.

Shows historical query results from Hive/Spark SQL over HDFS.
Per DESIGN.md §109-112.
"""
import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import ServingDB, get_hist_results
from dashboard.theme import LNTA_COLORS


def render_history_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the History tab with query selector and results."""
    st.markdown("## 📜 History")

    # Fetch available queries with loading state
    with st.spinner("Loading historical queries..."):
        try:
            hist_df = get_hist_results(db, limit=100)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load historical data: {e}")
            return

    # Empty state
    if hist_df.empty:
        st.markdown(
            """
            <div class="empty-state" role="status" aria-live="polite">
                <div class="empty-state-icon">📜</div>
                <div>No historical results yet.</div>
                <div style="font-size:12px; color:var(--text-muted); margin-top:8px;">
                    Run <code>run_historical.py</code> to generate historical query results.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # Parse JSON columns
    hist_df = hist_df.copy()
    hist_df["columns"] = hist_df["columns_json"].apply(lambda x: eval(x) if isinstance(x, str) else x)
    hist_df["rows"] = hist_df["rows_json"].apply(lambda x: eval(x) if isinstance(x, str) else x)

    # Check for stale data
    latest_run = hist_df["run_at"].max()
    latest_ts = pd.Timestamp(latest_run)
    now_utc = pd.Timestamp.now(tz="UTC")
    if latest_ts.tz is None:
        latest_ts = latest_ts.tz_localize("UTC")
    if latest_ts < now_utc - pd.Timedelta(hours=24):
        st.warning("⚠️ Historical data may be stale (last run > 24h ago)")

    # Parse JSON columns
    hist_df["columns"] = hist_df["columns_json"].apply(lambda x: eval(x) if isinstance(x, str) else x)
    hist_df["rows"] = hist_df["rows_json"].apply(lambda x: eval(x) if isinstance(x, str) else x)

    # Query selector with accessible label
    query_names = hist_df["query_name"].unique().tolist()
    selected_query = st.selectbox(
        "Select Historical Query",
        query_names,
        key="history_query_select",
        help="Choose a historical query to view its results",
    )

    # Filter to selected query
    query_data = hist_df[hist_df["query_name"] == selected_query].iloc[0]

    # Show last run timestamp
    st.caption(f"Last run: {query_data['run_at']}")

    # Build DataFrame from results
    columns = query_data["columns"]
    rows = query_data["rows"]
    result_df = pd.DataFrame(rows, columns=columns)

    # Display results table
    st.markdown("### Results")
    st.dataframe(result_df, use_container_width=True, hide_index=True)

    # Display chart based on data type
    st.markdown("### Chart")
    render_history_chart(result_df, columns)


def render_history_chart(df: pd.DataFrame, columns: list) -> None:
    """Render appropriate chart based on column types."""
    if df.empty or len(columns) < 2:
        st.info("Not enough data for chart")
        return

    # Try to identify categorical and numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = [c for c in columns if c not in numeric_cols]

    if not numeric_cols:
        st.info("No numeric columns for chart")
        return

    # Default: first categorical as x, first numeric as y
    x_col = categorical_cols[0] if categorical_cols else columns[0]
    y_col = numeric_cols[0]

    # Determine chart type
    if len(categorical_cols) > 0 and df[x_col].nunique() <= 20:
        # Bar chart for categorical
        fig = go.Figure(go.Bar(
            x=df[x_col],
            y=df[y_col],
            marker_color=LNTA_COLORS["accent"],
            hovertemplate=f"{x_col}: %{{x}}<br>{y_col}: %{{y:,}}<extra></extra>",
        ))
        fig.update_layout(
            template="lnta_dark",
            height=300,
            margin={"l": 60, "r": 20, "t": 20, "b": 40},
            xaxis={"title": x_col, "tickangle": 45},
            yaxis={"title": y_col},
        )
    else:
        # Line chart for time series or continuous
        fig = go.Figure(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode="lines+markers",
            line={"color": LNTA_COLORS["accent"], "width": 2},
            hovertemplate=f"{x_col}: %{{x}}<br>{y_col}: %{{y:,}}<extra></extra>",
        ))
        fig.update_layout(
            template="lnta_dark",
            height=300,
            margin={"l": 60, "r": 20, "t": 20, "b": 40},
            xaxis={"title": x_col},
            yaxis={"title": y_col},
            hovermode="x unified",
        )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
