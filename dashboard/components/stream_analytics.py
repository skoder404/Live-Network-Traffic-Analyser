"""
dashboard/components/stream_analytics.py — Stream Analytics tab for LNTA dashboard.

Shows 7 concept cards: Filtering, Sampling, Count Distinct, Counting Ones, Moments, Decay, Frequent Itemsets.
Each card shows exact vs approximate comparison per DESIGN.md §81-94.
"""
import sqlite3
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.data import (
    ServingDB,
    get_counting_ones,
    get_decay_top_keys,
    get_decay_traffic,
    get_distinct_counts,
    get_filter_counts,
    get_frequent_itemsets,
    get_moments,
    get_sampling_compare,
)
from dashboard.theme import LNTA_COLORS


def render_stream_analytics_tab(db: ServingDB, controls: dict[str, Any]) -> None:
    """Render the Stream Analytics tab with 7 concept cards."""
    st.markdown("## 🔬 Stream Analytics")

    window_len = controls["window_len_s"]

    # Fetch all data needed for the 7 concepts
    with st.spinner("Loading stream analytics..."):
        try:
            filter_df = get_filter_counts(db, window_len, limit=120)
            sampling_df = get_sampling_compare(db, limit=120)
            distinct_df = get_distinct_counts(db, window_len, limit=120)
            counting_ones_df = get_counting_ones(db, limit=120)
            moments_df = get_moments(db, window_len, limit=120)
            decay_df = get_decay_traffic(db, limit=120)
            decay_keys_df = get_decay_top_keys(db, limit=20)
            itemsets_df = get_frequent_itemsets(db, window_len, algorithm="A-Priori", limit=50)
        except (sqlite3.Error, pd.errors.DatabaseError, OSError) as e:
            st.error(f"Failed to load stream analytics: {e}")
            return

    # --- 7 Concept Cards in 2-column grid ---
    st.markdown("### Streaming Algorithm Concepts")

    # Row 1: Filtering, Sampling
    col1, col2 = st.columns(2)
    with col1:
        render_filtering_card(filter_df)
    with col2:
        render_sampling_card(sampling_df)

    # Row 2: Count Distinct, Counting Ones
    col3, col4 = st.columns(2)
    with col3:
        render_distinct_card(distinct_df)
    with col4:
        render_counting_ones_card(counting_ones_df)

    # Row 3: Moments, Decay
    col5, col6 = st.columns(2)
    with col5:
        render_moments_card(moments_df)
    with col6:
        render_decay_card(decay_df, decay_keys_df)

    # Row 4: Frequent Itemsets (full width)
    st.divider()
    render_itemsets_card(itemsets_df)


def render_concept_card_header(title: str, explanation: str, concept_key: str) -> None:
    """Render standard concept card header with chip and popover."""
    st.markdown(
        f"""
        <div class="concept-chip">CONCEPT · {concept_key}</div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander(f"ℹ️ What is {title}?", expanded=False):
        st.markdown(explanation)


def render_filtering_card(df: pd.DataFrame) -> None:
    """Card 1: Stream Filtering — filtered packet counts."""
    render_concept_card_header(
        "Stream Filtering",
        "Filters partition the stream into sub-streams (e.g., TCP only, port 443, packet size > N). "
        "This shows packet counts for each named filter as a fraction of total.",
        "FILTERING",
    )

    if df.empty:
        st.info("No filter data yet")
        return

    # Aggregate across windows
    agg = df.groupby("filter_name").agg({"packets": "sum", "bytes": "sum"}).reset_index()
    total_pkts = agg["packets"].sum()
    agg["pct"] = (agg["packets"] / total_pkts * 100).round(1)

    fig = go.Figure(go.Bar(
        x=agg["pct"],
        y=agg["filter_name"],
        orientation="h",
        marker_color=LNTA_COLORS["accent"],
        text=agg["pct"].astype(str) + "%",
        textposition="outside",
        hovertemplate="%{y}: %{x}% of total packets<extra></extra>",
    ))
    fig.update_layout(
        template="lnta_dark",
        height=250,
        margin={"l": 120, "r": 20, "t": 20, "b": 20},
        xaxis={"title": "% of Total Packets", "range": [0, max(agg["pct"]) * 1.2]},
        yaxis={"autorange": "reversed"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_sampling_card(df: pd.DataFrame) -> None:
    """Card 2: Sampling — sample mean vs full mean packet size."""
    render_concept_card_header(
        "Sampling",
        "Reservoir sampling maintains a fixed-size random sample. "
        "Shows sample mean packet length vs full stream mean, with error %. "
        "Demonstrates unbiased estimation with bounded memory.",
        "SAMPLING",
    )

    if df.empty:
        st.info("No sampling data yet")
        return

    latest = df.sort_values("ts").iloc[-1]
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Sample Size (k)", int(latest["k"]))
    with c2:
        st.metric("Sample Mean", f"{latest['sample_mean_len']:.0f} B")
    with c3:
        st.metric("Full Mean", f"{latest['full_mean_len']:.0f} B")

    # Error over time
    fig = go.Figure(go.Scatter(
        x=df["ts"],
        y=df["err_pct"],
        mode="lines+markers",
        line={"color": LNTA_COLORS["warn"]},
        fill="tozeroy",
        hovertemplate="Error: %{y:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        template="lnta_dark",
        height=200,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        yaxis={"title": "Error %"},
        xaxis={"title": "Time"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_distinct_card(df: pd.DataFrame) -> None:
    """Card 3: Count Distinct — Exact vs HLL vs Flajolet-Martin."""
    render_concept_card_header(
        "Count Distinct",
        "Estimates unique elements (source IPs, dest IPs, ports) in a stream. "
        "Exact: countDistinct (collect_set). Approximate: HyperLogLog (HLL) and Flajolet-Martin (FM). "
        "Shows % error of each estimator vs exact.",
        "DISTINCT",
    )

    if df.empty:
        st.info("No distinct count data yet")
        return

    latest = df.sort_values("window_start").iloc[-1]

    metrics = [
        ("Source IPs", "src_ips_exact", "src_ips_hll", "src_ips_fm"),
        ("Dest IPs", "dst_ips_exact", "dst_ips_hll", "dst_ips_fm"),
        ("Ports", "ports_exact", "ports_hll", "ports_fm"),
    ]

    for label, exact_col, hll_col, fm_col in metrics:
        exact = latest[exact_col]
        hll = latest[hll_col]
        fm = latest[fm_col]
        hll_err = abs(hll - exact) / exact * 100 if exact else 0
        fm_err = abs(fm - exact) / exact * 100 if exact else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(label, f"{int(exact):,}")
        with c2:
            st.metric("HLL", f"{int(hll):,}", f"{hll_err:.1f}% err", delta_color="inverse")
        with c3:
            st.metric("FM", f"{int(fm):,}", f"{fm_err:.1f}% err", delta_color="inverse")
        with c4:
            st.write("")  # spacer


def render_counting_ones_card(df: pd.DataFrame) -> None:
    """Card 4: Counting Ones (DGIM) — exact vs estimated 1s in bit stream."""
    render_concept_card_header(
        "Counting Ones (DGIM)",
        "Datar-Gionis-Indyk-Motwani algorithm estimates count of 1s in a sliding window over a bit stream. "
        "Here the stream is a boolean predicate (e.g., 'packet > 1000B', 'protocol == TCP'). "
        "Shows exact count vs DGIM estimate with error %.",
        "COUNTING ONES",
    )

    if df.empty:
        st.info("No counting ones data yet")
        return

    latest = df.sort_values("ts").iloc[-1]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Window Size (N)", int(latest["window_n"]))
    with c2:
        st.metric("Exact 1s", int(latest["exact_ones"]))
    with c3:
        st.metric("DGIM Estimate", int(latest["dgim_estimate"]), f"{latest['err_pct']:.1f}% err")

    # Mini bit stream visualization (last 50 bits)
    st.caption("Recent predicate evaluations (1 = true, 0 = false)")
    # Note: Actual bit stream not stored, showing error trend instead
    fig = go.Figure(go.Scatter(
        x=df["ts"],
        y=df["err_pct"],
        mode="lines",
        line={"color": LNTA_COLORS["proto_icmp"]},
        hovertemplate="DGIM Error: %{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        template="lnta_dark",
        height=180,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        yaxis={"title": "Error %"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_moments_card(df: pd.DataFrame) -> None:
    """Card 5: Estimating Moments — mean, variance, std, AMS F2."""
    render_concept_card_header(
        "Estimating Moments",
        "Computes 1st moment (mean), 2nd central moment (variance), std dev of packet lengths and inter-arrival times. "
        "Also shows AMS (Alon-Matias-Szegedy) sketch for 2nd frequency moment (F2) vs exact F2. "
        "Exact F2 = Σ fᵢ² where fᵢ is frequency of each distinct element.",
        "MOMENTS",
    )

    if df.empty:
        st.info("No moments data yet")
        return

    latest = df.sort_values("window_start").iloc[-1]

    # Packet length moments
    st.markdown("**Packet Length**")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Mean", f"{latest['mean_len']:.1f} B")
    with c2:
        st.metric("Variance", f"{latest['var_len']:.1f}")
    with c3:
        st.metric("Std Dev", f"{latest['std_len']:.1f}")

    # Inter-arrival moments
    st.markdown("**Inter-Arrival Time**")
    c4, c5, c6 = st.columns(3)
    with c4:
        st.metric("Mean", f"{latest['iat_mean_ms']:.2f} ms")
    with c5:
        st.metric("Variance", f"{latest['iat_var_ms']:.2f}")
    with c6:
        st.metric("Std Dev", f"{latest['iat_std_ms']:.2f}")

    # F2 comparison
    st.markdown("**2nd Frequency Moment (F₂)**")
    c7, c8, c9 = st.columns(3)
    with c7:
        st.metric("Exact F₂", f"{latest['f2_exact']:,.0f}")
    with c8:
        st.metric("AMS F₂", f"{latest['f2_ams']:,.0f}")
    with c9:
        if latest['f2_exact'] > 0:
            err = abs(latest['f2_ams'] - latest['f2_exact']) / latest['f2_exact'] * 100
            st.metric("Error", f"{err:.1f}%")


def render_decay_card(decay_df: pd.DataFrame, decay_keys_df: pd.DataFrame) -> None:
    """Card 6: Decaying Window — exponential decay score + top keys."""
    render_concept_card_header(
        "Decaying Window",
        "Exponential decay gives more weight to recent events: score_t = λ·score_{t-1} + current_value. "
        "Half-life controls decay rate. Shows live decay score and top decayed keys (IPs/ports).",
        "DECAY",
    )

    if decay_df.empty:
        st.info("No decay data yet")
        return

    latest = decay_df.sort_values("ts").iloc[-1]

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Decay Score", f"{latest['score']:.2f}")
    with c2:
        st.metric("Raw PPS", f"{latest['raw_pps']:.1f}")

    # Score over time
    fig = go.Figure(go.Scatter(
        x=decay_df["ts"],
        y=decay_df["score"],
        mode="lines",
        line={"color": LNTA_COLORS["proto_udp"]},
        fill="tozeroy",
        fillcolor="rgba(167, 139, 250, 0.1)",
        hovertemplate="Score: %{y:.2f}<extra></extra>",
    ))
    fig.update_layout(
        template="lnta_dark",
        height=200,
        margin={"l": 60, "r": 20, "t": 20, "b": 40},
        yaxis={"title": "Decay Score"},
        xaxis={"title": "Time"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Top decayed keys
    if not decay_keys_df.empty:
        st.markdown("**Top Decayed Source IPs**")
        top_keys = decay_keys_df.sort_values("rank").head(10)
        st.dataframe(
            top_keys[["key", "score", "rank"]].rename(columns={"key": "Source IP"}),
            use_container_width=True,
            hide_index=True,
        )


def render_itemsets_card(df: pd.DataFrame) -> None:
    """Card 7: Frequent Itemsets (A-Priori / PCY) — protocol:port patterns."""
    render_concept_card_header(
        "Frequent Itemsets (Market Basket)",
        "Treats each source IP's (protocol:port) set in a 1-second slice as a 'basket'. "
        "A-Priori finds all itemsets above min-support in multiple passes. "
        "PCY (Park-Chen-Yu) uses hash-based filtering to reduce candidate pairs. "
        "Shows top itemsets with support % and number of passes.",
        "ITEMSETS",
    )

    if df.empty:
        st.info("No frequent itemsets data yet")
        return

    # Algorithm selector
    algorithms = df["algorithm"].unique().tolist()
    if len(algorithms) > 1:
        selected_algo = st.selectbox("Algorithm", algorithms, key="itemsets_algo")
        df = df[df["algorithm"] == selected_algo]

    if df.empty:
        st.info(f"No itemsets for {selected_algo}")
        return

    latest_window = df["window_start"].max()
    latest_df = df[df["window_start"] == latest_window].sort_values("support_ratio", ascending=False).head(15)

    # Support bar chart
    fig = go.Figure(go.Bar(
        x=latest_df["support_ratio"] * 100,
        y=latest_df["itemset"],
        orientation="h",
        marker_color=LNTA_COLORS["accent_2"],
        text=(latest_df["support_ratio"] * 100).round(1).astype(str) + "%",
        textposition="outside",
        hovertemplate="%{y}: %{x}% support (count: %{customdata})<extra></extra>",
        customdata=latest_df["support_count"],
    ))
    fig.update_layout(
        template="lnta_dark",
        height=350,
        margin={"l": 200, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Support %", "range": [0, max(latest_df["support_ratio"]) * 120]},
        yaxis={"autorange": "reversed"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Table with details
    st.dataframe(
        latest_df[["itemset", "size", "support_count", "support_ratio", "passes"]].rename(columns={
            "itemset": "Itemset",
            "size": "Size",
            "support_count": "Count",
            "support_ratio": "Support %",
            "passes": "Passes",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Support %": st.column_config.ProgressColumn(
                "Support %", format="%.1f%%", min_value=0, max_value=1
            ),
        },
    )
