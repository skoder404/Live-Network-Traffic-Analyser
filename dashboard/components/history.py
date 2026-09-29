import pandas as pd
import streamlit as st

from ._ui import state


def render_history(s, window=10, ctx=None):
    q = st.selectbox("Question", ["Traffic over the last 3 minutes", "Alert log", "Top talkers now"])
    edges = s.get("edges") if isinstance(s, dict) else getattr(s, "edges", [])
    hist = s.get("history") if isinstance(s, dict) else getattr(s, "history", None)

    if q.startswith("Alert"):
        df = pd.DataFrame(st.session_state.get("alert_log", []))
    elif q.startswith("Top"):
        if edges:
            df = pd.DataFrame(edges).groupby("src_ip", as_index=False)[["packets", "bytes"]].sum().sort_values("packets", ascending=False).head(10)
        else:
            df = pd.DataFrame()
    else:
        if hist is not None and hasattr(hist, "reset_index"):
            df = hist.reset_index(names="time")
        else:
            df = pd.DataFrame()

    if df.empty:
        return state("empty", "Nothing stored for this question yet.")
    st.dataframe(df.drop(columns=["details_json"], errors="ignore"), hide_index=True, width="stretch")
    st.download_button("Download CSV", df.to_csv(index=False), "lnta_history.csv", "text/csv")


render_historical_tab = render_history
