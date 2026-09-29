import streamlit as st

from ._ui import state


def render_history(s, window, ctx):
    q = st.selectbox(
        "Question", ["Traffic over the last 3 minutes", "Alert log", "Top talkers now"]
    )
    if q.startswith("Alert"):
        df = __import__("pandas").DataFrame(st.session_state.get("alert_log", []))
    elif q.startswith("Top"):
        df = (
            __import__("pandas")
            .DataFrame(s["edges"])
            .groupby("src_ip", as_index=False)[["packets", "bytes"]]
            .sum()
            .sort_values("packets", ascending=False)
            .head(10)
        )
    else:
        df = s["history"].reset_index(names="time")
    if df.empty:
        return state("empty", "Nothing stored for this question yet.")
    st.dataframe(
        df.drop(columns=["details_json"], errors="ignore"), hide_index=True, width="stretch"
    )
    st.download_button("Download CSV", df.to_csv(index=False), "lnta_history.csv", "text/csv")
