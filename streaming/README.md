# streaming

Owners:
- **M A Sushil Kumar**: `common/`, `queries/window_metrics.py`, `queries/counts.py`, `queries/filter_counts.py`, `queries/distinct.py`, `analytics/{filters,sampling,fm,dgim}.py`
- **Yashwant Vadhan M**: `stream_app.py`, `queries/moments_window.py`, `analytics/{moments,decay,itemsets,edges}.py`

Contains Spark Structured Streaming application, Lane A native event-time windowed aggregations, and Lane B per-micro-batch algorithm plugins.
