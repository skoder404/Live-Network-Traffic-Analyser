# Demo runbook (dashboard)
| Failure | Fix |
|---|---|
| Blank / no data | `LNTA_MOCK=true streamlit run dashboard/app.py` |
| Radar turns amber | Stream stalled: restart Spark job or switch to mock |
| 3D graph blank | Hard refresh; browser needs canvas. Rankings table still shows the same data |
| No alerts | Normal until warmup (8 windows). Mock triggers a spike and a port scan periodically |
| Slow laptop | Turn off auto-refresh, lower "Nodes shown" |
| Real data missing | Needs table `ip_edges(window_start,src_ip,dst_ip,packets,bytes)`; else falls back to mock |
