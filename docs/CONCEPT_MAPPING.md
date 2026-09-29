# Priyan S: concept → code
| Concept | Code |
|---|---|
| IP communication graph, pruning | `linkanalysis/graph.py`, used in `dashboard/analytics.py` |
| Degree / betweenness centrality | `linkanalysis/centrality.py` → Link analysis table |
| PageRank (from scratch + NetworkX check) | `linkanalysis/pagerank.py` → 3D node size, rankings |
| Markov next-hop + transition matrix | `linkanalysis/markov.py` → Link analysis tab |
| Explainable alerts (spike, ports, fan-out) | `linkanalysis/alerts.py` + `config/alert_rules.yaml` → Alerts tab |
| Streamlit dashboard | `dashboard/` (6 tabs, `st.fragment(run_every=5)`) |
