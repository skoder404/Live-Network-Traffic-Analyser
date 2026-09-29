from dashboard.analytics import analyze, markov_top
from dashboard.components._ui import human, spark
from dashboard.components.graph3d import graph_html
from dashboard.data import MockServingDB
def test_snapshot_keys():
    s = MockServingDB().snapshot(30)
    assert {"pps", "edges", "history", "port_counts", "bucket"} <= set(s) and len(s["history"]) == 40
def test_analyze_pagerank_agrees():
    r = analyze(MockServingDB().snapshot(30)["edges"], 20)
    assert abs(sum(r["pr"].values()) - 1) < 1e-3 and r["agree"] < 1e-3
def test_markov_rows_sum_to_at_most_one():
    G = analyze(MockServingDB().snapshot(30)["edges"], 20)["G"]; nodes, M = markov_top(G, "packets", 8)
    assert len(nodes) == 8 and all(sum(r) <= 1.0001 for r in M)
def test_graph_html_embeds_data():
    r = analyze(MockServingDB().snapshot(30)["edges"], 15); h = graph_html(r["G"], r["pr"], r["deg"])
    assert '"nodes"' in h and "__DATA__" not in h
def test_ui_helpers():
    assert human(1500) == "1.5K" and "<svg" in spark([1, 3, 2]) and spark([1]) == ""
