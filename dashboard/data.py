"""Data layer. LNTA_MOCK=true (default) -> deterministic mock traffic; else reads SQLite serving store (falls back to mock)."""
import os, sqlite3, time
import numpy as np, pandas as pd
MOCK = os.getenv("LNTA_MOCK", "true").lower() == "true"
DB_PATH = os.getenv("LNTA_DB", "serving/lnta.db")
PROTOS = ["TCP", "UDP", "DNS", "HTTPS", "HTTP", "ICMP"]
SCANNER = "192.168.1.44"
PUB = ["142.250.183.14", "151.101.1.69", "104.16.132.229", "13.107.42.14", "52.94.236.248", "93.184.216.34"]
HOSTS = [f"10.0.0.{i}" for i in (1, 2, 5, 8, 12, 15, 21, 23, 30, 34, 42, 55)] + [f"192.168.1.{i}" for i in (10, 11, 20, 33, 44)] + PUB
_R = np.random.default_rng(7); BASE = []
for s in HOSTS:
    for d in _R.choice([h for h in HOSTS if h != s], size=int(_R.integers(2, 5)), replace=False):
        BASE.append((s, str(d), int(_R.integers(40, 900) * (4 if str(d) in HOSTS[:2] + PUB else 1))))
def pps_at(b): return float((320 + 110 * np.sin(b / 6) + np.random.default_rng(b).normal() * 18) * (4.2 if b % 17 >= 14 else 1))
class MockServingDB:
    def snapshot(self, window=30):
        b = int(time.time() // 5); r = np.random.default_rng(b); k = window / 10; f = 0.75 + 0.5 * np.random.default_rng(b // 6).random()
        edges = [{"src_ip": s, "dst_ip": d, "packets": int(w * f * k * (0.9 + 0.2 * r.random())), "bytes": int(w * f * k * (600 + 400 * r.random()))} for s, d, w in BASE]
        scan = b % 23 >= 20
        if scan: edges += [{"src_ip": SCANNER, "dst_ip": h, "packets": 60, "bytes": 40000} for h in HOSTS[:10]]
        return self._pack(b, edges, scan, k)
    def _pack(self, b, edges, scan, k=3.0):
        pkt, dst, prt = {}, {}, {}
        for e in edges:
            s = e["src_ip"]; pkt[s] = pkt.get(s, 0) + e["packets"]; dst[s] = dst.get(s, 0) + 1; prt[s] = prt.get(s, 0) + 2
        if scan: dst[SCANNER], prt[SCANNER] = 45, 60
        pps = pps_at(b); hist = pd.DataFrame({"pps": [pps_at(b - i) for i in range(39, -1, -1)]}, index=pd.to_datetime([(b - i) * 5 for i in range(39, -1, -1)], unit="s"))
        hist["bps"] = hist["pps"] * 700
        mix = np.random.default_rng(b // 6).dirichlet(np.ones(6) * 2) * 100
        return {"bucket": b, "edges": edges, "pps": pps, "bps": pps * 700, "hosts": len(pkt), "flows": len(edges), "history": hist,
                "protocols": pd.DataFrame({"protocol": PROTOS, "share": mix}).set_index("protocol"), "port_counts": prt, "dst_counts": dst, "pkt_counts": pkt, "scan": scan}
    def concepts(self, s):
        r = np.random.default_rng(s["bucket"]); return [("Sampling", "Reservoir sample of packets", 1000), ("Filtering", "Bloom filter on known hosts", int(s["hosts"])),
            ("Count distinct", "Flajolet-Martin estimate of IPs", int(s["hosts"] * (0.95 + 0.1 * r.random()))), ("Counting ones", "DGIM: SYNs in last 1000 packets", int(r.integers(20, 90))),
            ("Moments", "Second moment F2 of ports", int(r.integers(1e4, 9e4))), ("Decaying windows", "Score of busiest port", round(float(r.random() * 9), 2)), ("Itemsets", "PCY frequent pairs found", int(r.integers(3, 15)))]
    def pipeline(self, s):
        r = np.random.default_rng(s["bucket"]); names = ["Capture (TShark)", "Flume", "HDFS", "Spark Streaming", "Serving (SQLite)"]
        return pd.DataFrame({"stage": names, "latency_ms": r.integers(20, 380, 5), "rows_per_s": r.integers(800, 9000, 5), "status": r.choice(["ok", "ok", "ok", "lag"], 5)})
class SqliteServingDB(MockServingDB):
    def snapshot(self, window=30):
        try:
            con = sqlite3.connect(DB_PATH); rows = con.execute("SELECT src_ip,dst_ip,SUM(packets),SUM(bytes) FROM ip_edges WHERE window_start>=datetime('now',?) GROUP BY 1,2", (f"-{window} seconds",)).fetchall(); con.close()
            if rows: return self._pack(int(time.time() // 5), [dict(src_ip=a, dst_ip=b, packets=c, bytes=d) for a, b, c, d in rows], False)
        except Exception: pass
        return super().snapshot(window)
def get_db(): return MockServingDB() if MOCK else SqliteServingDB()
