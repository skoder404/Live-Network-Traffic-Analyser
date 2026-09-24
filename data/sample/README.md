# Sample Traffic Datasets (`data/sample/`)

> Owned by **Naveena MS**
> Standard benchmark datasets used for offline testing, CI validation, replay mode, Spark streaming cross-checks, and dashboard development without requiring live Wi-Fi capture.

All files are contract-compliant CSVs (UTF-8, no header row, comma-separated) matching the 11-field data contract defined in [`docs/DATA_CONTRACT.md`](../../docs/DATA_CONTRACT.md). Each `.csv` file is paired with an exact `<name>.truth.json` ground truth file computed during generation.

---

## Scenario Catalog

| File | Scenario | Duration | Expected Alerts / Behavior | Description |
|---|---|---|---|---|
| `normal.csv` | Baseline traffic | 300 s | None (stable baseline) | Normal web browsing (`TCP/443`, `TCP/80`), DNS lookups (`UDP/53`), and background ICMP ping from LAN clients `192.168.1.10`, `192.168.1.15`, `192.168.1.20`. Realistic packet size distribution (60–1500 B). |
| `spike.csv` | Volume spike | 300 s | **Traffic Spike Alert** (WARN/CRITICAL) | Normal rate for the first 200 s, followed by a 4× packet rate surge in the final 100 s ($t \ge 200$ s). Evaluates EWMA rate tracking and threshold triggering. |
| `portscan_like.csv` | Port scan probe | 300 s | **Unusual Port Activity Alert** (WARN) | Host `192.168.1.99` rapidly scans sequential TCP ports ($> 25$ distinct destination ports within 10-second windows) on target `198.51.100.20`. |
| `fanout.csv` | High fanout / sweep | 300 s | **High Fan-Out Alert** (WARN) | Host `192.168.1.10` opens HTTPS connections to $> 40$ distinct destination IP addresses within a single 10-second window. Tests distinct counting (Exact, HLL, FM). |
| `dns_heavy.csv` | High DNS traffic | 300 s | Protocol skew (UDP $> 85\%$) | High volume of DNS query and response packets on `UDP/53`. Evaluates protocol aggregation and filter counting. |
| `multi_host_graph.csv` | Toy IP graph | 300 s | Graph centrality & PageRank ground truth | Communication across four nodes: $A (10.0.0.1)$, $B (10.0.0.2)$, $C (10.0.0.3)$, $D (10.0.0.4)$ with directed edges $A \to B, A \to C, C \to B, D \to B$ plus responses. |

---

## Ground Truth Sidecars (`.truth.json`)

Each dataset includes:
- `packets`: Total frame count
- `bytes`: Total byte sum
- `unique_src_ips`, `unique_dst_ips`, `unique_ports`: Exact cardinalities
- `protocol_counts`: Per-protocol totals (`TCP`, `UDP`, `ICMP`, `OTHER`)
- `packet_length_mean`, `packet_length_variance_sample`, `packet_length_variance_population`
- `per_10s_window_packets`: Exact packet counts bucketed by 10-second tumbling windows

---

## Re-generating Datasets

To regenerate all sample datasets with fixed seeds:
```bash
python scripts/make_samples.py
```
Or generate an individual scenario:
```bash
python -m capture.generate --scenario spike --duration 300 --rate 20 --seed 42 --out data/sample/spike.csv
```
