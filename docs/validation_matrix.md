# LNTA Cross-Validation Matrix (T5-010)

This document records the cross-layer validation matrix asserting mathematical and relational consistency **between** the streaming algorithms implemented by **M A Sushil Kumar** (Group A: Window Metrics, Counts, Filters, Sampling, Distinct, Counting Ones) and **Yashwant Vadhan M** (Group B: Moments, Edges, Source Stats, Decay, Itemsets).

All checks are automated end-to-end via [`tests/e2e/test_cross_validation.py`](../tests/e2e/test_cross_validation.py) across multiple synthetic and benchmark traffic scenarios (`normal.csv`, `fanout.csv`, `multi_host_graph.csv`).

---

## 1. Cross-Validation Invariants Matrix

| ID | Concept | Invariant Check | Group A Component (Owner) | Group B Component (Owner) | Datasets Tested | Result |
|:---|:---|:---|:---|:---|:---|:---|
| **CV-01** | Packet Count Consistency | `moments.n == window_metrics.packets` for all identical tumbling windows | `window_metrics` (M A Sushil Kumar) | `moments_window` (Yashwant Vadhan M) | `normal`, `fanout`, `multi_host_graph` | **PASS** |
| **CV-02** | Edge Partition Conservation | `SUM(ip_edges.packets) == window_metrics.packets` (when window edge count < top-500 edge cap) | `window_metrics` (M A Sushil Kumar) | `ip_edges` (Yashwant Vadhan M) | `normal`, `fanout`, `multi_host_graph` | **PASS** |
| **CV-03** | Local vs Global Cardinality | $\max_{s}(\text{source\_stats.unique\_dst\_ips}) \le \text{distinct\_counts.dst\_ips\_exact}$ per window | `distinct_counts` (M A Sushil Kumar) | `source_stats` (Yashwant Vadhan M) | `normal`, `fanout`, `multi_host_graph` | **PASS** |
| **CV-04** | Service / Itemset Coherence | Frequent itemset `TCP:443` presence corresponds with dominant TCP traffic in `protocol_counts` and port 443 in `port_counts` | `counts` (M A Sushil Kumar) | `itemsets` (Yashwant Vadhan M) | `normal` | **PASS** |
| **CV-05** | AMS vs Exact Moment Bounds | `f2_ams` estimator is within theoretical $(1 \pm \epsilon)$ error of `f2_exact` for traffic windows with $n \ge 50$ | `window_metrics` (M A Sushil Kumar) | `moments` (Yashwant Vadhan M) | `normal`, `fanout` | **PASS** |
| **CV-06** | Stream Filter Sub-additivity | `filter_counts['tcp_only'] + filter_counts['udp_only'] <= window_metrics.packets` | `filter_counts` (M A Sushil Kumar) | `window_metrics` (M A Sushil Kumar / Integration) | `normal`, `fanout` | **PASS** |
| **CV-07** | Sampling vs Exact Rate | Reservoir sample rate matches full stream rate within 10% tolerance | `sampling` (M A Sushil Kumar) | `window_metrics` (M A Sushil Kumar / Integration) | `normal`, `dns_heavy` | **PASS** |

---

## 2. Invariant Proofs and Interpretations

### CV-01: Moments Count vs Window Metrics
- **Mathematical Invariant:**
  $$\sum_{i \in W} 1 = n_{\text{moments}}(W) = \text{packets}_{\text{window\_metrics}}(W)$$
- **Verification Method:**
  Both queries operate on the identical cleaned streaming DataFrame `valid_stream` using tumbling event-time windows of length 10 seconds. In each window, `moments.n` records the number of sampled packet lengths, which must be strictly identical to the packet count recorded by `window_metrics.packets`.

### CV-02: IP Edges Sum vs Window Metrics
- **Mathematical Invariant:**
  $$\sum_{(u, v) \in E(W)} \text{packets}(u, v) = \text{packets}_{\text{window\_metrics}}(W)$$
- **Verification Method:**
  Each packet in the cleaned stream contains a valid source IP and destination IP. The grouping over $(u, v)$ partitions the set of packets in window $W$. Unless the top-N edge truncation threshold (500 edges per window) is reached, the sum of edge packet counts equals the total window packet count.

### CV-03: Local Source Out-Degree vs Global Distinct Destination IPs
- **Mathematical Invariant:**
  $$\max_{s} \big|\{d \mid (s, d) \in E(W)\}\big| \le \big|\bigcup_{s} \{d \mid (s, d) \in E(W)\}\big| = \text{dst\_ips\_exact}(W)$$
- **Verification Method:**
  The number of unique destination IPs contacted by any single source host cannot exceed the total number of unique destination IPs observed across all hosts in the network during that window.

### CV-04: Frequent Service Itemsets vs Protocol / Port Counts
- **Mathematical Invariant:**
  If item `TCP:443` has support $\ge 5\%$ in service baskets, then port 443 must appear among the top ports in `port_counts` and protocol `TCP` must be active in `protocol_counts`.
- **Verification Method:**
  Ensures that the Lane B market-basket mining over 1-second slices yields service patterns consistent with the Lane A streaming aggregation.

---

## 3. Discrepancy Escalation Protocol

If any invariant fails in continuous integration or load testing:
1. Identify the failing check ID (`CV-01` through `CV-07`).
2. Pinpoint the offending window `window_start` and print raw row values from both SQLite tables.
3. Assign immediate triage to the component owners specified in the matrix table above.
