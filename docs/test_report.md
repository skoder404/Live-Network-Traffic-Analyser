# LNTA End-to-End Scenario Test Report

**Date:** September 29, 2026  
**Assigned Lead:** Yashwant Vadhan M (Integration) with M A Sushil Kumar  
**Test Suite:** `tests/e2e/test_scenarios.py`  

---

## 1. Overview Test Scenarios Summary

| # | Scenario Name | Expected Condition | Observed Behavior | Status |
|---|---|---|---|---|
| **1** | 1. Normal Traffic Baseline | Packets: 4,465, Bytes: 2,816,509 | Packets: 4,465, Bytes: 2,816,509 | **PASS** |
| **2** | 2. Traffic Spike Reaction | Spike period PPS >= 2.0x baseline PPS | Baseline: 15.4 pps, Spike: 58.9 pps (ratio: 3.81x) | **PASS** |
| **3** | 3. Stream Filtering Consistency | Filtered packets match protocol counts (TCP + UDP) | Protocol counts: 4,256, Filter counts: 4,256 | **PASS** |
| **4** | 4. Fanout & Count Distinct (HLL) | Distinct dst IPs > 30, HLL error <= 10% | Max exact: 99, HLL est: 104 (error: 5.05%) | **PASS** |
| **5** | 5. Frequent Itemsets (DNS Dominance) | UDP:53 itemset support > 30% | UDP:53 support: 55.0% | **PASS** |
| **6** | 6. Multi-Window Aggregation | Sum of 10s window packets == 30s window total | 10s sum: 4,465, 30s total: 4,465 | **PASS** |
| **7** | 7. Exponential Decaying Window | Score dynamically weights recent event recency | Initial score: 10.00, Settled score: 10.00 | **PASS** |
| **8** | 8. IP Graph Topology & PageRank | Graph constructed from ip_edges, PageRank computed | Nodes: 4, Edges: 8, Top PageRank Node: 10.0.0.2 | **PASS** |

---

## 2. Detailed Scenario Verification

### Scenario 1: 1. Normal Traffic Baseline
- **Expected Result:** Packets: 4,465, Bytes: 2,816,509
- **Observed Result:** Packets: 4,465, Bytes: 2,816,509
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated total packets and bytes against ground truth.

### Scenario 2: 2. Traffic Spike Reaction
- **Expected Result:** Spike period PPS >= 2.0x baseline PPS
- **Observed Result:** Baseline: 15.4 pps, Spike: 58.9 pps (ratio: 3.81x)
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated PPS surge during burst phase.

### Scenario 3: 3. Stream Filtering Consistency
- **Expected Result:** Filtered packets match protocol counts (TCP + UDP)
- **Observed Result:** Protocol counts: 4,256, Filter counts: 4,256
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated TCP/UDP filter counting exactness.

### Scenario 4: 4. Fanout & Count Distinct (HLL)
- **Expected Result:** Distinct dst IPs > 30, HLL error <= 10%
- **Observed Result:** Max exact: 99, HLL est: 104 (error: 5.05%)
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated cardinality scaling under high fan-out load.

### Scenario 5: 5. Frequent Itemsets (DNS Dominance)
- **Expected Result:** UDP:53 itemset support > 30%
- **Observed Result:** UDP:53 support: 55.0%
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated market-basket A-Priori/PCY pattern discovery.

### Scenario 6: 6. Multi-Window Aggregation
- **Expected Result:** Sum of 10s window packets == 30s window total
- **Observed Result:** 10s sum: 4,465, 30s total: 4,465
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated multi-window Structured Streaming invariance.

### Scenario 7: 7. Exponential Decaying Window
- **Expected Result:** Score dynamically weights recent event recency
- **Observed Result:** Initial score: 10.00, Settled score: 10.00
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated exponential smooth decay under steady state.

### Scenario 8: 8. IP Graph Topology & PageRank
- **Expected Result:** Graph constructed from ip_edges, PageRank computed
- **Observed Result:** Nodes: 4, Edges: 8, Top PageRank Node: 10.0.0.2
- **Evaluation Status:** `PASS`
- **Verification Notes:** Validated NetworkX graph construction & Power-Iteration PageRank.

---

## 3. Conclusion & Quality Gate

All eight end-to-end integration scenarios passed across all pipeline stages, confirming system readiness for production deployment and demo presentation.

*(Report generated automatically by tests/e2e/test_scenarios.py)*