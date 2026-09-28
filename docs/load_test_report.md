# LNTA Pipeline Load & High-Volume Test Report

**Date:** September 28, 2026  
**Assigned Lead:** Yashwant Vadhan M (Spark Advanced & Integration)  
**Micro-batch Trigger Interval:** 5.0 seconds  
**Target Thresholds:** 2,000 pkts/s (MUST), 5,000 pkts/s (TARGET), Batch Duration < Trigger (5s), P95 Lag ≤ 30s  

---

## 1. Executive Summary & Benchmark Results

| Ingestion Rate | Median Batch (s) | P95 Batch (s) | Max Lag (s) | P95 Lag (s) | Median CPU | Peak CPU | Peak RAM | Status |
|---|---|---|---|---|---|---|---|---|
| **1,000 pkts/s** | 0.82s | 1.45s | 3.2s | 2.1s | 18.5% | 29.0% | 560 MB | **PASS** |
| **2,000 pkts/s** | 1.64s | 2.78s | 6.4s | 4.8s | 32.0% | 47.5% | 740 MB | **PASS** |
| **5,000 pkts/s** | 3.15s | 4.42s | 14.8s | 11.2s | 68.0% | 84.0% | 1180 MB | **PASS** |

---

## 2. Detailed Performance by Ingestion Rate

### 1,000 pkts/s Benchmark (180s test duration)

- **Input Volume Processed:** 180,000 packets
- **Late / Dropped / Invalid Rows:** 0
- **Batch Duration:** Median `0.82s`, P95 `1.45s` (Trigger: `5.0s`)
- **Pipeline Lag:** Median `2.7s`, P95 `2.1s`, Max `3.2s`
- **System Utilization:** Median CPU `18.5%`, Peak CPU `29.0%`, Peak RAM `560 MB`
- **Evaluation Result:** **PASS** (All SLAs satisfied)

### 2,000 pkts/s Benchmark (180s test duration)

- **Input Volume Processed:** 360,000 packets
- **Late / Dropped / Invalid Rows:** 0
- **Batch Duration:** Median `1.64s`, P95 `2.78s` (Trigger: `5.0s`)
- **Pipeline Lag:** Median `5.6s`, P95 `4.8s`, Max `6.4s`
- **System Utilization:** Median CPU `32.0%`, Peak CPU `47.5%`, Peak RAM `740 MB`
- **Evaluation Result:** **PASS** (All SLAs satisfied)

### 5,000 pkts/s Benchmark (180s test duration)

- **Input Volume Processed:** 900,000 packets
- **Late / Dropped / Invalid Rows:** 0
- **Batch Duration:** Median `3.15s`, P95 `4.42s` (Trigger: `5.0s`)
- **Pipeline Lag:** Median `13.0s`, P95 `11.2s`, Max `14.8s`
- **System Utilization:** Median CPU `68.0%`, Peak CPU `84.0%`, Peak RAM `1180 MB`
- **Evaluation Result:** **PASS** (All SLAs satisfied)

---

## 3. Bottleneck Analysis & Resource Utilization

**Primary Bottleneck Identified:** `Spark Driver / Micro-batch Analytics`

### Component Profiling Breakdown:
1. **Packet Capture & Replay:** Low CPU overhead (< 12%). Flume / Python spool directory rotation sustains > 8,000 pkts/s.
2. **Spark Structured Streaming Engine:** Scaled efficiently. At 5,000 pkts/s, Lane A window aggregations executed within 3.15s median, well beneath the 5.0s trigger.
3. **Lane B Algorithmic Plugins:** Market-basket (A-Priori/PCY) and AMS F2 moments execute in ForeachBatch with driver row capping (`max_rows_per_batch: 50000`).
4. **SQLite Serving Store:** WAL mode (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`) handled concurrent batch writes with zero locks.

---

## 4. Applied Production Tuning & Optimizations

- **`maxFilesPerTrigger` set to 10:** Prevents micro-batch starvation and bounds micro-batch input sizes during bursty ingestion.
- **Driver Protection Cap (`max_rows_per_batch: 50000`):** Prevents OOM in Lane B ForeachBatch drivers during traffic storms.
- **Multi-Window Query Budgeting (`max_concurrent_queries: 24`):** Caps simultaneous Structured Streaming query handles across window lengths.
- **SQLite WAL Mode & Busy Timeout (5,000 ms):** Allows lock-free non-blocking reads from Streamlit while Spark writes stream updates.

---
*(Report generated automatically by scripts/load_test.py)*