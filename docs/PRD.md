# PRD — Live Network Traffic Analyser (LNTA)

> Version 1.0 · Course project: Big Data Analytics (streaming analytics & link analysis focus) · Team of 5

---

## Executive Summary

- **Project Name:** Live Network Traffic Analyser using Streaming Data Analytics (LNTA)
- **Problem Statement:** Network traffic is continuous, high-volume and constantly changing. Inspecting packets one by one in Wireshark, or analysing a static CSV, gives no continuous, scalable insight into *how much* traffic there is, *who* is talking, *which* patterns recur, or *which* nodes matter structurally.
- **Solution Overview:** A pipeline that captures live Wi-Fi traffic with TShark, ingests it with Apache Flume, stores it in HDFS, processes it in real time with Spark Structured Streaming, applies the core streaming algorithms, analyses IP-to-IP communication as a graph (PageRank, Markov), supports historical queries via Hive/Spark SQL, and shows everything on a live Streamlit dashboard with explainable alerts.
- **Target Audience:** (1) the course evaluators/faculty who assess Core-concept coverage and the Big Data stack; (2) the five team members who build and demo it; (3) students/network-curious users who want to understand their own traffic.

---

## Goals & Objectives

### Business (Academic) Goals
1. Demonstrate **every core streaming & link-analysis concept** from the syllabus in a working, explainable system.
2. Demonstrate the Big Data stack from the syllabus: **Flume → HDFS → Spark → Hive**.
3. Deliver a **live, end-to-end demo** that is reliable and can fall back to replay mode.
4. Produce a clean, documented, reproducible repository.

### User Goals
- See current traffic rate, protocol mix, active IPs/ports at a glance.
- Understand *why* something was flagged (explainable alerts).
- See recurring patterns and structurally important IPs.
- Compare recent vs older traffic, and live vs historical.

### Success Metrics (KPIs)

| KPI | Target | How measured |
|---|---|---|
| core concept coverage | All syllabus concepts visible in dashboard *and* mapped in `CONCEPT_MAPPING.md` | Demo checklist |
| Capture parse success | ≥ 99% of IP frames become valid records | `capture` stats vs Wireshark count on same interval |
| Ingestion loss (normal load) | 0 lost records; ≤ 0.1% under load test | `verify_ingestion.py` (lines captured vs lines in HDFS) |
| End-to-end latency (packet → dashboard) | p95 ≤ 30 s (budget in TECH_RULES §2.3) | Timestamp delta test |
| Sustained throughput | Must: 2,000 pkts/s · Target: 5,000 pkts/s on demo hardware | `load_test.py` (targets, not yet measured) |
| Distinct-count accuracy | HLL relative error ≤ 5%; FM demo error reported | Exact vs approx columns |
| DGIM accuracy | Error ≤ 50% guaranteed by algorithm; typical ≤ 10% reported | Exact vs DGIM |
| Dashboard refresh | ≤ 3 s render after new data | Manual + AppTest |
| Test coverage (core logic) | ≥ 70% line coverage for parser, analytics, graph, alerts | `pytest --cov` |
| Demo dry-runs | ≥ 3 full rehearsals passing the 16-step flow | Rehearsal log |

---

## User Personas

**P1 — Evaluator / Faculty ("Dr. Rao")**
- *Pain points:* Hard to verify that concepts are actually implemented and not just named; long demos with failures.
- *Goals:* See each core concept working, with exact-vs-approximate comparison and clear labels.

**P2 — Team Presenter ("Presenter")**
- *Pain points:* Wi-Fi/hardware failures during demo; five components owned by five people.
- *Goals:* One-command start, replay fallback, a dashboard that tells the story top to bottom.

**P3 — Network-curious Student ("Analyst")**
- *Pain points:* Wireshark is overwhelming; no sense of trends or structure.
- *Goals:* Plain-language live view of own traffic; know what each number means.

---

## User Stories

**Capture & Preprocessing**
- As a presenter, I want capture to auto-detect and use the Wi-Fi interface so that setup takes seconds.
- As a pipeline engineer, I want clean fixed-schema CSV records so that downstream jobs never crash on malformed rows.
- As a developer without live capture, I want synthetic and replayed data so that I can build without Wi-Fi.

**Ingestion & Storage**
- As a pipeline engineer, I want Flume to deliver records into HDFS continuously so that nothing is lost when Spark restarts.
- As an analyst, I want raw and historical data organised by date/hour so that historical queries are fast.

**Streaming & Stream Analytics**
- As an analyst, I want packets/sec, bytes/sec, protocol and port counts per time window so that I see current load.
- As an evaluator, I want sampling, filtering, distinct counting and counting-ones shown with exact-vs-approximate results so that I can verify correctness.
- As an analyst, I want mean/variance/std and inter-arrival stats so that I understand traffic behaviour.
- As an analyst, I want a decaying score so that recent traffic matters more than old traffic.
- As an analyst, I want frequent {protocol, port} patterns so that I see what recurs.

**Link Analysis**
- As an analyst, I want an IP communication graph with PageRank and Markov transitions so that I see structurally important hosts and likely next hops.

**Dashboard & Alerts**
- As a presenter, I want one dashboard covering all analytics with auto-refresh so that the demo flows without switching tools.
- As an analyst, I want alerts that show *current value, baseline, change and reason* so that I trust them.

**Historical**
- As an analyst, I want Hive/Spark SQL queries (top IPs, protocol totals, hourly trend) so that I can compare with live data.

---

## Functional Requirements

### FR-CAP — Traffic Capture & Preprocessing (Naveena MS)
- **Description:** Capture packets from a selected Wi-Fi interface with TShark; extract fields; normalise; emit CSV/JSON records.
- **Inputs:** Interface name (or auto-detect); config (rotation, output mode).
- **Outputs:** Headerless CSV rows in `data/capture/traffic_*.csv` following the Data Contract (TECH_RULES §5.1); optional JSON-lines; reject log; capture stats.
- **User Actions:** `python -m capture.run_capture --iface <name>`; list interfaces; stop with Ctrl-C.
- **Validation Rules:** Valid IPv4/IPv6; port 0–65535 or empty; length 1–65535; protocol ∈ {TCP, UDP, ICMP, OTHER}; timestamp in UTC.
- **Edge Cases:** ARP/non-IP frames (dropped, counted); ICMP with no ports (ports empty); IPv6; multi-occurrence fields (tunnelled/encapsulated → take first); missing MAC; permission denied; interface down; TShark crash (auto-restart with back-off); disk full (stop cleanly).

### FR-ING — Ingestion & Storage (Rithika GV)
- **Description:** Flume agent (Source → Channel → Sink) delivers records to HDFS: a durable raw/historical copy and a short-roll copy for Spark.
- **Inputs:** Capture CSV files (TAILDIR) or TCP lines (netcat mode).
- **Outputs:** `/traffic/raw/dt=YYYY-MM-DD/hr=HH/…`, `/traffic/stream_in/…`, `/traffic/processed/…`, `/traffic/historical/…`.
- **Validation Rules:** Ingestion verifier reports captured vs stored line counts, duplicates, and latency.
- **Edge Cases:** Flume restart (no duplicates via position file); HDFS NameNode down (channel buffers, sink retries); small-files explosion (compaction job); in-progress files must be invisible to Spark.

### FR-STR — Spark Streaming Core (M A Sushil Kumar)
- **Description:** Read `stream_in` as a stream, apply schema, clean, compute windowed metrics and Group A streaming analytics.
- **Outputs (serving store):** `window_metrics`, `protocol_counts`, `port_counts`, `filter_counts`, `distinct_counts`, `sampling_compare`, `counting_ones`, `pipeline_health`.
- **Validation Rules:** Malformed rows routed to a bad-record sink, never crash the query; late data beyond watermark dropped and counted.
- **Edge Cases:** Empty batches; clock skew; duplicate files; restart from checkpoint; schema drift.

### FR-ADV — Advanced Streaming (Yashwant Vadhan M)
- **Description:** Moments, decaying windows, market-basket frequent itemsets (A-Priori, PCY), edge & per-source aggregation; multi-window comparisons; load testing; integration.
- **Outputs:** `moments`, `decay_traffic`, `decay_top_keys`, `frequent_itemsets`, `ip_edges`, `source_stats`.
- **Edge Cases:** Single-packet windows (variance undefined → NULL); decay state persisted across restarts; itemset explosion (cap size ≤ 3, cap baskets); baskets = set of `PROTO:port` services one source IP used within a 1-second slice.

### FR-LNK — Link Analysis (Priyan S)
- **Description:** Build directed weighted IP graph from `ip_edges`; degree/centrality; PageRank; Markov transition matrix.
- **Outputs:** DataFrames consumed by the dashboard; graph figure.
- **Validation Rules:** Tiny known graphs (A→B, A→C, C→B, D→B) reproduce hand-computed PageRank; Markov rows sum to 1.
- **Edge Cases:** Dangling nodes; self-loops; empty graph; >500 nodes (prune to top-N by bytes).

### FR-ALR — Explainable Alerts (Priyan S)
- **Description:** Rule-based behavioural alerts: traffic spike vs EWMA baseline, high destination-port activity from one source, high fan-out.
- **Outputs:** `alerts` rows containing type, severity, source IP, current, baseline, change %, threshold, human-readable reason.
- **Edge Cases:** Cooldown to avoid alert storms; minimum-volume floor; cold start (no baseline yet → no alert).

### FR-DSH — Dashboard (Priyan S)
- **Description:** Streamlit multi-section dashboard (see DESIGN.md).
- **Validation Rules:** Every panel has loading, empty and error states; data older than 15 s shows a "stale" badge.
- **Edge Cases:** Serving DB locked/missing; pipeline not running; extremely high cardinality of IPs.

### FR-HIS — Historical Analytics (Rithika GV, with Spark/Hive)
- **Description:** Hive external tables over HDFS; query pack; results exported to `hist_results` for the dashboard.
- **Outputs:** Protocol totals, top source/destination IPs, traffic by hour, average packet size, trend.

### FR-INT — Integration & Test Harness (Yashwant Vadhan M, all)
- **Description:** Start/stop scripts, E2E tests for the eight overview tests, demo script, replay fallback.

---

## Non-Functional Requirements

- **Performance:** End-to-end p95 ≤ 30 s; sustained ≥ 2,000 pkts/s (target 5,000); micro-batch processing time < trigger interval.
- **Scalability:** Single-node pseudo-distributed for the course; code written so Spark/HDFS can scale out (no local-only assumptions in Spark code; partitioned HDFS layout).
- **Security & Privacy:** Only permitted capture; no real captures in Git; anonymisation tool; no secrets in repo; dashboard binds to localhost by default.
- **Accessibility:** Dashboard meets WCAG 2.1 AA contrast for text; colour is never the only signal (icons/labels on alerts and protocols).
- **Reliability:** Every stage restartable without data loss (position file, checkpoints); demo mode works offline.
- **Maintainability:** Typed Python, linted, documented modules, ≥ 70% coverage on core logic, single shared Data Contract.

---

## Assumptions & Constraints

1. Hardware: **two 16 GB laptops**. Laptop A (Ubuntu) runs HDFS + Flume + Spark + Hive + SQLite + dashboard; Laptop B runs live capture and streams records to A over TCP. The other members develop with replay/mock data (TECH_RULES §11.1). Both laptops (16 GB) dual-boot Windows + Ubuntu and run native Ubuntu; Rithika GV's laptop has 8 GB (dual-boot) and runs only HDFS + Flume + replay locally, never Spark. The team is distributed (two homes, one hostel), so machines are reached over a private VPN overlay + SSH (todo T1-007).
2. Wi-Fi capture in normal (managed) mode sees this device's traffic plus broadcast/multicast; **monitor mode / hotspot mode are optional extras**, not MVP.
3. Traffic volume from one device is modest (tens–thousands pkts/s).
4. **Hard deadline: submission + live demo on 1 Oct 2026** (work starts 21 Sep). Scope is cut accordingly — see ROADMAP.
5. Pig, HBase, Oozie are out of scope; MapReduce is optional and outside the live path.
6. Serving store is SQLite on the same host as Spark's driver and the dashboard.
7. Not an IDS; no claims of attack detection.

---

## MVP Scope

### Must Have
- TShark capture → CSV with the 7 core fields (+ MAC where available) and replay/synthetic data
- Flume (Source → Channel → Sink) → HDFS raw + stream_in
- Spark Structured Streaming: schema, cleaning, time windows, packets/sec, bytes/sec, protocol counts, port counts
- Streaming algorithms: filtering, sampling, count distinct, counting ones, moments (mean/var/std), decaying window, market-basket frequent itemsets (limited-pass A-Priori and PCY)
- Link analysis: IP graph, degree, PageRank, Markov transitions
- Streamlit dashboard showing all of the above + basic alerts
- Reliable replay-mode demo

### Should Have
- Hive external tables + historical query pack shown in dashboard
- Inter-arrival statistics; AMS second-moment and Flajolet–Martin estimators for algorithmic comparison
- Explainable alerts (spike, port activity, fan-out) with cooldown
- Multi-window comparison (10 s / 30 s / 60 s)
- Pipeline health page; load test report; compaction job

### Nice To Have
- Hotspot/monitor-mode capture; TCP-flag analytics; MAC vendor lookup
- Markov stationary distribution & n-step transitions
- MapReduce batch comparison; Docker Compose for Hadoop/Spark
- Exportable PDF/HTML session report

## Future Enhancements
- Kafka in place of file-based hand-off; Spark on a real cluster; GraphFrames/GraphX for distributed PageRank; anomaly models (isolation forest); alert notifications; GeoIP enrichment; multi-device capture.
