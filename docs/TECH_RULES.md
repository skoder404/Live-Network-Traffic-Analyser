# TECH_RULES — Live Network Traffic Analyser (LNTA)

> Binding technical rules for all five members and for any AI coding agent working in this repo.
> Items marked **⚠️ Verify** are version/behaviour details that must be confirmed on your machine before relying on them.

---

## 1. Architecture Overview

### 1.1 Layers
- **Capture layer (Naveena MS):** TShark subprocess → parser/normaliser → rotating CSV files (or TCP lines).
- **Ingestion layer (Rithika GV):** Flume agent: **Source → Channel → Sink**, with a *replicating* selector feeding two HDFS sinks.
- **Storage layer (Rithika GV):** HDFS zones `raw`, `stream_in`, `processed`, `historical`, `checkpoints`; Hive external tables.
- **Processing layer (M A Sushil Kumar, Yashwant Vadhan M):** PySpark app with two lanes — **Lane A** native windowed streaming aggregations; **Lane B** per-micro-batch algorithm plugins (`foreachBatch`).
- **Analytics/Link layer (Priyan S):** NetworkX graph, PageRank, Markov, alert engine reading the serving store.
- **Serving layer (Yashwant Vadhan M owns schema, Priyan S consumes):** SQLite (WAL) — the *only* interface between Spark and the dashboard.
- **Presentation layer (Priyan S):** Streamlit + Plotly.

### 1.2 Architecture Diagram

```text
 Wi-Fi iface ─▶ [tshark -l -T fields] ─▶ capture/parser ─▶ data/capture/traffic_*.csv
                                                                 │  (TAILDIR)
                                                        ┌────────▼─────────┐
                                                        │  Flume agent     │
                                                        │  source: TAILDIR │
                                                        │  selector: replicating
                                                        └───┬──────────┬───┘
                                            file channel ◀──┘          └──▶ memory channel
                                                 │                              │
                                       HDFS sink (roll 300 s)         HDFS sink (roll 5 s)
                                                 ▼                              ▼
                                  /traffic/raw/dt=…/hr=…           /traffic/stream_in/
                                        │        │                              │ (file source)
                                Hive ext. table  compaction                     ▼
                                        │        ▼            ┌──────────────────────────────┐
                                        │  /traffic/historical│  Spark Structured Streaming   │
                                        │                     │  Lane A: windows, counts,     │
                                        ▼                     │          approx distinct,     │
                                Spark SQL / Hive              │          moments (native)     │
                                        │                     │  Lane B: sampling, DGIM, FM,  │
                                        │                     │          decay, itemsets,     │
                                        │                     │          edges (foreachBatch) │
                                        │                     └──────────────┬───────────────┘
                                        ▼                                    ▼
                                 hist_results  ───────────────▶  serving/analytics.db (SQLite, WAL)
                                                                             │
                                          linkanalysis (NetworkX) + alerts ──┤
                                                                             ▼
                                                                  Streamlit dashboard
```

### 1.3 Key Design Decisions (with reasons)

| ID | Decision | Reason | Fallback |
|---|---|---|---|
| D-01 | Flume **replicates** to two HDFS sinks: `raw` (long roll, partitioned, durable) and `stream_in` (short roll, flat, low latency) | Historical layout and streaming-friendly layout have conflicting needs; replicating selector also shows Flume fan-out from the architecture diagram | One sink + Spark reads partitioned `raw` |
| D-02 | Spark reads `stream_in` with the **file source** | Spark's old DStream Flume receiver is deprecated/removed in recent versions (⚠️ Verify for your Spark version); files are simple and restart-safe | Flume → Kafka sink (Future) |
| D-03 | Flume source = **TAILDIR** (position file → restart-safe) | Better delivery semantics than `exec`; matches "continuous records" | `netcat` source when capture runs on another machine/OS (WSL2 cannot capture host Wi-Fi) |
| D-04 | Lane A native windows + Lane B `foreachBatch` plugins | Exact distinct, ordered inter-arrival, DGIM, decay, A-Priori/PCY are awkward/unsupported as pure streaming ops; `foreachBatch` allows batch APIs and custom Python state | Move algorithm to Lane A when feasible |
| D-05 | **SQLite (WAL)** serving store | Zero-ops, transactional upserts, trivially mockable for dashboard dev | Parquet snapshot folder |
| D-06 | Timestamps in **UTC** everywhere; dashboard converts to `Asia/Kolkata` via config | Avoid DST/tz bugs; Spark session tz pinned to UTC | — |
| D-07 | CSV **without header** | TAILDIR/Flume treat each line as an event; header would be ingested as data | — |
| D-08 | Exact distinct via `size(collect_set())`, approx via `approx_count_distinct`, plus Python **Flajolet–Martin** | Shows exact-vs-approx trade-off for streaming analytics | — |
| D-09 | Only non-IP frames (ARP etc.) are dropped in MVP | Graph and windows are IP-centric | Keep ARP as `OTHER` later |

### 1.4 Latency Budget (target, not measured)

| Stage | Budget |
|---|---|
| TShark → CSV flush | ≤ 1 s |
| TAILDIR poll + Flume batch | ≤ 2 s |
| HDFS `stream_in` roll | 5 s |
| Spark trigger (processingTime) | 5 s |
| Processing + SQLite upsert | ≤ 3 s |
| Dashboard refresh | 2 s |
| **Typical total** | **≈ 15–20 s; p95 target ≤ 30 s** |

Watermark = **30 s** (must exceed roll + trigger latency, otherwise windows close before data arrives).

---

## 2. Tech Stack

| Layer | Technology | Purpose | Why Chosen | Alternatives Considered |
|---|---|---|---|---|
| Capture | TShark (Wireshark CLI) 4.x | Live capture + field extraction | CLI, scriptable, `-T fields`, `-l` line-buffered | tcpdump (fewer dissectors), Scapy (slower) |
| Validation | Wireshark GUI | Visual validation, I/O graphs | Ground truth for capture correctness | — |
| Capture glue | Python 3.10/3.11 | Parser, writer, generator, replay | Same language as rest of stack | Bash/awk (fragile) |
| Ingestion | Apache Flume 1.11.x | Ingestion (Source→Channel→Sink) | In syllabus; native HDFS sink | Kafka (not in syllabus), Logstash |
| Storage | Hadoop HDFS 3.3.x (pseudo-dist.) | Distributed raw/historical storage | In syllabus; Spark-native | Local FS, S3/MinIO |
| Processing | Spark 3.5.x + PySpark, Structured Streaming | Stream engine | Unified batch+stream; event-time windows | Flink, Storm |
| SQL/History | Spark SQL + Hive 3.1.x | Historical queries | In syllabus | Presto/Trino |
| Graph | NetworkX 3.x | Graph, PageRank, centrality | Simple, exact, small graphs | GraphFrames/GraphX (heavier) |
| Market basket | Pure-Python **A-Priori + PCY** (limited-pass); SON on Spark (stretch) | Frequent itemsets | Syllabus algorithms, cheap per micro-batch | Spark MLlib `FPGrowth` (optional cross-check only) |
| Serving | SQLite 3 (WAL) | Spark → dashboard hand-off | Zero-ops, ACID upserts | Redis, Parquet, Postgres |
| Dashboard | Streamlit + Plotly | UI | Fast to build, Python-native | Dash, Grafana |
| Quality | pytest, pytest-cov, ruff, mypy (light), pre-commit | Tests & lint | Standard | flake8/black |
| VCS/CI | Git + GitHub Actions | Collaboration | Standard | GitLab CI |

⚠️ **Verify before pinning:** Flume (1.10+ bundles older Hadoop client libs) working against your Hadoop 3.x; Spark build matching your Hadoop; Hive version matching your Hadoop; Streamlit version supports your chosen auto-refresh API. Record the working combination in `docs/ENVIRONMENT.md` (task T1-005).

---

## 3. Component Rules

### 3.1 Capture (TShark)
- Run TShark as a **subprocess** with `-l` (line-buffered), `-T fields`, `-E separator=,`, `-E occurrence=f` (first occurrence only), `-E header=n`, `-E quote=n`.
- Fields (minimum): `frame.time_epoch`, `ip.src`, `ip.dst`, `ipv6.src`, `ipv6.dst`, `tcp.srcport`, `tcp.dstport`, `udp.srcport`, `udp.dstport`, `ip.proto`, `ipv6.nxt`, `frame.len`, `eth.src`, `eth.dst`, `tcp.flags`, `frame.time_delta`.
- **Protocol** is derived from IP protocol number: 6→TCP, 17→UDP, 1 or 58→ICMP, else OTHER. (Do **not** use TShark's "Protocol" column — it returns application protocols like TLS/DNS.)
- Coalesce IPv4/IPv6 and TCP/UDP fields. Ports empty for ICMP/OTHER.
- `iat_ms = frame.time_delta × 1000` (interval since the previous *captured* frame).
- Permissions: use the `wireshark` group / `setcap` — **never run the whole pipeline as root**.
- Managed-mode Wi-Fi shows this device's traffic + broadcast/multicast; that is the MVP scope.

### 3.2 Multi-machine / OS note
If capture runs where the pipeline cannot (e.g. WSL2, macOS), run `capture` on the native OS with `--sink tcp://<pipeline-host>:44444` and use Flume's `netcat` source variant.

### 3.3 Flume (property spec — not full config)

| Component | Key properties |
|---|---|
| Source `src_capture` | `type=TAILDIR`, `filegroups=f1`, `filegroups.f1=<repo>/data/capture/traffic_.*\.csv`, `positionFile=<repo>/.flume/taildir_position.json`, `batchSize=100`; `selector.type=replicating` |
| Channel `ch_raw` | `type=file`, checkpoint + data dirs under `.flume/`, `capacity≥100000` (durable) |
| Channel `ch_stream` | `type=memory`, `capacity=100000`, `transactionCapacity=1000` (low latency) |
| Sink `sink_raw` | `type=hdfs`, `hdfs.path=hdfs://<nn>/traffic/raw/dt=%Y-%m-%d/hr=%H`, `hdfs.useLocalTimeStamp=true` (time escapes need a timestamp), `hdfs.fileType=DataStream`, `hdfs.writeFormat=Text`, `hdfs.rollInterval=300`, `hdfs.rollSize=0`, `hdfs.rollCount=0`, `hdfs.inUsePrefix=.`, `hdfs.inUseSuffix=.tmp`, `hdfs.idleTimeout=60` |
| Sink `sink_stream` | same, `hdfs.path=hdfs://<nn>/traffic/stream_in`, `hdfs.rollInterval=5`, `hdfs.inUsePrefix=.` |

Rules: `hdfs.inUsePrefix=.` so Spark's file source ignores in-progress files (Spark skips names starting with `.` or `_` — ⚠️ **Verify** with the acceptance test in T3-004). Set `HADOOP_HOME` so Flume finds Hadoop jars. Log/`flume.monitoring` enabled for the Pipeline tab.

### 3.4 HDFS layout

```text
/traffic/
├── raw/dt=YYYY-MM-DD/hr=HH/         durable raw (Hive external table `traffic_raw`)
├── stream_in/                       short-roll feed for Spark (retention: 24 h)
├── processed/                       Spark outputs (Parquet) if any
├── historical/dt=YYYY-MM-DD/        compacted hourly/daily Parquet (`traffic_hist`)
└── checkpoints/<query_name>/        Spark checkpoint dirs (never delete while a query is live)
```

### 3.5 Spark
- `spark.sql.session.timeZone=UTC`, `spark.sql.shuffle.partitions=4` (small data), trigger `processingTime=5 seconds`, watermark `30 seconds`.
- Streams read with an **explicit schema** (no inference). `maxFilesPerTrigger` set via config.
- Aggregations use `update` output mode into `foreachBatch` **upserts** (`INSERT … ON CONFLICT DO UPDATE`).
- Each query has its **own** checkpoint dir. Names: `q_window_metrics`, `q_moments`, `q_batch_plugins`, etc.
- Lane B plugins process **collected** micro-batch rows on the driver: cap at `max_rows_per_batch` (default 50,000); if exceeded → sample, log a WARN, record in `pipeline_health`.
- Exact distinct uses `size(collect_set(col))`; approximate uses `approx_count_distinct(col, 0.05)`. (Exact `countDistinct` is not allowed in streaming aggregations — ⚠️ Verify on your Spark version.)
- Analytics plugin interface (Lane B):

```text
class Analytic:   name: str
                  process_batch(batch_df, batch_id, ctx) -> None   # ctx: config, db writer, clock, logger
```
  Each plugin: pure logic in a testable function + thin Spark adapter.

### 3.6 Link analysis
- Graph: **directed, weighted** (`packets`, `bytes`); node = IP; built from last *N* windows of `ip_edges` (config `graph.lookback_s`, default 60).
- `nx.pagerank(G, alpha=0.85, weight="packets")`; also ship a from-scratch power-iteration version and unit-test that both agree within 1e-4.
- Markov: `P(j|i) = w_ij / Σ_k w_ik`; rows of nodes with no out-edges are omitted (documented); stationary distribution optional (Nice-to-have).
- Betweenness centrality only when nodes ≤ 300 (else skip and show a note).

### 3.7 Alerts (defaults live in `config/alert_rules.yaml`)

| Rule | Condition (defaults) | Severity |
|---|---|---|
| Traffic spike | `pps > 3 × EWMA baseline` **and** `pps ≥ 50` (EWMA α=0.1, ≥ 30 windows warm-up) | WARN; CRITICAL if > 6× |
| Unusual port activity | one `src_ip` reaches > 20 distinct `dst_port` in a window | WARN |
| High fan-out | one `src_ip` reaches > 30 distinct `dst_ip` in a window | WARN |

Every alert stores `current_value, baseline_value, change_pct, threshold, reason`. Cooldown 60 s per (type, src_ip). Thresholds are **tunable placeholders** — calibrate during testing.

---

## 4. Folder Structure

```text
Live-Network-Traffic-Analyser/
├── docs/                      # PRD, DESIGN, TECH_RULES, ROADMAP, todo, DATA_CONTRACT, ENVIRONMENT, CONCEPT_MAPPING
├── config/                    # settings.example.yaml, alert_rules.yaml   (settings.yaml is git-ignored)
├── contracts/
│   ├── record_schema.py       # CSV field spec + validator + Spark StructType builder
│   └── serving_schema.sql     # SQLite DDL for all serving tables
├── common/                    # config loader, logging, serving_db (reader/writer), time utils
├── capture/                   # list_interfaces, tshark_cmd, parser, run_capture, writers, generator, replay, anonymize
├── ingestion/
│   ├── flume/traffic-agent.conf
│   ├── hdfs/                  # layout + permissions scripts
│   ├── hive/                  # DDL + partition scripts + sql/historical/*.sql
│   └── scripts/               # start_hdfs.sh, flume_ctl.sh, verify_ingestion.py, compact_raw.py, run_historical.py
├── streaming/
│   ├── common/                # session.py, schema.py, cleaning.py
│   ├── queries/               # Lane A: window_metrics.py, counts.py, moments_window.py
│   ├── analytics/             # Lane B plugins: filters, sampling, distinct, counting_ones, decay, itemsets, edges, moments
│   └── stream_app.py          # registry + foreachBatch dispatcher
├── linkanalysis/              # graph.py, pagerank.py, markov.py, centrality.py, alerts.py
├── dashboard/                 # app.py, theme.py, data.py, components/, assets/theme.css, .streamlit/config.toml
├── scripts/                   # run_stream.sh, run_demo.sh, verify_env.py, load_test.py
├── data/sample/               # ANONYMISED CSVs only
├── serving/                   # analytics.db (ignored)
├── logs/                      # ignored
└── tests/{unit,integration,e2e}/
```

---

## 5. Data Contract (single source of truth)

> Any change to `contracts/` must be announced in the team chat **before** it is pushed to `main`, and acknowledged by all five members.

### 5.1 Traffic record v1 (CSV, no header, UTF-8, `\n` line ends, comma separator)

| # | Field | Type | Format / Rules | Nullable |
|---|---|---|---|---|
| 1 | `timestamp` | string→timestamp | UTC `YYYY-MM-DD HH:MM:SS.mmm` | No |
| 2 | `src_ip` | string | valid IPv4/IPv6 | No |
| 3 | `dst_ip` | string | valid IPv4/IPv6 | No |
| 4 | `src_port` | int | 0–65535 | Yes (ICMP/OTHER) |
| 5 | `dst_port` | int | 0–65535 | Yes |
| 6 | `protocol` | string | `TCP`\|`UDP`\|`ICMP`\|`OTHER` | No |
| 7 | `packet_length` | int | 1–65535 (`frame.len`) | No |
| 8 | `src_mac` | string | `aa:bb:cc:dd:ee:ff` | Yes |
| 9 | `dst_mac` | string | same | Yes |
| 10 | `tcp_flags` | string | hex e.g. `0x0018` | Yes |
| 11 | `iat_ms` | double | ≥ 0, ms since previous captured frame | Yes |

Example: `2026-09-19 18:20:01.482,192.168.1.10,8.8.8.8,52341,443,TCP,1420,aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,0x0018,0.412`

Rules: no commas inside fields; missing → empty; rejected rows go to `data/capture/rejects_*.log` with reason; JSON-lines alternative uses the same field names.

### 5.2 Serving store (SQLite, `serving/analytics.db`, WAL, `busy_timeout=5000`)
All timestamps ISO-8601 UTC text (`YYYY-MM-DDTHH:MM:SS.sssZ`). All tables include `updated_at`. Windows are keyed by `(window_start, window_len_s)`.

| Table | Key | Columns (besides key) | Writer |
|---|---|---|---|
| `window_metrics` | window_start, window_len_s | packets, bytes, pps, bps | M A Sushil Kumar |
| `protocol_counts` | +protocol | packets, bytes | M A Sushil Kumar |
| `port_counts` | +port | packets, bytes (top-N dst ports) | M A Sushil Kumar |
| `filter_counts` | +filter_name | packets, bytes | M A Sushil Kumar |
| `distinct_counts` | window_start, window_len_s | src_ips_exact, dst_ips_exact, ports_exact, src_ips_hll, dst_ips_hll, ports_hll, src_ips_fm, dst_ips_fm | M A Sushil Kumar |
| `sampling_compare` | ts, method | k, sample_n, sample_mean_len, full_mean_len, err_pct | M A Sushil Kumar |
| `counting_ones` | ts, predicate_name | window_n, exact_ones, dgim_estimate, err_pct | M A Sushil Kumar |
| `moments` | window_start, window_len_s | n, mean_len, var_len, std_len, iat_mean_ms, iat_var_ms, iat_std_ms, f2_exact, f2_ams | Yashwant Vadhan M |
| `decay_traffic` | ts, half_life_s | score, raw_pps | Yashwant Vadhan M |
| `decay_top_keys` | ts, key_type, key | score, rank | Yashwant Vadhan M |
| `frequent_itemsets` | window_start, window_len_s, algorithm, itemset | size, support_count, support_ratio, passes | Yashwant Vadhan M |
| `ip_edges` | window_start, window_len_s, src_ip, dst_ip | packets, bytes | Yashwant Vadhan M |
| `source_stats` | window_start, window_len_s, src_ip | packets, bytes, unique_dst_ips, unique_dst_ports | Yashwant Vadhan M |
| `alerts` | alert_id | ts, type, severity, src_ip, metric, current_value, baseline_value, change_pct, threshold, reason, details_json | Priyan S |
| `pipeline_health` | ts, component, metric | value | M A Sushil Kumar (runner) |
| `hist_results` | query_name, run_at | columns_json, rows_json | Rithika GV |

Retention: keep last 24 h in live tables (cleanup job by Yashwant Vadhan M); history lives in HDFS/Hive.

---

## 6. Coding Standards

### Naming
- Python: `snake_case` functions/vars/modules, `PascalCase` classes, `UPPER_SNAKE` constants. Files are nouns (`sampling.py`), tests mirror (`test_sampling.py`).
- Spark columns: `snake_case`, exactly the contract names. SQL tables/columns: `snake_case`.
- Dashboard components: `render_<panel>()` functions in `dashboard/components/`.
- Config keys: `snake_case`, nested by component (`spark.trigger_s`, `graph.lookback_s`).

### Structure & Quality
- Separate **pure logic** (no Spark/IO) from adapters; algorithms (DGIM, FM, reservoir, decay, PageRank, alerts) must be plain-Python testable.
- Type hints on public functions; docstring states inputs, outputs, complexity where relevant.
- `ruff` clean; functions ≤ ~50 lines; no wildcard imports; no global mutable state except documented plugin state objects with `snapshot()/restore()`.
- **No hard-coded paths, hosts, ports, thresholds** — read from `config/`.

### Error Handling
- Capture: bad line → reject log + counter, never crash. TShark exit → restart with exponential back-off (max 5 tries) then exit non-zero.
- Spark: malformed CSV rows → bad-record counter; plugin exceptions are **caught per plugin per batch**, logged with `batch_id`, and never kill the query (`pipeline_health` records `plugin_error`).
- Dashboard: every data call returns `(data, status)`; UI renders the matching state (loading/empty/error/stale). Never show a Python traceback to the audience.
- CLIs: exit code 0 ok, 1 runtime error, 2 usage/config error.

### Logging
- Python `logging`, format `%(asctime)s %(levelname)s %(name)s | %(message)s`, UTC. No `print` in library code. Levels: DEBUG detail, INFO lifecycle/batch summary, WARN degraded, ERROR failure. Logs to `logs/<component>.log` with rotation.
- Never log full payloads per packet at INFO.

### API / Interface Standards
- No HTTP API in MVP. Interfaces are the **CSV contract**, the **SQLite contract**, and `Analytic` plugin interface. Versioned by `contracts/` (`RECORD_SCHEMA_VERSION`, `SERVING_SCHEMA_VERSION`).

---

## 7. Security & Privacy Rules

| Area | Rule |
|---|---|
| Authentication | None for localhost MVP. Dashboard binds `127.0.0.1`. If shown on LAN for a demo, do it deliberately (`--server.address`) and only on a trusted network; prefer SSH tunnel. |
| Authorization | N/A (single-user). Serving DB opened **read-only** by dashboard (`mode=ro`). |
| Data validation | Capture validates every field (types, ranges, IP parsing). Spark applies schema + range filters. Dashboard uses **parameterised SQL only** (no f-string SQL). |
| Rate limiting | N/A for public traffic; dashboard caches queries (TTL 2 s) to bound DB load; capture applies back-pressure (bounded queue). |
| Secrets | No secrets required. Host/port config in git-ignored `config/settings.yaml`; `.env` never committed. |
| Transport | Local only; TLS not applicable. If any service is exposed beyond localhost, tunnel it. |
| Privileges | Capture via capabilities/group, not root. Do not `chmod 777` HDFS; use a dedicated HDFS user dir. |
| Privacy | **Never commit real captures.** `data/`, `logs/`, `serving/`, `.flume/` are git-ignored except `data/sample/`. `capture/anonymize.py` (keyed hash or /24 masking) required before any dataset is shared. Pre-commit hook blocks `*.pcap*` and files with real-looking private IPs outside `data/sample`. |
| Legal | Capture only own device/permitted network. Add banner in README and dashboard footer. |

---

## 8. Performance Rules

- **Capture:** buffered file writes with periodic flush (≤ 1 s); bounded queue; drop-with-counter over unbounded memory.
- **Flume:** batch sizes ≥ 100; avoid `exec` source; channel capacity sized for 60 s of peak traffic.
- **HDFS:** avoid small-file explosion — `stream_in` retained 24 h; hourly compaction of `raw` into Parquet under `historical/`.
- **Spark:** small shuffle partitions; explicit schema; `maxFilesPerTrigger` cap; processing time must stay **< trigger interval** (alert if 3 consecutive batches exceed it); driver-side algorithms capped at `max_rows_per_batch`.
- **SQLite:** indexes on `(window_start)` per table; batched upserts in one transaction per batch; retention cleanup every 10 min.
- **Dashboard:** read only the latest N windows (default 120); TTL cache 2 s; downsample charts to ≤ 600 points; prune graph to top-N nodes (default 50).
- **Load targets:** must sustain 2,000 pkts/s; target 5,000 (measured by `scripts/load_test.py`, results recorded in `docs/test_report.md`).

---

## 9. Testing Rules

| Level | Tools | Scope | Target |
|---|---|---|---|
| Unit | pytest, pytest-cov | Parser/normaliser, generator, DGIM, FM, reservoir, decay, moments merge, itemsets, PageRank, Markov, alert rules, contract validator | ≥ 70% lines on core logic; **algorithms 90%** |
| Spark integration | pytest + local `SparkSession` (`local[2]`), file-source stream in `tmp_path`, `query.processAllAvailable()` | Each lane A query and lane B plugin against a fixture CSV with hand-computed expectations | Every plugin has ≥ 1 |
| Pipeline integration | shell + pytest | Capture→Flume→HDFS ingestion verification (counts match); Spark reads `stream_in` | Runs on integration host |
| E2E | replay scenarios + assertions | The 8 tests from the project overview (normal, increased, protocol filter, distinct, frequent patterns, window, decay, graph) | All pass before demo |
| Dashboard | Streamlit `AppTest`, mock data | Each tab renders in normal/empty/error states | Smoke per tab |
| Manual | Wireshark | Compare capture counts and Wireshark I/O graph over a 60 s interval | ≤ 1% mismatch on IP frames |

Ground truth rule: analytics tests compare against **pandas computed on the same CSV**.

---

## 10. Git Workflow (trunk-based — everyone pushes to `main`)

Decision: with an 11-day deadline the team pushes **directly to `main`** (no feature branches, no PR gate). This is fast but risky, so these rules are mandatory:

0. **Repository:** https://github.com/skoder404/Live-Network-Traffic-Analyser — clone it, work on `main`, `origin` = that URL. All docs live in `docs/`.
1. **Own your folder.** Only edit files in your own module folder (see README §1). Cross-folder edits → tell the owner first. This is what prevents merge conflicts.
2. **Pull before you push:** `git pull --rebase origin main` → run your tests → `git push origin main`. Never `git push --force` on `main`.
3. **Small, frequent commits** (at least daily, ideally after every finished task). Never leave `main` broken: if `pytest -m "not spark and not e2e"` fails locally, do not push.
4. **Commit messages:** Conventional Commits — `feat(capture): add IPv6 coalescing`, `fix(flume): set inUsePrefix`, `test(dgim): add edge cases`, `docs: update data contract`.
5. **Contract changes** (`contracts/`) are announced in chat first; whoever changes it fixes all breakage the same day.
6. **Shared files** (`config/settings.example.yaml`, `requirements.txt`, `README.md`): append-only edits, pull right before editing, push immediately after.
7. **No data files** in commits (`data/` except `data/sample/`, `logs/`, `serving/`, `.flume/`). CI fails if a `*.pcap*` or a real-looking IP appears outside `data/sample/`.
8. **Broke `main`?** Fix forward within 30 minutes or `git revert` your commit.
9. **Tags:** `m1-slice` (Sep 24), `m2-core` (Sep 27), `demo-v1` (Sep 30).

---

## 11. Deployment & Operations

- **Environments:** `dev` (each member, replay data, Spark `local[2]`), `integration` (one shared Ubuntu host with HDFS/Flume/Spark), `demo` (integration host frozen at tag `demo-v1`).
### 11.1 Machine Topology (2 laptops × 16 GB RAM)

| Machine | Role | Runs | Rough RAM budget (estimates — verify with `free -h`) |
|---|---|---|---|
| **Laptop A — "pipeline host"** (Ubuntu, native or dual-boot preferred) | Everything stateful | HDFS (NameNode+DataNode), Flume, Spark (`local[2]`), Hive (or Spark SQL fallback), alerts worker, SQLite serving DB, Streamlit dashboard | OS ~2 GB · HDFS ~1.5 GB · Flume ~0.5 GB · Spark driver+executor ~4 GB · Hive ~1 GB · Streamlit/alerts ~1 GB → ≈ 10 GB, leaving headroom |
| **Laptop B — "capture + presentation"** | Live Wi-Fi source and a second screen | TShark capture (`--sink tcp://A:44444`) → Flume `netcat` source on A; browser pointed at A's dashboard | Light (< 3 GB) |

Rules:
- The SQLite DB, Spark driver and dashboard **must be on Laptop A** (SQLite is not for network shares). Laptop B only opens the dashboard in a browser (`streamlit run … --server.address 0.0.0.0` on A, **trusted network only**, or use an SSH tunnel).
- Because capture runs on B, the **`netcat` Flume source variant and the TCP writer are MUST**, not optional (T2-005, T3-003, T8-001 `live` mode).
- Suggested settings on A: Spark `driver.memory≈3g`, `local[2]`, `shuffle.partitions=4`; Flume `-Xmx512m`; small HDFS daemon heaps. Close browsers/IDEs on A during the demo.
- **Rithika GV's laptop is 8 GB:** it runs HDFS + Flume + replay only (never Spark); Spark-based tests run on Laptop A over SSH.
- **Distributed team:** members are in different homes/hostels behind NAT, so all machine-to-machine traffic (SSH, Laptop B → A on TCP 44444, dashboard/Spark/HDFS UIs via `ssh -L`) runs over a private VPN overlay (Tailscale recommended; ZeroTier fallback — verify current free-plan limits). Never port-forward SSH on a home router. Setup: todo T1-007 / `docs/REMOTE_ACCESS.md`.
- ⚠️ **Laptop-to-laptop TCP can be blocked** by campus Wi-Fi client isolation or firewalls. Test on Sep 22 (right after T1-007). Fallbacks: phone hotspot or a direct Ethernet/USB link between A and B; capture on A itself; or replay mode (the demo still works).
- The other three members develop on their own machines using replay/mock data (pure-Python tests, dashboard `--mock`, small local Spark runs). Only integration runs need Laptop A. Book it in advance (integration day Sep 27, rehearsals Sep 30).

- **CI (GitHub Actions):** on every push to `main` — ruff, unit tests (no Spark-heavy), contract validation, no-captures check. Nightly/optional — Spark integration tests. No deployment step (academic).
- **Run order:** HDFS → Flume → data source (live/replay) → Spark → dashboard (`scripts/run_demo.sh` automates and health-checks each step).
- **Monitoring:** Dashboard *Pipeline* tab; Spark UI (`:4040`); HDFS NameNode UI (`:9870` default in Hadoop 3); Flume monitoring/log; `pipeline_health` table.
- **Backup:** Copy a demo dataset (`data/sample/`) and an HDFS export of one clean session; keep `serving/analytics.db` snapshot from the best rehearsal as an offline demo fallback.
- **Runbook:** `docs/RUNBOOK.md` — "if X fails during demo, do Y" (Wi-Fi drops → replay; Flume down → restart script; Spark lag → reduce window/`maxFilesPerTrigger`).
