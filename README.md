# Live Network Traffic Analyser (LNTA)

> 📦 Repository: https://github.com/skoder404/Live-Network-Traffic-Analyser

> **Real-time Big Data streaming analytics for Wi-Fi network traffic** — Big Data Analytics course project, focused on **streaming analytics & link analysis**.

```text
Wi-Fi ─▶ TShark ─▶ Apache Flume ─┬─▶ HDFS (raw / historical) ─▶ Hive / Spark SQL ─┐
                                 └─▶ HDFS (stream_in) ─▶ Spark Structured Streaming │
                                                          │                          │
                                        streaming analytics ┤                          ▼
                                        Link analysis     └──────────▶ Serving store ─▶ Streamlit Dashboard
```

We continuously observe traffic on a Wi-Fi interface we are permitted to monitor, turn it into structured stream records, ingest and store it with **Flume + HDFS**, process it in real time with **Spark Structured Streaming**, apply the **streaming algorithms**, model IP communication as a **graph (PageRank / Markov)**, query history with **Hive / Spark SQL**, and present everything on a live **Streamlit** dashboard with explainable alerts.

---

## 1. Team & Ownership

| # | Member | Owns | Hands off to |
|---|--------|------|--------------|
| 1 | **Naveena MS** | Wi-Fi capture (TShark), field extraction, cleaning, CSV/JSON output, sample & synthetic data | Rithika GV |
| 2 | **Rithika GV** | Flume agent, HDFS layout, historical storage, Hive tables, ingestion verification; helps connect Spark ↔ HDFS | M A Sushil Kumar, Yashwant Vadhan M |
| 3 | **M A Sushil Kumar** | Spark/PySpark setup, schema & parsing, windows, packets/sec, bytes/sec, protocol/port counts; **Sampling, Stream Filtering, Counting Ones, Count Distinct** | Yashwant Vadhan M, Priyan S |
| 4 | **Yashwant Vadhan M** | Advanced Structured Streaming; **Estimating Moments, Decaying Windows, Market-Basket Frequent Itemsets**; edge aggregation; **final integration & E2E testing** | Priyan S |
| 5 | **Priyan S** | IP graph, degree/centrality, **PageRank, Markov analysis**, alert engine, **Streamlit dashboard** | Final system |

## 2. Concepts Implemented → Where They Live

| core concept | Module (path) | Owner |
|---|---|---|
| Stream data model | `streaming/common/schema.py`, `stream_app.py` | M A Sushil Kumar |
| Filtering streams | `streaming/analytics/filters.py` | M A Sushil Kumar |
| Sampling | `streaming/analytics/sampling.py` | M A Sushil Kumar |
| Count distinct elements | `streaming/analytics/distinct.py` | M A Sushil Kumar |
| Counting ones | `streaming/analytics/counting_ones.py` (DGIM) | M A Sushil Kumar |
| Estimating moments | `streaming/analytics/moments.py` | Yashwant Vadhan M |
| Decaying windows | `streaming/analytics/decay.py` | Yashwant Vadhan M |
| Market basket & limited-pass frequent itemsets (A-Priori, PCY) | `streaming/analytics/itemsets.py` | Yashwant Vadhan M |
| Link analysis / PageRank / Markov | `linkanalysis/` | Priyan S |

## 3. Documentation Map

| Doc | Purpose |
|---|---|
| [`PRD.md`](docs/PRD.md) | What we are building and why; requirements; MVP scope |
| [`DESIGN.md`](docs/DESIGN.md) | Dashboard UX, visual system, screen specs |
| [`TECH_RULES.md`](docs/TECH_RULES.md) | Architecture, **data contract**, stack, standards, testing, Git rules |
| [`ROADMAP.md`](docs/ROADMAP.md) | Milestones, per-member hand-offs, risks |
| [`todo.md`](docs/todo.md) | Atomic tasks with acceptance criteria and **ready-to-paste AI-agent prompts** |

> Read order for a new contributor: README → PRD → TECH_RULES (§5 Data Contract) → your section of `todo.md`.

## 4. Prerequisites (baseline — verify versions on your machine)

| Component | Recommended baseline | Notes |
|---|---|---|
| OS | Ubuntu 22.04/24.04 (native or VM) on **Laptop A** (pipeline host); Laptop B only needs TShark | WSL2 generally cannot see the host Wi-Fi adapter — capture on the native OS and forward records (see TECH_RULES §3.2) |
| Java | 11 or 17 | Required by Hadoop, Flume, Spark |
| Hadoop / HDFS | 3.3.x, pseudo-distributed | ⚠️ Verify Flume HDFS-sink compatibility (TECH_RULES §3.4) |
| Apache Flume | 1.11.x (1.12.0 also exists) | Bundled Hadoop libs are older; put your Hadoop jars on the classpath |
| Apache Spark | 3.5.x (prebuilt for Hadoop 3) | Newer major versions exist; not validated for this stack |
| Python | 3.10 or 3.11 | PySpark, pandas, NetworkX, Streamlit, Plotly |
| Hive | 3.1.x (optional-but-planned) | Historical queries |
| TShark / Wireshark | 4.x | Capture permission needed (`wireshark` group or `setcap`) |

### Working remotely (team is in different places)
Laptop A (Yashwant, Ubuntu) is the pipeline host and Laptop B (M A Sushil Kumar, Ubuntu) is the capture machine. Members reach them over a private VPN overlay (Tailscale recommended) + SSH — no router port-forwarding. Setup guide: `docs/REMOTE_ACCESS.md` (task T1-007). Rithika GV's 8 GB laptop runs HDFS + Flume + replay only; Spark work goes to Laptop A.

## 5. Quick Start (target state — filled in as tasks complete)

```bash
git clone https://github.com/skoder404/Live-Network-Traffic-Analyser.git && cd Live-Network-Traffic-Analyser
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/settings.example.yaml config/settings.yaml      # edit interface, paths, ports

# 0. Verify environment
python scripts/verify_env.py

# 1. Start storage + ingestion
bash ingestion/scripts/start_hdfs.sh
bash ingestion/scripts/flume_ctl.sh start

# 2. Start the data source (choose ONE)
python -m capture.run_capture --iface wlan0                    # LIVE Wi-Fi
python -m capture.replay --file data/sample/normal.csv --speed 1   # REPLAY (no Wi-Fi needed)

# 3. Start Spark streaming
bash scripts/run_stream.sh

# 4. Start dashboard
streamlit run dashboard/app.py
```

One-command demo (after Milestone 5): `bash scripts/run_demo.sh --mode replay|live`

## 6. Repository Structure

```text
Live-Network-Traffic-Analyser/
├── README.md
├── docs/                    PRD.md · DESIGN.md · TECH_RULES.md · ROADMAP.md · todo.md · DATA_CONTRACT.md
├── config/                  settings.yaml · alert_rules.yaml
├── contracts/               record_schema.py · serving_schema.sql        ← shared contract
├── capture/                 (Naveena MS)  TShark wrapper, parser, writer, generator, replay
├── ingestion/               (Rithika GV)  flume/ · hdfs/ · hive/ · scripts/
├── streaming/               (M A Sushil Kumar + Yashwant Vadhan M)  common/ · queries/ · analytics/ · stream_app.py
├── linkanalysis/            (Priyan S)   graph · pagerank · markov · alerts
├── dashboard/               (Priyan S)   Streamlit app
├── serving/                 SQLite serving store (git-ignored)
├── data/sample/             anonymised sample & synthetic CSVs
├── scripts/                 run_stream.sh · run_demo.sh · verify_env.py
└── tests/                   unit/ · integration/ · e2e/
```

## 7. Ethics, Privacy & Legal

- Capture **only** traffic on your own device/network, or a network where you have **explicit permission**.
- Raw captures contain real IPs/MACs. **Never commit real captures.** Only anonymised data (`capture/anonymize.py`) may enter `data/sample/`.
- Alerts are **behavioural indicators**, not proof of attack. This is **not** an intrusion-detection system.
- PageRank scores mean *structural importance in the observed graph*, not maliciousness.

## 8. Schedule & Status (deadline: submission + demo on **1 Oct 2026**)

| Milestone | Date | Status |
|---|---|---|
| M0 Foundation & contract | 21–22 Sep | ⬜ |
| M1 Vertical slice (Capture → Flume → HDFS → Spark → SQLite) | by 24 Sep (noon) | ⬜ |
| M2 All streaming algorithms + link analysis | by 27 Sep | ⬜ |
| M3 Dashboard integrated, history + alerts | by 28 Sep | ⬜ |
| Feature freeze | 29 Sep 12:00 | ⬜ |
| M5 Rehearsals, fallback, submission package | 30 Sep | ⬜ |
| **Demo & submission** | **1 Oct** | ⬜ |

**Git workflow:** everyone pushes directly to `main` — own your folder, `git pull --rebase` before every push, never force-push, never push a failing build (TECH_RULES §10).

## 9. License & Credits

Academic project — Big Data Analytics. Add your institution, course code, faculty guide and team roll numbers here.
