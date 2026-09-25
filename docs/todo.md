# todo.md — Live Network Traffic Analyser (LNTA)
> Generated from: PRD.md · DESIGN.md · TECH_RULES.md · ROADMAP.md
> **Deadline: submission + live demo on Thursday 1 October 2026** · **Work starts Monday 21 September 2026** (11 days incl. demo day)
> Repository: https://github.com/skoder404/Live-Network-Traffic-Analyser
> Workflow: **everyone pushes directly to `main`** — own your folder, `git pull --rebase origin main` before every push

---

## 1. Who Does What (at a glance)

| Member | Owns | Main deliverables | Buddy (covers if you are stuck) |
|---|---|---|---|
| **Naveena MS** | `capture/`, `data/sample/`, sample/replay tools, validation docs, README polish | Contract-compliant CSV from live Wi-Fi, generator + replay, anonymised samples, Wireshark validation | Rithika GV |
| **Rithika GV** | `ingestion/` (Flume, HDFS, Hive, history queries), runbook, audit | Flume → HDFS raw + stream_in, verification, historical queries, fallback snapshot | Naveena MS |
| **M A Sushil Kumar** | `streaming/common`, Lane A window/count/filter/distinct queries, `analytics/{filters,sampling,fm,dgim}` | Spark setup, runner skeleton, windows, protocol/port counts, filtering, sampling, count distinct, counting ones | Yashwant Vadhan M |
| **Yashwant Vadhan M** | `contracts/`, `common/`, `scripts/`, `config/`, other `streaming/` files | Repo/DB/mock foundation, moments, decaying windows, market-basket itemsets, edges, integration, e2e, concept mapping | M A Sushil Kumar |
| **Priyan S** | `linkanalysis/`, `dashboard/` | Graph, degree/centrality, PageRank, Markov, alerts, full dashboard | Yashwant Vadhan M |

**Priority legend:** 🔴 MUST = required for the demo · 🟡 SHOULD = do if on schedule · ⚫ CUT = dropped for the deadline (documented, not forgotten).

## 2. Day-by-Day Schedule (task IDs)

| Date | Naveena MS | Rithika GV | M A Sushil Kumar | Yashwant Vadhan M | Priyan S |
|---|---|---|---|---|---|
| **Sep 21 (Mon)** | T1-003, T2-006, T2-008 | T3-001, T3-002 | T4-001, T4-002 | T1-001, T1-002, T1-004, T1-007 | T6-001, T6-002, T6-003 |
| **Sep 22 (Tue)** | T2-001, T2-002, T2-003, T2-005 | T3-003, T3-004 | T4-003, T4-004 | T1-005, T1-006, T5-003 | T6-004, T7-001, T7-002 |
| **Sep 23 (Wed)** | T2-004, T2-007 | T3-005, T3-006, T3-010 | T4-005 | T5-001, T5-005 | T7-003 |
| **Sep 24 (Thu)** | T2-009 | — | T4-006, T4-007 | T5-002 | T6-005, T7-004 |
| **Sep 25 (Fri)** | — | T3-008 | T4-008 | — | T7-005 |
| **Sep 26 (Sat)** | — | T3-009 | T4-009 | T5-004 | T6-006, T7-006, T7-007 |
| **Sep 27 (Sun)** | — | T3-011 | T4-010 | T5-009, T5-010, T8-001 | T7-008 |
| **Sep 28 (Mon)** | T8-003 | — | — | T5-006, T5-007 | T7-009, T7-010 |
| **Sep 29 (Tue)** | T9-002 | T8-004 | — | T8-002, T9-001 | — |
| **Sep 30 (Wed)** | — | T9-003 | — | — | — |
| **Oct 1 (Thu)** | **DEMO + SUBMISSION** — arrive early, run `verify_env.py`, one dry run, present | | | | |

**Note:** a few tasks are scheduled before their dependency lands (e.g. Priyan S's graph logic vs. the mock DB, Yashwant Vadhan M's decay logic vs. the runner). Build the **pure logic first** (it has no dependency), then wire it to the DB/runner when that task is pushed.

**Suggested daily sync:** 15 minutes at a fixed evening time (e.g. 9:00 PM) — each person says: done / next / blocked. Post blockers in chat immediately, not at the sync.

### Decision Gates
| Gate | When | Question | If NO |
|---|---|---|---|
| **G1** | Sep 22, 22:00 | Does Flume write to HDFS with your Hadoop version (T3-002)? | Switch to fallback: Flume `file_roll` sink → local dir read by Spark; keep HDFS raw copy via `hdfs dfs -put` |
| **G2** | Sep 24, 12:00 | Vertical slice: replay → Flume → HDFS → Spark → `window_metrics` → dashboard? | Everyone stops feature work and helps Rithika GV/M A Sushil Kumar/Yashwant Vadhan M until it works |
| **G3** | Sep 27, 22:00 | All MUST algorithms produce rows and appear on the dashboard? | Apply cut list: multi-window, load test, SON, Hive (use Spark SQL fallback) |
| **G4** | Sep 29, 12:00 | **Feature freeze.** Only bug fixes after this | — |

### Machines (2 × 16 GB laptops — see TECH_RULES §11.1)
- **Assignment (both laptops dual-boot Windows + Ubuntu → boot native Ubuntu on both):** **Laptop A = Yashwant Vadhan M's (integration lead, drives `run_demo.sh`)**, **Laptop B = M A Sushil Kumar's** (capture + second screen + backup). Enable SSH on A (`openssh-server`) so Rithika GV, Priyan S and Naveena MS can log in remotely on the same LAN for their integration slots. Keep ≥ 40 GB free on A's Ubuntu partition (HDFS + logs).
- **Rithika GV (8 GB laptop, dual-boot Windows + Ubuntu):** boot native Ubuntu and build/test **HDFS + Flume + replay data on her own laptop** (T3-001…T3-005 — this fits in 8 GB; do **not** run Spark there, and close the browser while testing). Her scripts (`init_layout.sh`, `render_conf.py`, `flume_ctl.sh`) and the exact versions in `docs/ENVIRONMENT.md` must be written so Yashwant can **replicate the install on Laptop A on Sep 21–22 in under an hour** using the same Hadoop/Flume tarball versions. Anything needing Spark (T3-004 hidden-file test, T3-010 smoke test, historical queries) is run **on Laptop A over SSH**.
- **Laptop A (Ubuntu) = pipeline host:** HDFS, Flume, Spark, Hive, SQLite, alerts worker, dashboard. Rithika GV and M A Sushil Kumar set it up first (Sep 21–22); **book it** for Sep 22–23 (slice), Sep 27 (integration) and Sep 30 (rehearsals).
- **Laptop B = live capture + second screen:** Naveena MS runs TShark here and sends records to Flume's `netcat` source on A (`--sink tcp://<A-ip>:44444`). So the TCP writer (T2-005) and the netcat Flume config (T3-003) are **MUST** and must be tested laptop-to-laptop on **Sep 22**.
- **Other members** work on their own devices with replay data, `--mock` dashboard and small local Spark runs; push to `main`, and run integration tests on Laptop A.
- **Team is distributed** (Yashwant at home, M A Sushil Kumar at home, Rithika GV/Priyan S/Naveena MS in the hostel), so laptops are reached through a **private VPN overlay (Tailscale) + SSH** — see **T1-007** and `docs/REMOTE_ACCESS.md`. No router port-forwarding. Laptop A must stay **powered on, booted into Ubuntu, plugged in, not sleeping**; use `tmux` so runs survive disconnects; book time slots because five people share one 16 GB machine.
- Laptop B → Laptop A capture traffic also goes over the overlay network, so it works even when the two laptops are in different homes. Pair-debugging tip: `tmate` or a screen-share call.
- ⚠️ If the network blocks the overlay or laptop-to-laptop traffic, use a phone hotspot, or fall back to replay.

### Scope decisions made for the 11-day deadline
- **Cut:** raw-data compaction (T3-007), retention job (T5-008), Spark MLlib FP-Growth, SON (stretch only), Docker, hotspot/monitor-mode capture.
- **Reduced:** load test = 1,000 and 2,000 pkts/s for 60–90 s; fault tests = one Flume-kill test; windows = 10 s and 60 s (30 s optional); alerts = three simple rules.
- **Fallbacks pre-agreed:** replay mode is a first-class demo path; Hive → Spark SQL over HDFS; Flume → HDFS conflict → `file_roll` sink.
- ⚠️ **Honest risk note:** this scope is ambitious for 11 days. The biggest risks are the Hadoop/Flume setup (G1) and late integration (G2, G3). If a gate fails, cut scope — never move the demo.

## 3. Per-Member Checklists

### Naveena MS — task checklist

| ✔ | Task | Title | Due | Priority | Effort |
|---|---|---|---|---|---|
| ☑ | T1-003 | Traffic record contract module (CSV schema v1) | Sep 21 | MUST | 60 min |
| ☑ | T2-006 | Synthetic traffic generator with scenarios and ground truth | Sep 21 | MUST | 90 min |
| ☑ | T2-008 | Anonymiser and committed sample datasets | Sep 21 | MUST | 45 min |
| ☑ | T2-001 | TShark setup, permissions and Wi-Fi interface detection | Sep 22 | MUST | 45 min |
| ☑ | T2-002 | TShark command builder | Sep 22 | MUST | 30 min |
| ☑ | T2-003 | Line parser / normaliser (raw TShark line → contract record) | Sep 22 | MUST | 90 min |
| ☑ | T2-005 | Output writers (rotating CSV, JSON-lines, TCP, stdout) | Sep 22 | MUST | 60 min |
| ☑ | T2-004 | Capture runner (subprocess, queue, restart, stats) | Sep 23 | MUST | 90 min |
| ☑ | T2-007 | Replay tool (paced, re-stamped) | Sep 23 | MUST | 45 min |
| ☑ | T2-009 | Capture integration test and Wireshark validation | Sep 24 | SHOULD | 60 min |
| ☐ | T8-003 | Wireshark/ingestion validation report | Sep 28 | SHOULD | 45 min |
| ☐ | T9-002 | Final README, architecture visuals and screenshots | Sep 29 | MUST | 60 min |

### Rithika GV — task checklist

| ✔ | Task | Title | Due | Priority | Effort |
|---|---|---|---|---|---|
| ☑ | T3-001 | HDFS pseudo-distributed setup and zone layout | Sep 21 | MUST | 60 min |
| ☑ | T3-002 | Flume install and Hadoop-compatibility check | Sep 21 | MUST | 60 min |
| ☑ | T3-003 | Flume agent config — TAILDIR source, channels, replicating selector | Sep 22 | MUST | 45 min |
| ☑ | T3-004 | Flume HDFS sinks and "in-progress files are invisible" test | Sep 22 | MUST | 60 min |
| ☑ | T3-005 | Flume control script and status JSON | Sep 23 | SHOULD | 45 min |
| ☑ | T3-006 | Ingestion verification (loss, duplicates, latency) | Sep 23 | MUST | 60 min |
| ☑ | T3-010 | Spark ↔ HDFS connection helper (secondary duty) | Sep 23 | MUST | 45 min |
| ☑ | T3-008 | Hive metastore and external tables | Sep 25 | SHOULD | 90 min |
| ☑ | T3-009 | Historical query pack and exporter to the serving store | Sep 26 | SHOULD | 75 min |
| ☐ | T3-011 | Failure and restart tests for ingestion | Sep 27 | SHOULD | 45 min |
| ☐ | T8-004 | Runbook and offline fallback snapshot | Sep 29 | MUST | 45 min |
| ☐ | T9-003 | Repository sanitisation audit and release tag | Sep 30 | MUST | 30 min |
| ☐ | T3-007 | Raw compaction and stream_in retention | not scheduled | CUT | 60 min |

### M A Sushil Kumar — task checklist

| ✔ | Task | Title | Due | Priority | Effort |
|---|---|---|---|---|---|
| ☑ | T4-001 | Spark environment, session factory and submit wrapper | Sep 21 | MUST | 45 min |
| ☑ | T4-002 | Stream schema, parsing and cleaning | Sep 21 | MUST | 60 min |
| ☑ | T4-003 | Streaming application runner (registry, dispatcher, health) | Sep 22 | MUST | 90 min |
| ☑ | T4-004 | Window metrics — packets/sec and bytes/sec (Lane A) | Sep 22 | MUST | 60 min |
| ☑ | T4-005 | Protocol counts and port counts (Lane A) | Sep 23 | MUST | 60 min |
| ☑ | T4-006 | Stream filtering (named filters + filtered sub-stream) | Sep 24 | MUST | 60 min |
| ☑ | T4-007 | Sampling (Bernoulli and reservoir) with sample-vs-full comparison | Sep 24 | MUST | 75 min |
| ☑ | T4-008 | Count distinct — exact, HyperLogLog and Flajolet–Martin (Lane A + B) | Sep 25 | MUST | 90 min |
| ☐ | T4-009 | Counting ones — DGIM sliding-window estimator | Sep 26 | MUST | 90 min |
| ☐ | T4-010 | Group A unit + streaming validation tests | Sep 27 | SHOULD | 75 min |

### Yashwant Vadhan M — task checklist

| ✔ | Task | Title | Due | Priority | Effort |
|---|---|---|---|---|---|
| ☑ | T1-001 | Repository scaffold, ignores, ownership map | Sep 21 | MUST | 30 min |
| ☑ | T1-002 | Config loader and logging setup | Sep 21 | MUST | 45 min |
| ☑ | T1-004 | Serving-store DDL and DB helper | Sep 21 | MUST | 60 min |
| ☑ | T1-007 | Remote-access network for a distributed team (VPN overlay + SSH) | Sep 21 | MUST | 60 min |
| ☑ | T1-005 | Mock data seeder for dashboard/analytics development | Sep 22 | MUST | 60 min |
| ☑ | T1-006 | Dev tooling, CI and ENVIRONMENT.md | Sep 22 | SHOULD | 60 min |
| ☑ | T5-003 | Exponentially decaying window (recent-traffic score) | Sep 22 | MUST | 90 min |
| ☑ | T5-001 | Windowed moments — mean, variance, std, inter-arrival (Lane A) | Sep 23 | MUST | 60 min |
| ☑ | T5-005 | Edge and per-source aggregation (feeds Link Analysis and Alerts) | Sep 23 | MUST | 60 min |
| ☑ | T5-002 | AMS second-moment (F2) estimator and iat fallback | Sep 24 | MUST | 75 min |
| ☐ | T5-004 | Market-basket model and limited-pass frequent itemsets (A-Priori + PCY) | Sep 26 | MUST | 90 min |
| ☐ | T5-009 | Group B unit and integration tests | Sep 27 | SHOULD | 60 min |
| ☐ | T5-010 | Cross-validation with M A Sushil Kumar's results | Sep 27 | SHOULD | 45 min |
| ☐ | T8-001 | One-command demo script and environment verifier | Sep 27 | MUST | 90 min |
| ☐ | T5-006 | Multi-window operations (10 s / 30 s / 60 s) | Sep 28 | SHOULD | 45 min |
| ☐ | T5-007 | Load and high-volume test | Sep 28 | SHOULD | 75 min |
| ☐ | T8-002 | End-to-end scenario suite (the eight overview tests) | Sep 29 | SHOULD | 90 min |
| ☐ | T9-001 | Concept mapping document | Sep 29 | MUST | 60 min |
| ☐ | T5-008 | Retention cleanup for live serving tables | not scheduled | CUT | 30 min |

### Priyan S — task checklist

| ✔ | Task | Title | Due | Priority | Effort |
|---|---|---|---|---|---|
| ☐ | T6-001 | Graph builder from `ip_edges` | Sep 21 | MUST | 60 min |
| ☐ | T6-002 | Degree and centrality metrics | Sep 21 | MUST | 45 min |
| ☐ | T6-003 | PageRank (NetworkX and from-scratch power iteration) | Sep 21 | MUST | 75 min |
| ☐ | T6-004 | Markov transition model | Sep 22 | MUST | 75 min |
| ☐ | T7-001 | Streamlit skeleton, theme and sidebar | Sep 22 | MUST | 75 min |
| ☐ | T7-002 | Data-access layer with status model and mock fallback | Sep 22 | MUST | 60 min |
| ☐ | T7-003 | KPI strip and Live Overview tab | Sep 23 | MUST | 90 min |
| ☐ | T6-005 | Explainable alert engine | Sep 24 | MUST | 90 min |
| ☐ | T7-004 | Stream Analytics tab — filtering, sampling, distinct, counting ones | Sep 24 | MUST | 75 min |
| ☐ | T7-005 | Stream Analytics tab — moments, decaying window, market basket | Sep 25 | MUST | 90 min |
| ☐ | T6-006 | Link-analysis test pack and toy-graph fixtures | Sep 26 | SHOULD | 60 min |
| ☐ | T7-006 | Link Analysis tab — graph, PageRank, centrality, Markov | Sep 26 | MUST | 90 min |
| ☐ | T7-007 | Alerts tab | Sep 26 | MUST | 60 min |
| ☐ | T7-008 | History and Pipeline tabs | Sep 27 | MUST | 75 min |
| ☐ | T7-009 | States, auto-refresh, responsiveness and accessibility pass | Sep 28 | MUST | 90 min |
| ☐ | T7-010 | Dashboard tests | Sep 28 | SHOULD | 60 min |

### Everyone

| ✔ | Task | Title | Due | Priority |
|---|---|---|---|---|
| ☐ | T9-004 | Demo script, roles and three rehearsals | Sep 30 | MUST |

---

## 4. How to Use This File (token-efficient)

1. Open **one long AI-agent session per person** (Cursor / Claude Code / Gemini CLI / Codex) and paste the **Context Header** below **once**. For each task paste only that task's **AI Agent Prompt** — the header stays in the session context, which saves tokens.
2. Work in due-date order; respect `Dependencies`.
3. Check the **Acceptance Criteria**, run the tests the agent wrote.
4. `git pull --rebase origin main` → tests → `git push origin main` (Conventional Commit message). Tick your checklist.
5. If a dependency isn't ready, use the mock DB (`scripts/seed_mock_db.py`) or replay data and keep going.

Adaptations to the standard template: pipeline phases instead of Auth/DB/Backend/Frontend; *Assigned To* uses member names; every task has **Due**, **Priority** and an **AI Agent Prompt**. Effort = review + agent iteration time (15–90 min).

### 📋 Context Header (paste ONCE per session)

```text
PROJECT: Live Network Traffic Analyser (LNTA) — Big Data Analytics course project: real-time streaming analytics and link analysis on Wi-Fi traffic. DEADLINE: demo + submission 1 Oct 2026.
PIPELINE: Wi-Fi → TShark → CSV → Flume (TAILDIR, replicating selector) → HDFS /traffic/raw + /traffic/stream_in → Spark Structured Streaming (Lane A = native windowed queries, Lane B = foreachBatch plugins) → SQLite serving store → Streamlit dashboard. NetworkX for link analysis. Hive/Spark SQL for history.
READ FIRST: docs/PRD.md, docs/TECH_RULES.md (§3 component rules, §5 DATA CONTRACT, §6 coding standards, §10 Git rules). For dashboard tasks also docs/DESIGN.md.
RULES: Python 3.10/3.11, type hints, ruff-clean, snake_case. Field/column/table names must match TECH_RULES §5 EXACTLY — never invent, rename or reorder them. No hard-coded paths, hosts, ports or thresholds (use config/settings.yaml via common/config.py). Keep pure algorithm logic separate from Spark/IO so it is unit-testable with pytest. Write pytest tests in the same task. Never commit real captures or secrets. Never mention the syllabus unit number anywhere in code, docs or commit messages — use descriptive concept names instead. Edit only files inside my own folder; if the contract looks wrong or ambiguous, STOP and tell me.
GIT: everyone works on main. Finish by: git pull --rebase origin main && run tests && git push origin main. Never force-push.
OUTPUT FORMAT: (1) files created/changed, (2) how to run it, (3) how to run its tests, (4) assumptions, (5) anything you could not verify.
```

---

## Phase 1: Project Setup & Tooling
*Goal: Reproducible dev environment, folder structure, shared contract, CI skeleton. Everyone unblocked.*

#### T1-001: Repository scaffold, ignores, ownership map
**Description:** In the existing GitHub repository (clone it first: `git clone https://github.com/skoder404/Live-Network-Traffic-Analyser.git`), create the folder structure from TECH_RULES §4 with `__init__.py` files and placeholder READMEs, a `.gitignore` that excludes `data/` (except `data/sample/`), `logs/`, `serving/`, `.flume/`, `.venv/`, `config/settings.yaml`, `*.pcap*`, `.env`; a `docs/OWNERS.md` listing which member may edit which folder (everyone pushes directly to `main`; edit only your own folder); copy the six planning docs into `docs/`.
**Dependencies:** None
**Acceptance Criteria:**
- [ ] `tree` matches TECH_RULES §4; `git status` shows no ignored artefacts
- [ ] `docs/OWNERS.md` present and consistent with README §1 and TECH_RULES §10
- [ ] `python -c "import capture, streaming, linkanalysis, dashboard, common, contracts"` succeeds
**Estimated Effort:** 30 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Create the LNTA repository scaffold exactly per TECH_RULES §4 (folders, __init__.py, one-line README.md in each top-level component folder stating its owner). Add .gitignore (ignore data/ except data/sample/, logs/, serving/, .flume/, .venv/, __pycache__/, config/settings.yaml, .env, *.pcap, *.pcapng) and docs/OWNERS.md: a table member → folders they may edit (capture/ Naveena MS; ingestion/ Rithika GV; streaming/common,queries/window_metrics,counts,filter_counts,distinct + analytics/{filters,sampling,fm,dgim} M A Sushil Kumar; other streaming/ files Yashwant Vadhan M; linkanalysis/ and dashboard/ Priyan S; contracts/, common/, scripts/, config/ Yashwant Vadhan M with team-chat announcement for changes) plus the rules: everyone pushes to main, git pull --rebase before every push, never force-push, never push failing tests. Copy the six planning docs into docs/. Do not add any application code.
```

#### T1-002: Config loader and logging setup
**Description:** `common/config.py` loads `config/settings.yaml` (falls back to `settings.example.yaml`), supports env-var overrides (`LNTA_<SECTION>__<KEY>`), validates required keys with clear errors. `config/settings.example.yaml` documents every key (capture, flume, hdfs, spark, serving, graph, alerts, dashboard, timezone). `common/logging_setup.py` configures UTC logging to console + rotating `logs/<component>.log`.
**Dependencies:** T1-001
**Acceptance Criteria:**
- [ ] Missing/invalid key raises `ConfigError` naming the key
- [ ] Env override changes a nested value in a unit test
- [ ] Log lines match `%(asctime)s %(levelname)s %(name)s | %(message)s` in UTC
**Estimated Effort:** 45 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement common/config.py and common/logging_setup.py per TECH_RULES §6. Config: load YAML into a frozen dataclass tree (sections: capture{iface, rotate_seconds, rotate_mb, out_dir, sink}, flume{tcp_host, tcp_port}, hdfs{namenode_uri, root}, spark{trigger_s, watermark_s, windows_s, max_files_per_trigger, max_rows_per_batch, checkpoint_root}, serving{db_path}, graph{lookback_s, top_n_nodes}, alerts{rules_path}, dashboard{refresh_s, timezone, stale_after_s}, plus spark{plugins_enabled, top_n_ports, max_edges_per_window, slide_s, filters[], predicates[]}, itemsets{window_s, every_s, min_support, num_buckets}, decay{half_lives_s}, serving{cleanup_every_s, retention_hours}). Env override format LNTA_SPARK__TRIGGER_S=10. Also write config/settings.example.yaml with sensible defaults from TECH_RULES (trigger 5, watermark 30, windows [10,30,60], max_rows_per_batch 50000, timezone Asia/Kolkata, stale 15) and comments. Add tests in tests/unit/test_config.py (defaults, override, missing key error).
```

#### T1-003: Traffic record contract module (CSV schema v1)
**Description:** `contracts/record_schema.py` is the executable form of TECH_RULES §5.1: ordered field list, types, nullability, `validate_row(fields) -> (ok, reason)`, `format_row(record) -> str`, `RECORD_SCHEMA_VERSION`, and `to_spark_schema()` (imports pyspark lazily). Also write `docs/DATA_CONTRACT.md` summarising §5.1 + examples of valid/invalid rows.
**Dependencies:** T1-001
**Acceptance Criteria:**
- [ ] Example record from TECH_RULES validates; 12 invalid variants (bad IP, port 70000, protocol `HTTP`, length 0, wrong column count, bad timestamp…) each fail with a distinct reason
- [ ] `to_spark_schema()` returns 11 fields with the contract names/types
- [ ] `format_row(parse(line)) == line` round-trip test passes
**Estimated Effort:** 60 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement contracts/record_schema.py from TECH_RULES §5.1 (11 fields, order fixed, no header, CSV). Provide: FIELDS (name,type,nullable), RECORD_SCHEMA_VERSION="1", parse_line(line)->list[str|None], validate_row(fields)->tuple[bool,str] using ipaddress for IPs, range checks for ports (0–65535) and packet_length (1–65535), protocol ∈ {TCP,UDP,ICMP,OTHER}, timestamp format "YYYY-MM-DD HH:MM:SS.mmm" (UTC), MAC regex, tcp_flags hex, iat_ms>=0; format_row(dict)->str (empty string for None); to_spark_schema() building a StructType (timestamp kept as StringType — parsing happens in Spark). Add tests/unit/test_record_schema.py with ≥12 invalid cases each asserting the specific reason string, plus round-trip. Also write docs/DATA_CONTRACT.md (table + valid/invalid examples).
```

#### T1-004: Serving-store DDL and DB helper
**Description:** `contracts/serving_schema.sql` defines every table in TECH_RULES §5.2 with the stated keys, `updated_at`, and an index on `window_start`. `common/serving_db.py`: `connect(path, read_only=False)` (WAL, `busy_timeout=5000`, `synchronous=NORMAL`), `init_schema()`, `upsert(table, key_cols, rows)` that updates **only the columns provided** (partial upsert, so two writers can fill different columns of the same row), `cleanup(retention_hours)`.
**Dependencies:** T1-001, T1-002
**Acceptance Criteria:**
- [ ] `init_schema()` is idempotent and creates all 16 tables
- [ ] Partial upsert test: writer A sets exact columns, writer B sets `_hll` columns on the same key → both preserved
- [ ] Read-only connection rejects writes; cleanup deletes only rows older than retention
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Create contracts/serving_schema.sql implementing every table, key and column in TECH_RULES §5.2 exactly (timestamps as TEXT ISO-8601 UTC, updated_at TEXT DEFAULT current time, PRIMARY KEYs as listed, index on window_start where applicable). Then implement common/serving_db.py: connect(path, read_only=False) with PRAGMA journal_mode=WAL, busy_timeout=5000, synchronous=NORMAL, uri mode=ro for read-only; init_schema(conn); upsert(conn, table, key_cols, rows: list[dict]) using INSERT ... ON CONFLICT(key) DO UPDATE SET only for the non-key columns present in each row and always refresh updated_at; cleanup(conn, retention_hours, tables=None). Parameterised SQL only. Write tests/unit/test_serving_db.py covering idempotent init, partial-upsert merge, read-only failure, and cleanup.
```

#### T1-005: Mock data seeder for dashboard/analytics development
**Description:** `scripts/seed_mock_db.py` fills a serving DB with 10 minutes of realistic, internally consistent mock data for **all** tables (including a small IP graph with a hub, a spike window that should trigger an alert, itemsets like `TCP|443`, `UDP|53`). `--live` mode appends a new window every 2 s so Priyan S can develop the live view without Spark.
**Dependencies:** T1-004
**Acceptance Criteria:**
- [ ] Every table has rows; `sum(protocol_counts.packets) == window_metrics.packets` per window
- [ ] `--live` keeps adding windows at the configured rate until Ctrl-C
- [ ] Deterministic with `--seed`
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write scripts/seed_mock_db.py using common/serving_db.py. Generate 10 minutes of mock data at 10-second windows (window_len_s=10; also 30 and 60 aggregates) for ALL tables in TECH_RULES §5.2 with internally consistent numbers: protocol_counts sum to window_metrics packets; distinct_counts exact vs hll within ±5%, fm within ±25%; counting_ones dgim within 10%; moments consistent with a packet-size distribution; frequent_itemsets for both algorithms ('apriori','pcy') containing TCP:443, UDP:53, TCP:80 and the pair 'UDP:53 + TCP:443' (passes=2); ip_edges forming a 12-node graph with one hub and one dangling node; one spike window (3× baseline) in the last 2 minutes; pipeline_health rows. Options: --db PATH, --seed N, --live (append a new 10 s window every 2 s), --reset. Add a small test that seeds into tmp_path and asserts cross-table consistency.
```

#### T1-006: Dev tooling, CI and ENVIRONMENT.md
**Description:** `requirements.txt` / `requirements-dev.txt` (pinned ranges), `pyproject.toml` (ruff, pytest, coverage config), `.pre-commit-config.yaml` (ruff + a hook that blocks `*.pcap*` and non-sample files containing real-looking IPs), GitHub Actions workflow (ruff + unit tests excluding `spark` marker), and `docs/ENVIRONMENT.md` — a table each member fills with the **verified** versions of Java, Hadoop, Flume, Spark, Hive, Python, TShark.
**Dependencies:** T1-001
**Acceptance Criteria:**
- [ ] `pre-commit run --all-files` and CI pass on a clean checkout
- [ ] Committing a `.pcap` file is blocked locally
- [ ] `pytest -m "not spark"` runs in CI; `pytest -m spark` is opt-in
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 22 (Tue) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Set up dev tooling: requirements.txt (pyspark 3.5.*, pandas, networkx, streamlit, plotly, pyyaml, numpy) and requirements-dev.txt (pytest, pytest-cov, ruff, pre-commit, mypy); pyproject.toml with ruff (line-length 100, select E,F,I,UP,B), pytest (markers: spark, slow, e2e) and coverage; .pre-commit-config.yaml with ruff and a local hook script scripts/check_no_captures.py that fails on *.pcap/*.pcapng anywhere and on files outside data/sample/ containing non-example private IPv4 addresses; .github/workflows/ci.yml running ruff and `pytest -m "not spark and not e2e"` with coverage. Create docs/ENVIRONMENT.md with a table (Component | Version | Verified by | Date) pre-filled with the baseline from README §4 and TODO markers. Do not pin pyspark to a newer major version than 3.5.
```

#### T1-007: Remote-access network for a distributed team (VPN overlay + SSH)
**Description:** The team is in different places (two homes, one hostel), behind NAT, so direct SSH will not work. Create a private overlay network (Tailscale recommended) and give every member SSH access to **Laptop A** (Yashwant Vadhan M's Ubuntu) and Laptop B (M A Sushil Kumar's Ubuntu). Install `openssh-server`, key-only login, one Linux user per member, `tmux` for persistent sessions, sleep/lid-close disabled, and a written access guide. Laptop A ↔ Laptop B capture traffic (TCP 44444) also travels over this network.
**Dependencies:** None
**Acceptance Criteria:**
- [ ] Each of the five members can `ssh <user>@<overlay-name-or-ip>` to Laptop A **from their own network** (hostel/home) with a key, no password
- [ ] Laptop B → Laptop A TCP test passes over the overlay (`nc -vz <A> 44444` with a dummy listener)
- [ ] Laptop A stays up: suspend on lid close disabled, plugged in; a `tmux` session survives a dropped SSH connection
- [ ] No router port-forwarding and no SSH port exposed to the public internet
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Integration) — M A Sushil Kumar on Laptop B; every member installs the client on their own device
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write docs/REMOTE_ACCESS.md and scripts/setup_remote_access.sh for an Ubuntu 22.04/24.04 laptop that must be reachable by 4 teammates on other networks (behind NAT/CGNAT, no port forwarding). Use Tailscale as the overlay (install via the official apt repo instructions, `tailscale up`, node sharing to teammates' accounts; note that free-plan limits may have changed so the doc must tell us to verify them, and give ZeroTier as the fallback). The script must: install and enable openssh-server; set PasswordAuthentication no and PermitRootLogin no; create a Linux user per member from a list (no sudo except the owner); install authorized_keys from files in scripts/keys/<user>.pub; install tmux and git; disable suspend/hibernate on lid close and AC (logind.conf + gsettings where applicable); print the overlay IP and a test command. The doc must include: client setup for Windows/Ubuntu teammates, VS Code Remote-SSH steps, `ssh -L` port-forward commands to view the dashboard (8501), Spark UI (4040) and HDFS UI (9870) in a local browser, a tmux cheat-sheet, etiquette for sharing one 16 GB machine (booking slots, who runs Spark when), and a security checklist. Do not include any real keys or IPs.
```

---

## Phase 2: Traffic Capture & Preprocessing (Naveena MS)
*Goal: Clean, contract-compliant traffic records from live Wi-Fi — plus synthetic/replay data so nobody is blocked.*

#### T2-001: TShark setup, permissions and Wi-Fi interface detection
**Description:** Install TShark/Wireshark, grant non-root capture (wireshark group or `setcap`), and write `capture/list_interfaces.py` which lists interfaces (via `tshark -D`), flags wireless ones (Linux: `/sys/class/net/<if>/wireless`), shows whether each has an IP, and supports `--auto` to pick the Wi-Fi interface. Document setup and troubleshooting in `capture/README.md`.
**Dependencies:** T1-001
**Acceptance Criteria:**
- [ ] `python -m capture.list_interfaces` prints a table with name, description, wireless?, has-IP?
- [ ] `--auto` selects the connected Wi-Fi interface (or exits with code 2 and a helpful message)
- [ ] Capturing works without `sudo` on your machine (documented steps)
**Estimated Effort:** 45 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/list_interfaces.py. Use subprocess to call `tshark -D`, parse the numbered list, and enrich each interface: wireless (Linux: exists /sys/class/net/<name>/wireless; otherwise heuristic on names wlan*/wlp*/wl*/Wi-Fi), has_ip (via `ip -o addr` on Linux, fallback psutil-free parsing). Print an aligned table. CLI: --auto prints only the chosen interface name (first wireless with an IP) and exits 2 with a clear message if none. Keep parsing in pure functions with unit tests using canned `tshark -D` outputs (tests/fixtures/tshark_D.txt). Also write capture/README.md covering install (apt), non-root capture (dpkg-reconfigure wireshark-common + usermod -aG wireshark, or setcap on dumpcap), and a troubleshooting section (permission denied, interface down, WSL2 cannot see host Wi-Fi → use --sink tcp from a native OS).
```

#### T2-002: TShark command builder
**Description:** `capture/tshark_cmd.py` builds the exact TShark argv list for a given interface: `-i`, `-l`, `-n` (no name resolution), `-T fields`, `-E separator=,`, `-E occurrence=f`, `-E header=n`, `-E quote=n`, and `-e` for each field in TECH_RULES §3.1, in a fixed order exposed as `RAW_FIELDS`.
**Dependencies:** T1-002
**Acceptance Criteria:**
- [ ] `build_command("wlan0")` returns the list with all flags and 16 `-e` fields in the documented order
- [ ] Optional capture filter (`-f`) and duration limit (`-a duration:N`) supported
- [ ] Unit test asserts the exact argv
**Estimated Effort:** 30 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Create capture/tshark_cmd.py exposing RAW_FIELDS (ordered list: frame.time_epoch, ip.src, ip.dst, ipv6.src, ipv6.dst, tcp.srcport, tcp.dstport, udp.srcport, udp.dstport, ip.proto, ipv6.nxt, frame.len, eth.src, eth.dst, tcp.flags, frame.time_delta) and build_command(iface, capture_filter=None, duration_s=None, tshark_path="tshark") -> list[str] that returns argv: tshark -i <iface> -l -n -T fields -E separator=, -E occurrence=f -E header=n -E quote=n plus one -e per field (and -f / -a duration:N when provided). No shell strings. Add tests/unit/test_tshark_cmd.py asserting the full argv and the optional flags.
```

#### T2-003: Line parser / normaliser (raw TShark line → contract record)
**Description:** `capture/parser.py::parse_line(raw) -> Record | Reject`. Coalesce `ip.src|ipv6.src`, `ip.dst|ipv6.dst`, `tcp.srcport|udp.srcport`, etc.; derive `protocol` from `ip.proto`/`ipv6.nxt` (6→TCP, 17→UDP, 1/58→ICMP, else OTHER); convert epoch to UTC `YYYY-MM-DD HH:MM:SS.mmm`; `iat_ms = frame.time_delta × 1000`; handle missing/incomplete fields; reject non-IP frames and malformed lines with an enum reason. Validate via `contracts.record_schema`.
**Dependencies:** T1-003, T2-002
**Acceptance Criteria:**
- [ ] ≥ 15 unit tests: TCP v4, UDP v4, UDP v6 DNS, ICMP (empty ports), ICMPv6, ARP/no-IP (reject `NON_IP`), short line, non-numeric length, missing MAC (kept, empty), unknown protocol → `OTHER`
- [ ] Output line passes `validate_row`
- [ ] Parser is pure (no IO), ≥ 90% coverage
**Estimated Effort:** 90 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/parser.py. Input: one comma-separated line in the exact order of capture.tshark_cmd.RAW_FIELDS (empty string = missing). Output: a Record dataclass (contract fields) or Reject(reason: RejectReason enum: WRONG_COLUMNS, NON_IP, BAD_TIMESTAMP, BAD_LENGTH, BAD_PORT, BAD_IP, VALIDATION). Rules: src_ip = ip.src or ipv6.src; dst_ip likewise; ports = tcp.* else udp.* else None; protocol from ip.proto or ipv6.nxt (6 TCP, 17 UDP, 1 or 58 ICMP, else OTHER; if both empty → NON_IP); timestamp = frame.time_epoch → UTC 'YYYY-MM-DD HH:MM:SS.mmm' (truncate not round); iat_ms = float(frame.time_delta)*1000 or None; tcp_flags kept as hex string; MACs lowercase or None. Validate with contracts.record_schema.validate_row. Write ≥15 pytest cases in tests/unit/test_parser.py using literal lines (TCP/443, UDP/53 IPv6, ICMP with empty ports, ARP, malformed) and keep the module free of IO.
```

#### T2-004: Capture runner (subprocess, queue, restart, stats)
**Description:** `capture/run_capture.py` runs TShark as a subprocess, reads stdout in a thread into a **bounded queue**, parses lines, and hands records to a writer. Auto-restarts TShark on unexpected exit (exponential back-off, max 5), shuts down cleanly on SIGINT/SIGTERM, drops-with-counter if the queue is full, and every 10 s writes `logs/capture_stats.json` (frames_seen, records_written, rejects_by_reason, dropped_queue_full, uptime, records_per_sec).
**Dependencies:** T2-003, T2-005 (writer interface), T1-002
**Acceptance Criteria:**
- [ ] Live 60 s capture produces valid records; stats file updates every 10 s
- [ ] Killing the TShark child causes a restart; Ctrl-C flushes and exits 0
- [ ] Reject lines go to `data/capture/rejects_<date>.log` with reason
**Estimated Effort:** 90 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/run_capture.py (entry: python -m capture.run_capture --iface wlan0|--auto [--sink file|tcp|stdout] [--format csv|json] [--filter BPF] [--duration S]). Use subprocess.Popen(build_command(...), stdout=PIPE, stderr=PIPE, text=True, bufsize=1). A reader thread pushes lines into queue.Queue(maxsize from config); on full queue drop and count. A consumer loop calls parser.parse_line and writer.write(record); rejects go to a rejects log with reason. Restart TShark on unexpected exit with exponential back-off (1,2,4,8,16 s, max 5) then exit 1. Handle SIGINT/SIGTERM: stop the child, drain the queue, flush the writer, exit 0. Every 10 s atomically write logs/capture_stats.json with frames_seen, records_written, rejects_by_reason, dropped_queue_full, uptime_s, records_per_sec. Read all settings from common.config. Add tests using a fake TShark script (tests/fixtures/fake_tshark.py) to test restart, drain and stats.
```

#### T2-005: Output writers (rotating CSV, JSON-lines, TCP, stdout)
**Description:** `capture/writers.py` with a common `Writer` interface: `CsvRotatingWriter` (new file `traffic_YYYYMMDD_HHMMSS.csv` every `rotate_seconds` or `rotate_mb`; **headerless**; flush ≤ 1 s; rotated files are never modified again), `JsonLinesWriter` (same field names), `TcpLineWriter` (reconnect with back-off; for the Flume `netcat` variant), `StdoutWriter`.
**Dependencies:** T1-003
**Acceptance Criteria:**
- [ ] Rotation by time and by size verified with a fake clock
- [ ] CSV lines validate against the contract; JSON lines contain identical field names
- [ ] TCP writer survives server restart in a test using a local socket server
**Estimated Effort:** 60 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/writers.py with an abstract Writer (write(record), flush(), close()) and four implementations per TECH_RULES §5.1: CsvRotatingWriter (out_dir, rotate_seconds, rotate_mb, injectable clock; filename traffic_YYYYMMDD_HHMMSS.csv; NO header line; flush at least every 1 s; after rotation never touch the old file; create the new file only when the first record arrives), JsonLinesWriter (one JSON object per line, contract field names, nulls as null), TcpLineWriter (host, port; reconnect with back-off; buffer up to N lines while disconnected then drop-with-counter), StdoutWriter. Tests in tests/unit/test_writers.py: rotation by time/size with fake clock, header-less output, JSON equivalence, TCP reconnect using a local socketserver.
```

#### T2-006: Synthetic traffic generator with scenarios and ground truth
**Description:** `capture/generate.py` produces contract-compliant CSV for named scenarios — `normal`, `spike`, `portscan_like` (one source → many dst ports), `fanout` (one source → many dst IPs), `dns_heavy`, `multi_host_graph` (known small graph for PageRank/Markov tests) — and a sidecar `<name>.truth.json` with exact counts (packets, bytes, unique IPs/ports, protocol counts, mean/variance of packet_length) for test assertions.
**Dependencies:** T1-003
**Acceptance Criteria:**
- [ ] Same `--seed` → byte-identical output; all rows validate
- [ ] `truth.json` numbers equal pandas recomputation from the CSV (test)
- [ ] `spike` scenario has a clear 3–5× rate increase in the last third; `multi_host_graph` contains the toy graph A→B, A→C, C→B, D→B
**Estimated Effort:** 90 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/generate.py (CLI: --scenario NAME --duration S --rate PPS --seed N --out PATH). Scenarios: normal (mostly TCP/443, some UDP/53 DNS, TCP/80, a few ICMP; one private client 192.168.1.10 plus 2–3 other LAN hosts; realistic packet-length mix ~ 60–1500 bytes), spike (normal, then 3–5× rate for the last third), portscan_like (one src hitting >25 distinct dst ports within 10 s), fanout (one src hitting >40 distinct dst IPs in 10 s), dns_heavy (UDP/53 dominant), multi_host_graph (deterministic edges implementing A→B, A→C, C→B, D→B plus replies). Timestamps monotonic UTC in contract format; iat_ms consistent with timestamps; use only documentation/private ranges (192.168.x.x, 10.x.x.x, 198.51.100.x, 203.0.113.x). Also write <out>.truth.json with packets, bytes, unique src/dst IPs, unique ports, protocol counts, mean and population/sample variance of packet_length, per-10s-window packet counts. Tests: determinism by seed, contract validity, truth.json vs pandas.
```

#### T2-007: Replay tool (paced, re-stamped)
**Description:** `capture/replay.py` reads a contract CSV and re-emits it through the same writers as live capture into `data/capture/`. `--restamp` rewrites timestamps to "now" preserving inter-arrival gaps (needed so Spark's event-time windows/watermark work); `--speed X` scales pacing; `--pps N` uses fixed rate; `--loop` repeats.
**Dependencies:** T2-005, T2-006
**Acceptance Criteria:**
- [ ] Re-stamped output timestamps are within 1 s of wall clock and monotonic
- [ ] `--speed 2` halves total duration (±5%) in a test with a fake clock
- [ ] Output files are indistinguishable (format-wise) from live capture output
**Estimated Effort:** 45 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/replay.py: python -m capture.replay --file data/sample/normal.csv [--speed 1.0|--pps N] [--restamp] [--loop] [--sink file|tcp]. Read the CSV (contract format, no header), compute original inter-arrival gaps from timestamps, sleep accordingly (injectable clock/sleep for tests), and write via capture.writers so the output is identical in format to live capture. With --restamp, rewrite each timestamp to (start_time_now + cumulative gap) in contract format, and recompute iat_ms. Handle Ctrl-C cleanly. Tests use a fake clock: pacing, speed scaling, restamp monotonicity, loop.
```

#### T2-008: Anonymiser and committed sample datasets
**Description:** `capture/anonymize.py` maps IPs with a keyed HMAC to stable pseudo-addresses (private→`10.x.y.z`, public→`198.18.x.y`), hashes MACs to locally administered MACs, keeps ports/protocol/length/timing. Generate and commit six 5–10 min datasets to `data/sample/` (from the generator, plus optionally one anonymised real capture) with a `data/sample/README.md` describing each.
**Dependencies:** T2-006
**Acceptance Criteria:**
- [ ] Same key → same mapping; different key → different mapping; private/public class preserved
- [ ] No real IPs/MACs in `data/sample/` (pre-commit hook passes)
- [ ] Each sample has its `.truth.json`
**Estimated Effort:** 45 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement capture/anonymize.py (CLI: --in FILE --out FILE --key-env LNTA_ANON_KEY). Use HMAC-SHA256(key, ip) to derive a deterministic pseudo-address: private IPv4 → 10.a.b.c; public IPv4 → 198.18.b.c (benchmarking range); IPv6 → fd00:: prefix + hash; MAC → locally-administered unicast (02:xx:...). Preserve everything else. Then write scripts/make_samples.sh that runs the generator for normal, spike, portscan_like, fanout, dns_heavy, multi_host_graph (300–600 s each, fixed seeds) into data/sample/ together with truth.json, and data/sample/README.md describing each scenario and what it should trigger. Tests: determinism, class preservation, key sensitivity, output validates against the contract.
```

#### T2-009: Capture integration test and Wireshark validation
**Description:** An integration test replays a recorded TShark output fixture through parser + writer. `scripts/compare_capture.py` compares CSV record count against `tshark -r <pcap> -Y ip||ipv6 | wc` for a parallel `dumpcap` capture over 60 s. `docs/validation_capture.md` documents the manual Wireshark I/O-graph comparison.
**Dependencies:** T2-004, T2-005
**Acceptance Criteria:**
- [ ] Fixture-based integration test passes in CI (no live capture needed)
- [ ] 60 s live comparison shows ≤ 1% difference on IP frames (record result in the doc)
- [ ] Procedure is reproducible by another member
**Estimated Effort:** 60 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 24 (Thu) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create (1) tests/fixtures/tshark_sample.txt with ~200 realistic raw TShark lines (TCP, UDP, ICMP, IPv6, ARP) and tests/integration/test_capture_pipeline.py that feeds them through parser → CsvRotatingWriter and asserts counts, reject reasons and contract validity; (2) scripts/compare_capture.py --pcap FILE --csv-glob 'data/capture/*.csv' that counts IP/IPv6 frames in the pcap using `tshark -r FILE -Y "ip || ipv6" -T fields -e frame.number` and compares with CSV record count within the same time range, printing PASS/FAIL with percentage difference (threshold 1%); (3) docs/validation_capture.md describing the 60-second procedure (dumpcap in parallel, Wireshark I/O graph comparison) and a results table to fill in.
```

---

## Phase 3: Stream Ingestion & Storage (Rithika GV)
*Goal: Reliable Flume → HDFS ingestion with raw, streaming-feed and historical zones, Hive tables, and proof of zero loss.*

#### T3-001: HDFS pseudo-distributed setup and zone layout
**Description:** Install/configure Hadoop 3.3.x in pseudo-distributed mode (replication 1), start HDFS, and create the zone layout from TECH_RULES §3.4 with an idempotent script. Provide `start_hdfs.sh` / `stop_hdfs.sh`.
**Dependencies:** T1-001
**Acceptance Criteria:**
- [ ] `hdfs dfs -ls /traffic` shows `raw`, `stream_in`, `processed`, `historical`, `checkpoints`
- [ ] `init_layout.sh` can be re-run safely; NameNode UI reachable (default port 9870)
- [ ] Put/cat/rm smoke test passes as a non-root user
**Estimated Effort:** 60 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write ingestion/hdfs/init_layout.sh (idempotent: hdfs dfs -mkdir -p for /traffic/{raw,stream_in,processed,historical,checkpoints}; set ownership to the current user; no chmod 777), ingestion/scripts/start_hdfs.sh and stop_hdfs.sh (wrap start-dfs.sh/stop-dfs.sh, wait until `hdfs dfsadmin -safemode get` reports OFF, print NameNode URL), and ingestion/hdfs/README.md with the minimal core-site.xml (fs.defaultFS) and hdfs-site.xml (dfs.replication=1, local name/data dirs) needed for pseudo-distributed mode on Ubuntu, including SSH-localhost setup and common errors. Config values (namenode URI) must come from config/settings.yaml when scripts need them. Add a smoke-test script ingestion/scripts/hdfs_smoke.sh.
```

#### T3-002: Flume install and Hadoop-compatibility check
**Description:** Install Flume (1.11.x), set `JAVA_HOME`/`HADOOP_HOME`, and prove the HDFS sink works with your Hadoop version using a throwaway agent (netcat source → memory channel → hdfs sink). Record working versions in `docs/ENVIRONMENT.md`; if the bundled Hadoop client libs conflict, document the fix.
**Dependencies:** T3-001
**Acceptance Criteria:**
- [ ] `flume-ng version` prints; test agent writes lines from `nc` into an HDFS file
- [ ] Any classpath/jar fix is written down step-by-step
- [ ] ENVIRONMENT.md updated with Java/Hadoop/Flume versions and date
**Estimated Effort:** 60 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Help me verify Apache Flume (1.11.x) against Hadoop 3.3.x on Ubuntu. Produce: ingestion/flume/conf/flume-env.sh (JAVA_HOME, HADOOP_HOME, FLUME_CLASSPATH additions, sensible heap), ingestion/flume/test-agent.conf (netcat source on localhost:44444 → memory channel → hdfs sink to hdfs://<namenode>/traffic/_flume_test with fileType=DataStream, writeFormat=Text, rollInterval=10), ingestion/scripts/flume_compat_test.sh that starts the agent, sends 100 lines with nc, waits, and verifies 100 lines in HDFS then cleans up, and a troubleshooting section in ingestion/flume/README.md for ClassNotFound/NoSuchMethod errors caused by Hadoop client library version mismatch (explain how to put the Hadoop jars on the Flume classpath). Do not assume versions — print detected versions in the script output.
```

#### T3-003: Flume agent config — TAILDIR source, channels, replicating selector
**Description:** In `ingestion/flume/traffic-agent.conf` define the source `src_capture` (TAILDIR over `data/capture/traffic_*.csv`, position file, `batchSize=100`), two channels (`ch_raw` file channel for durability, `ch_stream` memory channel for latency), and `selector.type=replicating`. Also ship `traffic-agent-netcat.conf` — the variant that receives lines over TCP for multi-machine capture.
**Dependencies:** T3-002, T1-002
**Acceptance Criteria:**
- [ ] Agent starts with `logger` sinks attached temporarily and prints captured lines from a replay
- [ ] Restarting the agent does not re-emit already-read lines (position file)
- [ ] Netcat variant accepts lines from `capture --sink tcp`
**Estimated Effort:** 45 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write ingestion/flume/traffic-agent.conf (agent name `lnta`) implementing TECH_RULES §3.3 for the SOURCE and CHANNEL parts only: TAILDIR source `src_capture` (filegroup f1 matching <repo>/data/capture/traffic_.*\.csv, positionFile <repo>/.flume/taildir_position.json, batchSize 100, fileHeader false), replicating selector to channels ch_raw (type=file with checkpointDir/dataDirs under <repo>/.flume/) and ch_stream (type=memory, capacity 100000, transactionCapacity 1000). Attach temporary logger sinks so the config is testable now. Also write traffic-agent-netcat.conf identical except the source is `netcat` bound to 0.0.0.0:44444. Use a small script ingestion/scripts/render_conf.py that substitutes absolute repo paths/hosts from config/settings.yaml into a generated conf (do not hard-code paths). Document how to test with `python -m capture.replay`.
```

#### T3-004: Flume HDFS sinks and "in-progress files are invisible" test
**Description:** Add the two HDFS sinks: `sink_raw` (`/traffic/raw/dt=%Y-%m-%d/hr=%H`, `rollInterval=300`, `useLocalTimeStamp=true`) and `sink_stream` (`/traffic/stream_in`, `rollInterval=5`), both with `inUsePrefix=.` and `inUseSuffix=.tmp`, `fileType=DataStream`, `writeFormat=Text`. Prove Spark's file source ignores in-progress files.
**Dependencies:** T3-003, T3-001
**Acceptance Criteria:**
- [ ] Replaying 5 min of sample traffic yields files in both zones; raw path is partitioned by `dt`/`hr`
- [ ] While a file is open it is named `.<name>.tmp` and a `spark.read.csv` on the folder does not count its rows
- [ ] `stream_in` closes a file at least every ~5 s while traffic flows
**Estimated Effort:** 60 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Extend ingestion/flume/traffic-agent.conf with the two HDFS sinks from TECH_RULES §3.3 (sink_raw on ch_raw; sink_stream on ch_stream), replacing the temporary logger sinks: hdfs.fileType=DataStream, hdfs.writeFormat=Text, hdfs.filePrefix=traffic, hdfs.rollSize=0, hdfs.rollCount=0, hdfs.inUsePrefix=., hdfs.inUseSuffix=.tmp, hdfs.idleTimeout=60, hdfs.useLocalTimeStamp=true for the partitioned raw path, roll intervals 300 s (raw) and 5 s (stream_in), hdfs.batchSize=100. Then write ingestion/scripts/test_inuse_hidden.py (PySpark) that: starts a replay, lists /traffic/stream_in while a .tmp file exists, and asserts that spark.read.csv(path) row count excludes the in-progress file's rows. Print PASS/FAIL. If it FAILS, document the fallback (write to a staging dir then atomic rename) in ingestion/flume/README.md.
```

#### T3-005: Flume control script and status JSON
**Description:** `ingestion/scripts/flume_ctl.sh start|stop|restart|status|logs` manages the agent (PID file, log dir). `status` also writes `logs/ingestion_status.json` (process alive, position-file age, newest HDFS file age in `stream_in`, files in last minute) for the dashboard's Pipeline tab.
**Dependencies:** T3-004
**Acceptance Criteria:**
- [ ] `start` is idempotent; `stop` terminates cleanly; `status` returns exit 0 only if healthy
- [ ] `ingestion_status.json` has `agent_alive`, `position_file_age_s`, `stream_in_newest_age_s`, `files_last_60s`
- [ ] Works after reboot with a stale PID file
**Estimated Effort:** 45 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 23 (Wed) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write ingestion/scripts/flume_ctl.sh (bash, set -euo pipefail) supporting start|stop|restart|status|logs for the `lnta` agent using flume-ng agent -n lnta -c conf -f <generated conf>, with a PID file in .flume/, logs in logs/flume.log, and stale-PID handling. `status` must also call a small python helper ingestion/scripts/ingestion_status.py that writes logs/ingestion_status.json atomically with agent_alive, position_file_age_s, stream_in_newest_age_s (via `hdfs dfs -ls -t`), files_last_60s, and exits non-zero if the agent is down or stream_in is stale > 30 s while capture files are growing. Include shellcheck-clean code and a test of the python helper's parsing using canned `hdfs dfs -ls` output.
```

#### T3-006: Ingestion verification (loss, duplicates, latency)
**Description:** `ingestion/scripts/verify_ingestion.py` compares lines in `data/capture/*.csv` with lines in HDFS `raw` and `stream_in` (excluding `.tmp`), detects duplicates (line-hash multiset), and estimates latency (newest record timestamp vs HDFS file modification time). Exit non-zero on loss/dup above threshold.
**Dependencies:** T3-004
**Acceptance Criteria:**
- [ ] 5-min replay: captured == raw == stream_in (0 loss, 0 duplicates)
- [ ] Killing/restarting Flume mid-run is detected correctly if it causes duplicates/loss
- [ ] Prints a summary table and writes `docs/ingestion_report.md`
**Estimated Effort:** 60 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement ingestion/scripts/verify_ingestion.py (args: --capture-glob, --since ISO, --loss-threshold-pct 0.1). It counts lines in local capture CSVs, and in HDFS /traffic/raw/** and /traffic/stream_in (skip names starting with '.' or ending .tmp) by streaming `hdfs dfs -cat` (or via pyarrow if available — keep it optional). Compute loss %, duplicate lines (Counter of line hashes: count>1 beyond the multiplicity in the capture set), and latency estimate = max(record timestamp in HDFS) vs HDFS modification time. Print a summary table, write docs/ingestion_report.md, exit 1 on threshold breach. Keep counting logic in pure functions with unit tests using in-memory iterables.
```

#### T3-007: Raw compaction and stream_in retention
**Description:** `ingestion/scripts/compact_raw.py` (Spark batch) reads a closed hour of `/traffic/raw/dt=…/hr=…` text CSV using the contract schema and writes coalesced Parquet to `/traffic/historical/dt=…/hr=…`. `cleanup_stream_in.sh` deletes `stream_in` files older than 24 h (configurable). Neither touches the current hour or open files.
**Dependencies:** T3-004, T1-003
**Acceptance Criteria:**
- [ ] Row counts before/after compaction are equal (script asserts)
- [ ] Re-running for the same hour is idempotent (overwrite that partition only)
- [ ] Cleanup never removes files newer than retention or any `.tmp`
**Estimated Effort:** 60 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** not scheduled · **Priority:** ⚫ CUT — skip unless everything else is done
**AI Agent Prompt:**
```text
Write ingestion/scripts/compact_raw.py (PySpark batch; args --dt YYYY-MM-DD --hr HH or --last-closed-hour). Read /traffic/raw/dt=<dt>/hr=<hr> as CSV with contracts.record_schema.to_spark_schema() (header=false), cast timestamp to TimestampType (UTC session tz), add partition columns dt/hr, coalesce to a small number of files, write Parquet to /traffic/historical/ with partitionOverwriteMode=dynamic so only that partition is replaced. Assert input count == output count. Also write ingestion/scripts/cleanup_stream_in.sh --retention-hours 24 using `hdfs dfs -ls` mtime filtering, skipping names starting with '.' or ending .tmp, with a --dry-run. Tests for the pure date/hour selection and mtime filtering logic.
```

#### T3-008: Hive metastore and external tables
**Description:** Set up Hive (embedded Derby metastore is acceptable for the demo). DDL: `traffic_raw` (external, CSV, partitioned by `dt`,`hr`, location `/traffic/raw`) and `traffic_hist` (external, Parquet, partitioned, location `/traffic/historical`). `add_partitions.sh` runs `MSCK REPAIR TABLE` (or `ALTER TABLE ADD PARTITION`) on demand or every N minutes.
**Dependencies:** T3-004, T3-007
**Acceptance Criteria:**
- [ ] `SELECT count(*) FROM traffic_raw` equals the ingested line count for the partitions present
- [ ] New hourly partitions become queryable after `add_partitions.sh`
- [ ] Setup steps and known version-compat notes recorded in ENVIRONMENT.md
**Estimated Effort:** 90 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 25 (Fri) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create ingestion/hive/ddl.sql: CREATE EXTERNAL TABLE traffic_raw (timestamp STRING, src_ip STRING, dst_ip STRING, src_port INT, dst_port INT, protocol STRING, packet_length INT, src_mac STRING, dst_mac STRING, tcp_flags STRING, iat_ms DOUBLE) PARTITIONED BY (dt STRING, hr STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/traffic/raw'; and traffic_hist (same columns but timestamp as TIMESTAMP, STORED AS PARQUET, LOCATION '/traffic/historical'). Column names must equal TECH_RULES §5.1 (quote/backtick reserved words like `timestamp` if Hive requires). Write ingestion/hive/README.md with Hive 3.1.x install notes on Hadoop 3.3.x with an embedded Derby metastore (schematool -initSchema), and ingestion/scripts/add_partitions.sh [--every-min N] using MSCK REPAIR TABLE. Also a test script ingestion/scripts/hive_smoke.sh comparing count(*) with verify_ingestion counts. List any version-compatibility problems encountered (guava/log4j jar conflicts are common) rather than hiding them.
```

#### T3-009: Historical query pack and exporter to the serving store
**Description:** Six SQL files in `ingestion/hive/sql/historical/`: `protocol_totals`, `top_src_ips`, `top_dst_ips`, `traffic_by_hour`, `avg_packet_size`, `traffic_trend`. `run_historical.py` executes them (Spark SQL with Hive support) and stores results in `hist_results` (`query_name`, `run_at`, `columns_json`, `rows_json`); `--every N` reruns periodically.
**Dependencies:** T3-008, T1-004
**Acceptance Criteria:**
- [ ] All six queries return rows on sample data and are parameterised by a `--since` window
- [ ] `hist_results` upserts one row per query per run (latest kept + last 5 runs)
- [ ] Row cap (e.g. 100) enforced so the dashboard never loads huge results
**Estimated Effort:** 75 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 26 (Sat) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write six Hive/Spark-SQL files under ingestion/hive/sql/historical/ against traffic_hist/traffic_raw: protocol_totals (packets, bytes by protocol), top_src_ips and top_dst_ips (top 20 by bytes), traffic_by_hour (packets/bytes per dt,hr), avg_packet_size (per protocol), traffic_trend (packets per 10-minute bucket). Use a :since parameter. Implement ingestion/scripts/run_historical.py: SparkSession with enableHiveSupport, run each file, cap 100 rows, serialise columns/rows as JSON and upsert into hist_results via common.serving_db (keep the latest 5 runs per query). Options --since, --every SECONDS, --queries a,b,c. Add tests for the JSON serialisation/row-cap helper and a seed-data based integration test marked `spark`.
```

#### T3-010: Spark ↔ HDFS connection helper (secondary duty)
**Description:** Help M A Sushil Kumar/Yashwant Vadhan M connect Spark to HDFS: `docs/spark_hdfs_connect.md` (URI formats, required env vars, checkpoint locations) and `scripts/smoke_read_stream_in.py`, a 30-second Structured Streaming job that reads `stream_in` with the contract schema and prints per-batch counts.
**Dependencies:** T3-004, T1-003
**Acceptance Criteria:**
- [ ] With a running replay, the smoke script prints increasing counts every trigger
- [ ] Doc lists the exact `spark-submit` command and how to view the Spark UI
- [ ] M A Sushil Kumar confirms it works on his machine (confirm in team chat)
**Estimated Effort:** 45 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write scripts/smoke_read_stream_in.py: create a SparkSession (from streaming.common.session if it exists, otherwise minimal with spark.sql.session.timeZone=UTC), readStream.schema(<contract schema>).option("header","false").option("maxFilesPerTrigger", cfg).csv(hdfs stream_in path from config), then writeStream with foreachBatch printing batch_id, row count and min/max timestamp, trigger processingTime cfg.spark.trigger_s, run for --seconds 30 and stop cleanly. Write docs/spark_hdfs_connect.md: hdfs:// URI format, HADOOP_CONF_DIR, spark-submit example, where checkpoints live (/traffic/checkpoints/<query>), how to open the Spark UI, and the three most common errors (connection refused to NameNode, schema mismatch, permission denied) with fixes.
```

#### T3-011: Failure and restart tests for ingestion
**Description:** Document and run three fault tests: (1) kill Flume mid-replay → restart, (2) stop the NameNode for 30 s → restart, (3) stop capture for 1 min → resume. Record outcomes (loss/duplicates) in `docs/ingestion_report.md` with the fix for any issue found.
**Dependencies:** T3-005, T3-006
**Acceptance Criteria:**
- [ ] Each scenario has a scripted procedure and a recorded result
- [ ] Any duplicate/loss found is explained (semantics of the source/channel) with a mitigation
- [ ] Results linked from ROADMAP risk register
**Estimated Effort:** 45 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 27 (Sun) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create scripts/fault_tests/ingestion_faults.sh with three subcommands: kill_flume (replay 3 min, `kill -9` Flume at t=60 s, restart via flume_ctl.sh), nn_down (stop NameNode for 30 s then start), capture_pause (pause capture/replay for 60 s then resume). After each, call ingestion/scripts/verify_ingestion.py and append a result row (scenario, loss %, duplicate lines, notes) to docs/ingestion_report.md. Explain in comments what to expect from Flume's TAILDIR position file and channel transactions (at-least-once semantics can produce duplicates after a crash) so the observed behaviour is interpreted correctly.
```

---

## Phase 4: Spark Core & Streaming Algorithms Group A (M A Sushil Kumar)
*Goal: Structured Streaming foundation — schema, cleaning, windows, counts — plus Sampling, Stream Filtering, Count Distinct and Counting Ones.*

#### T4-001: Spark environment, session factory and submit wrapper
**Description:** Set up a Python venv with PySpark 3.5.x, confirm Java, and create `streaming/common/session.py` (`get_spark(app_name)` with UTC session tz, `shuffle.partitions=4`, HDFS default FS, UI port, optional Hive support flag) and `scripts/run_stream.sh` (spark-submit wrapper reading config). Record verified versions in ENVIRONMENT.md.
**Dependencies:** T1-002
**Acceptance Criteria:**
- [ ] `python -c "from streaming.common.session import get_spark; get_spark('t').range(5).count()"` returns 5
- [ ] `spark.sql.session.timeZone` is `UTC`; Spark UI opens on the configured port
- [ ] `run_stream.sh --help` prints usage
**Estimated Effort:** 45 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/common/session.py: get_spark(app_name, hive=False) returning a SparkSession with master from config (default local[2]), spark.sql.session.timeZone=UTC, spark.sql.shuffle.partitions=cfg.spark.shuffle_partitions (default 4), spark.ui.port from config, spark.sql.streaming.schemaInference=false, spark.sql.streaming.forceDeleteTempCheckpointLocation=true, optional enableHiveSupport. Also write scripts/run_stream.sh: activates .venv, reads config via a python one-liner, and runs `spark-submit --master <cfg> streaming/stream_app.py "$@"`. Add a spark-marked test (tests/unit/test_session.py) verifying tz and a trivial count. Update docs/ENVIRONMENT.md rows for Java/Spark/PySpark/Python with the versions detected by a small scripts/verify_env.py (create a minimal version that prints detected versions).
```

#### T4-002: Stream schema, parsing and cleaning
**Description:** `streaming/common/schema.py::read_stream(spark, cfg)` reads `stream_in` with the contract schema and `maxFilesPerTrigger`. `streaming/common/cleaning.py::clean(df)` parses `timestamp` → `event_time` (TimestampType), validates ranges (length 1–65535, ports ≤ 65535, protocol in set), adds derived columns `is_private_src`, `is_private_dst` (RFC1918/link-local via Spark expressions, no Python UDF), and `port_class` (well-known/registered/dynamic); invalid rows are counted, not crashed on.
**Dependencies:** T4-001, T1-003, T3-004 (or local folder of CSVs)
**Acceptance Criteria:**
- [ ] Malformed rows (bad ints, empty timestamp) are filtered out of the clean stream and counted in a `bad_rows` metric
- [ ] Unit tests on a static DataFrame cover 10+ edge cases including empty ports and IPv6
- [ ] No Python UDFs used in the hot path
**Estimated Effort:** 60 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/common/schema.py (read_stream(spark, cfg) -> streaming DataFrame using contracts.record_schema.to_spark_schema(), csv source, header=false, mode=PERMISSIVE, maxFilesPerTrigger) and streaming/common/cleaning.py: clean(df) -> DataFrame that (1) creates event_time = to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS'), (2) casts numeric columns (nulls for bad values), (3) sets is_valid = event_time IS NOT NULL AND src_ip/dst_ip not null AND packet_length BETWEEN 1 AND 65535 AND protocol IN (TCP,UDP,ICMP,OTHER) AND ports null-or-<=65535, (4) adds is_private_src/is_private_dst via regex/rlike/expr for 10/8, 172.16/12, 192.168/16, 169.254/16 (and fc00::/7 for IPv6), (5) adds port_class. Provide split_valid(df) -> (valid_df, invalid_df). No Python UDFs. Tests (spark-marked) use a static DataFrame with ≥10 edge cases (empty ports, IPv6, bad timestamp, length 0, protocol 'HTTP').
```

#### T4-003: Streaming application runner (registry, dispatcher, health)
**Description:** `streaming/stream_app.py` is the single entry point. It builds the clean stream, starts **Lane A queries** (each with its own checkpoint), and one **Lane B dispatcher** (`foreachBatch`) that calls every enabled `Analytic.process_batch` inside its own try/except. It writes `pipeline_health` (batch duration, input rows, invalid rows, late rows estimate, lag = now − max(event_time), plugin errors) and shuts down gracefully on SIGINT.
**Dependencies:** T4-002, T1-004
**Acceptance Criteria:**
- [ ] A plugin that raises an exception does not stop the query; error recorded as `plugin_error`
- [ ] `pipeline_health` rows appear each batch; processing time > trigger for 3 batches raises a WARN log
- [ ] Ctrl-C stops all queries cleanly; restart resumes from checkpoint
**Estimated Effort:** 90 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/stream_app.py and streaming/registry.py. Define in streaming/analytics/base.py: class Analytic (Protocol/ABC) with name: str and process_batch(self, batch_df, batch_id: int, ctx: BatchContext) -> None, and BatchContext(cfg, db_path, clock, logger, batch_time). registry.py exposes register(analytic) and enabled(cfg) reading cfg.spark.plugins_enabled. stream_app.py: build clean valid stream; start Lane A queries by importing streaming.queries.* modules that each expose start(spark, stream_df, cfg) -> StreamingQuery (if none exist yet, skip gracefully); start the Lane B query = valid_stream.writeStream.foreachBatch(dispatcher).option(checkpointLocation=<hdfs checkpoints>/q_batch_plugins).trigger(processingTime=cfg.spark.trigger_s). dispatcher: cache batch_df, compute input/invalid counts and lag, call each plugin in try/except (log + write pipeline_health metric plugin_error), unpersist, write pipeline_health rows (batch_duration_s, input_rows, lag_s, invalid_rows) via common.serving_db.upsert. Handle SIGINT by stopping all queries and awaiting termination. Provide a trivial NoopAnalytic to test wiring. Tests: dispatcher isolation (plugin exception), health rows written (spark-marked, using a temp folder file stream).
```

#### T4-004: Window metrics — packets/sec and bytes/sec (Lane A)
**Description:** `streaming/queries/window_metrics.py`: watermark (`spark.watermark_s`) + tumbling `window(event_time, "<len> seconds")`; compute `packets`, `bytes`, `pps = packets/len`, `bps = bytes/len`; `outputMode(update)` with `foreachBatch` upsert into `window_metrics` keyed by `(window_start, window_len_s)`.
**Dependencies:** T4-003
**Acceptance Criteria:**
- [ ] Replay of `normal.csv` yields per-window packet/byte totals equal to pandas ground truth (`truth.json`)
- [ ] Partial windows update as more data arrives (upsert, no duplicate rows)
- [ ] Checkpoint dir is `<root>/q_window_metrics_<len>`
**Estimated Effort:** 60 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/queries/window_metrics.py: start(spark, stream_df, cfg, window_len_s) -> StreamingQuery. Steps: withWatermark('event_time', f'{cfg.spark.watermark_s} seconds'); groupBy(window('event_time', f'{window_len_s} seconds')).agg(count('*') as packets, sum('packet_length') as bytes); derive window_start (ISO-8601 UTC text 'YYYY-MM-DDTHH:MM:SS.000Z'), pps=packets/window_len_s, bps=bytes/window_len_s; writeStream.outputMode('update').foreachBatch(upsert).option('checkpointLocation', f'{cfg.spark.checkpoint_root}/q_window_metrics_{window_len_s}').trigger(processingTime=...). The upsert writes to table window_metrics via common.serving_db.upsert with key (window_start, window_len_s). Include a pure function build_window_metrics(df, window_len_s, watermark_s) for testing. Test (spark-marked): feed data/sample/normal.csv split into files in a tmp folder, processAllAvailable(), compare per-window packets/bytes with pandas groupby on the same file.
```

#### T4-005: Protocol counts and port counts (Lane A)
**Description:** `streaming/queries/counts.py`: per window count/bytes by `protocol` → `protocol_counts`; per window top-N destination ports (N from config, default 10) → `port_counts` (rank computed in `foreachBatch` on the window's aggregated rows).
**Dependencies:** T4-004
**Acceptance Criteria:**
- [x] Protocol counts sum to `window_metrics.packets` for each window (test)
- [x] `port_counts` keeps only the top-N per window by packets; empty/NULL ports excluded
- [x] Verified against pandas on `dns_heavy.csv` (UDP/53 dominant)
**Estimated Effort:** 60 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/queries/counts.py with start_protocol_counts(...) and start_port_counts(...): both use the watermark and tumbling windows like window_metrics. protocol_counts: groupBy(window, protocol) agg packets, bytes → upsert key (window_start, window_len_s, protocol). port_counts: groupBy(window, dst_port) agg packets, bytes with dst_port not null; in foreachBatch, keep the top cfg.spark.top_n_ports (default 10) per window_start by packets using a Window function on the batch DataFrame; upsert key (window_start, window_len_s, port). Checkpoints q_protocol_counts_<len>, q_port_counts_<len>. Tests (spark-marked): consistency (sum protocol packets == window packets) and top-N correctness against pandas for data/sample/dns_heavy.csv.
```

#### T4-006: Stream filtering (named filters + filtered sub-stream)
**Description:** `streaming/analytics/filters.py`: config-defined named filters (`tcp_only`, `dport_443`, `udp_only`, `size_gt_1000`, `src_ip=<ip>`) implemented as Spark column expressions via `FilterSpec`; `apply_filter(df, name)` returns the filtered stream; a Lane A query counts packets/bytes per filter per window using conditional aggregation → `filter_counts`.
**Dependencies:** T4-004
**Acceptance Criteria:**
- [x] Adding a filter in `config/settings.yaml` creates a new `filter_name` series without code changes
- [x] `tcp_only` count equals `protocol_counts` TCP packets in the same window
- [x] Invalid filter spec fails at startup with a clear error
**Estimated Effort:** 60 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 24 (Thu) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/filters.py (pure spec parsing + Spark expression builder) and streaming/queries/filter_counts.py. Config example: filters: [{name: tcp_only, expr: "protocol = 'TCP'"}, {name: dport_443, expr: "dst_port = 443"}, {name: udp_only, expr: "protocol = 'UDP'"}, {name: size_gt_1000, expr: "packet_length > 1000"}]. Validate each expr by compiling it with F.expr against an empty DataFrame of the contract schema at startup (fail fast). apply_filter(df, name) returns df.filter(expr). filter_counts query: one windowed aggregation with, for every filter, sum(when(expr,1)) and sum(when(expr,packet_length)); explode into rows (window_start, window_len_s, filter_name, packets, bytes) and upsert to filter_counts. Tests (spark-marked): tcp_only equals TCP packets from pandas; invalid expr raises ConfigError.
```

#### T4-007: Sampling (Bernoulli and reservoir) with sample-vs-full comparison
**Description:** `streaming/analytics/sampling.py`: pure-Python `ReservoirSampler(k, seed)` (Algorithm R) and `bernoulli_sample(rows, p, seed)`; `SamplingAnalytic` (Lane B) feeds each batch's `packet_length`/protocol into the sampler, and writes to `sampling_compare`: sample size, sample mean packet length vs full-batch mean, error %.
**Dependencies:** T4-003
**Acceptance Criteria:**
- [x] Reservoir sampler is uniform (chi-square-style statistical test with fixed seed passes)
- [x] Sample mean converges to full mean as k grows (test with k=100/1000)
- [x] Rows per batch capped at `max_rows_per_batch` with a WARN when exceeded
**Estimated Effort:** 75 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 24 (Thu) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/sampling.py: ReservoirSampler (Algorithm R; k, seed; add(item), sample(), n_seen, snapshot()/restore()), bernoulli_sample(iterable, p, seed), and SamplingAnalytic(Analytic) that per micro-batch collects packet_length (and protocol) from batch_df (cap at cfg.spark.max_rows_per_batch, sample down + WARN if exceeded), updates two samplers (reservoir k from config, default 1000), computes sample_mean_len (reservoir) vs full_mean_len (batch aggregate) and err_pct, and upserts into sampling_compare with keys (ts=batch time ISO, method in {'reservoir','bernoulli'}) and columns k, sample_n, sample_mean_len, full_mean_len, err_pct. Pure functions must have unit tests: uniformity (each of N items appears ~k/N times across many seeded trials within tolerance), determinism with seed, convergence test. One spark-marked test for the analytic using a static DataFrame.
```

#### T4-008: Count distinct — exact, HyperLogLog and Flajolet–Martin (Lane A + B)
**Description:** Lane A `streaming/queries/distinct.py` computes per window `src_ips_exact`, `dst_ips_exact`, `ports_exact` using `size(collect_set(...))` and `src_ips_hll`, `dst_ips_hll`, `ports_hll` using `approx_count_distinct(col, 0.05)` → upsert into `distinct_counts`. Lane B `analytics/fm.py` implements Flajolet–Martin (multiple hash functions, groups → median of means) and updates the `src_ips_fm`, `dst_ips_fm` columns of the same rows (partial upsert).
**Dependencies:** T4-004, T1-004
**Acceptance Criteria:**
- [x] HLL within ±5% of exact on sample data; FM error is measured and reported (expected to be looser)
- [x] The two writers produce one merged row per window (no overwrites of each other's columns)
- [x] FM implementation unit-tested on known cardinalities (100, 1,000, 10,000) with tolerance
**Estimated Effort:** 90 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 25 (Fri) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement (a) streaming/queries/distinct.py: windowed aggregation producing src_ips_exact=size(collect_set(src_ip)), dst_ips_exact, ports_exact=size(collect_set(dst_port)) plus src_ips_hll=approx_count_distinct(src_ip,0.05), dst_ips_hll, ports_hll; upsert to distinct_counts (key window_start, window_len_s). (b) streaming/analytics/fm.py: pure FlajoletMartin(num_hashes, group_size, seed): add(item) using several independent hash functions (e.g. salted 64-bit hashes), trailing-zero maximum per hash, estimate() = median over groups of the mean of 2^R; provide snapshot()/restore(). (c) FMAnalytic(Analytic): per batch, group rows by window_start (floor of event_time to cfg window length), maintain per-window sketches for src_ip and dst_ip (drop sketches older than the watermark), and partially upsert src_ips_fm/dst_ips_fm into distinct_counts using the same keys. Document in docstrings that FM is a teaching estimator with high variance. Tests: FM on 100/1,000/10,000 distinct items within a documented tolerance; spark-marked test comparing exact vs HLL on data/sample/fanout.csv.
```

#### T4-009: Counting ones — DGIM sliding-window estimator
**Description:** `streaming/analytics/dgim.py`: pure-Python `DGIM(window_n)` (buckets of power-of-two sizes, ≤ 2 per size, merge on overflow, expire by position, `query()` = sum of buckets minus half of the oldest). `CountingOnesAnalytic` converts configured predicates (e.g. `packet_length > 1000`, `dst_port = 443`, `protocol = 'UDP'`) into bit streams over the last `window_n` packets (ordered by `event_time`) and writes exact vs DGIM to `counting_ones`.
**Dependencies:** T4-003, T1-004
**Acceptance Criteria:**
- [ ] DGIM estimate within the theoretical ≤ 50% bound on random bit streams and typically < 10% (test reports max/mean error)
- [ ] Exact count uses a deque over the same last N bits
- [ ] State survives restart via `snapshot()/restore()` JSON in the checkpoint area
**Estimated Effort:** 90 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 26 (Sat) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/dgim.py: class DGIM(window_n) with update(bit: int) (timestamps as a running counter modulo 2*window_n or unbounded ints), buckets stored as lists per power-of-two size (at most 2 buckets per size; when a third appears merge the two oldest into one of double size), expiry of buckets whose newest timestamp is older than window_n, and query() = sum(bucket sizes) - floor(oldest_bucket_size/2). Add snapshot()/restore() (JSON-serialisable). Implement CountingOnesAnalytic(Analytic): config predicates list [{name, expr}], for each batch sort rows by event_time (collect only needed columns, cap by max_rows_per_batch), evaluate the predicate bit per row using a Spark expression on the batch (collect the boolean column), feed bits to the predicate's DGIM and to an exact deque(maxlen=window_n); write counting_ones rows (ts, predicate_name, window_n, exact_ones, dgim_estimate, err_pct). Persist snapshots to a local state dir each batch. Tests: property-style random bit streams (fixed seeds) asserting |dgim-exact| <= 0.5*exact and reporting mean error; edge cases (all zeros, all ones, window smaller than stream); snapshot/restore round trip.
```

#### T4-010: Group A unit + streaming validation tests
**Description:** Make M A Sushil Kumar's modules verifiable: ensure unit tests exist for each pure algorithm, and add `tests/e2e/test_group_a.py` which replays `normal`, `dns_heavy`, `fanout` and asserts the serving tables against `truth.json`/pandas (packets, bytes, protocol counts, filters, distinct exact vs HLL, DGIM bound).
**Dependencies:** T4-004 … T4-009, T2-006
**Acceptance Criteria:**
- [ ] `pytest -m "spark or e2e" tests -k group_a` passes locally
- [ ] Coverage ≥ 90% on pure algorithm modules (`dgim`, `fm`, `sampling`, `filters`)
- [ ] Discrepancies found are logged as issues with the responsible task ID
**Estimated Effort:** 75 min
**Assigned To:** M A Sushil Kumar (Spark core)
**Due:** Sep 27 (Sun) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create tests/e2e/test_group_a.py (markers: spark, e2e). Harness: start streaming.stream_app in-process against a tmp directory stream source (local filesystem paths via config override, not HDFS), feed data/sample/{normal,dns_heavy,fanout}.csv split into files of ~1,000 rows, wait via query.processAllAvailable(), then open the temp serving DB read-only and assert: window_metrics packets/bytes equal pandas groupby of the same CSV; protocol_counts sums; filter_counts.tcp_only equals TCP packets; distinct exact equals pandas nunique and hll within 5%; counting_ones dgim within bound; sampling_compare mean error < 10% for k=1000. Provide tests/conftest.py fixtures for the tmp stream dir, config override and serving DB. Print a coverage summary for streaming/analytics/{dgim,fm,sampling,filters}.py and add missing unit tests until each is ≥ 90%.
```

---

## Phase 5: Advanced Streaming & Streaming Algorithms Group B (Yashwant Vadhan M)
*Goal: Moments, decaying windows, frequent itemsets, edge/per-source aggregation, multi-window ops, load validation.*

#### T5-001: Windowed moments — mean, variance, std, inter-arrival (Lane A)
**Description:** `streaming/queries/moments_window.py`: per window compute `n`, `mean_len`, `var_len` (sample variance), `std_len` for `packet_length`, and `iat_mean_ms`, `iat_var_ms`, `iat_std_ms` from the `iat_ms` column (ignore NULLs). Variance/std are NULL when `n < 2`. Upsert into `moments` (F2 columns are filled by T5-002).
**Dependencies:** T4-004
**Acceptance Criteria:**
- [ ] Values match pandas (`mean`, `var(ddof=1)`, `std(ddof=1)`) within 1e-6 on all sample datasets
- [ ] `n < 2` yields NULL variance/std (no crash, no NaN string)
- [ ] Inter-arrival stats ignore NULL `iat_ms`
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/queries/moments_window.py: start(spark, stream_df, cfg, window_len_s). Use watermark + tumbling window; aggregate count(*) as n, avg(packet_length) as mean_len, var_samp(packet_length) as var_len, stddev_samp(packet_length) as std_len, avg(iat_ms) as iat_mean_ms, var_samp(iat_ms) as iat_var_ms, stddev_samp(iat_ms) as iat_std_ms; update output mode; foreachBatch upsert into moments keyed (window_start, window_len_s) writing ONLY these columns (partial upsert, f2_* are owned by another task). Convert NaN to NULL. Checkpoint q_moments_<len>. Tests (spark-marked): compare against pandas for data/sample/normal.csv and a window with a single row (variance NULL).
```

#### T5-002: AMS second-moment (F2) estimator and iat fallback
**Description:** `streaming/analytics/moments.py`: pure `AMSF2(num_estimators, seed)` (Alon–Matias–Szegedy) over the destination-IP frequency stream per window, plus exact F2 = Σ m_i² for comparison; `MomentsAnalytic` (Lane B) writes `f2_exact` and `f2_ams` into `moments`. If `iat_ms` is NULL for a row, compute the gap from sorted timestamps within the batch.
**Dependencies:** T5-001, T4-003
**Acceptance Criteria:**
- [ ] AMS estimate is within a documented tolerance of exact F2 on skewed and uniform test streams (error reported)
- [ ] `f2_exact` equals a pandas `value_counts()**2` sum
- [ ] Partial upsert never clobbers T5-001 columns
**Estimated Effort:** 75 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 24 (Thu) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/moments.py: (1) AMSF2 — maintain `num_estimators` random positions/variables: for a stream of items, each estimator picks a random stream position (reservoir-style choice), tracks the count r of subsequent occurrences of the item at its position, and contributes n*(2r-1); combine by median of means over groups. Provide add(item), estimate(), snapshot()/restore(), deterministic with seed. (2) exact_f2(counter). (3) MomentsAnalytic(Analytic): for each window_start present in the batch, accumulate per-window dst_ip Counter (for exact) and AMSF2 (for estimate) with eviction after the watermark; partially upsert f2_exact and f2_ams into moments. (4) helper compute_iat_from_timestamps(sorted_ts) used when iat_ms is NULL. Tests: AMS vs exact on uniform and Zipf-like streams (fixed seeds, tolerances documented), exact F2 vs pandas, iat helper.
```

#### T5-003: Exponentially decaying window (recent-traffic score)
**Description:** `streaming/analytics/decay.py`: `DecayingWindow(half_life_s)` maintaining `score = Σ weight × exp(−λ·age)` with `λ = ln 2 / half_life_s`; time-based decay applied using batch time deltas. `DecayAnalytic` maintains (a) total-traffic score (`decay_traffic`) and (b) top-K decayed keys for `dst_ip` and `dst_port` (`decay_top_keys`, K=10). State persisted via `snapshot()/restore()`; pruning of keys below an epsilon.
**Dependencies:** T4-003, T1-004
**Acceptance Criteria:**
- [ ] Unit test: a single burst decays to exactly 50% after one half-life (±1e-9)
- [ ] After a rate change, the score moves toward the new rate faster than a fixed 60 s window would (test compares both)
- [ ] Restart restores state; keys below epsilon are pruned so memory is bounded
**Estimated Effort:** 90 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/decay.py. Pure classes: DecayingCounter(half_life_s) with add(value, t), value(t) (lazy decay: score *= 2**(-(t - last_t)/half_life_s)), snapshot()/restore(); DecayingKeyTable(half_life_s, epsilon, max_keys) supporting add(key, value, t), top_k(k, t), prune(t). DecayAnalytic(Analytic): each batch, aggregate packets per dst_ip and dst_port for the batch (collect small aggregates only, not raw rows), advance decay to the batch time, add counts, then write decay_traffic (ts, half_life_s, score, raw_pps) and decay_top_keys (ts, key_type in {'dst_ip','dst_port'}, key, score, rank). Support multiple half-lives from config (default [10, 60]). Persist state JSON in a local state dir each batch and restore on start. Tests: half-life property test, comparison against an equivalent fixed-window counter after a step change in rate, prune bounds memory, snapshot round-trip.
```

#### T5-004: Market-basket model and limited-pass frequent itemsets (A-Priori + PCY)
**Description:** `streaming/analytics/itemsets.py`. **Baskets:** for each source IP and each 1-second slice, the *set of services* it used, written `PROTO:port` (e.g. `{UDP:53, TCP:443}`); a rolling buffer keeps the last `window_s` (default 60) seconds, capped at 20,000 baskets. Every `every_s` (default 10) run two limited-pass algorithms on the buffer: **A-Priori** (pass 1 counts single items; pass 2 counts only pairs whose members are both frequent) and **PCY** (pass 1 also hashes every pair into a bucket table and builds a bitmap of frequent buckets; pass 2 counts only pairs that are frequent items *and* hit a frequent bucket). Report `passes` and candidate-pair counts to show PCY's memory saving. Frequent singletons such as `TCP:443` are the protocol+port patterns from the project overview. *Stretch:* SON (local A-Priori per micro-batch with a lowered threshold + one verification pass).
**Dependencies:** T4-003, T1-004
**Acceptance Criteria:**
- [ ] A-Priori, PCY and a brute-force counter return identical frequent itemsets/supports on fixed test baskets
- [ ] PCY counts ≤ as many candidate pairs as A-Priori on the test data (numbers shown in the test output)
- [ ] On `normal.csv`, `TCP:443` and `UDP:53` are frequent items and `{UDP:53, TCP:443}` appears as a frequent pair; writes rows for both algorithms into `frequent_itemsets`
**Estimated Effort:** 90 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 26 (Sat) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/analytics/itemsets.py. Pure part: make_baskets(rows) -> dict[(src_ip, slice_start_s)] -> frozenset[str] of 'PROTO:port' items (skip rows with null port by using just 'PROTO'), RollingBasketBuffer(window_s, max_baskets) with add/evict, apriori(baskets, min_support_ratio, max_size=2 or 3) -> (dict[frozenset,int], stats{passes, candidate_pairs}), pcy(baskets, min_support_ratio, num_buckets=cfg default 10007) -> same shape (pass 1: count items and hash pairs to buckets; between passes build a bitmap of buckets whose count >= support; pass 2: count only candidate pairs that are frequent-item pairs AND hash to a frequent bucket), brute_force(baskets, min_support_ratio) for test validation, format_itemset(itemset) -> 'UDP:53 + TCP:443' (sorted). Analytic part: ItemsetsAnalytic(Analytic) collects only (event_time, src_ip, protocol, dst_port) from batch_df (cap rows via cfg.spark.max_rows_per_batch), updates the buffer, and every cfg.itemsets.every_s seconds runs both algorithms and upserts frequent_itemsets rows (window_start, window_len_s = window_s, algorithm in {'apriori','pcy'}, itemset, size, support_count, support_ratio, passes). Optional stretch: son(partitions, min_support) using per-batch partitions and a verification pass. Tests: apriori == pcy == brute_force on random seeded baskets and on a hand-made dataset; PCY candidate_pairs <= apriori candidate_pairs; normal.csv ground truth via pandas.
```

#### T5-005: Edge and per-source aggregation (feeds Link Analysis and Alerts)
**Description:** `streaming/queries/edges.py` (Lane A): per window aggregate `(src_ip, dst_ip)` → `packets`, `bytes` into `ip_edges` (keep top 500 edges per window by bytes) and per `src_ip` → `packets`, `bytes`, `unique_dst_ips`, `unique_dst_ports` (`size(collect_set(...))`) into `source_stats`. Contract for Priyan S.
**Dependencies:** T4-004
**Acceptance Criteria:**
- [ ] `multi_host_graph.csv` produces exactly the expected edge list (test)
- [ ] `portscan_like.csv` shows `unique_dst_ports > 25` for the scanner in one window; `fanout.csv` shows `unique_dst_ips > 40`
- [ ] Priyan S confirms the tables satisfy his graph/alert needs (confirm in team chat)
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement streaming/queries/edges.py: start_edges(spark, stream_df, cfg, window_len_s) and start_source_stats(...). Edges: watermark + tumbling window groupBy(window, src_ip, dst_ip) agg count(*) as packets, sum(packet_length) as bytes; in foreachBatch keep top cfg.spark.max_edges_per_window (default 500) by bytes per window_start; upsert to ip_edges (key window_start, window_len_s, src_ip, dst_ip). Source stats: groupBy(window, src_ip) agg packets, bytes, size(collect_set(dst_ip)) as unique_dst_ips, size(collect_set(dst_port)) as unique_dst_ports; upsert to source_stats. Checkpoints q_edges_<len>, q_source_stats_<len>. Tests (spark-marked) with data/sample/multi_host_graph.csv (expected edge list), portscan_like.csv and fanout.csv thresholds.
```

#### T5-006: Multi-window operations (10 s / 30 s / 60 s)
**Description:** Make all Lane A queries instantiate once per configured window length (`spark.windows_s`), with checkpoint names including the length, so the dashboard can compare windows. Add a sliding-window option (`slide_s`) for `window_metrics` only. Validate Test 6 (window behaviour) from the overview.
**Dependencies:** T4-004, T4-005, T5-001, T5-005
**Acceptance Criteria:**
- [ ] With `windows_s: [10,30,60]` all tables contain rows for all three lengths
- [ ] Sum of the three 10 s windows equals the 30 s window totals (test)
- [ ] Memory/CPU stays within the load-test budget (see T5-007)
**Estimated Effort:** 45 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 28 (Mon) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Refactor streaming/stream_app.py so each Lane A module's start(...) is invoked once per window length in cfg.spark.windows_s, with checkpoint dir suffix _<len>. Add optional sliding windows for window_metrics via cfg.spark.slide_s (window(event_time, f'{len} seconds', f'{slide} seconds')) keyed additionally by slide in window_len_s semantics documented in the code. Do not change table schemas. Add a spark-marked test asserting that sum of 10 s window packets within a 30 s window equals the 30 s window's packets for data/sample/normal.csv. Document CPU/memory implications in a code comment and cap max simultaneous queries via config.
```

#### T5-007: Load and high-volume test
**Description:** `scripts/load_test.py` replays a sample at 1,000 / 2,000 / 5,000 pkts/s for 3 minutes each while sampling `pipeline_health` (batch duration vs trigger, input rows, lag) and system CPU/RAM; produces `docs/load_test_report.md` with pass/fail against the targets (2,000 must, 5,000 target) and tuning notes.
**Dependencies:** T5-006, T2-007
**Acceptance Criteria:**
- [ ] Report contains a table per rate: median/p95 batch duration, lag, dropped/late rows, CPU, RAM
- [ ] Bottleneck identified (capture, Flume, Spark or SQLite) with a recommendation
- [ ] Documented tuning applied (e.g. `maxFilesPerTrigger`, trigger, driver-row cap)
**Estimated Effort:** 75 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 28 (Mon) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write scripts/load_test.py: for each rate in [1000, 2000, 5000] pps, run capture.replay with --pps <rate> --restamp for --seconds 180 (a scenario CSV looped), poll the serving DB pipeline_health every 5 s (batch_duration_s, input_rows, lag_s, invalid_rows) and psutil-free CPU/RAM sampling via /proc, then generate docs/load_test_report.md with a table per rate (median/p95 batch duration, max lag, late/dropped rows, CPU, RAM), PASS/FAIL vs targets (2,000 must, 5,000 target, batch duration < trigger, p95 lag <= 30 s), and an auto-generated 'bottleneck' section that names the stage with the highest utilisation. Do not modify pipeline code; only measure. Include a --dry-run that simulates results for testing the report generator; unit-test the report generator with synthetic measurements.
```

#### T5-008: Retention cleanup for live serving tables
**Description:** `streaming/analytics/retention.py`: a lightweight plugin/timer that every 10 minutes deletes rows older than `serving.retention_hours` (default 24) from live tables and runs `PRAGMA wal_checkpoint(TRUNCATE)`; never touches `hist_results` or `alerts` newer than 7 days.
**Dependencies:** T1-004, T4-003
**Acceptance Criteria:**
- [ ] Old rows removed, recent rows kept (test with fake clock)
- [ ] `hist_results` untouched; alerts kept 7 days
- [ ] DB file size stays bounded in a 30-minute soak
**Estimated Effort:** 30 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** not scheduled · **Priority:** ⚫ CUT — skip unless everything else is done
**AI Agent Prompt:**
```text
Implement streaming/analytics/retention.py: RetentionAnalytic(Analytic) whose process_batch runs at most once per cfg.serving.cleanup_every_s (default 600) using ctx.clock; it calls common.serving_db.cleanup for all live tables (retention_hours from config, timestamp column per table as defined in serving_schema.sql), keeps alerts for 7 days, skips hist_results, then executes PRAGMA wal_checkpoint(TRUNCATE). Register it in the plugin registry (enabled by default). Unit tests with a fake clock and an in-memory/tmp SQLite DB.
```

#### T5-009: Group B unit and integration tests
**Description:** Complete unit tests for `AMSF2`, `DecayingCounter/KeyTable`, `apriori/RollingBasketBuffer`, and Spark integration tests for the Lane A/B modules in Phase 5.
**Dependencies:** T5-001 … T5-005
**Acceptance Criteria:**
- [ ] ≥ 90% coverage on `moments.py`, `decay.py`, `itemsets.py` pure logic
- [ ] Each Phase 5 Spark module has ≥ 1 spark-marked test against fixture data
- [ ] Tests run in < 5 minutes total on the integration host
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Spark advanced)
**Due:** Sep 27 (Sun) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Review tests for streaming/analytics/{moments,decay,itemsets}.py and streaming/queries/{moments_window,edges}.py. Add missing unit tests to reach ≥90% line coverage on the pure logic, plus one spark-marked integration test per Spark module using fixtures from data/sample and pandas ground truth. Use fixed seeds, keep the Spark session session-scoped (tests/conftest.py) to control runtime, and print a coverage report. Do not change production code except to fix bugs you find — list each bug in the output.
```

#### T5-010: Cross-validation with M A Sushil Kumar's results
**Description:** `tests/e2e/test_cross_validation.py` runs one replay and asserts consistency **between** M A Sushil Kumar's and Yashwant Vadhan M's outputs: `moments.n == window_metrics.packets`; `sum(ip_edges.packets) == window_metrics.packets`; `frequent_itemsets` support consistent with `port_counts`/`protocol_counts`; `source_stats` unique counts consistent with `distinct_counts`. Produces `docs/validation_matrix.md` (concept × check × result).
**Dependencies:** T4-010, T5-009
**Acceptance Criteria:**
- [ ] All cross-checks pass for `normal`, `fanout`, `multi_host_graph`
- [ ] Any mismatch has a linked issue naming the owner
- [ ] Validation matrix committed
**Estimated Effort:** 45 min
**Assigned To:** Yashwant Vadhan M (Integration) with M A Sushil Kumar
**Due:** Sep 27 (Sun) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write tests/e2e/test_cross_validation.py (markers spark, e2e) reusing the harness from tests/e2e/test_group_a.py. After replaying normal, fanout and multi_host_graph, open the serving DB read-only and assert per window: moments.n == window_metrics.packets; SUM(ip_edges.packets) == window_metrics.packets (allowing for the top-N edge cap when active — assert equality when cap not hit); MAX(source_stats.unique_dst_ips) <= distinct_counts.dst_ips_exact; frequent_itemsets support_ratio for TCP|443 within 2% of protocol/port counts. Generate docs/validation_matrix.md with columns Concept | Check | Owner | Result. Fail with clear messages naming the owning member.
```

---

## Phase 6: Link Analysis & Alert Engine (Priyan S)
*Goal: IP communication graph, centrality, PageRank, Markov transitions and explainable rule-based alerts — all testable without Spark.*

#### T6-001: Graph builder from `ip_edges`
**Description:** `linkanalysis/graph.py::build_graph(edges_df, top_n_nodes=None)` returns a `networkx.DiGraph` with edge attributes `packets`, `bytes` (summed across the lookback windows) and node attributes `is_private`, `total_bytes`. Prunes to top-N nodes by bytes when requested; handles empty input and self-loops.
**Dependencies:** T1-004, T1-005
**Acceptance Criteria:**
- [ ] Toy graph (A→B, A→C, C→B, D→B) builds with correct nodes/edges/weights
- [ ] Pruning keeps the highest-byte nodes and only edges between kept nodes
- [ ] Empty DataFrame returns an empty graph without exception
**Estimated Effort:** 60 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement linkanalysis/graph.py: load_edges(conn, lookback_s, window_len_s=10) reading ip_edges for the last lookback_s seconds (parameterised SQL, read-only connection) into a pandas DataFrame, and build_graph(edges_df, top_n_nodes=None) -> nx.DiGraph summing packets/bytes per (src,dst) with node attrs is_private (ipaddress.ip_address(...).is_private) and total_bytes; prune to top_n_nodes by total_bytes and induced edges; keep self-loops but flag attr self_loop=True. Return empty graph for empty input. Pure functions except load_edges. Tests in tests/unit/test_graph.py: the toy graph from the project overview, pruning, empty input, self-loop, IPv6 nodes.
```

#### T6-002: Degree and centrality metrics
**Description:** `linkanalysis/centrality.py::graph_metrics(G)` returns a DataFrame with `in_degree`, `out_degree`, weighted in/out (packets & bytes), `degree_centrality`, and — only when `nodes ≤ 300` — `betweenness_centrality`; otherwise the column is NULL with a `betweenness_skipped` flag.
**Dependencies:** T6-001
**Acceptance Criteria:**
- [ ] Values match hand-calculated results on the toy graph
- [ ] Betweenness skipped (flag set) for a 400-node random graph, computed for a 50-node one
- [ ] Runtime for 300 nodes < 2 s
**Estimated Effort:** 45 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement linkanalysis/centrality.py: graph_metrics(G, betweenness_max_nodes=300) -> pandas.DataFrame indexed by node with columns in_degree, out_degree, in_packets, out_packets, in_bytes, out_bytes, degree_centrality (nx.degree_centrality), betweenness_centrality (nx.betweenness_centrality with weight=None, k-sampling not used; NULL when node_count > betweenness_max_nodes) and a boolean attribute in df.attrs['betweenness_skipped']. Tests: hand-computed values for the toy graph A→B, A→C, C→B, D→B; large-graph skip; empty graph returns empty DataFrame with the right columns.
```

#### T6-003: PageRank (NetworkX and from-scratch power iteration)
**Description:** `linkanalysis/pagerank.py`: `pagerank_nx(G, alpha=0.85, weight="packets")` and `pagerank_power(G, alpha, weight, tol=1e-10, max_iter=200)` implemented with explicit power iteration (dangling nodes redistribute uniformly). A `compare()` helper asserts both agree within 1e-4. Results include rank, score and in/out degree.
**Dependencies:** T6-001
**Acceptance Criteria:**
- [ ] Toy graph: scores sum to 1; B ranks highest; matches hand calculation within 1e-6
- [ ] NetworkX vs power iteration agree within 1e-4 on 20 random graphs (fixed seeds)
- [ ] Dangling nodes and disconnected components handled
**Estimated Effort:** 75 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 21 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement linkanalysis/pagerank.py. pagerank_nx wraps nx.pagerank(G, alpha, weight). pagerank_power(G, alpha=0.85, weight='packets', tol=1e-10, max_iter=200) builds the column-stochastic transition from weighted out-edges, treats dangling nodes as linking uniformly to all nodes, iterates r = alpha*M r + (1-alpha)/N until L1 change < tol, returns dict node->score. rank_table(scores, G) -> DataFrame(rank, ip, score, in_degree, out_degree). compare(G) returns max abs difference. Add docs comment: the score means structural importance in the observed graph, not maliciousness. Tests: compute the toy graph's PageRank by hand in the test (document the arithmetic) and compare; 20 seeded random digraphs nx vs power; dangling and disconnected cases; empty graph.
```

#### T6-004: Markov transition model
**Description:** `linkanalysis/markov.py`: `transition_matrix(G, weight="packets")` (row-normalised, nodes without out-edges omitted from rows but kept as columns), `next_hop(P, node, top=5)`, `two_step(P)`, and optional `stationary_distribution(P, teleport=0.05)` via power iteration. Output feeds the heatmap and "next-hop probabilities" panel.
**Dependencies:** T6-001
**Acceptance Criteria:**
- [ ] Every row sums to 1.0 (±1e-9) on random graphs
- [ ] `P(B|A)`, `P(C|B)`, `P(B|C)` from the overview's example are reproduced exactly
- [ ] Dangling-node handling documented and tested; stationary distribution sums to 1
**Estimated Effort:** 75 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement linkanalysis/markov.py: transition_matrix(G, weight='packets') -> pandas.DataFrame (rows = source nodes that have out-edges, columns = all nodes) with P(j|i)=w_ij/sum_k w_ik; next_hop(P, node, top=5) -> list[(dst, prob)]; two_step(P) using matrix multiplication on aligned square matrices (nodes without rows get zero rows and are documented as absorbing/omitted); stationary_distribution(P, teleport=0.05) using power iteration with teleportation so it converges on non-ergodic graphs. Tests: reproduce the overview example (IP A→B, B→C, C→B) probabilities exactly; row sums; dangling node behaviour; stationary distribution sums to 1 and is stable across seeds.
```

#### T6-005: Explainable alert engine
**Description:** `linkanalysis/alerts.py` + `config/alert_rules.yaml`: three rules — **traffic spike** (`pps > 3 × EWMA baseline` and `pps ≥ 50`, α=0.1, warm-up 30 windows, CRITICAL above 6×), **unusual port activity** (`unique_dst_ports > 20` per `src_ip`), **high fan-out** (`unique_dst_ips > 30`). Each alert stores `current_value, baseline_value, change_pct, threshold, reason`, with a 60 s cooldown per (type, src_ip). A worker loop (`python -m linkanalysis.alerts_worker`) reads the serving DB and writes `alerts`.
**Dependencies:** T1-004, T5-005 (or mock data)
**Acceptance Criteria:**
- [ ] `spike.csv`, `portscan_like.csv`, `fanout.csv` each trigger exactly their intended alert with a correct explanation; `normal.csv` triggers none
- [ ] Cooldown prevents repeats within 60 s; cold start (< warm-up) raises no spike alert
- [ ] Reason text follows: "Traffic rate exceeded the configured threshold (current X pkts/s vs baseline Y, +Z%)"
**Estimated Effort:** 90 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 24 (Thu) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement linkanalysis/alerts.py with pure rule classes (SpikeRule with EWMA baseline excluding the current point, PortActivityRule, FanOutRule), an Alert dataclass (alert_id uuid, ts, type in {TRAFFIC_SPIKE, UNUSUAL_PORT_ACTIVITY, HIGH_FAN_OUT}, severity in {INFO, WARN, CRITICAL}, src_ip, metric, current_value, baseline_value, change_pct, threshold, reason, details_json), a Cooldown helper (60 s per (type, src_ip) using an injectable clock), and load_rules(config/alert_rules.yaml). Add linkanalysis/alerts_worker.py: every 2 s read the newest window_metrics/source_stats (read-only), evaluate rules, and write new alerts to the alerts table via common.serving_db (busy_timeout). Wording must be neutral — never 'attack'. Tests: synthetic series producing exactly one spike alert with correct change_pct; warm-up suppression; cooldown; port/fan-out thresholds; alerts from a mock-DB seeded spike (scripts/seed_mock_db.py).
```

#### T6-006: Link-analysis test pack and toy-graph fixtures
**Description:** Consolidate fixtures (`tests/fixtures/toy_graph.csv`, `star_graph.csv`, `two_components.csv`) and tests for graph/centrality/PageRank/Markov; publish `docs/link_analysis_notes.md` with the hand calculation of the toy PageRank and Markov matrix (used in the report).
**Dependencies:** T6-001 … T6-004
**Acceptance Criteria:**
- [ ] ≥ 90% coverage on `linkanalysis/{graph,pagerank,markov,centrality}.py`
- [ ] Notes contain the toy example with numbers and the interpretation caveat
- [ ] Tests run < 30 s and need no Spark
**Estimated Effort:** 60 min
**Assigned To:** Priyan S (Link analysis)
**Due:** Sep 26 (Sat) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create tests/fixtures/{toy_graph,star_graph,two_components}.csv (edge lists with columns src_ip,dst_ip,packets,bytes) and consolidate/extend tests for linkanalysis/{graph,centrality,pagerank,markov}.py to ≥90% coverage. Then write docs/link_analysis_notes.md: show the toy graph (A→B, A→C, C→B, D→B) with the hand-calculated PageRank (alpha=0.85, uniform teleport) and the Markov transition matrix, explain dangling-node handling, and add the caveat that PageRank/centrality indicate structural importance in the observed graph and are not evidence of malicious behaviour. Verify your hand calculation numerically in a test.
```

---

## Phase 7: Dashboard (Priyan S)
*Goal: One professional Streamlit dashboard covering every analytic, built on the mock DB first and switched to live data by 27 Sep. Follow `DESIGN.md` for every visual decision.*

#### T7-001: Streamlit skeleton, theme and sidebar
**Description:** `dashboard/app.py` with page config, header (logo wordmark, source badge LIVE/REPLAY/MOCK, live-pulse dot, "updated Ns ago"), sidebar controls (window length 10/30/60, refresh interval, top-N, source mode) and six tabs (Live Overview, Stream Analytics, Link Analysis, Alerts, History, Pipeline). `.streamlit/config.toml` theme, `dashboard/assets/theme.css` (tokens from DESIGN.md), `dashboard/theme.py` (Plotly template `lnta_dark`). Runs with `--mock` using the seeded DB.
**Dependencies:** T1-002, T1-005
**Acceptance Criteria:**
- [ ] `streamlit run dashboard/app.py -- --mock` shows dark NOC-style shell with all six tabs and no console errors
- [ ] All colours come from tokens (no hard-coded hex in components)
- [ ] Fonts fall back gracefully offline
**Estimated Effort:** 75 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Read docs/DESIGN.md fully. Build dashboard/app.py, dashboard/theme.py, dashboard/assets/theme.css and .streamlit/config.toml: dark theme with the exact tokens in DESIGN.md (bg #0A0F1C, panel #101828, accent #22D3EE, protocol colours, semantic colours), Inter + JetBrains Mono with the fallback stacks, header (inline SVG logo of three arcs + pulse line, 'LNTA · Live Network Traffic Analyser', source badge, pulse dot, last-update text), sidebar controls, and st.tabs for: Live Overview, Stream Analytics, Link Analysis, Alerts, History, Pipeline (each rendering a placeholder function from dashboard/components/<tab>.py). Define Plotly template 'lnta_dark' once in theme.py and a helper apply_theme(fig). CLI flag --mock uses serving/mock.db. No business logic in app.py. Make it look like a professional network-operations console (dense, calm, no emojis).
```

#### T7-002: Data-access layer with status model and mock fallback
**Description:** `dashboard/data.py`: read-only SQLite access (parameterised SQL), 2 s TTL caching, functions per table returning `(DataFrame, Status)` where `Status ∈ {OK, EMPTY, STALE, ERROR}` (stale if newest row older than `dashboard.stale_after_s`). Timezone conversion to `Asia/Kolkata` for display only.
**Dependencies:** T7-001, T1-004
**Acceptance Criteria:**
- [ ] Every table in TECH_RULES §5.2 has a getter; missing DB → `ERROR` status, never an exception
- [ ] Only the latest N windows are read (default 120)
- [ ] Unit tests with tmp DBs cover OK/EMPTY/STALE/ERROR
**Estimated Effort:** 60 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 22 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/data.py: connect read-only via common.serving_db.connect(path, read_only=True); a generic fetch(table, where, params, limit) using parameterised SQL only; one getter per table in TECH_RULES §5.2 (get_window_metrics(window_len_s, n), get_protocol_counts, get_port_counts, get_filter_counts, get_distinct_counts, get_sampling_compare, get_counting_ones, get_moments, get_decay_traffic, get_decay_top_keys, get_frequent_itemsets, get_ip_edges, get_source_stats, get_alerts, get_pipeline_health, get_hist_results). Each returns (df, Status) with Status enum OK/EMPTY/STALE/ERROR (stale if newest updated_at older than cfg.dashboard.stale_after_s). Wrap with st.cache_data(ttl=2). Add to_local_time(df, tz) helper. Tests in tests/unit/test_dashboard_data.py using tmp SQLite DBs for each status.
```

#### T7-003: KPI strip and Live Overview tab
**Description:** KPI cards (Packets/s, Bytes/s, Unique IPs, Active Ports, Decay Score, Active Alerts) with delta vs previous window and 60 s sparkline; Live Overview tab: traffic-over-time dual-axis chart, protocol donut, top-ports bar with service names, recent-behaviour decay strip.
**Dependencies:** T7-002
**Acceptance Criteria:**
- [ ] All panels render with mock data and with an empty DB (empty-state message)
- [ ] Protocol colours match DESIGN tokens in every chart
- [ ] Window toggle (10/30/60) changes the charts
**Estimated Effort:** 90 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 23 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/components/kpi.py and dashboard/components/overview.py per DESIGN.md ('Global Header + KPI Strip' and '1 · Live Overview'). KPIs read window_metrics, distinct_counts, port_counts, decay_traffic and alerts via dashboard.data. Charts (Plotly, template lnta_dark): dual-axis traffic-over-time (pps left, bps right), protocol donut from protocol_counts using the protocol colour tokens, top destination ports horizontal bar with a small SERVICE_NAMES dict (443 HTTPS, 53 DNS, 80 HTTP, 22 SSH, 123 NTP, 5353 mDNS…), decay-score line with trend arrow. Each panel handles OK/EMPTY/STALE/ERROR with consistent skeleton/empty/error components in dashboard/components/states.py. Add 'View as table' toggles for accessibility. Keep functions ≤50 lines each.
```

#### T7-004: Stream Analytics tab — filtering, sampling, distinct, counting ones
**Description:** Concept cards (title chip, one-line plain-English explanation, visual, exact-vs-estimate footer): Stream Filtering (named filter chips + counts), Sampling (sample vs full mean, error %), Count Distinct (Exact · HLL · Flajolet–Martin with % error), Counting Ones (predicate selector; exact vs DGIM).
**Dependencies:** T7-003
**Acceptance Criteria:**
- [ ] Each card shows exact and estimated values side by side with % error
- [ ] Info popover per card explains the concept and formula in ≤ 3 lines
- [ ] Not-enough-data and error states handled per card
**Estimated Effort:** 75 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 24 (Thu) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/components/stream_analytics.py (part 1) with a reusable concept_card(title_chip, explanation, body_fn, footer) helper and four cards per DESIGN.md '2 · Stream Analytics': Stream Filtering (filter_counts; chips for tcp_only, dport_443, udp_only, size_gt_1000; bar of filtered share), Sampling (sampling_compare: k, sample mean vs full mean, err_pct, small line over time), Count Distinct (distinct_counts: table Exact | HyperLogLog | Flajolet–Martin for src IPs, dst IPs, ports with % error badges), Counting Ones (counting_ones: predicate selectbox, exact ones vs DGIM estimate over last N packets, error %). Copy must be plain English, no jargon without a tooltip. Two columns collapsing to one under 900 px via CSS.
```

#### T7-005: Stream Analytics tab — moments, decaying window, market basket
**Description:** Cards for Estimating Moments (mean/variance/std of packet size, inter-arrival stats, AMS F2 vs exact F2), Decaying Window (live score line, half-life selector, top decayed IPs/ports), Market Basket · Frequent Itemsets (table with support bars, algorithm toggle A-Priori/PCY, passes shown, min-support slider filter).
**Dependencies:** T7-004
**Acceptance Criteria:**
- [ ] Variance/std show "n < 2" message instead of NaN
- [ ] Half-life selector switches between 10 s and 60 s series
- [ ] Itemsets shown as `TCP:443 + UDP:53` chips with support %
**Estimated Effort:** 90 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 25 (Fri) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Extend dashboard/components/stream_analytics.py with three more concept_card panels per DESIGN.md: Estimating Moments (moments: mean/var/std, iat mean/std, f2_exact vs f2_ams with % error), Decaying Window (decay_traffic score line for the selected half-life; decay_top_keys top 5 IPs and ports as horizontal bars), Market Basket · Frequent Itemsets (frequent_itemsets filtered to latest window and selected algorithm apriori|pcy; table with itemset chips, support_ratio bar, support_count, passes; min-support slider filters client-side). Include a short 'why this matters' line per card. Handle all states via dashboard/components/states.py.
```

#### T7-006: Link Analysis tab — graph, PageRank, centrality, Markov
**Description:** Interactive Plotly network graph (node size = PageRank, colour = private vs public, edge width = bytes, top-N slider), PageRank table, degree/centrality bar, Markov panel (transition heatmap for top-K nodes and next-hop probabilities for a selected IP). Caption: "Structural importance in the observed graph — not a measure of risk."
**Dependencies:** T6-001, T6-002, T6-003, T6-004, T7-003
**Acceptance Criteria:**
- [ ] Graph renders for the mock 12-node graph and for the toy graph; empty state when < 2 nodes
- [ ] Selecting an IP updates the next-hop panel
- [ ] Graph computation is cached (recomputed at most every 5 s) and pruned to top-N nodes
**Estimated Effort:** 90 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 26 (Sat) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/components/link_analysis.py per DESIGN.md '3 · Link Analysis'. Use linkanalysis.graph.load_edges/build_graph, pagerank.pagerank_nx + rank_table, centrality.graph_metrics, markov.transition_matrix/next_hop. Graph figure: networkx spring_layout with fixed seed, Plotly scatter for nodes (size by PageRank, colour by is_private using tokens) and line traces for edges (width by bytes, log-scaled), hover shows IP, PageRank, in/out degree. Controls: top-N nodes slider (default 50), selected-IP selectbox. Markov: heatmap of the top-K nodes and a bar list of next-hop probabilities. Cache computations with st.cache_data(ttl=5). Add the required caption about interpretation. Provide empty/error states.
```

#### T7-007: Alerts tab
**Description:** Active alert cards (severity icon + label, type, source IP, Current / Baseline / Change / Threshold, reason sentence, timestamp) and a recent-alert timeline with type/severity filters; green "No alerts in the last N minutes" empty state; KPI alert count wired.
**Dependencies:** T6-005, T7-003
**Acceptance Criteria:**
- [ ] Seeded spike shows a card with correct numbers and reason
- [ ] Severity conveyed by icon + text, not colour alone
- [ ] Critical alerts have `role="alert"` semantics where Streamlit allows (via markdown/HTML)
**Estimated Effort:** 60 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 26 (Sat) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/components/alerts.py per DESIGN.md '4 · Alerts'. Read alerts via dashboard.data.get_alerts. Render alert cards in HTML/CSS (severity icon + text label, TRAFFIC SPIKE / UNUSUAL PORT ACTIVITY / HIGH FAN-OUT, mono source IP, four metric tiles Current/Baseline/Change/Threshold, reason sentence, relative time), filters by type and severity, a timeline chart of alerts per minute, and the green empty state. Neutral wording only — never 'attack'. Tests: render function returns expected text for a seeded alert row.
```

#### T7-008: History and Pipeline tabs
**Description:** History: query selector over `hist_results` (protocol totals, top src/dst, traffic by hour, avg packet size, trend), table + chart, last-run time, data-source note. Pipeline: stage tiles Capture → Flume → HDFS → Spark → Serving with status dots, batch-duration vs trigger chart, input rows/batch, end-to-end lag, bad/late counters (from `pipeline_health`, `logs/capture_stats.json`, `logs/ingestion_status.json`).
**Dependencies:** T7-003, T3-009 (or mock), T4-003
**Acceptance Criteria:**
- [ ] History tab works with mock `hist_results` and shows a helpful empty state
- [ ] Pipeline tiles turn amber/red on stale/missing status files
- [ ] Missing JSON files never raise
**Estimated Effort:** 75 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 27 (Sun) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Implement dashboard/components/history.py and dashboard/components/pipeline.py per DESIGN.md sections 5 and 6. History: parse hist_results columns_json/rows_json into DataFrames, selectbox of query_name, table + auto-chosen chart, 'last run' badge. Pipeline: read pipeline_health (batch_duration_s, input_rows, lag_s, invalid_rows, plugin_error) plus optional JSON status files logs/capture_stats.json and logs/ingestion_status.json (tolerate missing/invalid JSON); render five stage tiles with status dot + last-seen, and three small charts (batch duration vs trigger line, input rows, lag). Statuses: OK / LAGGING / DOWN.
```

#### T7-009: States, auto-refresh, responsiveness and accessibility pass
**Description:** Unified loading/empty/error/stale components; auto-refresh via `st.fragment(run_every=…)` (verify Streamlit version supports it; else `streamlit-autorefresh`); responsive CSS breakpoints; keyboard focus rings; chart text summaries; `prefers-reduced-motion`; contrast check of every token pair.
**Dependencies:** T7-003 … T7-008
**Acceptance Criteria:**
- [ ] Stopping the pipeline flips the header pulse to amber then red without exceptions
- [ ] No full-page flicker on refresh; refresh ≤ 3 s render
- [ ] Contrast table of token pairs recorded in `docs/DESIGN_CHECKS.md` (≥ 4.5:1 body text)
**Estimated Effort:** 90 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 28 (Mon) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Do a polish pass on the dashboard against DESIGN.md: (1) verify installed Streamlit version and use st.fragment(run_every=cfg.dashboard.refresh_s) if available, otherwise streamlit-autorefresh, with per-tab fragments so charts don't flicker; (2) ensure every panel uses dashboard/components/states.py for loading/empty/error/stale; (3) add responsive CSS breakpoints (≥1200, 900–1199, 600–899, <600) and 2→1 column collapse; (4) focus rings, aria-labels/captions summarising each chart, prefers-reduced-motion; (5) compute WCAG contrast ratios for all token pairs in DESIGN.md with a small script scripts/check_contrast.py and write results to docs/DESIGN_CHECKS.md, changing tokens only if a body-text pair is below 4.5:1 (and update DESIGN.md accordingly).
```

#### T7-010: Dashboard tests
**Description:** Streamlit `AppTest` smoke tests per tab in normal / empty / error states using tmp DBs; unit tests for data layer and formatting helpers.
**Dependencies:** T7-009
**Acceptance Criteria:**
- [ ] Six tab smoke tests pass; empty and error DB variants covered
- [ ] Coverage ≥ 70% on `dashboard/data.py` and helpers
- [ ] Tests run in < 2 minutes
**Estimated Effort:** 60 min
**Assigned To:** Priyan S (Dashboard)
**Due:** Sep 28 (Mon) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write tests/unit/test_dashboard_smoke.py using streamlit.testing.v1.AppTest to load dashboard/app.py in --mock mode and assert each tab renders without exceptions with (a) the seeded mock DB, (b) an empty initialised DB, (c) a missing DB path. Add unit tests for formatting helpers (humanised bytes, pps, relative time) and the Status logic. Print coverage for dashboard/.
```

---

## Phase 8: Integration, Testing & Runbook
*Goal: One command starts the whole pipeline, the tests prove correctness, and the demo has a fallback.*

#### T8-001: One-command demo script and environment verifier
**Description:** `scripts/run_demo.sh --mode live|replay` starts HDFS → Flume → data source → Spark → alerts worker → dashboard in order, health-checking each step (waits for HDFS out of safe mode, Flume alive, first `stream_in` file, first `window_metrics` row) and `scripts/stop_demo.sh` stops everything. Extend `scripts/verify_env.py` to check Java, Hadoop, Flume, Spark, Python packages, TShark, permissions.
**Dependencies:** T3-005, T4-003, T6-005, T7-001
**Acceptance Criteria:**
- [ ] `run_demo.sh --mode replay` brings the full system up from cold in < 3 minutes on the integration host
- [ ] A failed step aborts with a clear message and the fix hint
- [ ] `stop_demo.sh` leaves no orphan processes
**Estimated Effort:** 90 min
**Assigned To:** Yashwant Vadhan M (Integration)
**Due:** Sep 27 (Sun) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write scripts/run_demo.sh (bash, set -euo pipefail, --mode live|replay, --scenario NAME for replay) that starts in order: ingestion/scripts/start_hdfs.sh; flume_ctl.sh start; data source (python -m capture.run_capture --auto OR python -m capture.replay --file data/sample/<scenario>.csv --restamp --loop); scripts/run_stream.sh; python -m linkanalysis.alerts_worker; streamlit run dashboard/app.py. Between steps run health checks with timeouts (HDFS safemode off, flume status, at least one file in /traffic/stream_in, at least one row in window_metrics) and print colour-coded progress. Store PIDs in .run/, and write scripts/stop_demo.sh that stops all in reverse order. Extend scripts/verify_env.py to check versions/permissions and print a PASS/FAIL table with fix hints. Do not modify component code.
```

#### T8-002: End-to-end scenario suite (the eight overview tests)
**Description:** `tests/e2e/test_scenarios.py` automates: (1) normal traffic metrics, (2) increased traffic raises pps/bps, (3) TCP-only/UDP-only filtering, (4) distinct-count growth with `fanout`, (5) frequent patterns with `dns_heavy`, (6) window-length comparison, (7) decay reacts to a rate change, (8) graph metrics change with `multi_host_graph`. Results summarised in `docs/test_report.md`.
**Dependencies:** T5-010, T4-010
**Acceptance Criteria:**
- [ ] All eight scenarios pass on the integration host
- [ ] Report lists each scenario, expected vs observed, PASS/FAIL
- [ ] Runs in < 15 minutes
**Estimated Effort:** 90 min
**Assigned To:** Yashwant Vadhan M (Integration) with M A Sushil Kumar
**Due:** Sep 29 (Tue) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Write tests/e2e/test_scenarios.py (markers e2e, spark) that reuses the tests/e2e harness and the generator/replay tools to run eight scenarios and assert on the serving DB: 1) normal: packets/bytes equal truth.json; 2) spike: pps in last third ≥ 3× first third; 3) filters: tcp_only + udp_only counts equal protocol_counts; 4) fanout: dst_ips_exact grows above 40 in a window and HLL within 5%; 5) dns_heavy: frequent_itemsets contains UDP:53 with support > 40%; 6) window lengths: 10 s windows sum to 30 s window; 7) decay: score after rate drop is below the 60 s fixed-window average; 8) multi_host_graph: ip_edges match the expected edge list and PageRank ranks B highest. Generate docs/test_report.md (scenario | expected | observed | PASS/FAIL). Do not change production code; report bugs with the owner's name.
```

#### T8-003: Wireshark/ingestion validation report
**Description:** Consolidate evidence that the data is right: capture-vs-Wireshark comparison (T2-009), ingestion loss/duplicate results (T3-006/T3-011) and cross-validation matrix (T5-010) into `docs/validation_report.md` with three screenshots.
**Dependencies:** T2-009, T3-006, T5-010
**Acceptance Criteria:**
- [ ] Report has three sections with numbers and screenshots
- [ ] Any mismatch above threshold is explained
- [ ] Linked from README
**Estimated Effort:** 45 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 28 (Mon) · **Priority:** 🟡 SHOULD
**AI Agent Prompt:**
```text
Create docs/validation_report.md that pulls together (a) capture vs Wireshark I/O graph comparison (from docs/validation_capture.md), (b) ingestion loss/duplicates/latency (docs/ingestion_report.md), (c) cross-validation matrix (docs/validation_matrix.md). Use short tables, one screenshot placeholder each with caption instructions, a 'What we verified / What we did not' section, and honest notes for any threshold miss. Add a link from README.md (append-only edit).
```

#### T8-004: Runbook and offline fallback snapshot
**Description:** `docs/RUNBOOK.md` — "if X fails during the demo, do Y" (Wi-Fi drops → replay; Flume down → `flume_ctl.sh restart`; Spark lag → smaller window; dashboard blank → mock DB). Also create the offline fallback: a frozen `serving/demo_snapshot.db` from the best rehearsal and `run_demo.sh --mode snapshot` (dashboard only).
**Dependencies:** T8-001
**Acceptance Criteria:**
- [ ] Runbook covers ≥ 8 failure cases with exact commands
- [ ] `--mode snapshot` starts dashboard from the frozen DB with a visible `SNAPSHOT` badge
- [ ] Fallback tested with Wi-Fi off
**Estimated Effort:** 45 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 29 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write docs/RUNBOOK.md with a table Symptom | Likely cause | Command to fix | Time to recover, for: Wi-Fi capture fails/permission denied, capture running but no CSV growth, Flume down, HDFS safe mode / NameNode down, files in stream_in but Spark shows nothing (watermark/late data), Spark batch time > trigger, SQLite database is locked, dashboard blank/stale, Hive unavailable. Add a 'Nuclear option' section: run_demo.sh --mode snapshot. Implement --mode snapshot in scripts/run_demo.sh (dashboard only, DB path serving/demo_snapshot.db, badge SNAPSHOT) as an append-only change, and a scripts/make_snapshot.sh that copies the current serving DB to serving/demo_snapshot.db (checkpointing the WAL first).
```

---

## Phase 9: Documentation, Submission & Demo
*Goal: Recruiter-readable repository, a submission package, and a demo everyone can present.*

#### T9-001: Concept mapping document
**Description:** `docs/CONCEPT_MAPPING.md`: for every syllabus topic — stream data model, sampling, filtering, count distinct, counting ones, estimating moments, decaying windows, link analysis, PageRank, market-basket model, limited-pass frequent itemsets — give a plain-English definition, where it lives in code, which dashboard card shows it, and a screenshot. Use recruiter-friendly names (no unit numbers).
**Dependencies:** T7-005, T7-006
**Acceptance Criteria:**
- [ ] All topics covered with file path + dashboard location + screenshot
- [ ] Each entry states the exact-vs-approximate comparison where applicable
- [ ] No syllabus unit-number wording anywhere in the repo (`grep -ri "unit[ -]\?\(iv\|4\)"` returns nothing)
**Estimated Effort:** 60 min
**Assigned To:** Yashwant Vadhan M (Integration) — M A Sushil Kumar and Priyan S supply their screenshots
**Due:** Sep 29 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write docs/CONCEPT_MAPPING.md as a table plus short sections: Concept | Plain-English meaning (1 sentence) | Implementation (file path) | Where it appears in the dashboard | Accuracy/comparison shown | Screenshot. Cover: stream data model, sampling (reservoir/Bernoulli), filtering streams, count distinct (exact/HyperLogLog/Flajolet–Martin), counting ones (DGIM), estimating moments (mean/variance/AMS F2), decaying windows, link analysis, PageRank, Markov transitions, market-basket model, limited-pass frequent itemsets (A-Priori, PCY). Never mention the syllabus unit number. Finish with a 'How to verify' checklist. Then run grep -ri "unit[ -]\?\(iv\|4\)" over the repo and fix any hits.
```

#### T9-002: Final README, architecture visuals and screenshots
**Description:** Polish `README.md` for recruiters: one-paragraph pitch, animated/annotated dashboard screenshots, architecture diagram (SVG/PNG in `docs/img/`), tech-stack badges, results (throughput, accuracy numbers from reports), how to run, team & roles, ethics note.
**Dependencies:** T7-009, T8-002
**Acceptance Criteria:**
- [ ] First screen of README explains what it is and shows a screenshot
- [ ] Numbers quoted match the reports (no invented metrics)
- [ ] Links to all docs work
**Estimated Effort:** 60 min
**Assigned To:** Naveena MS (Capture)
**Due:** Sep 29 (Tue) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Polish README.md for a recruiter audience: a two-sentence pitch, screenshot section (paths docs/img/*.png with alt text), an architecture diagram exported as docs/img/architecture.svg (create it from the ASCII diagram), tech badges (Python, Spark, Hadoop, Flume, Streamlit, NetworkX), a 'Results' table filled ONLY from docs/load_test_report.md, docs/test_report.md and docs/validation_report.md (leave TODO if a number is missing — never invent), quick start, repo structure, team & roles, ethics section. Keep the doc map and Git-workflow note. Do not mention unit numbers of any syllabus.
```

#### T9-003: Repository sanitisation audit and release tag
**Description:** Audit the repo before submission: no real IPs/MACs/pcaps in history or files, no secrets, no `serving/`/`logs/`, no syllabus unit-number wording, `ruff` clean, tests pass; tag `demo-v1`.
**Dependencies:** T9-001, T9-002
**Acceptance Criteria:**
- [ ] `scripts/check_no_captures.py` and the wording grep both pass
- [ ] `git log --all -- data/` shows only `data/sample/`
- [ ] Tag `demo-v1` pushed on the final commit
**Estimated Effort:** 30 min
**Assigned To:** Rithika GV (Ingestion)
**Due:** Sep 30 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Give me a checklist script scripts/pre_submit_audit.sh that: runs scripts/check_no_captures.py; greps tracked files for real-looking private IPs/MACs outside data/sample (allow 192.168.1.10 and RFC5737/198.18 ranges used in samples); checks `git log --all --name-only` for *.pcap*, .env, settings.yaml, serving/*.db, logs/; greps -ri "unit[ -]\?\(iv\|4\)" over tracked files; runs ruff and `pytest -m "not spark and not e2e"`; prints PASS/FAIL per check and exits non-zero on any failure. If history contains a real capture, print the exact git filter-repo command needed (do not run it).
```

#### T9-004: Demo script, roles and three rehearsals
**Description:** `docs/DEMO_SCRIPT.md`: the 16-step demo flow with **who speaks and operates each step**, timing (target 10–12 min), what to say, what should be visible, and the fallback per step. Then run three full rehearsals (live once, replay twice) and log results.
**Dependencies:** T8-004, T9-002
**Acceptance Criteria:**
- [ ] Every step has a named presenter and a fallback
- [ ] Three rehearsals logged with pass/fail and fixes
- [ ] Final rehearsal completes within the time limit without intervention
**Estimated Effort:** 90 min
**Assigned To:** All members (Priyan S drives the dashboard; Yashwant Vadhan M drives the terminal)
**Due:** Sep 30 (Wed) · **Priority:** 🔴 MUST
**AI Agent Prompt:**
```text
Write docs/DEMO_SCRIPT.md for a 10–12 minute demo with this run-of-show: 1 Naveena MS — connect Wi-Fi, start capture, show live records + Wireshark comparison; 2 Rithika GV — show Flume ingest + HDFS files + Hive/history query; 3 M A Sushil Kumar — start Spark stream, demonstrate filtering, window, distinct counts, counting ones, sampling; 4 Yashwant Vadhan M — moments, decaying window, market-basket itemsets, trigger the spike scenario; 5 Priyan S — graph, PageRank/Markov, alerts, full dashboard, history tab; 6 Yashwant Vadhan M — wrap-up mapping to the syllabus concepts. For each step: presenter, exact command or click path, what the audience should see, spoken line (≤2 sentences), fallback if it fails, time budget. Add a rehearsal log table (date, mode live/replay, duration, issues, fixes).
```

---

## Appendix

### Critical Path
> Anything on this path slipping by one day slips the demo — watch these daily.
1. T1-003 (record contract) → T2-006/T2-008 (sample data) → everyone's testing
2. T3-001 → T3-002 (Hadoop + Flume compatibility) → T3-004 (sinks + hidden-file test) → T3-010 (Spark connect)
3. T4-002 → T4-003 (runner) → all Spark plugins/queries (T4-004…, T5-001…)
4. T1-004/T1-005 (serving DB + mock) → all dashboard tasks (T7-*)
5. T8-001 (run_demo.sh) → T8-004 → T9-004 (rehearsals)

### Parallelizable Tasks
- Day 1–2: T1-003+T2-006 (Naveena MS) ‖ T3-001+T3-002 (Rithika GV) ‖ T4-001+T4-002 (M A Sushil Kumar) ‖ T1-001/2/4/5 (Yashwant Vadhan M) ‖ T6-001…T6-004 (Priyan S, pure Python)
- Day 4–7: M A Sushil Kumar's T4-006…T4-009 ‖ Yashwant Vadhan M's T5-002…T5-004 ‖ Priyan S's T7-004…T7-007 ‖ Rithika GV's T3-008/T3-009
- T6-006, T5-009, T4-010 (tests) can run in parallel after their modules land

### Risk Register
| Risk | Affected Tasks | Likelihood | Mitigation |
|---|---|---|---|
| Hadoop 3.x ↔ Flume jar conflict | T3-002, T3-004 | Medium | Gate G1 (22 Sep 22:00); fallback: Flume `file_roll` sink → local dir that Spark reads, HDFS copy via `hdfs dfs -put` cron |
| Spark reads half-written files | T3-004 | Medium | Hidden-file test; fallback atomic rename staging |
| No time for Hive | T3-008, T3-009 | Medium | Fallback: Spark SQL over `/traffic/raw` CSV (same queries), Hive as stretch |
| Live Wi-Fi capture blocked (permissions/campus network) | T2-001, T2-004 | High | Replay mode from day 1; demo can run fully on replay; live capture attempted 23 Sep |
| Integration surprises late | T4-003, T8-001 | High | Vertical slice by 23 Sep; integration day 27 Sep; feature freeze 29 Sep 12:00 |
| Push-to-main breakage | all | Medium | Folder ownership, pull --rebase, CI on every push, fix-forward in 30 min |
| Scope too big for 11 days | T5-006/7, T3-011, T6-006 | High | Priority tags; cut list: compaction, retention, FPGrowth, SON, Docker |
| Single point of failure member | any | Medium | Each stage has a documented owner AND a buddy (see schedule) |
| SQLite lock contention | T4-003, T6-005, T7-002 | Low | WAL + busy_timeout + read-only dashboard |

### Recommended Build Order (single team, 11 days)
1. Contract + sample data + repo + mock DB (Sep 21–22)
2. Flume/HDFS + Spark runner + first window metrics = **vertical slice** (Sep 22–23)
3. Algorithms in parallel (Sep 24–26); dashboard on mock DB in parallel
4. Switch dashboard to live data, integrate, cross-validate (Sep 27)
5. History, alerts, polish, tests (Sep 28); **freeze 29 Sep 12:00**
6. Rehearsals, fallback snapshot, audit, tag (Sep 29–30); demo Oct 1
