# ROADMAP — Live Network Traffic Analyser (LNTA)

> **Hard deadline: submission + live demo on Thursday 1 October 2026.** **Work starts Monday 21 September 2026** → 11 days including demo day. The demo date does not move; scope does. Day-by-day task assignments per member are in [`todo.md`](todo.md).

---

## Overview

Delivery is **contract-first → vertical slice → breadth → freeze → rehearse**:

```text
Sep 21  22 | 23  24 | 25  26 | 27 | 28 | 29 | 30 | Oct 1
[M0 Foundation][M1 Slice ][ M2 Algorithms + Link + Dashboard ][INT][M3 ][FRZ][M5 ][DEMO]
              G1(21st)    G2(23rd)                             G3    Polish G4   Rehearse
```

Team: Naveena MS (capture) · Rithika GV (ingestion/storage) · M A Sushil Kumar (Spark core + counting/sampling/distinct) · Yashwant Vadhan M (advanced streaming + integration) · Priyan S (link analysis + dashboard). Everyone pushes to `main` (TECH_RULES §10).

### Decision Gates
| Gate | When | Question | Fallback if NO |
|---|---|---|---|
| G1 | Sep 22, 22:00 | Flume → HDFS works with our Hadoop version? | Flume `file_roll` sink → local dir for Spark; raw copy to HDFS via `hdfs dfs -put` |
| G2 | Sep 24, 12:00 | Vertical slice runs end-to-end (replay → … → dashboard)? | All hands on the slice; algorithms wait |
| G3 | Sep 27, 22:00 | All MUST algorithms visible on the dashboard? | Cut list (below) |
| G4 | Sep 29, 12:00 | Feature freeze | Bug fixes only |

### Cut List (pre-agreed, in this order)
1. Multi-window 30 s, load-test extremes, SON · 2. Hive (use Spark SQL over HDFS) · 3. Fault tests beyond Flume-kill · 4. E2E suite reduced to 4 scenarios · 5. Alerts reduced to spike + port activity. **Never cut:** replay mode, dashboard, the Data Contract, rehearsals.

---

## Milestone Structure

### Milestone 0 — Foundation & Contract (Sep 21–22)
- **Objective:** Everyone can work independently on the same contract with mock/sample data.
- **Tasks/Features:** remote-access VPN + SSH for the distributed team (T1-007, Yashwant Vadhan M), repo + ownership map (Yashwant Vadhan M), config/logging, record contract (Naveena MS), serving DDL + mock DB (Yashwant Vadhan M), generator + samples (Naveena MS), Hadoop + Flume install and compatibility check (Rithika GV), Spark env + schema/cleaning (M A Sushil Kumar), graph/PageRank/Markov pure logic + dashboard skeleton (Priyan S).
- **Dependencies:** None.
- **Estimated Complexity:** Medium.

#### Milestone 0 Progress Tracker (as of Sep 23)
| Task ID | Component / Description | Owner | Status | Details / Commit |
|---|---|---|---|---|
| **T1-001** | Repo scaffold, `.gitignore`, `docs/OWNERS.md` | Yashwant Vadhan M | ✅ **Completed** | Clean modular scaffold across all layers (`a552884`) |
| **T1-002** | Config loader (`common/config.py`), logging setup, sample YAML | Yashwant Vadhan M | ✅ **Completed** | Tested defaults, env overrides, UTC logging (`045ab5e`) |
| **T1-004** | Serving DDL (`contracts/serving_schema.sql`), SQLite helper (`common/serving_db.py`) | Yashwant Vadhan M | ✅ **Completed** | All 16 tables, WAL mode, partial upserts, cleanup (`474a465`) |
| **T1-007** | Remote-access VPN overlay + SSH hardening (`docs/REMOTE_ACCESS.md`, script) | Yashwant Vadhan M | ✅ **Completed** | Host hardening script, client guide, tunnel guide (`602680d`) |
| **T1-005** | Mock data seeder with live mode (`scripts/seed_mock_db.py`) | Yashwant Vadhan M | ✅ **Completed** | 16-table generator with cross-table consistency & live feed (`c32d582`) |
| **T1-006** | Dev tooling, pre-commit capture check, CI, `docs/ENVIRONMENT.md` | Yashwant Vadhan M | ✅ **Completed** | Pinned deps, ruff, pytest markers, GitHub Actions workflow (`7a31e55`) |
| **T4-001** | Spark env, session factory (`streaming/common/session.py`) | M A Sushil Kumar | ✅ **Completed** | Committed in `0345f97` |
| **T4-002** | Stream schema, parsing and cleaning (`streaming/common/{schema,cleaning}.py`) | M A Sushil Kumar | ✅ **Completed** | Committed in `0345f97` |
| **T4-003** | Streaming App runner & plugin dispatcher (`streaming/stream_app.py`) | M A Sushil Kumar | ✅ **Completed** | Committed in `7da02a4` |
| **T4-004** | Tumbling window metrics query (`streaming/queries/window_metrics.py`) | M A Sushil Kumar | ✅ **Completed** | Committed in `7da02a4` |
| **T2-001** | TShark subprocess wrapper & command builder (`capture/tshark_cmd.py`) | Naveena MS | ✅ **Completed** | Committed in `c0580a7` |
| **T3-001** | Flume agent configuration (`ingestion/flume/lnta-agent.conf`) | Rithika GV | ✅ **Completed** | Committed in `190c6b0` |
| **T3-002** | HDFS zone layout script (`ingestion/hdfs/init_zones.sh`) | Rithika GV | ✅ **Completed** | Committed in `190c6b0` |
| **T3-003** | Hive external tables DDL & historical queries (`ingestion/hive/`) | Rithika GV | ✅ **Completed** | Committed in `190c6b0` |
| **T3-004** | Ingestion control & verification (`ingestion/scripts/ingestion_ctl.py`) | Rithika GV | ✅ **Completed** | Committed in `190c6b0` |

- **Acceptance Criteria:**
  - [x] Contract validator, generator scaffold pushed
  - [x] Serving DDL (`contracts/serving_schema.sql`) and database helper pushed
  - [x] Flume and HDFS zone setup scripts pushed
  - [x] CI runs on push to `main`

### Milestone 1 — Vertical Slice (Sep 23–24)
- **Objective:** Prove Capture/Replay → Flume → HDFS → Spark → SQLite → dashboard with the simplest analytics.
- **Features (MUST):** rotating CSV writer + capture runner + replay; Flume TAILDIR + two HDFS sinks + hidden-file test; Spark runner skeleton, window metrics, protocol/port counts; KPI + overview charts on live DB; ingestion verifier.
- **Dependencies:** M0.
- **Estimated Complexity:** High (first integration of four tools).

#### Advanced Analytics Progress Tracker (Milestones 1 & 2 items completed early)
| Task ID | Component / Description | Owner | Status | Details / Commit |
|---|---|---|---|---|
| **T5-003** | Exponentially decaying window (`streaming/analytics/decay.py`) | Yashwant Vadhan M | ✅ **Completed** | Continuous decay, top-k heavy hitters, bounded memory (`9c698c6`) |
| **T5-001** | Windowed moments query (`streaming/queries/moments_window.py`) | Yashwant Vadhan M | ✅ **Completed** | Packet length moments, inter-arrival stats, null variance handling (`25e83ec`) |
| **T5-005** | IP edges and source stats queries (`streaming/queries/edges.py`) | Yashwant Vadhan M | ✅ **Completed** | Directed edges aggregation & per-source fan-out stats (`580cb7a`) |

- **Acceptance Criteria:**
  - [x] `window_metrics` and advanced queries (`moments`, `edges`, `source_stats`, `decay`) implemented in Spark Lane A & B
  - [ ] 5-minute replay: captured lines == lines in HDFS raw and stream_in
  - [ ] Tag `m1-slice` (Gate G2, Sep 24 noon)

### Milestone 2 — Algorithms, Link Analysis & Dashboard Tabs (Sep 24–27)
- **Objective:** Every concept produces correct, verifiable output and is visible.
- **Features:** M A Sushil Kumar — filtering, sampling, distinct (exact/HLL/FM), counting ones (DGIM). Yashwant Vadhan M — moments (+AMS F2), decaying windows, market-basket itemsets (A-Priori, PCY), edges/source stats. Priyan S — alerts, Stream Analytics tab, Link Analysis tab (graph, PageRank, centrality, Markov), Alerts tab. Rithika GV — Hive/Spark SQL historical queries.
- **Dependencies:** M1.
- **Estimated Complexity:** High.
- **Acceptance Criteria:**
  - [ ] Each algorithm has unit tests against hand-computed/pandas ground truth
  - [ ] Exact-vs-estimate columns populated (distinct, DGIM, sampling, F2)
  - [ ] PageRank on the toy graph (A→B, A→C, C→B, D→B) matches hand calculation; Markov rows sum to 1
  - [ ] **Sep 27 integration day:** dashboard on live data; cross-validation passes (Gate G3); tag `m2-core`

### Milestone 3 — History, Alerts, Polish (Sep 28)
- **Objective:** Complete SHOULD items; make the dashboard demo-grade.
- **Features:** History + Pipeline tabs, states/auto-refresh/accessibility pass, dashboard tests, multi-window (10 s/60 s), light load test, validation report.
- **Dependencies:** M2.
- **Estimated Complexity:** Medium.
- **Acceptance Criteria:**
  - [ ] All tabs render with live, mock and empty data; refresh ≤ 3 s without flicker
  - [ ] Spike, port-activity and fan-out scenarios trigger the intended alerts with correct explanations
  - [ ] Load test at 1,000 and 2,000 pkts/s recorded (whatever the result — honestly)

### Feature Freeze — Sep 29, 12:00 (G4)
Bug fixes only. Afternoon: E2E scenario suite, runbook + offline snapshot, concept-mapping doc, README with screenshots.

### Milestone 5 — Rehearse & Submit (Sep 30)
- **Objective:** A demo that cannot embarrass the team.
- **Features:** demo script with named presenters, three rehearsals (one live, two replay), sanitisation audit, tag `demo-v1`, submission package.
- **Acceptance Criteria:**
  - [ ] Three rehearsals logged; last one finishes in 10–12 min without intervention
  - [ ] Replay-mode demo works with Wi-Fi off; snapshot fallback works
  - [ ] `scripts/pre_submit_audit.sh` passes (no real captures, no secrets, no syllabus unit numbers in the repo)

### Oct 1 — Demo & Submission
Arrive early → `python scripts/verify_env.py` → one dry run → present per `docs/DEMO_SCRIPT.md` → submit.

### Post-Submission (optional growth)
Hotspot/monitor-mode capture, Kafka hand-off, distributed PageRank (GraphFrames), SON on Spark, Docker Compose, PDF session report.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Flume ↔ Hadoop 3.x jar mismatch | Medium | High | Test Sep 21–22; Gate G1; `file_roll` fallback |
| Spark reads half-written Flume files | Medium | High | `inUsePrefix=.` + acceptance test; atomic-rename fallback |
| Live Wi-Fi capture blocked (permissions, Windows/WSL2, campus network) | High | High | Replay + generator from day 1; live capture tried Sep 23–24; demo can run fully on replay |
| Scope too large for 11 days | High | High | MUST/SHOULD/CUT tags, cut list, gates |
| Late integration surprises | High | High | Vertical slice by Sep 24 noon; integration day Sep 27; freeze Sep 29 |
| Broken `main` (no branches/PRs) | Medium | Medium | Folder ownership, `pull --rebase`, CI on every push, fix-forward in 30 min |
| Team is in three places (two homes, one hostel) behind NAT | High | High | Private VPN overlay + SSH (T1-007, Sep 21); Laptop A kept powered/awake with tmux sessions; screen-share fallback; replay-only development for everyone else |
| Only two laptops for a 5-person team; laptop-to-laptop TCP may be blocked | Medium | High | Laptop A = pipeline, Laptop B = capture (TECH_RULES §11.1); test link Sep 22; fallback hotspot/Ethernet/replay; book Laptop A for Sep 27 and Sep 30 |
| A member blocked or unavailable | Medium | High | Buddy pairs (todo.md §1); documented interfaces |
| Spark batch time > trigger | Medium | Medium | Row caps, larger trigger, reduce windows |
| Managed-mode capture shows only own device → simple graph | Medium | Medium | Include replies/broadcast; synthetic multi-host scenario for graph demo (clearly labelled) |
| SQLite lock contention | Low | Medium | WAL, busy_timeout, read-only dashboard |
| Privacy incident (real captures pushed) | Low | High | `.gitignore`, CI check, anonymiser, audit script |
| Alert thresholds noisy | Medium | Low | Config-driven; calibrate on rehearsal data; cooldown |
