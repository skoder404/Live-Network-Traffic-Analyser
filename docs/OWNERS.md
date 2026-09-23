# OWNERS.md — Component Ownership & Git Workflow

> Binding ownership map and trunk-based Git rules for the Live Network Traffic Analyser (LNTA) project.
> References: [`TECH_RULES.md`](TECH_RULES.md) §10, [`README.md`](../README.md) §1.

---

## 1. Component Ownership Map

To maintain velocity with an 11-day delivery deadline while avoiding merge conflicts, each team member directly owns specific modules. **You may only edit files inside your owned folders.**

| Member | Owned Folders / Components | Description & Scope |
|---|---|---|
| **Naveena MS** | `capture/`, `data/sample/` | TShark subprocess, packet line parser/normaliser, rotating CSV/JSON/TCP writers, synthetic generator, replay tool, anonymisation |
| **Rithika GV** | `ingestion/` (`flume/`, `hdfs/`, `hive/`, `scripts/`) | Flume agent configuration, HDFS directory layout & permissions, Hive external tables & historical queries, ingestion verification |
| **M A Sushil Kumar** | `streaming/common/`<br>`streaming/queries/window_metrics.py`<br>`streaming/queries/counts.py`<br>`streaming/queries/filter_counts.py`<br>`streaming/queries/distinct.py`<br>`streaming/analytics/filters.py`<br>`streaming/analytics/sampling.py`<br>`streaming/analytics/fm.py`<br>`streaming/analytics/dgim.py` | PySpark session setup, stream schema & cleaning, Lane A native windowed queries (packets/sec, bytes/sec, protocol & port counts), stream filtering, sampling (Bernoulli & reservoir), HyperLogLog & Flajolet–Martin distinct counting, DGIM stream algorithm |
| **Yashwant Vadhan M** | `contracts/`<br>`common/`<br>`scripts/`<br>`config/`<br>other `streaming/` files (`streaming/stream_app.py`, `streaming/queries/moments_window.py`, `streaming/analytics/{moments,decay,itemsets,edges}.py`) | Shared contracts (record & serving schemas), configuration loader & logging, SQLite serving database interface (WAL, partial upserts), remote-access infrastructure, runner dispatcher, windowed moments, AMS second moment (F2), decaying windows, market-basket itemsets (A-Priori & PCY), end-to-end integration & demo automation |
| **Priyan S** | `linkanalysis/`, `dashboard/` | IP communication graph builder, degree & centrality metrics, PageRank (NetworkX & from-scratch power iteration), Markov transition model, rule-based explainable alerts, Streamlit multi-tab dashboard & UI components |

> [!IMPORTANT]
> **Cross-Folder Edits:**
> Any proposed modification to `contracts/` or shared files (`common/`, `config/settings.example.yaml`, `requirements.txt`, `README.md`) must be announced in the team chat **before** pushing to `main` and acknowledged by all affected members.

---

## 2. Trunk-Based Git Rules

All team members push directly to the `main` branch. This requires strict adherence to the following safeguards:

1. **Own Your Folder:** Never edit files outside your assigned folder without prior coordination.
2. **Rebase Before Every Push:** Always synchronize with remote changes before pushing:
   ```bash
   git pull --rebase origin main
   ```
3. **Run Tests Locally:** Never push failing tests. At minimum run:
   ```bash
   python3 -m unittest discover -s tests/unit -p "test_*.py"
   # or when pytest is installed:
   pytest -m "not spark and not e2e"
   ```
4. **Never Force-Push:** `git push --force` or `--force-with-lease` is strictly prohibited on `main`.
5. **Small, Frequent Commits:** Commit often using Conventional Commits naming:
   - `feat(capture): add IPv6 coalescing in parser`
   - `fix(flume): configure inUsePrefix for active HDFS files`
   - `test(dgim): add edge cases for bucket merging`
   - `docs: update data contract specification`
6. **No Data Files or Secrets:**
   - Never commit raw `.pcap` or `.pcapng` capture files.
   - Never commit `.env` or files containing live private credentials.
   - Only anonymised sample CSVs are permitted inside `data/sample/`.
7. **Fix-Forward Rule:** If a commit inadvertently breaks `main`, the author must fix-forward within 30 minutes or immediately `git revert` the commit.

