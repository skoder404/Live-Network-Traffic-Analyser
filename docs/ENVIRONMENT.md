# ENVIRONMENT.md — Verified Component Environment

> Single reference table for all verified library, framework, and tool versions across the team.
> References: [`TECH_RULES.md`](TECH_RULES.md) §2, [`README.md`](../README.md) §4.

---

## 1. Verified Component Stack

| Component | Target Baseline | Minimum Supported | Verified Version | Verified By | Date | Notes |
|---|---|---|---|---|---|---|
| **OS (Laptop A - Pipeline)** | Ubuntu 22.04 / 24.04 LTS | Ubuntu 20.04 | Ubuntu 24.04 LTS | Yashwant Vadhan M | 2026-09-21 | Native Linux on Laptop A host |
| **Java Development Kit (JDK)** | OpenJDK 11 / 17 | JDK 11 | OpenJDK 17.0.10 | Yashwant Vadhan M | 2026-09-21 | Required by Hadoop, Flume, Spark |
| **Python** | 3.10 / 3.11 | 3.10 | 3.12 (host) / 3.11 (CI) | Yashwant Vadhan M | 2026-09-21 | PySpark 3.5 requires Python >= 3.8 |
| **Apache Spark** | 3.5.x | 3.5.0 | 3.5.1 | M A Sushil Kumar | 2026-09-21 | PySpark Structured Streaming (`local[2]`) |
| **Apache Hadoop / HDFS** | 3.3.x | 3.3.0 | 3.3.6 | Rithika GV | 2026-09-22 | Pseudo-distributed mode on Laptop A |
| **Apache Flume** | 1.11.x | 1.10.0 | 1.11.0 | Rithika GV | 2026-09-22 | TAILDIR source -> replicating selector |
| **Apache Hive** | 3.1.x | 3.1.0 | 3.1.3 | Rithika GV | 2026-09-23 | Metastore & external tables over HDFS |
| **TShark / Wireshark** | 4.x | 4.0.0 | 4.2.2 | Naveena MS | 2026-09-22 | Non-root capture via `wireshark` group |
| **Streamlit** | 1.30.x | 1.28.0 | 1.32.0 | Priyan S | 2026-09-22 | Dashboard live view with auto-refresh |
| **NetworkX** | 3.x | 3.0 | 3.2.1 | Priyan S | 2026-09-22 | Graph analysis, PageRank, Markov |
| **SQLite** | 3.37+ | 3.35.0 (for RETURNING / WAL) | 3.45.1 | Yashwant Vadhan M | 2026-09-21 | Serving store with WAL mode & partial upsert |
| **Tailscale (VPN Mesh)** | Latest (1.60+) | 1.40+ | 1.62.0 | Yashwant Vadhan M | 2026-09-21 | Private encrypted overlay for distributed team |

---

## 2. Environment Verification Commands

Run the automated verification script on Laptop A before rehearsals and demo:

```bash
python3 scripts/verify_env.py
```

### Manual Sanity Checks:

```bash
# 1. Java check
java -version

# 2. Python check
python3 --version

# 3. SQLite WAL check
sqlite3 :memory: "PRAGMA journal_mode=WAL;"

# 4. Git status
git status
```

