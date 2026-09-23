"""
scripts/seed_mock_db.py — High-fidelity mock data generator for LNTA serving store.

Populates all 16 SQLite tables defined in TECH_RULES §5.2 with internally consistent,
realistic streaming analytics records.
Supports:
  --db PATH       Target SQLite database path (default from config or serving/analytics.db)
  --seed N        Random seed for deterministic generation
  --live          Continuously appends new 10s windows every 2 seconds for UI development
  --reset         Drops existing tables / resets database before seeding
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from common.config import load_config
from common.serving_db import connect, init_schema, upsert

# Node topology for the 12-node graph:
# Node 1 is the main gateway/hub (high degree).
# Nodes 2-10 are regular LAN clients communicating with external services and hub.
# Node 11 is an isolated/dangling client.
# Node 12 is a DNS/NTP resolver.
MOCK_IPS = [
    "192.168.1.1",     # Hub / Router Gateway
    "192.168.1.50",    # Client A
    "192.168.1.51",    # Client B
    "192.168.1.52",    # Client C
    "192.168.1.53",    # Client D
    "8.8.8.8",         # Public DNS 1
    "1.1.1.1",         # Public DNS 2
    "142.250.190.46",  # Web Server (HTTPS)
    "157.240.22.35",   # Social Media Server
    "104.244.42.1",    # CDN Server
    "192.168.1.99",    # Dangling / Isolated Node
    "192.168.1.254",   # Local DNS / PiHole
]

COMMON_PORTS = [443, 80, 53, 22, 8080, 123]


def format_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def generate_window_dataset(
    start_dt: datetime,
    window_len_s: int = 10,
    is_spike: bool = False,
    rng: random.Random | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Generates a single consistent slice of data for all 16 tables across the specified window.
    """
    if rng is None:
        rng = random.Random(42)

    ws_str = format_iso(start_dt)
    mult = 3.5 if is_spike else 1.0

    # 1. window_metrics & protocol_counts (Exact sum consistency)
    tcp_pkts = int(rng.randint(250, 400) * mult)
    udp_pkts = int(rng.randint(60, 120) * mult)
    icmp_pkts = int(rng.randint(5, 15) * mult)
    other_pkts = int(rng.randint(2, 8) * mult)
    total_pkts = tcp_pkts + udp_pkts + icmp_pkts + other_pkts

    tcp_bytes = tcp_pkts * rng.randint(650, 950)
    udp_bytes = udp_pkts * rng.randint(90, 180)
    icmp_bytes = icmp_pkts * 64
    other_bytes = other_pkts * 128
    total_bytes = tcp_bytes + udp_bytes + icmp_bytes + other_bytes

    pps = round(total_pkts / window_len_s, 2)
    bps = round(total_bytes * 8 / window_len_s, 2)

    window_metrics = [
        {
            "window_start": ws_str,
            "window_len_s": window_len_s,
            "packets": total_pkts,
            "bytes": total_bytes,
            "pps": pps,
            "bps": bps,
        }
    ]

    protocol_counts = [
        {"window_start": ws_str, "window_len_s": window_len_s, "protocol": "TCP", "packets": tcp_pkts, "bytes": tcp_bytes},
        {"window_start": ws_str, "window_len_s": window_len_s, "protocol": "UDP", "packets": udp_pkts, "bytes": udp_bytes},
        {"window_start": ws_str, "window_len_s": window_len_s, "protocol": "ICMP", "packets": icmp_pkts, "bytes": icmp_bytes},
        {"window_start": ws_str, "window_len_s": window_len_s, "protocol": "OTHER", "packets": other_pkts, "bytes": other_bytes},
    ]

    # 2. port_counts (Top destination ports)
    port_distribution = [
        (443, int(tcp_pkts * 0.75)),
        (80, int(tcp_pkts * 0.15)),
        (53, int(udp_pkts * 0.85)),
        (22, int(tcp_pkts * 0.05)),
        (8080, int(tcp_pkts * 0.05)),
    ]
    port_counts = [
        {
            "window_start": ws_str,
            "window_len_s": window_len_s,
            "port": p,
            "packets": pkts,
            "bytes": pkts * rng.randint(500, 1100),
        }
        for p, pkts in port_distribution
    ]

    # 3. filter_counts
    web_pkts = sum(r["packets"] for r in port_counts if r["port"] in (80, 443))
    dns_pkts = sum(r["packets"] for r in port_counts if r["port"] == 53)
    filter_counts = [
        {"window_start": ws_str, "window_len_s": window_len_s, "filter_name": "web_traffic", "packets": web_pkts, "bytes": web_pkts * 800},
        {"window_start": ws_str, "window_len_s": window_len_s, "filter_name": "dns_traffic", "packets": dns_pkts, "bytes": dns_pkts * 120},
    ]

    # 4. distinct_counts (exact vs HLL within 5%, FM within 25%)
    src_exact = rng.randint(8, 11)
    dst_exact = rng.randint(10, 12)
    ports_exact = rng.randint(15, 25)

    src_hll = int(round(src_exact * rng.uniform(0.97, 1.03)))
    dst_hll = int(round(dst_exact * rng.uniform(0.96, 1.04)))
    ports_hll = int(round(ports_exact * rng.uniform(0.96, 1.04)))

    src_fm = int(round(src_exact * rng.uniform(0.80, 1.20)))
    dst_fm = int(round(dst_exact * rng.uniform(0.80, 1.20)))

    distinct_counts = [
        {
            "window_start": ws_str,
            "window_len_s": window_len_s,
            "src_ips_exact": src_exact,
            "dst_ips_exact": dst_exact,
            "ports_exact": ports_exact,
            "src_ips_hll": src_hll,
            "dst_ips_hll": dst_hll,
            "ports_hll": ports_hll,
            "src_ips_fm": src_fm,
            "dst_ips_fm": dst_fm,
        }
    ]

    # 5. moments (Packet length moments & inter-arrival stats)
    mean_len = round(total_bytes / total_pkts, 2)
    var_len = round(rng.uniform(45000.0, 95000.0), 2)
    std_len = round(math.sqrt(var_len), 2)
    iat_mean = round(window_len_s * 1000.0 / total_pkts, 3)
    iat_var = round((iat_mean * 0.8) ** 2, 3)
    iat_std = round(math.sqrt(iat_var), 3)

    f2_exact = round(sum(r["packets"] ** 2 for r in protocol_counts), 2)
    f2_ams = round(f2_exact * rng.uniform(0.92, 1.08), 2)

    moments = [
        {
            "window_start": ws_str,
            "window_len_s": window_len_s,
            "n": total_pkts,
            "mean_len": mean_len,
            "var_len": var_len,
            "std_len": std_len,
            "iat_mean_ms": iat_mean,
            "iat_var_ms": iat_var,
            "iat_std_ms": iat_std,
            "f2_exact": f2_exact,
            "f2_ams": f2_ams,
        }
    ]

    # 6. frequent_itemsets (A-Priori and PCY)
    frequent_itemsets = [
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "apriori", "itemset": "TCP:443", "size": 1, "support_count": int(total_pkts * 0.55), "support_ratio": 0.55, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "apriori", "itemset": "UDP:53", "size": 1, "support_count": int(total_pkts * 0.22), "support_ratio": 0.22, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "apriori", "itemset": "TCP:80", "size": 1, "support_count": int(total_pkts * 0.12), "support_ratio": 0.12, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "apriori", "itemset": "UDP:53 + TCP:443", "size": 2, "support_count": int(total_pkts * 0.10), "support_ratio": 0.10, "passes": 2},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "pcy", "itemset": "TCP:443", "size": 1, "support_count": int(total_pkts * 0.55), "support_ratio": 0.55, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "pcy", "itemset": "UDP:53", "size": 1, "support_count": int(total_pkts * 0.22), "support_ratio": 0.22, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "pcy", "itemset": "TCP:80", "size": 1, "support_count": int(total_pkts * 0.12), "support_ratio": 0.12, "passes": 1},
        {"window_start": ws_str, "window_len_s": window_len_s, "algorithm": "pcy", "itemset": "UDP:53 + TCP:443", "size": 2, "support_count": int(total_pkts * 0.10), "support_ratio": 0.10, "passes": 2},
    ]

    # 7. ip_edges (12-node graph with hub MOCK_IPS[0] and dangling node MOCK_IPS[10])
    ip_edges = [
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[1], "dst_ip": MOCK_IPS[0], "packets": int(total_pkts * 0.25), "bytes": int(total_bytes * 0.25)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[2], "dst_ip": MOCK_IPS[0], "packets": int(total_pkts * 0.20), "bytes": int(total_bytes * 0.20)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[3], "dst_ip": MOCK_IPS[0], "packets": int(total_pkts * 0.15), "bytes": int(total_bytes * 0.15)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[0], "dst_ip": MOCK_IPS[7], "packets": int(total_pkts * 0.20), "bytes": int(total_bytes * 0.20)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[0], "dst_ip": MOCK_IPS[8], "packets": int(total_pkts * 0.10), "bytes": int(total_bytes * 0.10)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[4], "dst_ip": MOCK_IPS[11], "packets": int(total_pkts * 0.05), "bytes": int(total_bytes * 0.05)},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[0], "dst_ip": MOCK_IPS[5], "packets": int(total_pkts * 0.05), "bytes": int(total_bytes * 0.05)},
    ]

    # 8. source_stats
    source_stats = [
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[0], "packets": int(total_pkts * 0.35), "bytes": int(total_bytes * 0.35), "unique_dst_ips": 5, "unique_dst_ports": 4},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[1], "packets": int(total_pkts * 0.25), "bytes": int(total_bytes * 0.25), "unique_dst_ips": 2, "unique_dst_ports": 3},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[2], "packets": int(total_pkts * 0.20), "bytes": int(total_bytes * 0.20), "unique_dst_ips": 2, "unique_dst_ports": 2},
        {"window_start": ws_str, "window_len_s": window_len_s, "src_ip": MOCK_IPS[3], "packets": int(total_pkts * 0.15), "bytes": int(total_bytes * 0.15), "unique_dst_ips": 1, "unique_dst_ports": 2},
    ]

    # 9. Time-series timestamp tables (ts = window end)
    end_dt = start_dt + timedelta(seconds=window_len_s)
    ts_str = format_iso(end_dt)

    sampling_compare = [
        {"ts": ts_str, "method": "bernoulli", "k": 1000, "sample_n": int(total_pkts * 0.1), "sample_mean_len": round(mean_len * rng.uniform(0.96, 1.04), 2), "full_mean_len": mean_len, "err_pct": round(rng.uniform(1.2, 4.5), 2)},
        {"ts": ts_str, "method": "reservoir", "k": 1000, "sample_n": min(total_pkts, 1000), "sample_mean_len": round(mean_len * rng.uniform(0.97, 1.03), 2), "full_mean_len": mean_len, "err_pct": round(rng.uniform(0.8, 3.2), 2)},
    ]

    exact_tcp_ones = tcp_pkts
    dgim_est = int(round(exact_tcp_ones * rng.uniform(0.93, 1.07)))
    counting_ones = [
        {"ts": ts_str, "predicate_name": "is_tcp", "window_n": total_pkts, "exact_ones": exact_tcp_ones, "dgim_estimate": dgim_est, "err_pct": round(abs(dgim_est - exact_tcp_ones) * 100.0 / exact_tcp_ones, 2)}
    ]

    decay_traffic = [
        {"ts": ts_str, "half_life_s": 10, "score": round(pps * 1.05, 2), "raw_pps": pps},
        {"ts": ts_str, "half_life_s": 30, "score": round(pps * 0.98, 2), "raw_pps": pps},
        {"ts": ts_str, "half_life_s": 60, "score": round(pps * 0.92, 2), "raw_pps": pps},
    ]

    decay_top_keys = [
        {"ts": ts_str, "key_type": "dst_ip", "key": MOCK_IPS[0], "score": round(pps * 0.40, 2), "rank": 1},
        {"ts": ts_str, "key_type": "dst_ip", "key": MOCK_IPS[7], "score": round(pps * 0.25, 2), "rank": 2},
        {"ts": ts_str, "key_type": "dst_port", "key": "443", "score": round(pps * 0.60, 2), "rank": 1},
        {"ts": ts_str, "key_type": "dst_port", "key": "53", "score": round(pps * 0.20, 2), "rank": 2},
    ]

    pipeline_health = [
        {"ts": ts_str, "component": "spark", "metric": "batch_duration_s", "value": round(rng.uniform(0.8, 2.1), 3)},
        {"ts": ts_str, "component": "spark", "metric": "input_rows_per_batch", "value": float(total_pkts)},
        {"ts": ts_str, "component": "flume", "metric": "events_per_sec", "value": pps},
        {"ts": ts_str, "component": "capture", "metric": "drop_queue_full", "value": 0.0},
    ]

    alerts: list[dict[str, Any]] = []
    if is_spike:
        alerts.append({
            "alert_id": f"alt-spike-{int(start_dt.timestamp())}",
            "ts": ts_str,
            "type": "spike",
            "severity": "CRITICAL" if mult > 3.0 else "WARN",
            "src_ip": MOCK_IPS[1],
            "metric": "pps",
            "current_value": pps,
            "baseline_value": round(pps / mult, 2),
            "change_pct": round((mult - 1.0) * 100.0, 1),
            "threshold": 3.0,
            "reason": f"Traffic volume spiked {mult:.1f}x above EWMA baseline",
            "details_json": json.dumps({"window_pkts": total_pkts, "normal_pkts": int(total_pkts / mult)}),
        })

    hist_results = [
        {
            "query_name": "daily_protocol_distribution",
            "run_at": ts_str,
            "columns_json": json.dumps(["protocol", "packets", "bytes"]),
            "rows_json": json.dumps([
                ["TCP", tcp_pkts * 10, tcp_bytes * 10],
                ["UDP", udp_pkts * 10, udp_bytes * 10],
                ["ICMP", icmp_pkts * 10, icmp_bytes * 10],
            ]),
        }
    ]

    return {
        "window_metrics": window_metrics,
        "protocol_counts": protocol_counts,
        "port_counts": port_counts,
        "filter_counts": filter_counts,
        "distinct_counts": distinct_counts,
        "moments": moments,
        "frequent_itemsets": frequent_itemsets,
        "ip_edges": ip_edges,
        "source_stats": source_stats,
        "sampling_compare": sampling_compare,
        "counting_ones": counting_ones,
        "decay_traffic": decay_traffic,
        "decay_top_keys": decay_top_keys,
        "pipeline_health": pipeline_health,
        "alerts": alerts,
        "hist_results": hist_results,
    }


TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "window_metrics": ["window_start", "window_len_s"],
    "protocol_counts": ["window_start", "window_len_s", "protocol"],
    "port_counts": ["window_start", "window_len_s", "port"],
    "filter_counts": ["window_start", "window_len_s", "filter_name"],
    "distinct_counts": ["window_start", "window_len_s"],
    "sampling_compare": ["ts", "method"],
    "counting_ones": ["ts", "predicate_name"],
    "moments": ["window_start", "window_len_s"],
    "decay_traffic": ["ts", "half_life_s"],
    "decay_top_keys": ["ts", "key_type", "key"],
    "frequent_itemsets": ["window_start", "window_len_s", "algorithm", "itemset"],
    "ip_edges": ["window_start", "window_len_s", "src_ip", "dst_ip"],
    "source_stats": ["window_start", "window_len_s", "src_ip"],
    "alerts": ["alert_id"],
    "pipeline_health": ["ts", "component", "metric"],
    "hist_results": ["query_name", "run_at"],
}


def seed_database(
    db_path: str | Path,
    duration_min: int = 10,
    seed: int = 42,
    reset: bool = False,
) -> None:
    """
    Seeds the serving store with full historical mock data for all tables.
    """
    rng = random.Random(seed)
    conn = connect(db_path)

    if reset:
        # Re-initialize schema
        init_schema(conn)

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=duration_min)
    window_s = 10

    total_windows = (duration_min * 60) // window_s
    spike_window_idx = total_windows - 6  # Spike occurs in the last 60-90s

    print(f"[+] Seeding {duration_min} minutes ({total_windows} windows) into {db_path}...")

    for i in range(total_windows):
        w_start = start_time + timedelta(seconds=i * window_s)
        is_spike = i >= spike_window_idx

        # 10s Window slice
        data_10s = generate_window_dataset(w_start, window_len_s=10, is_spike=is_spike, rng=rng)
        for tbl, rows in data_10s.items():
            if rows:
                upsert(conn, tbl, TABLE_PRIMARY_KEYS[tbl], rows)

        # Periodically generate 30s and 60s aggregates
        if (i + 1) % 3 == 0:
            w_30s_start = w_start - timedelta(seconds=20)
            data_30s = generate_window_dataset(w_30s_start, window_len_s=30, is_spike=is_spike, rng=rng)
            for tbl in ["window_metrics", "protocol_counts", "port_counts", "filter_counts", "distinct_counts", "moments", "frequent_itemsets", "ip_edges", "source_stats"]:
                upsert(conn, tbl, TABLE_PRIMARY_KEYS[tbl], data_30s[tbl])

        if (i + 1) % 6 == 0:
            w_60s_start = w_start - timedelta(seconds=50)
            data_60s = generate_window_dataset(w_60s_start, window_len_s=60, is_spike=is_spike, rng=rng)
            for tbl in ["window_metrics", "protocol_counts", "port_counts", "filter_counts", "distinct_counts", "moments", "frequent_itemsets", "ip_edges", "source_stats"]:
                upsert(conn, tbl, TABLE_PRIMARY_KEYS[tbl], data_60s[tbl])

    conn.close()
    print(f"[✓] Seeding complete! All 16 tables populated in {db_path}")


def run_live_mode(db_path: str | Path, seed: int = 42) -> None:
    """
    Appends a new 10-second mock window every 2 seconds until interrupted.
    """
    rng = random.Random(seed)
    conn = connect(db_path)
    init_schema(conn)

    print(f"[*] Starting LIVE mock feed into {db_path} (Ctrl+C to stop)...")
    curr_time = datetime.now(timezone.utc) - timedelta(seconds=10)

    try:
        step = 0
        while True:
            step += 1
            curr_time += timedelta(seconds=10)
            is_spike = (step % 20 == 0)  # Periodic spike every 20 windows

            data = generate_window_dataset(curr_time, window_len_s=10, is_spike=is_spike, rng=rng)
            for tbl, rows in data.items():
                if rows:
                    upsert(conn, tbl, TABLE_PRIMARY_KEYS[tbl], rows)

            print(f"    [+] Emitted window {format_iso(curr_time)} (pkts={data['window_metrics'][0]['packets']}, pps={data['window_metrics'][0]['pps']})")
            time.sleep(2.0)
    except KeyboardInterrupt:
        print("\n[*] Live mock seeder stopped.")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="LNTA Mock Data Seeder")
    parser.add_argument("--db", type=str, default=None, help="Target SQLite database path")
    parser.add_argument("--duration", type=int, default=10, help="Duration in minutes to seed (default: 10)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--live", action="store_true", help="Run in continuous live mode appending windows")
    parser.add_argument("--reset", action="store_true", help="Reset database and schema before seeding")

    args = parser.parse_args()

    if args.db:
        target_db = Path(args.db)
    else:
        try:
            cfg = load_config()
            target_db = Path(cfg.serving.db_path)
        except Exception:
            target_db = Path("serving/analytics.db")

    target_db.parent.mkdir(parents=True, exist_ok=True)

    if args.live:
        run_live_mode(target_db, seed=args.seed)
    else:
        seed_database(target_db, duration_min=args.duration, seed=args.seed, reset=args.reset)


if __name__ == "__main__":
    main()
