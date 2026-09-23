"""
capture/generate.py — Synthetic traffic generator with scenario profiles and ground truth.

Generates contract-compliant CSV records and sidecar <out>.truth.json metadata for:
- normal: Realistic web/DNS/ping mix
- spike: Surge (3-5x) in the final third
- portscan_like: Single source probing >25 ports in 10s
- fanout: Single source hitting >40 IPs in 10s
- dns_heavy: UDP/53 dominant
- multi_host_graph: Deterministic toy graph A->B, A->C, C->B, D->B
"""

from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from capture.parser import Record
from contracts.record_schema import format_row, validate_row

SCENARIOS = [
    "normal",
    "spike",
    "portscan_like",
    "fanout",
    "dns_heavy",
    "multi_host_graph",
]


def _format_ts(epoch_sec: float) -> str:
    dt = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
    millis = int((epoch_sec - int(epoch_sec)) * 1000)
    if millis < 0:
        millis = 0
    elif millis > 999:
        millis = 999
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{millis:03d}"


def _window_key_10s(epoch_sec: float) -> str:
    window_start_sec = math.floor(epoch_sec / 10.0) * 10.0
    dt = datetime.fromtimestamp(window_start_sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def generate_records(
    scenario: str,
    duration_s: float = 60.0,
    rate_pps: float = 20.0,
    seed: int = 42,
    base_epoch: float = 1758412800.0,  # 2026-09-21 00:00:00 UTC
) -> list[Record]:
    """Generate a list of Record instances according to scenario specifications."""
    rng = random.Random(seed)
    records: list[Record] = []

    current_time = base_epoch
    end_time = base_epoch + duration_s
    last_time = current_time

    # Pre-defined hosts and endpoints
    lan_clients = ["192.168.1.10", "192.168.1.15", "192.168.1.20"]
    servers_web = ["198.51.100.10", "198.51.100.20", "203.0.113.5", "203.0.113.50"]
    dns_servers = ["192.168.1.1", "198.51.100.53"]

    lan_macs = {
        "192.168.1.10": "00:11:22:33:44:01",
        "192.168.1.15": "00:11:22:33:44:02",
        "192.168.1.20": "00:11:22:33:44:03",
        "192.168.1.1": "00:11:22:33:44:fe",
        "192.168.1.99": "00:11:22:33:44:99",
    }
    router_mac = "00:11:22:33:44:fe"

    # Multi-host toy graph nodes
    graph_nodes = {
        "A": ("10.0.0.1", "02:00:00:00:00:01"),
        "B": ("10.0.0.2", "02:00:00:00:00:02"),
        "C": ("10.0.0.3", "02:00:00:00:00:03"),
        "D": ("10.0.0.4", "02:00:00:00:00:04"),
    }
    # Edges: A->B, A->C, C->B, D->B
    graph_edges = [
        ("A", "B", 443, "TCP"),
        ("A", "C", 80, "TCP"),
        ("C", "B", 443, "TCP"),
        ("D", "B", 53, "UDP"),
    ]

    portscan_ports = list(range(20, 100))
    rng.shuffle(portscan_ports)
    portscan_idx = 0

    fanout_ips = [f"198.51.100.{i}" for i in range(1, 100)]
    fanout_idx = 0

    while current_time < end_time:
        # Pacing calculation
        effective_rate = rate_pps
        if scenario == "spike":
            elapsed = current_time - base_epoch
            if elapsed >= (2.0 / 3.0) * duration_s:
                effective_rate = rate_pps * 4.0

        gap = rng.expovariate(effective_rate) if effective_rate > 0 else 0.05
        # Bound gap slightly to avoid extreme jumps
        gap = max(0.001, min(gap, 1.0))
        current_time += gap
        if current_time >= end_time:
            break

        iat_ms = (current_time - last_time) * 1000.0 if records else 0.0
        last_time = current_time
        ts_str = _format_ts(current_time)

        if scenario == "multi_host_graph":
            src_lbl, dst_lbl, dst_p, proto = rng.choice(graph_edges)
            src_ip, src_mac = graph_nodes[src_lbl]
            dst_ip, dst_mac = graph_nodes[dst_lbl]
            src_p = rng.randint(49152, 65535)
            flags = "0x0018" if proto == "TCP" else None
            pkt_len = rng.choice([64, 128, 512, 1024, 1420])

            # Occasionally emit response packet in reverse
            if rng.random() < 0.35:
                src_ip, dst_ip = dst_ip, src_ip
                src_mac, dst_mac = dst_mac, src_mac
                src_p, dst_p = dst_p, src_p

            records.append(
                Record(
                    timestamp=ts_str,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    src_port=src_p,
                    dst_port=dst_p,
                    protocol=proto,
                    packet_length=pkt_len,
                    src_mac=src_mac,
                    dst_mac=dst_mac,
                    tcp_flags=flags,
                    iat_ms=round(iat_ms, 3),
                )
            )
            continue

        if scenario == "portscan_like":
            src_ip = "192.168.1.99"
            dst_ip = "198.51.100.20"
            src_mac = lan_macs[src_ip]
            dst_mac = router_mac
            dst_p = portscan_ports[portscan_idx % len(portscan_ports)]
            portscan_idx += 1
            src_p = rng.randint(40000, 60000)

            records.append(
                Record(
                    timestamp=ts_str,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    src_port=src_p,
                    dst_port=dst_p,
                    protocol="TCP",
                    packet_length=60,
                    src_mac=src_mac,
                    dst_mac=dst_mac,
                    tcp_flags="0x0002",  # SYN
                    iat_ms=round(iat_ms, 3),
                )
            )
            continue

        if scenario == "fanout":
            src_ip = "192.168.1.10"
            dst_ip = fanout_ips[fanout_idx % len(fanout_ips)]
            fanout_idx += 1
            src_mac = lan_macs[src_ip]
            dst_mac = router_mac
            src_p = rng.randint(40000, 60000)
            dst_p = 443

            records.append(
                Record(
                    timestamp=ts_str,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    src_port=src_p,
                    dst_port=dst_p,
                    protocol="TCP",
                    packet_length=64,
                    src_mac=src_mac,
                    dst_mac=dst_mac,
                    tcp_flags="0x0002",
                    iat_ms=round(iat_ms, 3),
                )
            )
            continue

        # Normal, Spike, and DNS-Heavy scenarios
        p_roll = rng.random()
        if scenario == "dns_heavy":
            is_dns = p_roll < 0.85
            is_web = not is_dns and p_roll < 0.95
            is_icmp = not is_dns and not is_web
        else:
            is_dns = p_roll < 0.15
            is_web = not is_dns and p_roll < 0.95
            is_icmp = not is_dns and not is_web

        client_ip = rng.choice(lan_clients)
        client_mac = lan_macs.get(client_ip, "00:11:22:33:44:00")

        if is_dns:
            src_ip = client_ip
            dst_ip = rng.choice(dns_servers)
            src_port = rng.randint(49152, 65535)
            dst_port = 53
            proto = "UDP"
            pkt_len = rng.randint(60, 140)
            flags = None
            src_mac = client_mac
            dst_mac = router_mac
        elif is_icmp:
            src_ip = client_ip
            dst_ip = "198.51.100.1"
            src_port = None
            dst_port = None
            proto = "ICMP"
            pkt_len = rng.choice([64, 84])
            flags = None
            src_mac = client_mac
            dst_mac = router_mac
        else:
            # Web TCP
            src_ip = client_ip
            dst_ip = rng.choice(servers_web)
            src_port = rng.randint(40000, 65000)
            dst_port = 443 if rng.random() < 0.8 else 80
            proto = "TCP"
            pkt_len = rng.choice([64, 128, 512, 1024, 1420, 1500])
            flags = rng.choice(["0x0018", "0x0010", "0x0002"])
            src_mac = client_mac
            dst_mac = router_mac

        # Bidirectional simulation (40% packets are incoming replies)
        if rng.random() < 0.40 and proto in ("TCP", "UDP"):
            src_ip, dst_ip = dst_ip, src_ip
            src_port, dst_port = dst_port, src_port
            src_mac, dst_mac = dst_mac, client_mac

        records.append(
            Record(
                timestamp=ts_str,
                src_ip=src_ip,
                dst_ip=dst_ip,
                src_port=src_port,
                dst_port=dst_port,
                protocol=proto,
                packet_length=pkt_len,
                src_mac=src_mac,
                dst_mac=dst_mac,
                tcp_flags=flags,
                iat_ms=round(iat_ms, 3),
            )
        )

    return records


def compute_ground_truth(records: list[Record]) -> dict[str, Any]:
    """Compute ground truth metrics matching pandas / statistical formulas."""
    total_packets = len(records)
    if total_packets == 0:
        return {
            "packets": 0,
            "bytes": 0,
            "unique_src_ips": 0,
            "unique_dst_ips": 0,
            "unique_ports": 0,
            "protocol_counts": {},
            "packet_length_mean": 0.0,
            "packet_length_variance_population": 0.0,
            "packet_length_variance_sample": 0.0,
            "per_10s_window_packets": {},
        }

    total_bytes = sum(r.packet_length for r in records)
    unique_src_ips = len({r.src_ip for r in records})
    unique_dst_ips = len({r.dst_ip for r in records})

    ports: set[int] = set()
    proto_counts: dict[str, int] = {}
    lengths: list[int] = []
    window_counts: dict[str, int] = {}

    for r in records:
        if r.src_port is not None:
            ports.add(r.src_port)
        if r.dst_port is not None:
            ports.add(r.dst_port)

        proto_counts[r.protocol] = proto_counts.get(r.protocol, 0) + 1
        lengths.append(r.packet_length)

        # Parse epoch for 10s window bucket
        dt = datetime.strptime(r.timestamp.split(".")[0], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        w_key = _window_key_10s(dt.timestamp())
        window_counts[w_key] = window_counts.get(w_key, 0) + 1

    mean_len = sum(lengths) / total_packets
    pop_var = sum((x - mean_len) ** 2 for x in lengths) / total_packets
    sample_var = sum((x - mean_len) ** 2 for x in lengths) / (total_packets - 1) if total_packets > 1 else 0.0

    return {
        "packets": total_packets,
        "bytes": total_bytes,
        "unique_src_ips": unique_src_ips,
        "unique_dst_ips": unique_dst_ips,
        "unique_ports": len(ports),
        "protocol_counts": proto_counts,
        "packet_length_mean": round(mean_len, 4),
        "packet_length_variance_population": round(pop_var, 4),
        "packet_length_variance_sample": round(sample_var, 4),
        "per_10s_window_packets": window_counts,
    }


def write_dataset(records: list[Record], out_path: str | Path) -> None:
    """Write records to CSV and companion .truth.json file."""
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "w", encoding="utf-8") as f:
        for r in records:
            f.write(format_row(r.to_dict()) + "\n")

    truth = compute_ground_truth(records)
    truth_path = p.with_name(f"{p.stem}.truth.json")
    with open(truth_path, "w", encoding="utf-8") as f:
        json.dump(truth, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic traffic generator for LNTA.")
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="normal",
        help="Scenario profile to generate.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Duration in seconds.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Average rate in packets/sec.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic generation.",
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output CSV file path.",
    )
    args = parser.parse_args()

    records = generate_records(
        scenario=args.scenario,
        duration_s=args.duration,
        rate_pps=args.rate,
        seed=args.seed,
    )

    # Validate all generated rows against contract
    for idx, r in enumerate(records):
        ok, reason = validate_row(r.to_dict())
        if not ok:
            raise ValueError(f"Generated invalid row at index {idx}: {reason}")

    write_dataset(records, args.out)
    print(f"Generated {len(records)} records for scenario '{args.scenario}' -> {args.out}")


if __name__ == "__main__":
    main()
