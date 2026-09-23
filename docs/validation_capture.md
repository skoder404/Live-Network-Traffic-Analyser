# Capture Validation Protocol & Wireshark Comparison (T2-009)

> Owned by **Naveena MS**
> Documents the dual-capture validation procedure and Wireshark I/O graph verification to confirm that the live capture pipeline does not lose packets or corrupt records under normal operation.

---

## 1. Objective

Validate that `capture/run_capture.py` captures all IP packets without exceeding a $1\%$ discrepancy when compared against a parallel ground-truth packet capture recorded directly with Wireshark / `dumpcap`.

---

## 2. 60-Second Validation Procedure

### Step 1: Start Ground-Truth Capture (`dumpcap`)
Open a dedicated terminal and start raw packet capture to a temporary PCAP file:
```bash
dumpcap -i <iface> -a duration:60 -w /tmp/reference_capture.pcap
```

### Step 2: Concurrently Start LNTA Live Capture Runner
Simultaneously start the LNTA capture pipeline in a separate terminal:
```bash
python -m capture.run_capture --iface <iface> --duration 60 --sink file --format csv --out-dir /tmp/capture_run
```

### Step 3: Run Validation Script
After both processes complete, compare the reference PCAP frame count with the pipeline CSV records using `scripts/compare_capture.py`:
```bash
python scripts/compare_capture.py --pcap /tmp/reference_capture.pcap --csv-glob '/tmp/capture_run/traffic_*.csv'
```

---

## 3. Wireshark I/O Graph Comparison

To visually verify packet rate fidelity:
1. Open `/tmp/reference_capture.pcap` in Wireshark GUI.
2. Navigate to **Statistics $\to$ I/O Graphs**.
3. Add a graph with filter `ip || ipv6` and unit `Packets/s` with interval `1 sec`.
4. Plot the per-second packet rate against the LNTA `records_per_sec` logged in `logs/capture_stats.json`.
5. Verify that traffic peaks and idle periods align temporally.

---

## 4. Validation Results Table

| Date | Environment / OS | Interface | PCAP IP Frames | Pipeline CSV Records | Diff (%) | Verdict | Notes |
|---|---|---|---|---|---|---|---|
| 2026-09-23 | Ubuntu 22.04 LTS (Laptop B) | `wlan0` | 3,842 | 3,839 | 0.08% | **PASS** | Synthetic fixture & live dual-capture |
| 2026-09-23 | Windows 11 (Npcap) | `Wi-Fi` | 2,150 | 2,148 | 0.09% | **PASS** | Standard web & streaming workload |
