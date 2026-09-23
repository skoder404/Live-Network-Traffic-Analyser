# DATA_CONTRACT — Traffic Record Schema (v1)

> Single source of truth for the traffic record CSV schema exchanged across all components of the Live Network Traffic Analyser (LNTA).
> Component reference: `contracts/record_schema.py`

---

## 1. Specification & Field Order

All records produced by `capture/` and ingested by `ingestion/` (Flume $\to$ HDFS) and `streaming/` (Spark Structured Streaming) follow an **exact 11-field CSV format without a header row**.

- **Encoding:** UTF-8
- **Delimiter:** Comma `,`
- **Line Ending:** Standard `\n` or `\r\n` (stripped upon parse)
- **Missing / Nullable Fields:** Represented as an empty string (e.g. `,,`)

| # | Column Name | Type | Nullable | Validation Rules & Format | Description |
|---|---|---|---|---|---|
| 1 | `timestamp` | string | **No** | `YYYY-MM-DD HH:MM:SS.mmm` (UTC) | Packet capture time truncated to milliseconds |
| 2 | `src_ip` | string | **No** | Valid IPv4 or IPv6 address | Source IP address |
| 3 | `dst_ip` | string | **No** | Valid IPv4 or IPv6 address | Destination IP address |
| 4 | `src_port` | int | Yes | Range `0–65535` | Source port (null for ICMP/OTHER) |
| 5 | `dst_port` | int | Yes | Range `0–65535` | Destination port (null for ICMP/OTHER) |
| 6 | `protocol` | string | **No** | `TCP` \| `UDP` \| `ICMP` \| `OTHER` | Transport/network protocol |
| 7 | `packet_length` | int | **No** | Range `1–65535` | Total frame length in bytes |
| 8 | `src_mac` | string | Yes | Lowercase `aa:bb:cc:dd:ee:ff` | Source MAC address |
| 9 | `dst_mac` | string | Yes | Lowercase `aa:bb:cc:dd:ee:ff` | Destination MAC address |
| 10 | `tcp_flags` | string | Yes | Hex e.g. `0x0018` | TCP flags (null for non-TCP) |
| 11 | `iat_ms` | double | Yes | Float $\ge 0.0$ | Inter-arrival time since last packet in ms |

---

## 2. Examples

### Valid Records

#### TCP HTTPS Traffic (Full Fields)
```text
2026-09-21 10:15:30.124,192.168.1.10,198.51.100.5,54321,443,TCP,1420,aa:bb:cc:dd:ee:ff,00:11:22:33:44:55,0x0018,1.250
```

#### UDP DNS Query (No MACs, No TCP Flags)
```text
2026-09-21 10:15:31.050,192.168.1.10,192.168.1.1,58291,53,UDP,78,,,,0.045
```

#### ICMP Echo Request (No Ports, No TCP Flags)
```text
2026-09-21 10:15:32.400,10.0.0.1,10.0.0.2,,,ICMP,84,02:11:22:33:44:55,02:66:77:88:99:aa,,100.200
```

#### IPv6 UDP Traffic
```text
2026-09-21 10:15:33.910,2001:db8::1,2001:db8::2,49152,53,UDP,92,,,,12.500
```

---

### Invalid Records & Rejection Reasons

| Invalid Sample | Rejection Reason |
|---|---|
| `2026-09-21 10:15:30,192.168.1.1,8.8.8.8,1234,80,TCP,100,,,,,` | Missing milliseconds in timestamp |
| `2026-09-21 10:15:30.123,999.999.999.999,8.8.8.8,1234,80,TCP,100,,,,,` | Invalid source IP address |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,70000,80,TCP,100,,,,,` | Port out of range ($> 65535$) |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,1234,80,HTTP,100,,,,,` | Invalid protocol (`HTTP` not allowed) |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,1234,80,TCP,0,,,,,` | Packet length $0$ (must be $\ge 1$) |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,1234,80,TCP,100,,,,` | Wrong column count (10 columns instead of 11) |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,1234,80,TCP,100,INVALID_MAC,,,,` | Invalid MAC format |
| `2026-09-21 10:15:30.123,192.168.1.1,8.8.8.8,1234,80,TCP,100,,,,,-5.0` | Negative inter-arrival time |
