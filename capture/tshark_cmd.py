"""
capture/tshark_cmd.py — Build TShark command lines for live packet capture.

Generates the exact argument vector for tshark subprocess execution according
to TECH_RULES §3.1.
"""

from __future__ import annotations

RAW_FIELDS: list[str] = [
    "frame.time_epoch",
    "ip.src",
    "ip.dst",
    "ipv6.src",
    "ipv6.dst",
    "tcp.srcport",
    "tcp.dstport",
    "udp.srcport",
    "udp.dstport",
    "ip.proto",
    "ipv6.nxt",
    "frame.len",
    "eth.src",
    "eth.dst",
    "tcp.flags",
    "frame.time_delta",
]

# Backward compatibility alias
TSHARK_FIELDS = RAW_FIELDS


def build_command(
    iface: str,
    capture_filter: str | None = None,
    duration_s: int | None = None,
    tshark_path: str = "tshark",
) -> list[str]:
    """
    Build the exact argv list for running TShark.

    Args:
        iface: Network interface name or index (e.g. 'wlan0' or '1')
        capture_filter: Optional BPF capture filter (e.g. 'tcp or udp')
        duration_s: Optional capture duration in seconds
        tshark_path: Executable path for tshark (defaults to 'tshark')

    Returns:
        argv list suitable for subprocess.Popen
    """
    cmd: list[str] = [
        tshark_path,
        "-i",
        str(iface),
        "-l",
        "-n",
        "-T",
        "fields",
        "-E",
        "separator=,",
        "-E",
        "occurrence=f",
        "-E",
        "header=n",
        "-E",
        "quote=n",
    ]

    for field in RAW_FIELDS:
        cmd.extend(["-e", field])

    if capture_filter:
        cmd.extend(["-f", capture_filter])

    if duration_s is not None:
        if duration_s <= 0:
            raise ValueError("duration_s must be greater than 0")
        cmd.extend(["-a", f"duration:{duration_s}"])

    return cmd


def build_tshark_command(
    interface: str,
    bpf_filter: str | None = None,
    duration_seconds: int | None = None,
) -> list[str]:
    """Backward compatibility alias for build_command."""
    return build_command(
        iface=interface,
        capture_filter=bpf_filter,
        duration_s=duration_seconds,
    )
