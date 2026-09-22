"""Build TShark command lines for live packet capture."""

from __future__ import annotations


TSHARK_FIELDS = [
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


def build_tshark_command(
    interface: str,
    bpf_filter: str | None = None,
    duration_seconds: int | None = None,
) -> list[str]:
    """Build the TShark subprocess command."""

    command = [
        "tshark",
        "-i",
        interface,
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

    for field in TSHARK_FIELDS:
        command.extend(["-e", field])

    if bpf_filter:
        command.extend(["-f", bpf_filter])

    if duration_seconds is not None:
        if duration_seconds <= 0:
            raise ValueError("duration_seconds must be greater than 0")
        command.extend(["-a", f"duration:{duration_seconds}"])

    return command