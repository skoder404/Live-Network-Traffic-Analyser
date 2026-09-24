"""
capture/list_interfaces.py — Enumerate network interfaces and detect active Wi-Fi.

Runs `tshark -D` to enumerate packet capture interfaces, enriches each interface
with wireless and IP status, and provides an --auto flag to select the active Wi-Fi interface.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class InterfaceInfo:
    index: int
    name: str
    description: str
    is_wireless: bool
    has_ip: bool
    ip_address: str | None = None


_TSHARK_D_LINE_RE = re.compile(r"^\s*(\d+)\.\s+(\S+)(?:\s+[\(\[](.*)[\)\]])?\s*$")


def parse_tshark_d_output(text: str) -> list[InterfaceInfo]:
    r"""
    Parse the stdout of `tshark -D` into a list of InterfaceInfo objects.
    Line formats:
      1. eth0
      2. wlan0 (Wireless network adapter)
      3. \Device\NPF_{GUID} (Wi-Fi)
      4. lo [Loopback]
    """
    interfaces: list[InterfaceInfo] = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        match = _TSHARK_D_LINE_RE.match(line)
        if match:
            idx = int(match.group(1))
            name = match.group(2)
            desc = match.group(3) or ""
            is_wl = check_is_wireless(name, desc)
            interfaces.append(
                InterfaceInfo(
                    index=idx,
                    name=name,
                    description=desc,
                    is_wireless=is_wl,
                    has_ip=False,
                )
            )
        else:
            # Fallback for unexpected format
            parts = line.split(maxsplit=1)
            if parts and parts[0].endswith("."):
                try:
                    idx = int(parts[0][:-1])
                    rest = parts[1] if len(parts) > 1 else ""
                    name = rest.split()[0] if rest else parts[0]
                    desc = rest[len(name) :].strip(" ()[]") if len(rest) > len(name) else ""
                    interfaces.append(
                        InterfaceInfo(
                            index=idx,
                            name=name,
                            description=desc,
                            is_wireless=check_is_wireless(name, desc),
                            has_ip=False,
                        )
                    )
                except ValueError:
                    continue
    return interfaces


def check_is_wireless(name: str, desc: str = "") -> bool:
    """Detect if an interface is wireless via sysfs (Linux) or heuristics."""
    # Linux sysfs check
    sysfs_wireless = Path(f"/sys/class/net/{name}/wireless")
    if sysfs_wireless.exists():
        return True

    # Common wireless prefixes and keywords (Linux, Windows, macOS)
    name_lower = name.lower()
    desc_lower = desc.lower()

    wireless_indicators = [
        "wlan",
        "wlp",
        "wls",
        "wl",
        "wi-fi",
        "wifi",
        "wireless",
        "802.11",
        "air",
    ]

    for ind in wireless_indicators:
        if ind in name_lower or ind in desc_lower:
            return True

    return False


def get_interface_ips_linux() -> dict[str, str]:
    """Parse `ip -o addr` on Linux without external dependencies."""
    ips: dict[str, str] = {}
    try:
        res = subprocess.run(["ip", "-o", "addr"], capture_output=True, text=True, timeout=3)
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                parts = line.split()
                # Format: 2: wlan0    inet 192.168.1.10/24 ...
                if len(parts) >= 4 and parts[2] in ("inet", "inet6"):
                    iface_name = parts[1]
                    cidr = parts[3]
                    ip = cidr.split("/")[0]
                    if iface_name not in ips or parts[2] == "inet":
                        ips[iface_name] = ip
    except Exception:
        pass
    return ips


def get_interface_ips_fallback() -> dict[str, str]:
    """Cross-platform IP lookup using psutil if available or socket."""
    ips: dict[str, str] = {}
    try:
        import psutil

        for iface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.family.name in ("AF_INET", "AF_INET6"):
                    if addr.address and not addr.address.startswith("127."):
                        ips[iface] = addr.address
                        if addr.family.name == "AF_INET":
                            break
    except ImportError:
        pass
    return ips


def enrich_interfaces(interfaces: list[InterfaceInfo]) -> list[InterfaceInfo]:
    """Enrich interfaces with IP information."""
    ips = get_interface_ips_linux() if os.name != "nt" else {}
    if not ips:
        ips = get_interface_ips_fallback()

    for iface in interfaces:
        # Match by name or description
        for if_name, ip in ips.items():
            if (
                if_name.lower() == iface.name.lower()
                or if_name.lower() in iface.description.lower()
                or iface.name.lower() in if_name.lower()
            ):
                iface.has_ip = True
                iface.ip_address = ip
                break

    return interfaces


def list_interfaces(tshark_path: str = "tshark") -> list[InterfaceInfo]:
    """Execute `tshark -D` and return enriched interface list."""
    try:
        proc = subprocess.run(
            [tshark_path, "-D"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode != 0:
            return []
        raw_interfaces = parse_tshark_d_output(proc.stdout)
        return enrich_interfaces(raw_interfaces)
    except FileNotFoundError:
        return []
    except Exception:
        return []


def format_table(interfaces: list[InterfaceInfo]) -> str:
    """Format interface list as an aligned text table."""
    headers = ["#", "Name", "Description", "Wireless?", "Has IP?", "IP Address"]
    rows = []
    for iface in interfaces:
        rows.append(
            [
                str(iface.index),
                iface.name,
                iface.description or "-",
                "Yes" if iface.is_wireless else "No",
                "Yes" if iface.has_ip else "No",
                iface.ip_address or "-",
            ]
        )

    # Compute col widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(val))

    header_line = "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    sep_line = "  ".join("-" * col_widths[i] for i in range(len(headers)))
    data_lines = [
        "  ".join(row[i].ljust(col_widths[i]) for i in range(len(headers))) for row in rows
    ]

    return "\n".join([header_line, sep_line] + data_lines)


def auto_select_wifi(interfaces: list[InterfaceInfo]) -> InterfaceInfo | None:
    """Select the first wireless interface with an active IP address."""
    # First priority: wireless with IP
    for iface in interfaces:
        if iface.is_wireless and iface.has_ip:
            return iface
    # Second priority: any wireless interface if none has IP
    for iface in interfaces:
        if iface.is_wireless:
            return iface
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Enumerate TShark interfaces and detect Wi-Fi.")
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Print only the selected active Wi-Fi interface name and exit.",
    )
    parser.add_argument(
        "--tshark-path",
        default="tshark",
        help="Path to tshark executable.",
    )
    args = parser.parse_args()

    interfaces = list_interfaces(tshark_path=args.tshark_path)

    if args.auto:
        selected = auto_select_wifi(interfaces)
        if selected is not None:
            print(selected.name)
            sys.exit(0)
        else:
            sys.stderr.write(
                "Error: No connected Wi-Fi interface detected with an active IP.\n"
                "Please specify interface manually with --iface <name>.\n"
            )
            sys.exit(2)
    else:
        if not interfaces:
            print("No interfaces found or TShark is not installed/reachable.")
            sys.exit(1)
        print(format_table(interfaces))


if __name__ == "__main__":
    main()
