"""
capture/anonymize.py — Keyed HMAC anonymiser for IP and MAC addresses.

Applies HMAC-SHA256 to map IP and MAC addresses deterministically while
preserving network class distinctions:
- Private IPv4 -> 10.x.y.z
- Public IPv4  -> 198.18.x.y (RFC 2544 benchmark reservation)
- IPv6         -> fd00:: prefix + hash
- MAC          -> locally-administered unicast (02:xx:xx:xx:xx:xx)
All other fields (timing, ports, lengths, protocols, flags) remain unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import ipaddress
import os
import re
from pathlib import Path
from typing import Any

from contracts.record_schema import FIELD_NAMES, format_row, parse_line

_MAC_REGEX = re.compile(r"^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$")


def _get_digest(key: str, val: str) -> bytes:
    return hmac.new(
        key.encode("utf-8"), val.strip().lower().encode("utf-8"), hashlib.sha256
    ).digest()


_RFC1918_NETWORKS = (
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    ipaddress.IPv4Network("127.0.0.0/8"),
    ipaddress.IPv4Network("169.254.0.0/16"),
)


def _is_private_ipv4(ip: ipaddress.IPv4Address) -> bool:
    return any(ip in net for net in _RFC1918_NETWORKS)


def anonymize_ip(ip_str: str, key: str) -> str:
    """Deterministically anonymize IPv4/IPv6 preserving address class."""
    if not ip_str:
        return ip_str

    try:
        ip = ipaddress.ip_address(ip_str.strip())
    except ValueError:
        return ip_str

    digest = _get_digest(key, ip_str)

    if ip.version == 4:
        assert isinstance(ip, ipaddress.IPv4Address)
        if _is_private_ipv4(ip):
            # Map private IPv4 to 10.a.b.c
            a = digest[0]
            b = digest[1]
            c = (digest[2] % 253) + 1  # 1 to 254
            return f"10.{a}.{b}.{c}"
        else:
            # Map public IPv4 to 198.18.x.y (benchmarking range RFC 2544)
            x = digest[0]
            y = (digest[1] % 253) + 1
            return f"198.18.{x}.{y}"
    else:
        # IPv6: map to unique local address fd00::...
        g1 = digest[:2].hex()
        g2 = digest[2:4].hex()
        g3 = digest[4:6].hex()
        g4 = digest[6:8].hex()
        return f"fd00::{g1}:{g2}:{g3}:{g4}"


def anonymize_mac(mac_str: str | None, key: str) -> str | None:
    """Map MAC to a locally-administered unicast address (02:xx:xx:xx:xx:xx)."""
    if not mac_str or not _MAC_REGEX.match(mac_str.strip()):
        return mac_str

    digest = _get_digest(key, mac_str)
    # 02 ensures b0=0 (unicast) and b1=1 (locally administered)
    return f"02:{digest[0]:02x}:{digest[1]:02x}:{digest[2]:02x}:{digest[3]:02x}:{digest[4]:02x}"


def anonymize_record_dict(rec: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a new record dict with anonymized IP and MAC addresses."""
    out = dict(rec)
    if out.get("src_ip"):
        out["src_ip"] = anonymize_ip(str(out["src_ip"]), key)
    if out.get("dst_ip"):
        out["dst_ip"] = anonymize_ip(str(out["dst_ip"]), key)
    if out.get("src_mac"):
        out["src_mac"] = anonymize_mac(str(out["src_mac"]), key)
    if out.get("dst_mac"):
        out["dst_mac"] = anonymize_mac(str(out["dst_mac"]), key)
    return out


def anonymize_file(in_path: str | Path, out_path: str | Path, key: str) -> int:
    """Anonymize an entire contract CSV file."""
    in_p = Path(in_path)
    out_p = Path(out_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(in_p, encoding="utf-8") as f_in, open(out_p, "w", encoding="utf-8") as f_out:
        for line in f_in:
            line_str = line.strip()
            if not line_str:
                continue
            fields = parse_line(line_str)
            if len(fields) != len(FIELD_NAMES):
                continue
            rec_dict = dict(zip(FIELD_NAMES, fields, strict=True))
            anon_dict = anonymize_record_dict(rec_dict, key)
            f_out.write(format_row(anon_dict) + "\n")
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Anonymize IP and MAC addresses in LNTA CSV files."
    )
    parser.add_argument("--in", dest="in_file", required=True, help="Input CSV path.")
    parser.add_argument("--out", dest="out_file", required=True, help="Output CSV path.")
    parser.add_argument(
        "--key",
        default=None,
        help="HMAC key (defaults to LNTA_ANON_KEY env var or fixed default).",
    )
    parser.add_argument(
        "--key-env",
        default="LNTA_ANON_KEY",
        help="Environment variable name for HMAC key.",
    )
    args = parser.parse_args()

    key = args.key or os.environ.get(args.key_env) or "lnta-anon-secret-key-2026"
    count = anonymize_file(args.in_file, args.out_file, key)
    print(f"Anonymized {count} records from {args.in_file} -> {args.out_file}")


if __name__ == "__main__":
    main()
