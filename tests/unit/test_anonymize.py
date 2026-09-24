"""
tests/unit/test_anonymize.py — Unit tests for capture/anonymize.py.
"""

from capture.anonymize import (
    anonymize_ip,
    anonymize_mac,
    anonymize_record_dict,
)
from contracts.record_schema import validate_row


def test_determinism_and_key_sensitivity():
    key1 = "secret-key-1"
    key2 = "secret-key-2"
    ip = "192.168.1.10"

    res1_a = anonymize_ip(ip, key1)
    res1_b = anonymize_ip(ip, key1)
    res2 = anonymize_ip(ip, key2)

    assert res1_a == res1_b
    assert res1_a != res2


def test_private_ipv4_class_preservation():
    key = "test-key"
    assert anonymize_ip("192.168.1.1", key).startswith("10.")
    assert anonymize_ip("10.0.0.5", key).startswith("10.")
    assert anonymize_ip("172.16.0.1", key).startswith("10.")


def test_public_ipv4_class_preservation():
    key = "test-key"
    assert anonymize_ip("8.8.8.8", key).startswith("198.18.")
    assert anonymize_ip("203.0.113.1", key).startswith("198.18.")


def test_ipv6_class_preservation():
    key = "test-key"
    anon_v6 = anonymize_ip("2001:db8::1", key)
    assert anon_v6.startswith("fd00::")


def test_mac_anonymization():
    key = "test-key"
    anon_mac = anonymize_mac("AA:BB:CC:DD:EE:FF", key)
    assert anon_mac is not None
    assert anon_mac.startswith("02:")
    assert len(anon_mac.split(":")) == 6


def test_record_anonymization_and_validation():
    key = "test-key"
    rec = {
        "timestamp": "2026-09-22 10:00:00.123",
        "src_ip": "192.168.1.10",
        "dst_ip": "8.8.8.8",
        "src_port": 54321,
        "dst_port": 443,
        "protocol": "TCP",
        "packet_length": 1420,
        "src_mac": "aa:bb:cc:dd:ee:ff",
        "dst_mac": "11:22:33:44:55:66",
        "tcp_flags": "0x0018",
        "iat_ms": 1.25,
    }

    anon = anonymize_record_dict(rec, key)
    assert anon["src_ip"].startswith("10.")
    assert anon["dst_ip"].startswith("198.18.")
    assert anon["src_mac"].startswith("02:")
    assert anon["dst_mac"].startswith("02:")
    assert anon["src_port"] == 54321
    assert anon["dst_port"] == 443
    assert anon["packet_length"] == 1420

    ok, reason = validate_row(anon)
    assert ok, f"Anonymized row failed validation: {reason}"
