"""
tests/unit/test_tshark_cmd.py — Unit tests for capture/tshark_cmd.py.
"""

import pytest

from capture.tshark_cmd import (
    RAW_FIELDS,
    TSHARK_FIELDS,
    build_command,
    build_tshark_command,
)


def test_raw_fields_spec():
    expected_16 = [
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
    assert RAW_FIELDS == expected_16
    assert TSHARK_FIELDS == RAW_FIELDS


def test_build_command_default():
    cmd = build_command("wlan0")

    expected_prefix = [
        "tshark",
        "-i",
        "wlan0",
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
    assert cmd[: len(expected_prefix)] == expected_prefix

    # Check that all 16 fields are included in order with -e
    extracted_fields = []
    i = len(expected_prefix)
    while i < len(cmd):
        if cmd[i] == "-e":
            extracted_fields.append(cmd[i + 1])
            i += 2
        else:
            break

    assert extracted_fields == RAW_FIELDS
    assert len(cmd) == len(expected_prefix) + 2 * len(RAW_FIELDS)


def test_build_command_custom_tshark_path():
    cmd = build_command("eth0", tshark_path="/usr/bin/tshark")
    assert cmd[0] == "/usr/bin/tshark"


def test_build_command_with_bpf_filter():
    cmd = build_command("wlan0", capture_filter="tcp port 443")
    assert cmd[-2:] == ["-f", "tcp port 443"]


def test_build_command_with_duration():
    cmd = build_command("wlan0", duration_s=45)
    assert cmd[-2:] == ["-a", "duration:45"]


def test_build_command_with_both_filter_and_duration():
    cmd = build_command("wlan0", capture_filter="udp port 53", duration_s=10)
    assert cmd[-4:] == ["-f", "udp port 53", "-a", "duration:10"]


def test_build_command_invalid_duration():
    with pytest.raises(ValueError, match="duration_s must be greater than 0"):
        build_command("wlan0", duration_s=0)

    with pytest.raises(ValueError, match="duration_s must be greater than 0"):
        build_command("wlan0", duration_s=-5)


def test_backward_compatibility_alias():
    cmd1 = build_command("wlan0", capture_filter="tcp", duration_s=30)
    cmd2 = build_tshark_command("wlan0", bpf_filter="tcp", duration_seconds=30)
    assert cmd1 == cmd2
