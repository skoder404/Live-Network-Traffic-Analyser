"""
tests/unit/test_list_interfaces.py — Unit tests for capture/list_interfaces.py.
"""

from pathlib import Path

from capture.list_interfaces import (
    InterfaceInfo,
    auto_select_wifi,
    check_is_wireless,
    format_table,
    parse_tshark_d_output,
)


def test_parse_tshark_d_fixture():
    fixture_path = Path("tests/fixtures/tshark_D.txt")
    text = fixture_path.read_text(encoding="utf-8")
    interfaces = parse_tshark_d_output(text)

    assert len(interfaces) == 5
    assert interfaces[0].name == "eth0"
    assert not interfaces[0].is_wireless
    assert interfaces[1].name == "wlan0"
    assert interfaces[1].is_wireless
    assert interfaces[2].name == "any"
    assert "Pseudo-device" in interfaces[2].description
    assert interfaces[3].name == "lo"
    assert "Loopback" in interfaces[3].description


def test_check_is_wireless_heuristics():
    assert check_is_wireless("wlan0")
    assert check_is_wireless("wlp2s0")
    assert check_is_wireless("eth0", "Intel Wireless-AC 9560")
    assert check_is_wireless(r"\Device\NPF_{GUID}", "Wi-Fi")
    assert not check_is_wireless("eth0", "Realtek PCIe GbE Family Controller")
    assert not check_is_wireless("lo", "Loopback adapter")


def test_auto_select_wifi_with_ip():
    ifaces = [
        InterfaceInfo(1, "eth0", "Ethernet", is_wireless=False, has_ip=True, ip_address="192.168.1.5"),
        InterfaceInfo(2, "wlan0", "Wi-Fi", is_wireless=True, has_ip=True, ip_address="192.168.1.10"),
        InterfaceInfo(3, "docker0", "Bridge", is_wireless=False, has_ip=True, ip_address="172.17.0.1"),
    ]
    selected = auto_select_wifi(ifaces)
    assert selected is not None
    assert selected.name == "wlan0"


def test_auto_select_wifi_none():
    ifaces = [
        InterfaceInfo(1, "eth0", "Ethernet", is_wireless=False, has_ip=True),
        InterfaceInfo(2, "lo", "Loopback", is_wireless=False, has_ip=True),
    ]
    selected = auto_select_wifi(ifaces)
    assert selected is None


def test_format_table_output():
    ifaces = [
        InterfaceInfo(1, "eth0", "Ethernet adapter", is_wireless=False, has_ip=False),
        InterfaceInfo(2, "wlan0", "Wireless adapter", is_wireless=True, has_ip=True, ip_address="192.168.1.10"),
    ]
    table = format_table(ifaces)
    assert "wlan0" in table
    assert "Yes" in table
    assert "192.168.1.10" in table
