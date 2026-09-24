# Capture Component — Live Network Traffic Analyser

> Owned by **Naveena MS**
> Responsibilities: Live packet capture via TShark subprocess, interface detection, line normalisation, rotating output writers, synthetic generator, dataset anonymisation, replay tooling.

---

## 1. Prerequisites & Installation

### Linux (Ubuntu 22.04 / 24.04 LTS)
```bash
sudo apt update
sudo apt install -y wireshark tshark
```

### Windows
1. Download and install Wireshark from [wireshark.org](https://www.wireshark.org/).
2. Ensure **Npcap** is installed with "Install Npcap in WinPcap API-compatible Mode".
3. Add the Wireshark directory (`C:\Program Files\Wireshark`) to your system `PATH`.

---

## 2. Non-Root Packet Capture Setup (Linux)

Running the entire capture pipeline as `root` is prohibited for security. Follow these steps to enable non-root capture:

```bash
# 1. Enable non-superusers to capture packets
sudo dpkg-reconfigure wireshark-common
# (Select <Yes> when prompted)

# 2. Add your user to the wireshark group
sudo usermod -aG wireshark $USER

# 3. Apply raw network capture capabilities to dumpcap
sudo setcap cap_net_raw,cap_net_admin+eip $(which dumpcap)

# 4. Log out and back in for group membership to take effect
newgrp wireshark
```

---

## 3. Detecting & Selecting Interfaces

Enumerate available interfaces and check their status:
```bash
python -m capture.list_interfaces
```

Automatically pick the active Wi-Fi interface:
```bash
python -m capture.list_interfaces --auto
```

If successful, `--auto` prints the interface identifier (e.g. `wlan0` or `\Device\NPF_...`) and exits with code 0. If no active wireless interface with an IP is detected, it exits with code 2.

---

## 4. Running Packet Capture

### Direct live capture to rotating CSV:
```bash
python -m capture.run_capture --auto --sink file --format csv
```

### Live capture with BPF filter:
```bash
python -m capture.run_capture --iface wlan0 --filter "tcp or udp" --sink file
```

### Multi-machine / TCP Stream Mode:
If running capture on a separate host (e.g. native Windows or Laptop B) and sending to Flume's Netcat source on Laptop A:
```bash
python -m capture.run_capture --auto --sink tcp
```

---

## 5. Troubleshooting & FAQ

| Problem | Root Cause | Solution |
|---|---|---|
| `Permission denied` on capture | User lacks packet capture privileges | Follow §2 non-root setup instructions (`setcap` on dumpcap). |
| `tshark: command not found` | TShark is not installed or not in `PATH` | Verify installation: `tshark -v`. Add Wireshark install folder to `PATH`. |
| `Interface down` or no packets captured | Network interface is disconnected or radio is off | Reconnect Wi-Fi network and verify `ip addr` or `ipconfig`. |
| WSL2 cannot capture host Wi-Fi | WSL2 virtual switch does not expose physical Wi-Fi promiscuously | Run `capture` directly on native Windows / Ubuntu and send records via `--sink tcp`. |
| Dropped frames reported in stats | Reader queue is full under burst traffic | Adjust queue buffer size in configuration or check storage write speed. |
