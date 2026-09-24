# REMOTE_ACCESS.md — Distributed Team Network Access Guide

> Secure private overlay network (VPN) and SSH guide for the LNTA team.
> References: [`TECH_RULES.md`](TECH_RULES.md) §11.1, [`todo.md`](todo.md) T1-007.

---

## 1. Network Architecture Overview

Our 5-member team is geographically distributed across two homes and one hostel behind NAT/CGNAT. Direct SSH port-forwarding on home routers is insecure and often impossible under CGNAT.

We establish a **zero-configuration private mesh overlay network** using **Tailscale** (WireGuard-based) with **ZeroTier** as a documented fallback.

```text
Hostel / Home Teammates                  Pipeline Host (Laptop A)
(Naveena, Rithika, Priyan)             (Yashwant — Ubuntu 22.04/24.04)
┌───────────────────────┐             ┌─────────────────────────────┐
│ Laptop / Workstation  │             │ HDFS + Flume + Spark + Hive │
│ Tailscale Mesh Client │──WireGuard─▶│ SQLite Serving Store        │
│ SSH Key-Only Auth     │  (Overlay)  │ Streamlit Dashboard (:8501) │
└───────────────────────┘             └──────────────▲──────────────┘
                                                     │ WireGuard
Capture Machine (Laptop B)                           │ (TCP 44444)
(M A Sushil Kumar)                                   │
┌───────────────────────┐                            │
│ Live Wi-Fi TShark     │────────────────────────────┘
│ Tailscale Mesh Client │
└───────────────────────┘
```

### Key Security & Stability Properties
- **No router port-forwarding:** No ports are exposed to the public internet.
- **Key-only SSH authentication:** Password logins and root logins are strictly disabled.
- **Persistent sessions:** `tmux` ensures long-running ingestion or Spark jobs survive SSH disconnections.
- **High-availability host:** Power settings on Laptop A prevent sleeping or suspending when the lid is closed while connected to AC power.

---

## 2. Host Machine Setup (Laptop A & Laptop B)

### Step 2.1: Run Automated Host Configuration
On **Laptop A** (Yashwant), place teammates' public keys into `scripts/keys/<username>.pub`, then execute:
```bash
sudo bash scripts/setup_remote_access.sh
```
This script automates:
1. Package installation: `openssh-server`, `tmux`, `git`, `curl`.
2. Hardening `/etc/ssh/sshd_config.d/50-lnta.conf`:
   - `PasswordAuthentication no`
   - `PermitRootLogin no`
   - `PubkeyAuthentication yes`
3. Creating local user accounts without `sudo` privileges:
   - `naveena`, `rithika`, `sushil`, `priyan`, `yashwant`
4. Deploying authorized public keys into `~<user>/.ssh/authorized_keys`.
5. Disabling system sleep on lid close in `/etc/systemd/logind.conf`:
   - `HandleLidSwitch=ignore`
   - `HandleLidSwitchExternalPower=ignore`

### Step 2.2: Install and Start Tailscale on Ubuntu
```bash
# 1. Install Tailscale official package
curl -fsSL https://tailscale.com/install.sh | sh

# 2. Authenticate and join the network
sudo tailscale up

# 3. Verify status and retrieve overlay IP
tailscale ip -4
```
*Note: Verify free-tier user and node limits on Tailscale. If node sharing or team size exceeds limits, use ZeroTier fallback (see Section 8).*

---

## 3. Client Setup for Teammates (Windows / macOS / Ubuntu)

### Step 3.1: Install Tailscale Client
- **Windows:** Download and install from [tailscale.com/download/windows](https://tailscale.com/download/windows). Sign in using the invited account.
- **macOS:** Install from the Mac App Store.
- **Ubuntu:** Run `curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up`.

### Step 3.2: Generate SSH Keypair (if not already done)
In your local terminal (PowerShell or Bash):
```bash
ssh-keygen -t ed25519 -C "your_name@lnta"
```
Send your public key file (`~/.ssh/id_ed25519.pub`) to Yashwant to be added to Laptop A.

### Step 3.3: Configure SSH Client (`~/.ssh/config`)
Add the following block to your local `~/.ssh/config`:
```ssh-config
Host lnta-host
    HostName <LAPTOP_A_TAILSCALE_IP>
    User <your_username>
    IdentityFile ~/.ssh/id_ed25519
    ServerAliveInterval 60
    ServerAliveCountMax 3
```
Test logging in:
```bash
ssh lnta-host
```

---

## 4. Port-Forwarding Cheat Sheet (`ssh -L`)

Laptop A runs several web UIs. You can forward these ports over your encrypted SSH session and view them directly in your local desktop browser (`http://localhost:<port>`):

| Service | Port on Laptop A | SSH Tunnel Command | Browser URL |
|---|---|---|---|
| **Streamlit Dashboard** | `8501` | `ssh -L 8501:127.0.0.1:8501 lnta-host` | `http://localhost:8501` |
| **Spark Application UI** | `4040` | `ssh -L 4040:127.0.0.1:4040 lnta-host` | `http://localhost:4040` |
| **HDFS NameNode UI** | `9870` | `ssh -L 9870:127.0.0.1:9870 lnta-host` | `http://localhost:9870` |

**Single Combined Tunnel:**
```bash
ssh -L 8501:127.0.0.1:8501 -L 4040:127.0.0.1:4040 -L 9870:127.0.0.1:9870 lnta-host
```

---

## 5. Persistent Sessions with `tmux`

Never start long-running pipeline jobs directly in a basic SSH prompt. Always use `tmux` so the processes continue running if your network connection drops:

```bash
# Start a new named session
tmux new -s lnta-session

# Detach from session (leaves job running in background)
Press: Ctrl+B then release and press D

# List active sessions
tmux ls

# Reattach to existing session
tmux attach -t lnta-session

# Kill session when completely finished
tmux kill-session -t lnta-session
```

---

## 6. Live Capture Network Test (Laptop B ➔ Laptop A)

The live capture forwarder sends raw packet records from Laptop B to Flume's Netcat source on Laptop A over TCP port `44444`.

### Verification Test:
1. **On Laptop A (Listener Test):**
   ```bash
   nc -l -p 44444
   ```
2. **On Laptop B (Sender Test):**
   ```bash
   nc -vz <LAPTOP_A_TAILSCALE_IP> 44444
   # Or send a test string:
   echo "TEST_LINE" | nc -w 2 <LAPTOP_A_TAILSCALE_IP> 44444
   ```
3. If the connection fails, verify that `ufw` on Laptop A permits incoming traffic on interface `tailscale0`:
   ```bash
   sudo ufw allow in on tailscale0 to any port 44444 proto tcp
   ```

---

## 7. Machine Etiquette & Resource Sharing

Laptop A has **16 GB RAM**. To prevent Out-Of-Memory (OOM) failures:

1. **Book Integration Slots:** Post in the team group before running heavy Spark integration tests or starting HDFS clusters.
2. **One Spark Instance at a Time:** PySpark Structured Streaming with local master consumes ~3–4 GB RAM. Only one member should run Spark streaming jobs at any given moment.
3. **Keep Laptop A Clean:** Run all scratch analysis in your owned directories or in local mock mode.
4. **Offline Development:** Develop pure algorithm logic (DGIM, FM, PageRank, Markov, Alert rules) on your own laptops using `pytest` and replay CSVs. Only use Laptop A for pipeline integration.

---

## 8. Fallback: ZeroTier Setup

If Tailscale is blocked by a hostel network or free node limits are reached:
1. Install ZeroTier: `curl -s https://install.zerotier.com | sudo bash`
2. Join network ID: `sudo zerotier-one join <16-digit-network-id>`
3. Admin approves node in the ZeroTier Central web console.
4. Use the assigned ZeroTier managed IP for SSH and Netcat feeds.

