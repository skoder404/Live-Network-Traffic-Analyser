#!/usr/bin/env bash
# scripts/setup_remote_access.sh
# Automates SSH server hardening, user creation, lid-close power management,
# and tool setup for Ubuntu 22.04/24.04 on Laptop A.
#
# Usage:
#   sudo bash scripts/setup_remote_access.sh
#

set -euo pipefail

MEMBERS=("naveena" "rithika" "sushil" "priyan" "yashwant")
HOST_OWNER="yashwant"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYS_DIR="${SCRIPT_DIR}/keys"

if [[ "$(id -u)" -ne 0 ]]; then
    echo "[-] Error: This script must be run as root (sudo bash scripts/setup_remote_access.sh)." >&2
    exit 1
fi

echo "[+] Step 1: Installing required packages (openssh-server, tmux, git, curl)..."
apt-get update -qq
apt-get install -y -qq openssh-server tmux git curl ufw

echo "[+] Step 2: Hardening SSH server configuration..."
SSH_CONF_DIR="/etc/ssh/sshd_config.d"
mkdir -p "${SSH_CONF_DIR}"

cat > "${SSH_CONF_DIR}/50-lnta.conf" << 'EOF'
# LNTA Secure SSH Configuration
PasswordAuthentication no
PermitRootLogin no
PubkeyAuthentication yes
X11Forwarding no
AllowTcpForwarding yes
ClientAliveInterval 60
ClientAliveCountMax 3
EOF

systemctl enable ssh
systemctl restart ssh
echo "[✓] SSH daemon configured for key-only authentication."

echo "[+] Step 3: Configuring user accounts and authorized keys..."
for user in "${MEMBERS[@]}"; do
    if ! id -u "${user}" >/dev/null 2>&1; then
        echo "    Creating non-sudo user account: ${user}"
        useradd -m -s /bin/bash "${user}"
    else
        echo "    User account exists: ${user}"
    fi

    # Only HOST_OWNER gets sudo
    if [[ "${user}" == "${HOST_OWNER}" ]]; then
        usermod -aG sudo "${user}" || true
    fi

    USER_HOME="/home/${user}"
    SSH_DIR="${USER_HOME}/.ssh"
    mkdir -p "${SSH_DIR}"
    chmod 700 "${SSH_DIR}"

    PUB_KEY_FILE="${KEYS_DIR}/${user}.pub"
    if [[ -f "${PUB_KEY_FILE}" ]]; then
        echo "    Deploying public key for ${user} from ${PUB_KEY_FILE}"
        cat "${PUB_KEY_FILE}" > "${SSH_DIR}/authorized_keys"
        chmod 600 "${SSH_DIR}/authorized_keys"
    else
        echo "    [!] Notice: No public key found at ${PUB_KEY_FILE}. Please copy teammate's key here."
        touch "${SSH_DIR}/authorized_keys"
        chmod 600 "${SSH_DIR}/authorized_keys"
    fi

    chown -R "${user}:${user}" "${SSH_DIR}"
done

echo "[+] Step 4: Disabling system suspend on lid close..."
LOGIND_CONF="/etc/systemd/logind.conf"
if [[ -f "${LOGIND_CONF}" ]]; then
    sed -i -E 's/^#?HandleLidSwitch=.*/HandleLidSwitch=ignore/' "${LOGIND_CONF}"
    sed -i -E 's/^#?HandleLidSwitchExternalPower=.*/HandleLidSwitchExternalPower=ignore/' "${LOGIND_CONF}"
    sed -i -E 's/^#?HandleLidSwitchDocked=.*/HandleLidSwitchDocked=ignore/' "${LOGIND_CONF}"
    systemctl restart systemd-logind || echo "    [!] Note: systemd-logind restart skipped (may need reboot)."
    echo "[✓] Configured systemd logind to ignore lid-close events."
fi

# Disable screen blanking / suspend in GNOME if gsettings is accessible
if command -v gsettings >/dev/null 2>&1; then
    sudo -u "${HOST_OWNER}" gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type 'nothing' 2>/dev/null || true
fi

echo "[+] Step 5: Checking Overlay Network (Tailscale)..."
if command -v tailscale >/dev/null 2>&1; then
    TAILSCALE_IP=$(tailscale ip -4 2>/dev/null || echo "")
    if [[ -n "${TAILSCALE_IP}" ]]; then
        echo "============================================================"
        echo "  Laptop A Tailscale Overlay IP: ${TAILSCALE_IP}"
        echo "  Teammates can connect via:"
        echo "    ssh <username>@${TAILSCALE_IP}"
        echo "============================================================"
    else
        echo "    Tailscale is installed but not authenticated yet."
        echo "    Run: sudo tailscale up"
    fi
else
    echo "    Tailscale not detected. Install via:"
    echo "      curl -fsSL https://tailscale.com/install.sh | sh"
    echo "      sudo tailscale up"
fi

echo "[+] Setup script complete."

