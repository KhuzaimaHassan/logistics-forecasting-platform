#!/usr/bin/env bash
# Generic cloud-agnostic provisioning script for Ubuntu ARM64 hosts (Hetzner CAX21, Oracle Ampere A1, etc.)
# Automates Docker Engine, Docker Compose Plugin, UFW firewall (22/80/443), and swap space.

set -euo pipefail

CURRENT_USER="${USER:-$(whoami)}"

echo "===> [1/4] Updating package indices..."
sudo apt-get update -y && sudo apt-get upgrade -y

echo "===> [2/4] Installing base utilities and configuring UFW firewall (22, 80, 443 only)..."
sudo apt-get install -y curl wget git ufw apt-transport-https ca-certificates gnupg lsb-release
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp comment 'SSH'
sudo ufw allow 80/tcp comment 'HTTP (Caddy TLS challenge)'
sudo ufw allow 443/tcp comment 'HTTPS (Caddy TLS)'
sudo ufw --force enable
sudo ufw status verbose

echo "===> [3/4] Installing Docker Engine and Docker Compose Plugin..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    rm get-docker.sh
    if id "$CURRENT_USER" &>/dev/null; then
        sudo usermod -aG docker "$CURRENT_USER" || true
    fi
    echo "Docker Engine installed successfully."
else
    echo "Docker Engine is already installed."
fi
docker --version
docker compose version

echo "===> [4/4] Ensuring swap space (2GB) for memory headroom on 8GB host..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "2GB Swap configured and mounted."
else
    echo "Swapfile already exists."
fi
free -h

echo "===> VM provisioning complete! Docker, Compose, UFW (22/80/443), and Swap are verified."
