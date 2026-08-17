#!/usr/bin/env bash
# Provisioning script for Oracle Cloud Always Free Ampere A1 VM (Ubuntu 22.04/24.04 LTS or Oracle Linux 9)
# Automates Docker, Docker Compose, Caddy prerequisites, and OS firewall rules.

set -euo pipefail

echo "===> Updating package indices..."
sudo apt-get update -y && sudo apt-get upgrade -y

echo "===> Installing base utilities..."
sudo apt-get install -y curl wget git ufw apt-transport-https ca-certificates gnupg lsb-release

echo "===> Configuring host firewall (ports 22, 80, 443 only)..."
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp comment 'SSH'
sudo ufw allow 80/tcp comment 'HTTP (Caddy TLS challenge)'
sudo ufw allow 443/tcp comment 'HTTPS (Caddy TLS)'
sudo ufw --force enable

echo "===> Installing Docker Engine and Docker Compose Plugin..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    rm get-docker.sh
    sudo usermod -aG docker "$USER"
    echo "Docker installed successfully."
fi

echo "===> Ensuring swap space (2GB) for Ampere VM memory headroom during training..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "2GB Swap configured."
fi

echo "===> Oracle VM provisioning complete! Log out and back in to apply Docker group permissions."
