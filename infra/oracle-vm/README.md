# Oracle Cloud VM Provisioning & Deployment Guide

This directory contains provisioning scripts and network configuration guidelines for running the Logistics Demand & ETA Forecasting Platform on an **Oracle Cloud Always Free Ampere A1 VM** (2 OCPU / 12GB RAM / 200GB block storage).

## 1. Initial VM Setup

1. Launch an Ampere A1 compute instance in the Oracle Cloud Console (Ubuntu 22.04/24.04 ARM64).
2. Assign a reserved public IP to the instance.
3. In your **OCI Virtual Cloud Network (VCN) Security List**, ensure Ingress Rules exist for:
   - Port `22` (SSH)
   - Port `80` (HTTP for Caddy ACME challenges)
   - Port `443` (HTTPS for web/API traffic)
   - *Do NOT open ports 5432, 6379, 9092, 5000, 8000, or 8501.*

## 2. Running the Provisioning Script

SSH into your Oracle VM and run:

```bash
git clone https://github.com/KhuzaimaHassan/logistics-forecasting-platform.git
cd logistics-forecasting-platform/infra/oracle-vm
chmod +x provision.sh
./provision.sh
```

## 3. Starting the Platform

```bash
# Copy and populate environment variables
cp .env.example .env
nano .env

# Start all infrastructure and service containers
docker compose up -d
```
