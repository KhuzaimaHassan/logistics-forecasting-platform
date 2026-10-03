# Deployment

## 1. Infrastructure

Single Hetzner Cloud CAX21 ARM64 VM — 4 vCPU / 8GB RAM / 80GB NVMe storage, 1 public IPv4 (supersedes Oracle Always Free A1.Flex per Decisions.md ADR-027).

## 2. Topology (Docker Compose, one file, one host)

| Service | Container | Notes |
|---|---|---|
| Caddy | `caddy:2-alpine` | reverse proxy, terminates TLS on 80/443, routes to FastAPI and Streamlit |
| Postgres | `postgres:16` | warehouse + Feast offline store + MLflow backend |
| Redis | `redis:7` | Feast online store |
| Redpanda | `redpandadata/redpanda` | single-node broker |
| Stream producer | custom | replay + live-feed polling |
| Stream consumer | custom | transform/load |
| MLflow server | `ghcr.io/mlflow/mlflow:v3.15.1` | official image, tracking + registry UI |
| FastAPI | custom | serving + agent endpoint |
| Streamlit | custom | UI |
| Prefect worker | `prefecthq/prefect` | executes flows scheduled by Prefect Cloud |

All on a shared Docker network; only Caddy ports 80/443 exposed externally (via the VM's public IP), everything else internal-only.

## 3. Networking

- Public IP → Caddy reverse proxy (terminating TLS automatically on 80/443) routing to FastAPI and Streamlit on distinct paths/subdomains.
- Firewall: only 80/443 open externally; Postgres/Redis/Redpanda and internal service ports never exposed beyond the Docker network.

## 4. Backups

- Postgres: scheduled `pg_dump` to local disk, rotated; periodic push to Cloudflare R2 (10GB free tier) once Phase 3+ (see Decisions.md ADR-007).
- MLflow artifacts: same R2 backup target.

## 5. Resource budget (4 vCPU / 8GB RAM + Swap)

Deliberately lean stack given the constraint — no Spark/Flink, no multi-broker Kafka cluster, no separate training cluster. Training runs happen on the same VM during off-peak (batch job via Prefect), not continuously. On an 8GB budget, services are tuned for low baseline memory footprint with a 2GB–4GB host NVMe swapfile configured by `infra/provision.sh` for headroom during batch training and peak load.

## 6. CI/CD Automated Deployment

The deployment pipeline is automated via `.github/workflows/deploy.yml`:
- **Trigger:** Automatic upon merge to `main`, or manually via GitHub Actions `workflow_dispatch`.
- **Secret Guard:** The workflow inspects repository secrets `DEPLOY_VM_HOST` (or legacy `ORACLE_HOST`) and `DEPLOY_VM_SSH_KEY` (or legacy `ORACLE_SSH_KEY`). If they are not configured, the workflow gracefully skips execution with an informational notice, avoiding false CI failures while the VM is pending provisioning.
- **SSH Deployment:** When secrets are present, the workflow connects to the VM, fetches the latest `main` commit, executes `docker compose up -d --build`, and verifies the Caddy reverse proxy `/health` probe.

### Required GitHub Secrets for Live Deployment
| Secret | Description | Example |
|---|---|---|
| `DEPLOY_VM_HOST` | Public IP or DNS hostname of the VM | `159.69.x.x` |
| `DEPLOY_VM_SSH_KEY` | Private SSH key authorized in `~/.ssh/authorized_keys` | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| `DEPLOY_VM_USER` | SSH user account on the VM (optional, defaults to `root` on Hetzner, `ubuntu`) | `root` |
| `DEPLOY_VM_PORT` | SSH port (optional, defaults to `22`) | `22` |

## 7. Resolved & Open Questions
 
- **Swap space:** Automated via `infra/provision.sh` (2GB–4GB swapfile configured on Ubuntu ARM64 host). Resolved in Phase 0.
- **VM Provisioning Status:** Migrated from Oracle Always Free to Hetzner Cloud CAX21 (ARM64) per ADR-027 to eliminate indefinite capacity queue blocking. Deployment executes via `infra/provision.sh` and Docker Compose.

