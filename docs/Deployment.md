# Deployment

## 1. Infrastructure

Single Oracle Cloud Always Free Ampere A1 VM — 2 OCPU / 12GB RAM / 200GB storage, 1 reserved public IP (see Decisions.md ADR-002 for why).

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

All on a shared Docker network; only Caddy ports 80/443 exposed externally (via the VM's reserved IP), everything else internal-only.

## 3. Networking

- Reserved public IP → Caddy reverse proxy (terminating TLS automatically on 80/443) routing to FastAPI and Streamlit on distinct paths/subdomains.
- Firewall: only 80/443 open externally; Postgres/Redis/Redpanda and internal service ports never exposed beyond the Docker network.

## 4. Backups

- Postgres: scheduled `pg_dump` to local disk, rotated; periodic push to Cloudflare R2 (10GB free tier) once Phase 3+ (see Decisions.md ADR-007).
- MLflow artifacts: same R2 backup target.

## 5. Resource budget (2 OCPU / 12GB)

Deliberately lean stack given the constraint — no Spark/Flink, no multi-broker Kafka cluster, no separate training cluster. Training runs happen on the same VM during off-peak (batch job via Prefect), not continuously.

## 6. CI/CD Automated Deployment

The deployment pipeline is automated via `.github/workflows/deploy.yml`:
- **Trigger:** Automatic upon merge to `main`, or manually via GitHub Actions `workflow_dispatch`.
- **Secret Guard:** The workflow inspects repository secrets `ORACLE_HOST` and `ORACLE_SSH_KEY`. If they are not configured, the workflow gracefully skips execution with an informational notice, avoiding false CI failures while the VM is pending provisioning.
- **SSH Deployment:** When secrets are present, the workflow connects to the VM, fetches the latest `main` commit, executes `docker compose up -d --build`, and verifies the Caddy reverse proxy `/health` probe.

### Required GitHub Secrets for Live Deployment
| Secret | Description | Example |
|---|---|---|
| `ORACLE_HOST` | Reserved public IP or DNS hostname of the Oracle VM | `150.136.x.x` |
| `ORACLE_SSH_KEY` | Private SSH key authorized in `~/.ssh/authorized_keys` | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| `ORACLE_USER` | SSH user account on the VM (optional, defaults to `ubuntu`) | `ubuntu` |
| `ORACLE_PORT` | SSH port (optional, defaults to `22`) | `22` |

## 7. Resolved & Open Questions
 
- **Swap space:** Automated via `infra/oracle-vm/provision.sh` (2GB swapfile configured on Oracle Linux/Ubuntu ARM64 host). Resolved in Phase 0.
- **VM Provisioning Status:** Oracle Ampere A1 compute instance provisioning is pending manual creation in OCI Console. Automatic deployment is gracefully gated until credentials are populated.

