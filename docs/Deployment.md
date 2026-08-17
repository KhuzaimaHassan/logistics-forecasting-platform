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
| MLflow server | custom (mlflow image) | tracking + registry UI |
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

## 6. Open questions

- Swap space configuration to give headroom on 12GB RAM during training runs — needed before Phase 3.
