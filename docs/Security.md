# Security

## 1. Secrets management

- All API keys/credentials in `.env` (gitignored) locally, GitHub Actions repo secrets in CI, and injected as environment variables into the Oracle VM's Docker Compose stack (never baked into images).
- No secrets in logs — request/response logging for the agent and API explicitly excludes API key values.

## 2. Least-privilege access

- Postgres: separate DB roles for the stream consumer (write access to `raw`/`warehouse`), the API (read-only), and Feast/MLflow (their own schemas only).
- Redis: single instance, no auth needed at v1 scale since it's internal-network-only (not exposed externally) — revisit if that assumption changes.

## 3. Network exposure

- Only FastAPI and Streamlit reachable externally (via reverse proxy on 80/443). Postgres, Redis, Redpanda never exposed beyond the Docker internal network (see Deployment.md).

## 4. Agent-specific: prompt-injection resistance

Because the agent takes free-text input and one of its tools retrieves external-ish content (pipeline logs, model cards, and eventually possibly retrieved web/doc content), this needs actual design attention, not a disclaimer:

- **Tool allowlisting** — the agent can only call the four tools defined in Agents.md, nothing dynamically discovered.
- **Read-only tools** — no tool can write/mutate data, so a successful injection has a bounded blast radius (bad answer, not bad action).
- **No instruction-following from retrieved content** — text pulled back by `search_logs_and_model_cards` is treated as data to summarize, never as instructions to execute. This needs to be explicit in the system prompt and, ideally, tested with adversarial log/doc content during Phase 7.

## 5. API auth

- v1: no auth (personal project, not public-facing). Documented gap: **before sharing any public demo link**, add at minimum an API key header for write-adjacent endpoints (there currently are none, but `/agent/chat` should still be rate-limited to avoid free-tier LLM quota exhaustion from a shared link).

## 6. Open questions

- Rate limiting strategy for `/agent/chat` once/if a public demo link exists — needed before Phase 9 (Polish), not before.
