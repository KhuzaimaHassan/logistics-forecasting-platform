# Performance

## 1. Latency budgets (targets, to validate once built)

| Path | Target | Notes |
|---|---|---|
| `/predict/demand`, `/predict/eta` | < 200ms | Online feature read (Redis) + model inference, no external calls |
| `/agent/chat` | < 5s | Dominated by LLM inference (Groq is fast; Gemini fallback slower) |
| Stream consumer, event to online-store update | < 5s end-to-end | "Near-real-time," not hard real-time |

## 2. Known constraints

- 2 OCPU / 12GB Oracle VM hosts everything — training runs and API serving compete for the same CPU. Training should run as a scheduled batch job during low-traffic windows, not on-demand.
- Redpanda single-node, low partition count — fine for personal-project traffic volume, would need real re-architecture (not just more partitions) to handle production-scale load. Explicitly out of scope; noted so it's not mistaken for an oversight.

## 3. Load testing

- Basic load test (e.g., `locust` or a simple async script) against `/predict/*` endpoints planned for Phase 5, to confirm the latency budget holds under concurrent requests, not just single-request testing.
- No load testing planned for `/agent/chat` beyond manual use — LLM API rate limits (Groq/Gemini free tier) are the real constraint there, not our own infra.

## 4. Open questions

- Whether model inference should be cached (e.g., don't recompute a zone's demand prediction more than once per minute even under repeated requests) — likely yes, simple TTL cache, to implement during Phase 5.
