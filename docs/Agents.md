# Agents

## 1. Purpose

The Ops Copilot agent is the pipeline's differentiator (see Architecture.md) — it turns a set of predictions and dashboards into something a human can ask questions of directly, using the same live state a human debugging the system would look at.

## 2. Framework

LangGraph, chosen for explicit control over the tool-calling graph (vs. a more opaque single-agent loop) — matters here because tool calls hit real infra (DB, feature store) and predictable, debuggable execution order is worth more than flexibility.

## 3. LLM providers

Groq (primary, fast inference) and Gemini (fallback), both free-tier, both already used in prior projects — no new LLM vendor evaluation needed.

## 4. Tools

| Tool | Backing | Example use |
|---|---|---|
| `get_features(entity_type, entity_id)` | Feast online store | "what's the current feature state for zone 161?" |
| `query_recent_predictions(entity_type, entity_id, window)` | Postgres prediction log | "how has the ETA prediction for this corridor changed today?" |
| `query_pipeline_status()` | `pipeline_runs` table | "did the retraining job run this week?" |
| `search_logs_and_model_cards(query)` | FAISS RAG index | "what changed in the last model version?" |

## 5. RAG (folded in, not a separate system)

- FAISS index over: Prefect/pipeline run logs, MLflow model cards/changelogs, and this docs/ folder itself (so the agent can answer questions about its own design, e.g. "why NYC and not Karachi").
- Re-indexed on a schedule (daily) rather than real-time — logs/model cards don't change fast enough to need more.
- Deliberately small and self-contained — not a general-purpose document Q&A system, scoped tightly to this project's own artifacts.

## 6. Graph structure (draft)

1. **Router node** — classifies the question type (feature lookup / prediction history / pipeline health / general/doc question) to decide which tool(s) to call.
2. **Tool-call node(s)** — executes the relevant tool(s), possibly in parallel for multi-part questions.
3. **Synthesis node** — combines tool outputs into a natural-language answer, cites which tool(s) informed the answer (surfaced in the API's `tools_used` field).

## 7. Guardrails

- Tools are read-only — the agent cannot trigger retraining, modify data, or take any write action. Any future write-capable tool would need explicit reconsideration of this constraint.
- Since the agent takes free-text natural-language input, prompt-injection resistance is a real concern, not boilerplate — see Security.md for the specific approach (input scoping, tool allowlisting, no execution of instructions found inside retrieved documents).

## 8. Open questions

- Whether the router node should be a small classifier or just let the LLM pick tools directly via function-calling — leaning toward direct function-calling (simpler, LangGraph handles this natively) unless routing accuracy turns out to be a problem.
