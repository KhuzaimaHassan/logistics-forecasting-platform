"""LangGraph Ops Copilot state graph workflow.

ADR-023: Implements an explicit LangGraph StateGraph with four nodes:
1. input_guardrail: Advisory regex and prompt-injection defense.
2. router_node: Intent classification and allowlisted tool selection.
3. tool_executor: Enforces strict read-only tool allowlist (definitive security boundary).
4. synthesis_node: Natural language synthesis, citation attribution, and mock labeling.

ADR-024: Transparent fallback cascade (Groq -> Gemini -> Mock), with explicit
mock response labeling ([MOCK / OFFLINE MODE] and provider: 'mock').
"""

import logging
import time
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph

from src.agents.guardrails import (
    check_prompt_injection,
    isolate_retrieved_content,
    sanitize_output,
    validate_tool_execution,
)
from src.agents.providers import (
    BaseLLMProvider,
    MockLLMProvider,
    get_llm_provider,
)
from src.agents.state import CopilotState
from src.agents.tools import TOOL_ALLOWLIST

logger = logging.getLogger(__name__)


def _serialize_tool_for_llm(tool_obj: Any) -> Dict[str, Any]:
    """Serialize LangChain tool to OpenAI function definition format."""
    return {
        "type": "function",
        "function": {
            "name": tool_obj.name,
            "description": tool_obj.description or "",
            "parameters": (
                (
                    tool_obj.args_schema.model_json_schema()
                    if hasattr(tool_obj.args_schema, "model_json_schema")
                    else tool_obj.args_schema.schema()
                )
                if hasattr(tool_obj, "args_schema") and tool_obj.args_schema
                else {"type": "object", "properties": {}}
            ),
        },
    }


def input_guardrail_node(state: CopilotState) -> Dict[str, Any]:
    """Inspect input query with advisory prompt injection checks.

    Advisory defense-in-depth layer. If hostile patterns are detected, flags state
    to route directly to END with an advisory rejection notice.
    """
    query = state.get("query", "")
    is_safe, reason = check_prompt_injection(query)

    if not is_safe:
        logger.warning("Query rejected by advisory input guardrail: %s", reason)
        return {
            "status": "guardrail_blocked",
            "response": (
                "[ADVISORY GUARD] Query rejected by security filter: potentially "
                "adversarial or hostile prompt pattern detected. The Ops Copilot "
                "operates under strict read-only parameters."
            ),
            "tools_used": [],
            "sources": [],
            "error": reason,
        }

    return {"status": "in_progress"}


def route_after_guardrail(state: CopilotState) -> str:
    """Route to router_node if safe, or END if blocked."""
    if state.get("status") == "guardrail_blocked":
        return "end"
    return "router"


def create_router_node(provider: BaseLLMProvider):
    """Factory creating the router / tool selection node for a given provider."""

    def router_node(state: CopilotState) -> Dict[str, Any]:
        query = state.get("query", "")
        tools_schema = [_serialize_tool_for_llm(t) for t in TOOL_ALLOWLIST.values()]

        messages = [{"role": "user", "content": query}]

        try:
            llm_resp = provider.generate(messages=messages, tools=tools_schema)
            return {
                "provider": llm_resp.provider,
                "model_name": llm_resp.model_name,
                "tool_calls": llm_resp.tool_calls,
                "response": llm_resp.content,
            }
        except Exception as exc:
            logger.error(
                "Router generation failed with provider %s: %s",
                provider.provider_name,
                exc,
            )
            # Fallback to MockLLMProvider on failure
            mock = MockLLMProvider()
            mock_resp = mock.generate(messages=messages, tools=tools_schema)
            return {
                "provider": mock.provider_name,
                "model_name": mock.model_name,
                "tool_calls": mock_resp.tool_calls,
                "response": mock_resp.content,
                "error": f"Provider {provider.provider_name} failed ({exc}); defaulted to mock engine.",
            }

    return router_node


def route_after_router(state: CopilotState) -> str:
    """Route to tool_executor if tool calls are present, else synthesizer."""
    tool_calls = state.get("tool_calls", [])
    if tool_calls and len(tool_calls) > 0:
        return "tool_executor"
    return "synthesizer"


def tool_execution_node(state: CopilotState) -> Dict[str, Any]:
    """Execute requested tools enforcing the ADR-023 immutable read-only allowlist.

    CRITICAL SECURITY GUARANTEE:
    Every single tool call is validated via validate_tool_execution() before invocation.
    Unallowlisted tools raise SecurityViolationError and are strictly prevented from running.
    """
    tool_calls = state.get("tool_calls", [])
    tools_used: List[str] = list(state.get("tools_used", []))
    sources: List[str] = list(state.get("sources", []))
    tool_results: List[Dict[str, Any]] = []

    for call in tool_calls:
        tool_name = call.get("name", "")
        tool_args = call.get("args", {})

        # Definitive structural security boundary enforcement
        try:
            validate_tool_execution(tool_name)
        except Exception as sec_err:
            logger.error(
                "Structural tool allowlist blocked execution of: %s", tool_name
            )
            tool_results.append(
                {
                    "tool": tool_name,
                    "status": "security_blocked",
                    "error": str(sec_err),
                }
            )
            continue

        tool_obj = TOOL_ALLOWLIST[tool_name]
        try:
            # Invoke the tool
            func = getattr(tool_obj, "func", tool_obj)
            res = func(**tool_args)

            tools_used.append(tool_name)
            tool_results.append({"tool": tool_name, "args": tool_args, "result": res})

            # Extract cited sources
            if tool_name == "get_features":
                etype = tool_args.get("entity_type", "zone")
                sources.append(f"feast_redis:{etype}_features")
            elif tool_name == "query_recent_predictions":
                sources.append("warehouse.predictions")
            elif tool_name == "query_pipeline_status":
                sources.append("warehouse.pipeline_runs")
            elif tool_name == "search_logs_and_model_cards":
                for item in res.get("results", []):
                    if "source" in item and item["source"] not in sources:
                        sources.append(item["source"])

        except Exception as exc:
            logger.error("Error executing allowlisted tool %s: %s", tool_name, exc)
            tool_results.append(
                {"tool": tool_name, "status": "error", "error": str(exc)}
            )

    # Deduplicate tools_used and sources
    clean_tools_used = list(dict.fromkeys(tools_used))
    clean_sources = list(dict.fromkeys(sources))

    return {
        "tools_used": clean_tools_used,
        "sources": clean_sources,
        "tool_results": tool_results,
    }


def _format_features_section(res: Dict[str, Any]) -> List[str]:
    etype = res.get("entity_type", "zone")
    eid = res.get("entity_id", "")
    features = res.get("features", {})
    cache_hit = res.get("cache_hit", False)
    warning = res.get("warning")

    cache_badge = "[Live Redis Hit]" if cache_hit else "[Imputed Offline Fallback]"
    lines = [f"### Feature State: {etype.capitalize()} `{eid}` ({cache_badge})"]
    if warning:
        lines.append(f"> *{warning}*")

    feature_lines = "\n".join(f"- **{k}**: `{v}`" for k, v in sorted(features.items()))
    lines.append(feature_lines + "\n")
    return lines


def _format_predictions_section(res: Dict[str, Any]) -> List[str]:
    count = res.get("count", 0)
    preds = res.get("predictions", [])
    lines = [
        f"### Recent Predictions (`warehouse.predictions`) — {count} records found"
    ]
    if preds:
        for p in preds[:5]:
            lines.append(
                f"- `{p.get('predicted_at')}` | Entity `{p.get('entity_id')}` | "
                f"Value: **{p.get('predicted_value'):.2f}** | Model: `{p.get('model_version')}`"
            )
    else:
        lines.append(
            "- *No recent prediction logs recorded for this entity in the specified window.*"
        )
    lines.append("")
    return lines


def _format_pipeline_section(res: Dict[str, Any]) -> List[str]:
    overall = res.get("overall_status", "unknown").upper()
    total_runs = res.get("total_runs", 0)
    runs = res.get("runs", [])
    lines = [f"### Pipeline Execution Health: `{overall}` ({total_runs} recent runs)"]
    for r in runs[:5]:
        status_icon = "[OK]" if r.get("status") == "success" else "[FAILED]"
        lines.append(
            f"- {status_icon} **{r.get('pipeline_name')}** ({r.get('run_type')}) | "
            f"Status: `{r.get('status')}` | Processed: {r.get('records_processed')} rows | "
            f"Duration: {r.get('duration_seconds'):.1f}s"
        )
    lines.append("")
    return lines


def _format_rag_section(res: Dict[str, Any]) -> List[str]:
    rag_results = res.get("results", [])
    count = len(rag_results)
    lines = [f"### Platform Documentation & Architecture Insights ({count} matches)"]
    for r in rag_results[:3]:
        isolated_snippet = isolate_retrieved_content(
            r.get("content", ""), source_label=r.get("source")
        )
        lines.append(
            f"#### [DOC] {r.get('title')} (Relevance: {r.get('score', 0):.2f})"
        )
        lines.append(f"{isolated_snippet}\n")
    return lines


def synthesis_node(state: CopilotState) -> Dict[str, Any]:
    """Synthesize natural language response, incorporate citations, and apply mock labeling."""
    provider_name = state.get("provider", "mock")
    sources = state.get("sources", [])
    tool_results = state.get("tool_results", [])
    existing_response = state.get("response", "")

    # If already responded (e.g. direct response without tools), ensure mock header if applicable
    if existing_response and not tool_results:
        final_text = existing_response
        if provider_name == "mock" and not final_text.startswith(
            MockLLMProvider.MOCK_HEADER
        ):
            final_text = f"{MockLLMProvider.MOCK_HEADER} {final_text}"
        return {
            "response": sanitize_output(final_text),
            "status": "success",
        }

    # Synthesize response from tool outputs
    header = (
        f"{MockLLMProvider.MOCK_HEADER} **Ops Copilot Analysis**\n"
        if provider_name == "mock"
        else "**Ops Copilot Analysis**\n"
    )
    sections: List[str] = [header]

    for item in tool_results:
        tname = item.get("tool")
        res = item.get("result", {})
        err = item.get("error")

        if err:
            sections.append(f"[WARNING] **{tname}**: Execution warning — {err}\n")
            continue

        if tname == "get_features":
            sections.extend(_format_features_section(res))
        elif tname == "query_recent_predictions":
            sections.extend(_format_predictions_section(res))
        elif tname == "query_pipeline_status":
            sections.extend(_format_pipeline_section(res))
        elif tname == "search_logs_and_model_cards":
            sections.extend(_format_rag_section(res))

    if sources:
        citations = ", ".join(f"`{s}`" for s in sources)
        sections.append(f"\n---\n**Data Sources Cited**: {citations}")

    final_content = "\n".join(sections).strip()
    return {
        "response": sanitize_output(final_content),
        "status": "success",
    }


def create_copilot_graph(provider: Optional[BaseLLMProvider] = None) -> Any:
    """Build and compile the LangGraph StateGraph workflow."""
    active_provider = provider or get_llm_provider()

    workflow = StateGraph(CopilotState)

    # Add workflow nodes
    workflow.add_node("guardrail", input_guardrail_node)
    workflow.add_node("router", create_router_node(active_provider))
    workflow.add_node("tool_executor", tool_execution_node)
    workflow.add_node("synthesizer", synthesis_node)

    # Add edges and conditional branches
    workflow.add_edge(START, "guardrail")
    workflow.add_conditional_edges(
        "guardrail",
        route_after_guardrail,
        {"end": END, "router": "router"},
    )
    workflow.add_conditional_edges(
        "router",
        route_after_router,
        {"tool_executor": "tool_executor", "synthesizer": "synthesizer"},
    )
    workflow.add_edge("tool_executor", "synthesizer")
    workflow.add_edge("synthesizer", END)

    return workflow.compile()


def run_copilot(
    query: str,
    conversation_id: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
    force_provider: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute an Ops Copilot conversational turn.

    Args:
        query: User input prompt.
        conversation_id: Optional conversation session tracking identifier.
        history: Optional previous turn message history.
        force_provider: Optional override ('groq', 'gemini', 'mock').

    Returns:
        Structured response dictionary matching FastAPI /agent/chat schema:
        {
            "response": str,
            "tools_used": List[str],
            "sources": List[str],
            "provider": str,
            "model_name": str,
            "latency_ms": float,
            "status": str,
            "error": Optional[str],
        }
    """
    start_time = time.perf_counter()
    clean_query = str(query).strip()

    provider = get_llm_provider(force_provider=force_provider)
    graph = create_copilot_graph(provider=provider)

    initial_state: CopilotState = {
        "messages": [HumanMessage(content=clean_query)],
        "query": clean_query,
        "tools_used": [],
        "sources": [],
        "provider": provider.provider_name,
        "model_name": provider.model_name,
        "response": "",
        "status": "initialized",
        "error": None,
        "tool_calls": [],
        "tool_results": [],
    }

    try:
        final_state = graph.invoke(initial_state)
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        return {
            "response": final_state.get("response", ""),
            "tools_used": final_state.get("tools_used", []),
            "sources": final_state.get("sources", []),
            "provider": final_state.get("provider", provider.provider_name),
            "model_name": final_state.get("model_name", provider.model_name),
            "latency_ms": round(elapsed_ms, 2),
            "status": final_state.get("status", "success"),
            "error": final_state.get("error"),
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        logger.error("Unhandled copilot graph error: %s", exc)
        return {
            "response": f"[MOCK / OFFLINE MODE] Error executing agent query: {exc}",
            "tools_used": [],
            "sources": [],
            "provider": "mock",
            "model_name": "mock-rule-engine",
            "latency_ms": round(elapsed_ms, 2),
            "status": "error",
            "error": str(exc),
        }
