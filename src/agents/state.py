"""Agent state schemas for the LangGraph Ops Copilot.

ADR-023: State is strictly typed and tracks query input, allowlisted tool execution,
retrieved context citations, active provider/model identification, execution latency,
and diagnostic status codes.
"""

from typing import Annotated, Any, Dict, List, Optional, Sequence

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class CopilotState(TypedDict, total=False):
    """LangGraph execution state representing a single Ops Copilot turn."""

    messages: Annotated[Sequence[BaseMessage], add_messages]
    query: str
    tools_used: List[str]
    sources: List[str]
    provider: str  # "groq", "gemini", "mock"
    model_name: str
    response: str
    latency_ms: float
    status: str  # "success", "guardrail_blocked", "error", "degraded"
    error: Optional[str]
    tool_calls: List[Dict[str, Any]]
    tool_results: List[Dict[str, Any]]
    retrieved_context: List[str]
