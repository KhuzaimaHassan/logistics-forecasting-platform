"""Streamlit user interface for the NYC Logistics Demand & ETA Forecasting Platform.

Features:
- Ops Copilot tab: conversational assistant calling FastAPI POST /agent/chat.
- Surfaces active LLM provider and model name (Groq llama-3.3-70b-versatile, Gemini gemini-2.0-flash, or mock-rule-engine).
- Interactive quick-action prompt chips for common operational questions.
- Metadata inspection for tools used, sources cited, execution latency, and turn status.
- Live Map and Pipeline Observability tabs for Phase 8 extension.
"""

import os
import time
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

# Configure page settings
st.set_page_config(
    page_title="NYC Logistics Ops Copilot",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Constants & Configuration
FASTAPI_DEFAULT_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
REQUEST_TIMEOUT_SECONDS = 30


# ---------------------------------------------------------------------------
# API Client Helpers
# ---------------------------------------------------------------------------


def check_backend_health(base_url: str) -> Dict[str, Any]:
    """Check health and dependencies of the FastAPI serving backend."""
    url = f"{base_url.rstrip('/')}/health"
    try:
        resp = requests.get(url, timeout=3.0)
        if resp.status_code == 200:
            return {"healthy": True, "data": resp.json()}
        return {
            "healthy": False,
            "error": f"HTTP {resp.status_code}: {resp.text[:100]}",
        }
    except Exception as exc:
        return {"healthy": False, "error": str(exc)}


def send_chat_query(
    base_url: str,
    query: str,
    conversation_id: Optional[str] = None,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Submit a conversational query to POST /agent/chat."""
    url = f"{base_url.rstrip('/')}/agent/chat"
    payload = {
        "query": query,
        "conversation_id": conversation_id,
        "history": history,
    }
    t0 = time.perf_counter()
    try:
        resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if resp.status_code == 200:
            data = resp.json()
            data.setdefault("latency_ms", round(elapsed_ms, 2))
            return {"success": True, "data": data}
        return {
            "success": False,
            "error": f"Backend returned HTTP {resp.status_code}: {resp.text}",
        }
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": f"Query timed out after {REQUEST_TIMEOUT_SECONDS}s.",
        }
    except Exception as exc:
        return {"success": False, "error": f"Failed to connect to backend: {exc}"}


def get_pipeline_status(base_url: str) -> Dict[str, Any]:
    """Query GET /pipeline/status for orchestration run history."""
    url = f"{base_url.rstrip('/')}/pipeline/status"
    try:
        resp = requests.get(url, timeout=5.0)
        if resp.status_code == 200:
            return {"success": True, "data": resp.json()}
        return {"success": False, "error": f"HTTP {resp.status_code}"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Sidebar Navigation & Platform Status
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🚖 Logistics Platform")
    st.caption("NYC TLC Demand & ETA Forecasting")

    st.markdown("---")
    st.subheader("🔌 Backend Service")

    api_url = st.text_input(
        "FastAPI Service URL",
        value=FASTAPI_DEFAULT_URL,
        help="Base URL for FastAPI serving container or reverse proxy.",
    )

    # Health check status indicator
    health = check_backend_health(api_url)
    if health["healthy"]:
        h_data = health["data"]
        models_loaded = h_data.get("models_loaded", {})
        status_label = h_data.get("status", "ok").upper()
        st.success(f"● Serving Online ({status_label})")
        with st.expander("Model Registry Status", expanded=False):
            for m_name, m_info in models_loaded.items():
                st.write(
                    f"**{m_name}**: Version `{m_info.get('version')}` (`{m_info.get('stage')}`)"
                )
    else:
        st.error(f"● Serving Offline: {health.get('error')}")
        st.caption("Ensure FastAPI container or local dev server is running.")

    st.markdown("---")
    st.subheader("🛡️ Agent Architecture")
    st.markdown("""
        - **Framework**: LangGraph stategraph
        - **Primary LLM**: Groq (`llama-3.3-70b-versatile`)
        - **Fallback LLM**: Gemini (`gemini-2.0-flash`)
        - **Offline Fallback**: Rule Engine (`mock-rule-engine`)
        - **Security Boundary**: Read-only tool allowlist (ADR-023)
        """)

    with st.expander("Allowlisted Tools", expanded=False):
        st.markdown("""
            1. `get_features`: Feast Redis online store
            2. `query_recent_predictions`: PostgreSQL predictions log
            3. `query_pipeline_status`: PostgreSQL pipeline runs log
            4. `search_logs_and_model_cards`: FAISS CPU vector index
            """)

    st.markdown("---")
    if st.button("🗑️ Clear Chat History", use_container_width=True):
        st.session_state["messages"] = []
        st.session_state["conversation_id"] = None
        st.rerun()


# ---------------------------------------------------------------------------
# Main Content & Multi-Tab View
# ---------------------------------------------------------------------------

st.title("NYC Logistics Forecasting Platform")
st.markdown(
    "Real-time NYC taxi demand and corridor trip duration forecasting with integrated LangGraph Ops Copilot."
)

tab_copilot, tab_map, tab_pipeline = st.tabs(
    [
        "🤖 Ops Copilot",
        "🗺️ Live Forecasts & Map",
        "📈 Pipeline Observability",
    ]
)


# ---------------------------------------------------------------------------
# TAB 1: LangGraph Ops Copilot
# ---------------------------------------------------------------------------

with tab_copilot:
    st.subheader("Ops Copilot — Conversational Platform Observability")
    st.caption(
        "Ask operational questions regarding zone demand, corridor ETAs, model promotion safety gates, or pipeline execution health."
    )

    # Initialize conversational state
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    if "conversation_id" not in st.session_state:
        st.session_state["conversation_id"] = None

    # Quick-Action Prompt Chips
    st.markdown("**Quick Operational Queries:**")
    col1, col2, col3, col4 = st.columns(4)
    quick_query = None

    with col1:
        if st.button("🚕 Zone 161 Demand", use_container_width=True):
            quick_query = (
                "What is the current feature state and demand forecast for zone 161?"
            )
    with col2:
        if st.button("⏱️ Corridor 161 → 236 ETA", use_container_width=True):
            quick_query = "What is the ETA prediction for corridor 161 to 236?"
    with col3:
        if st.button("🔄 Retraining Status", use_container_width=True):
            quick_query = "Did the model retraining pipeline run this week?"
    with col4:
        if st.button("📋 Promotion Hurdle (ADR-021)", use_container_width=True):
            quick_query = "How does the model promotion hurdle rate work in ADR-021?"

    # Render conversational message thread
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("metadata"):
                meta = msg["metadata"]
                prov = meta.get("provider", "unknown")
                model = meta.get("model_name", "unknown")
                lat = meta.get("latency_ms", 0.0)
                status = meta.get("status", "success")

                # Metrics badges
                badge_col1, badge_col2, badge_col3 = st.columns([2, 1, 1])
                with badge_col1:
                    st.caption(f"🧠 **Provider**: `{prov}` (`{model}`)")
                with badge_col2:
                    st.caption(f"⚡ **Latency**: `{lat:.1f}ms`")
                with badge_col3:
                    status_badge = "🟢 OK" if status == "success" else "🟡 Guarded"
                    st.caption(f"Status: **{status_badge}**")

                # Expandable tool & source metadata
                tools_used = meta.get("tools_used", [])
                sources = meta.get("sources", [])
                if tools_used or sources:
                    with st.expander(
                        f"🔍 Inspect Tools ({len(tools_used)}) & Citations ({len(sources)})",
                        expanded=False,
                    ):
                        if tools_used:
                            st.write(
                                f"**Allowlisted Tools Executed**: {', '.join([f'`{t}`' for t in tools_used])}"
                            )
                        if sources:
                            st.write(
                                f"**Data Sources Cited**: {', '.join([f'`{s}`' for s in sources])}"
                            )

    # Chat Input Box
    user_input = st.chat_input(
        "Ask Ops Copilot an operational question (e.g. 'Show recent predictions for zone 161')..."
    )
    prompt_to_submit = quick_query or user_input

    if prompt_to_submit:
        # Append and display user message
        st.session_state["messages"].append(
            {"role": "user", "content": prompt_to_submit}
        )
        with st.chat_message("user"):
            st.markdown(prompt_to_submit)

        # Query backend and display response
        with st.chat_message("assistant"):
            with st.spinner("Ops Copilot analyzing live state..."):
                resp = send_chat_query(
                    base_url=api_url,
                    query=prompt_to_submit,
                    conversation_id=st.session_state["conversation_id"],
                )

            if resp["success"]:
                data = resp["data"]
                answer = data.get("response", "")
                st.session_state["conversation_id"] = data.get(
                    "conversation_id", st.session_state["conversation_id"]
                )

                st.markdown(answer)

                meta = {
                    "provider": data.get("provider", "unknown"),
                    "model_name": data.get("model_name", "unknown"),
                    "latency_ms": data.get("latency_ms", 0.0),
                    "status": data.get("status", "success"),
                    "tools_used": data.get("tools_used", []),
                    "sources": data.get("sources", []),
                }

                # Metrics badges
                badge_col1, badge_col2, badge_col3 = st.columns([2, 1, 1])
                with badge_col1:
                    st.caption(
                        f"🧠 **Provider**: `{meta['provider']}` (`{meta['model_name']}`)"
                    )
                with badge_col2:
                    st.caption(f"⚡ **Latency**: `{meta['latency_ms']:.1f}ms`")
                with badge_col3:
                    status_badge = (
                        "🟢 OK" if meta["status"] == "success" else "🟡 Guarded"
                    )
                    st.caption(f"Status: **{status_badge}**")

                if meta["tools_used"] or meta["sources"]:
                    with st.expander(
                        f"🔍 Inspect Tools ({len(meta['tools_used'])}) & Citations ({len(meta['sources'])})",
                        expanded=False,
                    ):
                        if meta["tools_used"]:
                            st.write(
                                f"**Allowlisted Tools Executed**: {', '.join([f'`{t}`' for t in meta['tools_used']])}"
                            )
                        if sources_list := meta["sources"]:
                            st.write(
                                f"**Data Sources Cited**: {', '.join([f'`{s}`' for s in sources_list])}"
                            )

                st.session_state["messages"].append(
                    {"role": "assistant", "content": answer, "metadata": meta}
                )
            else:
                err_msg = f"⚠️ Ops Copilot connection failed: {resp.get('error')}"
                st.error(err_msg)
                st.session_state["messages"].append(
                    {"role": "assistant", "content": err_msg, "metadata": None}
                )

        if quick_query:
            st.rerun()


# ---------------------------------------------------------------------------
# TAB 2: Live Forecasts & Map (Phase 8 Preview)
# ---------------------------------------------------------------------------

with tab_map:
    st.subheader("Live Demand & Corridor ETA Map")
    st.info(
        "🗺️ Phase 8 Interactive Choropleth Map & Zone Centroid Visualization — Coming in Phase 8."
    )
    st.markdown("""
        - **Vectorized Prediction**: Batch demand scoring for all 263 NYC TLC zones via `POST /predict/demand/batch`.
        - **Feast Online Redis Store**: Real-time feature enrichment with 15-minute pickup aggregates.
        - **PyDeck Geospatial Layer**: Hexagon & polygon layers colored by forecast density.
        """)


# ---------------------------------------------------------------------------
# TAB 3: Pipeline Observability (Phase 8 Preview)
# ---------------------------------------------------------------------------

with tab_pipeline:
    st.subheader("Pipeline & Model Observability")
    st.caption(
        "Live health state across batch ETL, streaming, and scheduled retraining flows."
    )

    pipe_info = get_pipeline_status(api_url)
    if pipe_info["success"]:
        p_data = pipe_info["data"]
        overall = p_data.get("status", "empty").upper()
        runs = p_data.get("latest_runs", [])
        st.metric("Orchestration Health", overall, f"{len(runs)} recent runs")

        if runs:
            st.markdown("### Recent Pipeline Runs (`warehouse.pipeline_runs`)")
            import pandas as pd

            df_runs = pd.DataFrame(runs)
            st.dataframe(df_runs, use_container_width=True)
        else:
            st.info(
                "No pipeline run records currently recorded in `warehouse.pipeline_runs`."
            )
    else:
        st.warning(f"Could not retrieve pipeline status: {pipe_info.get('error')}")
