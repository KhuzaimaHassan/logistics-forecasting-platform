"""Opt-in real Groq/tool diagnostics; never serialize credentials or exception text."""

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from src.agents.graph import _serialize_tool_for_llm, tool_execution_node
from src.agents.providers import GroqProvider
from src.agents.tools import TOOL_ALLOWLIST


def safe_provider_message(message):
    """Redact configured secrets and URLs before retaining a bounded API schema error."""
    value = str(message)
    for name, secret in os.environ.items():
        if secret and any(
            word in name.upper()
            for word in ("KEY", "TOKEN", "PASSWORD", "DATABASE_URL", "REDIS_URL")
        ):
            value = value.replace(secret, "[REDACTED]")
    value = re.sub(r"https?://\S+", "[URL REDACTED]", value)
    return value[:400]


def diagnose():
    result = {"generated_at": datetime.now(timezone.utc).isoformat()}
    # Third-party exceptions may contain connection strings. Record types/statuses only.
    logging.disable(logging.CRITICAL)
    try:
        response = GroqProvider().generate(
            messages=[
                {
                    "role": "user",
                    "content": "Summarize recent pipeline status using the pipeline status tool.",
                }
            ],
            tools=[_serialize_tool_for_llm(t) for t in TOOL_ALLOWLIST.values()],
        )
        result.update(provider=response.provider, model_name=response.model_name)
        result["calls"] = [
            {
                "tool": call["name"] if call["name"] in TOOL_ALLOWLIST else "unknown",
                "argument_keys": (
                    sorted(call["args"])
                    if isinstance(call["args"], dict)
                    else ["invalid_object"]
                ),
            }
            for call in response.tool_calls
        ]
        executed = tool_execution_node({"tool_calls": response.tool_calls})
        result["tools_used"] = executed["tools_used"]
        result["sources"] = executed["sources"]
        result["tool_statuses"] = [
            {
                "tool": item["tool"],
                "execution_status": item.get("status", "completed"),
                "dependency_status": item.get("result", {}).get("status"),
                "pipeline_run_count": item.get("result", {}).get("run_count"),
            }
            for item in executed["tool_results"]
        ]
        result["status"] = (
            "VERIFIED"
            if "query_pipeline_status" in executed["tools_used"]
            and all(
                row["execution_status"] == "completed"
                and row["dependency_status"] != "error"
                for row in result["tool_statuses"]
            )
            else "FAILED"
        )
    except Exception as exc:
        result.update(status="FAILED", error_type=type(exc).__name__)
        cause = exc.__cause__
        if cause is not None:
            result["cause_type"] = type(cause).__name__
            response = getattr(cause, "response", None)
            if response is not None:
                result["provider_http_status"] = response.status_code
                # API error codes/types are useful; unrestricted messages are not safe logs.
                try:
                    error = response.json().get("error", {})
                    result["provider_error_type"] = error.get("type")
                    result["provider_error_code"] = error.get("code")
                    result["provider_message"] = safe_provider_message(
                        error.get("message", "")
                    )
                except (ValueError, AttributeError):
                    pass
    finally:
        logging.disable(logging.NOTSET)
    try:
        from src.features.client import FeastOnlineClient
        from src.serving.cloud_policy import missing_feature_fields

        features = FeastOnlineClient().get_zone_demand_features([7, 74, 75, 166])
        result["online_feature_readback"] = [
            {
                "zone_id": row.zone_id,
                "missing_fields": missing_feature_fields(row),
                "observed_at": str(row.observed_at),
            }
            for row in features
        ]
    except Exception as exc:
        result["online_feature_error_type"] = type(exc).__name__
    return result


if __name__ == "__main__":
    evidence = diagnose()
    destination = Path("artifacts/agent-diagnostics.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    raise SystemExit(0 if evidence["status"] == "VERIFIED" else 1)
