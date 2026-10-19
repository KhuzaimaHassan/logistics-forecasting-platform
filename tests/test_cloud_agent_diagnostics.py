from unittest.mock import patch

import requests

from scripts.diagnose_cloud_agent import diagnose
from src.agents.providers import ProviderError


def test_provider_diagnostics_never_echo_exception_or_error_message():
    response = requests.Response()
    response.status_code = 400
    response._content = b'{"error":{"type":"invalid_request_error","code":"tool_use_failed","message":"private-token"}}'
    cause = requests.HTTPError("private-token", response=response)
    error = ProviderError("private-token")
    error.__cause__ = cause
    with patch("scripts.diagnose_cloud_agent.GroqProvider") as provider:
        provider.return_value.generate.side_effect = error
        result = diagnose()
    assert result["status"] == "FAILED"
    assert result["provider_http_status"] == 400
    assert result["provider_error_code"] == "tool_use_failed"
    assert "private-token" not in str(result)
