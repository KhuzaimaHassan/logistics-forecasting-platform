"""Unit tests for Streamlit UI monitoring helpers.

Tests client requests against FastAPI endpoints:
- get_monitoring_reports: retrieval, filtering, error handling
- get_monitoring_report_html: retrieval, 404 handling, error handling
"""

from unittest.mock import MagicMock, patch

import requests

from ui.app import get_monitoring_report_html, get_monitoring_reports


def test_get_monitoring_reports_success():
    """Verify get_monitoring_reports properly calls endpoint and parses response."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "status": "success",
        "count": 1,
        "has_active_alerts": False,
        "reports": [
            {
                "report_id": "rep_123",
                "report_type": "data_drift",
                "generated_at": "2026-09-22T10:00:00Z",
                "drift_detected": False,
                "retrain_recommended": False,
                "alert_severity": "INFO",
                "drift_share": 0.1,
                "number_of_drifted_columns": 1,
                "number_of_columns": 10,
                "drifted_features": ["pickup_count_last_1h"],
                "file_path": "artifacts/monitoring_reports/rep_123.html",
                "summary_json": {},
            }
        ],
        "retrieved_at": "2026-09-22T10:05:00Z",
    }

    with patch("requests.get", return_value=mock_resp) as mock_get:
        result = get_monitoring_reports(
            base_url="http://localhost:8000", report_type="data_drift", limit=10
        )
        assert result["success"] is True
        assert result["data"]["count"] == 1
        assert result["data"]["reports"][0]["report_id"] == "rep_123"

        mock_get.assert_called_once_with(
            "http://localhost:8000/monitoring/reports",
            params={"limit": 10, "report_type": "data_drift"},
            timeout=5.0,
        )


def test_get_monitoring_reports_http_error():
    """Verify get_monitoring_reports handles non-200 responses gracefully."""
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"

    with patch("requests.get", return_value=mock_resp):
        result = get_monitoring_reports(base_url="http://localhost:8000")
        assert result["success"] is False
        assert "500" in result["error"]


def test_get_monitoring_reports_timeout():
    """Verify get_monitoring_reports handles network timeout gracefully."""
    with patch(
        "requests.get", side_effect=requests.exceptions.Timeout("Connection timed out")
    ):
        result = get_monitoring_reports(base_url="http://localhost:8000")
        assert result["success"] is False
        assert (
            "timed out" in result["error"].lower()
            or "timeout" in result["error"].lower()
        )


def test_get_monitoring_report_html_success():
    """Verify get_monitoring_report_html returns raw HTML string."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "<html><body>Evidently Report</body></html>"

    with patch("requests.get", return_value=mock_resp) as mock_get:
        html = get_monitoring_report_html(
            base_url="http://localhost:8000", report_id="rep_abc_789"
        )
        assert html is not None
        assert "Evidently Report" in html
        mock_get.assert_called_once_with(
            "http://localhost:8000/monitoring/reports/rep_abc_789/html",
            timeout=10.0,
        )


def test_get_monitoring_report_html_404():
    """Verify get_monitoring_report_html handles missing report gracefully returning None."""
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.text = "Report not found"

    with patch("requests.get", return_value=mock_resp):
        html = get_monitoring_report_html(
            base_url="http://localhost:8000", report_id="missing_rep_404"
        )
        assert html is None
