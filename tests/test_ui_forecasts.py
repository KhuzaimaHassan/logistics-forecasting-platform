"""Exercise Streamlit reruns without external infrastructure or credentials."""

from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from ui.app import choose_agent_examples


def test_cloud_examples_only_offer_observed_entities():
    zones = [
        {"zone_id": 161, "has_observed_features": False},
        {"zone_id": 75, "has_observed_features": True},
    ]
    assert choose_agent_examples(True, zones, {"available_corridors": ["75_237"]}) == (
        75,
        "75_237",
    )
    assert choose_agent_examples(True, [], {}) == (None, None)
    assert choose_agent_examples(False, [], {}) == (161, "161_236")


def test_forecasts_survive_eta_rerun_and_disabled_agent_is_honest(monkeypatch):
    api = "https://api.example.test"
    monkeypatch.setenv("FASTAPI_URL", api)
    monkeypatch.delenv("AGENT_API_KEY", raising=False)
    timestamp = "2023-01-31T23:00:00+00:00"
    zones = [
        {
            "zone_id": z,
            "zone_name": str(z),
            "lat": 40.75,
            "lon": -73.95,
            "has_observed_features": True,
        }
        for z in [74, 75, 166]
    ]

    def response(body):
        result = Mock()
        result.status_code = 200
        result.json.return_value = body
        return result

    def get(url, **kwargs):
        bodies = {
            "/health": {"status": "ok", "models": {}},
            "/deployment/status": {
                "mode": "scheduled",
                "agent_enabled": False,
                "observations": {
                    "available_corridors": ["75_237"],
                    "feature_materialized_at": "2026-10-19T10:00:00+00:00",
                },
            },
            "/zones": {"zones": zones},
            "/pipeline/status": {"status": "healthy", "latest_runs": []},
            "/monitoring/reports": {"reports": [], "has_active_alerts": False},
            "/predict/eta": {
                "predicted_duration_minutes": 11.93,
                "status": "ok",
                "data_as_of": timestamp,
            },
        }
        assert url.startswith(api)
        return response(bodies[url.removeprefix(api)])

    payload = {
        "model_version": "1",
        "as_of": "2026-10-19T10:00:00+00:00",
        "predictions": [
            {
                "zone_id": z,
                "predicted_pickups": value,
                "data_as_of": timestamp,
                "horizon_minutes": 60,
                "status": "ok",
            }
            for z, value in [(74, 3.69), (75, 5.55), (166, 2.26)]
        ],
    }
    with (
        patch("requests.get", side_effect=get),
        patch("requests.post", return_value=response(payload)) as post,
    ):
        app = AppTest.from_file("ui/app.py", default_timeout=30).run()
        assert not app.exception
        assert any(button.label == "🚕 Zone 74 Features" for button in app.button)
        assert any(button.label == "⏱️ Corridor 75 → 237" for button in app.button)
        assert all(
            button.disabled for button in app.button if "Drift Alerts" in button.label
        )
        assert app.chat_input[0].disabled
        assert any("mock responses are disabled" in item.value for item in app.caption)
        next(
            button
            for button in app.button
            if button.label == "Predict next-hour demand"
        ).click().run()
        assert not app.exception
        assert app.dataframe[0].value.predicted_pickups.tolist() == [3.69, 5.55, 2.26]
        next(
            button for button in app.button if button.label == "Predict corridor ETA"
        ).click().run()
        assert not app.exception
        assert app.dataframe[0].value.predicted_pickups.tolist() == [3.69, 5.55, 2.26]
        assert any(metric.value == "11.9 minutes" for metric in app.metric)
        post.assert_called_once()
        # Changing the requested zone set invalidates its displayed forecast.
        app.multiselect[0].set_value([74]).run()
        assert not app.exception
        assert not app.dataframe
