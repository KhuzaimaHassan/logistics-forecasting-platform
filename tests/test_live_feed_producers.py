"""Unit tests for live external feed polling producers (Traffic, Transit, Weather)."""

from unittest.mock import MagicMock, patch

from src.common.kafka_utils import (
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_WEATHER_SNAPSHOTS,
)
from src.extract.live_feed_producers import (
    LiveFeedPollerManager,
    TrafficSpeedProducer,
    TransitPositionsProducer,
    WeatherSnapshotsProducer,
)


def test_traffic_speed_producer_poll_live_mocked():
    """Verify TrafficSpeedProducer parses live Socrata JSON response correctly."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {
            "id": "101",
            "speed": "32.5",
            "travel_time": "180",
            "borough": "Manhattan",
            "link_name": "FDR Drive SB",
            "data_as_of": "2026-09-04T10:00:00.000",
        },
        {
            "id": "102",
            "speed": "24.0",
            "travel_time": "240",
            "borough": "Brooklyn",
            "link_name": "BQE NB",
            "data_as_of": "2026-09-04T10:00:00.000",
        },
    ]
    mock_response.raise_for_status = MagicMock()

    mock_producer = MagicMock()
    producer = TrafficSpeedProducer(producer=mock_producer, app_token="test_token")

    with patch("requests.get", return_value=mock_response) as mock_get:
        snapshots = producer.poll_live(limit=10)
        mock_get.assert_called_once()
        assert len(snapshots) == 2

        s1 = snapshots[0]
        assert s1["segment_id"] == "101"
        assert s1["speed_mph"] == 32.5
        assert s1["speed_kmh"] == round(32.5 * 1.60934, 2)
        assert s1["travel_time_seconds"] == 180
        assert s1["borough"] == "Manhattan"
        assert s1["link_name"] == "FDR Drive SB"
        assert s1["source"] == "socrata_live"


def test_traffic_speed_producer_synthetic_fallback():
    """Verify TrafficSpeedProducer generates synthetic corridors on API error."""
    mock_producer = MagicMock()
    producer = TrafficSpeedProducer(producer=mock_producer)

    with patch("requests.get", side_effect=Exception("Network Timeout")):
        res = producer.poll_and_publish(force_synthetic=False, limit=5)
        assert res["feed"] == "traffic"
        assert res["records_published"] == 10
        assert res["source"] == "synthetic_fallback"
        assert res["errors"] == 0
        assert mock_producer.send.call_count == 10

        call_args = mock_producer.send.call_args_list[0]
        assert call_args.kwargs["topic"] == TOPIC_TRAFFIC_SNAPSHOTS
        val = call_args.kwargs["value"]
        assert val["source"] == "synthetic_fallback"
        assert "speed_kmh" in val
        assert "travel_time_seconds" in val


def test_transit_positions_producer_poll_live_mocked():
    """Verify TransitPositionsProducer parses MTA subway alerts and computes delays."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entity": [
            {
                "id": "alert-001",
                "alert": {
                    "informed_entity": [{"route_id": "A", "stop_id": "A15N"}],
                    "header_text": {
                        "translation": [{"text": "Severe delays on the A train"}]
                    },
                },
            },
            {
                "id": "alert-002",
                "alert": {
                    "informed_entity": [{"route_id": "7", "stop_id": "710S"}],
                    "header_text": {
                        "translation": [{"text": "Normal weekend schedule"}]
                    },
                },
            },
        ]
    }
    mock_response.raise_for_status = MagicMock()

    mock_producer = MagicMock()
    producer = TransitPositionsProducer(producer=mock_producer, api_key="test_mta_key")

    with patch("requests.get", return_value=mock_response):
        snapshots = producer.poll_live()
        assert len(snapshots) == 2

        s_delayed = snapshots[0]
        assert s_delayed["route_id"] == "A"
        assert s_delayed["delay_seconds"] == 180
        assert s_delayed["congestion_level"] == "HEAVY_DELAY"
        assert s_delayed["current_status"] == "SERVICE_ALERT"
        assert s_delayed["source"] == "mta_gtfs_live"

        s_normal = snapshots[1]
        assert s_normal["route_id"] == "7"
        assert s_normal["delay_seconds"] == 0
        assert s_normal["congestion_level"] == "NORMAL"
        assert s_normal["current_status"] == "NORMAL"


def test_transit_positions_producer_synthetic_fallback():
    """Verify TransitPositionsProducer generates synthetic subway line status when no key is set."""
    mock_producer = MagicMock()
    producer = TransitPositionsProducer(producer=mock_producer, api_key=None)

    res = producer.poll_and_publish(force_synthetic=False)
    assert res["feed"] == "transit"
    assert res["records_published"] == 10
    assert res["source"] == "synthetic_fallback"
    assert res["errors"] == 0
    assert mock_producer.send.call_count == 10

    call_args = mock_producer.send.call_args_list[0]
    assert call_args.kwargs["topic"] == TOPIC_TRANSIT_POSITIONS
    val = call_args.kwargs["value"]
    assert "route_id" in val
    assert "congestion_level" in val
    assert val["source"] == "synthetic_fallback"


def test_weather_snapshots_producer_poll_live_mocked():
    """Verify WeatherSnapshotsProducer converts temp and parses precipitation flags."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "main": {
            "temp": 15.5,
            "humidity": 78,
        },
        "weather": [
            {
                "main": "Rain",
                "description": "moderate rain",
            }
        ],
        "rain": {
            "1h": 2.4,
        },
        "wind": {
            "speed": 5.0,
        },
    }
    mock_response.raise_for_status = MagicMock()

    mock_producer = MagicMock()
    producer = WeatherSnapshotsProducer(
        producer=mock_producer, api_key="test_weather_key"
    )

    with patch("requests.get", return_value=mock_response):
        snapshot = producer.poll_live()
        assert snapshot["location"] == "New York, NY"
        assert snapshot["temp_c"] == 15.5
        assert snapshot["temp_f"] == round(15.5 * 9 / 5 + 32, 2)
        assert snapshot["humidity_pct"] == 78
        assert snapshot["condition"] == "Rain"
        assert snapshot["is_precipitating"] is True
        assert snapshot["precipitation_mm_1h"] == 2.4
        assert snapshot["wind_speed_kmh"] == round(5.0 * 3.6, 2)
        assert snapshot["source"] == "openweathermap_live"


def test_weather_snapshots_producer_synthetic_fallback():
    """Verify WeatherSnapshotsProducer fallback publishes synthetic weather snapshot."""
    mock_producer = MagicMock()
    producer = WeatherSnapshotsProducer(producer=mock_producer, api_key=None)

    res = producer.poll_and_publish(force_synthetic=False)
    assert res["feed"] == "weather"
    assert res["records_published"] == 1
    assert res["source"] == "synthetic_fallback"
    assert res["errors"] == 0
    assert mock_producer.send.call_count == 1

    call_args = mock_producer.send.call_args_list[0]
    assert call_args.kwargs["topic"] == TOPIC_WEATHER_SNAPSHOTS
    assert call_args.kwargs["key"] == b"NYC"
    val = call_args.kwargs["value"]
    assert val["location"] == "New York, NY"
    assert "temp_c" in val
    assert "condition" in val


def test_live_feed_manager_poll_all_once():
    """Verify LiveFeedPollerManager runs single-pass poll across all three feeds."""
    mock_traffic = MagicMock()
    mock_traffic.poll_and_publish.return_value = {
        "feed": "traffic",
        "records_published": 10,
        "source": "synthetic_fallback",
    }

    mock_transit = MagicMock()
    mock_transit.poll_and_publish.return_value = {
        "feed": "transit",
        "records_published": 10,
        "source": "synthetic_fallback",
    }

    mock_weather = MagicMock()
    mock_weather.poll_and_publish.return_value = {
        "feed": "weather",
        "records_published": 1,
        "source": "synthetic_fallback",
    }

    manager = LiveFeedPollerManager(
        traffic_producer=mock_traffic,
        transit_producer=mock_transit,
        weather_producer=mock_weather,
    )

    summary = manager.poll_all_once(force_synthetic=True)
    assert "traffic" in summary
    assert "transit" in summary
    assert "weather" in summary
    assert summary["traffic"]["records_published"] == 10
    assert summary["transit"]["records_published"] == 10
    assert summary["weather"]["records_published"] == 1

    mock_traffic.poll_and_publish.assert_called_once_with(force_synthetic=True)
    mock_transit.poll_and_publish.assert_called_once_with(force_synthetic=True)
    mock_weather.poll_and_publish.assert_called_once_with(force_synthetic=True)
