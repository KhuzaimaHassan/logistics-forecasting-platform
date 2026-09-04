"""Live external feed polling producers for NYC traffic, MTA transit, and weather data.

Publishes streaming snapshots to Redpanda topics:
- traffic.snapshots: Live traffic speed and travel time by road segment (NYC Open Data Socrata).
- transit.positions: Transit vehicle positions, delay proxies, and congestion levels (MTA GTFS-RT).
- weather.snapshots: NYC meteorological observations (OpenWeatherMap).

Includes resilient network error handling, rate limiting/backoff, and graceful synthetic
fallback when third-party API credentials are not configured.
"""

import argparse
import logging
import random
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from kafka import KafkaProducer

from src.common.config import get_settings
from src.common.kafka_utils import (
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_WEATHER_SNAPSHOTS,
    ensure_topics_exist,
    get_kafka_producer,
)

logger = logging.getLogger(__name__)

# External Endpoint Constants
SOCRATA_TRAFFIC_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
MTA_SUBWAY_ALERTS_URL = (
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"
)
OPENWEATHERMAP_CURRENT_URL = "https://api.openweathermap.org/data/2.5/weather"

# Representative NYC Corridors for Synthetic Fallback
SYNTHETIC_TRAFFIC_CORRIDORS = [
    {
        "segment_id": "101",
        "link_name": "FDR Drive SB (63rd St to 34th St)",
        "borough": "Manhattan",
        "base_speed": 35.0,
    },
    {
        "segment_id": "102",
        "link_name": "FDR Drive NB (Houston St to 42nd St)",
        "borough": "Manhattan",
        "base_speed": 30.0,
    },
    {
        "segment_id": "103",
        "link_name": "West Side Highway NB (Battery Pl to 14th St)",
        "borough": "Manhattan",
        "base_speed": 28.0,
    },
    {
        "segment_id": "104",
        "link_name": "West Side Highway SB (57th St to Canal St)",
        "borough": "Manhattan",
        "base_speed": 25.0,
    },
    {
        "segment_id": "105",
        "link_name": "Brooklyn Bridge EB (Manhattan to Brooklyn)",
        "borough": "Manhattan",
        "base_speed": 22.0,
    },
    {
        "segment_id": "106",
        "link_name": "Manhattan Bridge WB (Brooklyn to Manhattan)",
        "borough": "Brooklyn",
        "base_speed": 24.0,
    },
    {
        "segment_id": "107",
        "link_name": "Queensboro Bridge Upper EB",
        "borough": "Queens",
        "base_speed": 26.0,
    },
    {
        "segment_id": "108",
        "link_name": "Queens Midtown Tunnel WB",
        "borough": "Queens",
        "base_speed": 20.0,
    },
    {
        "segment_id": "109",
        "link_name": "Lincoln Tunnel EB",
        "borough": "Manhattan",
        "base_speed": 18.0,
    },
    {
        "segment_id": "110",
        "link_name": "BQE NB (Atlantic Ave to Tillary St)",
        "borough": "Brooklyn",
        "base_speed": 32.0,
    },
]

# Representative MTA Subway Lines for Synthetic Fallback
SYNTHETIC_TRANSIT_ROUTES = [
    {
        "route_id": "1",
        "terminal": "South Ferry / Van Cortlandt Park",
        "stop_ids": ["120N", "124N", "128S", "137S"],
    },
    {
        "route_id": "2",
        "terminal": "Flatbush Ave / Wakefield",
        "stop_ids": ["220N", "225N", "230S", "235S"],
    },
    {
        "route_id": "4",
        "terminal": "Crown Heights / Woodlawn",
        "stop_ids": ["415N", "420N", "425S", "430S"],
    },
    {
        "route_id": "6",
        "terminal": "Brooklyn Bridge / Pelham Bay",
        "stop_ids": ["620N", "625N", "630S", "635S"],
    },
    {
        "route_id": "7",
        "terminal": "34 St-Hudson Yards / Flushing",
        "stop_ids": ["710N", "715N", "720S", "725S"],
    },
    {
        "route_id": "A",
        "terminal": "Inwood-207 St / Far Rockaway",
        "stop_ids": ["A15N", "A20N", "A25S", "A30S"],
    },
    {
        "route_id": "E",
        "terminal": "World Trade Center / Jamaica Center",
        "stop_ids": ["E05N", "E10N", "E15S", "E20S"],
    },
    {
        "route_id": "L",
        "terminal": "8 Ave / Canarsie-Rockaway Pkwy",
        "stop_ids": ["L01N", "L05N", "L10S", "L15S"],
    },
    {
        "route_id": "N",
        "terminal": "Coney Island / Astoria-Ditmars",
        "stop_ids": ["N05N", "N10N", "N15S", "N20S"],
    },
    {
        "route_id": "Q",
        "terminal": "Coney Island / 96 St-2 Ave",
        "stop_ids": ["Q01N", "Q05N", "Q10S", "Q15S"],
    },
]


class TrafficSpeedProducer:
    """Producer for NYC Open Data real-time traffic speed readings (Socrata API)."""

    def __init__(
        self,
        producer: Optional[KafkaProducer] = None,
        broker: Optional[str] = None,
        topic: str = TOPIC_TRAFFIC_SNAPSHOTS,
        app_token: Optional[str] = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.settings = get_settings()
        self.broker = broker or self.settings.redpanda_broker
        self.topic = topic
        self.app_token = (
            app_token if app_token is not None else self.settings.nyc_traffic_app_token
        )
        self.timeout_seconds = timeout_seconds
        self.producer = producer or get_kafka_producer(broker=self.broker)

    def poll_live(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch real-time traffic speed readings from NYC Open Data Socrata API."""
        headers = {}
        if self.app_token:
            headers["X-App-Token"] = self.app_token

        params = {
            "$limit": limit,
            "$order": "data_as_of DESC",
        }

        response = requests.get(
            SOCRATA_TRAFFIC_URL,
            headers=headers,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        records = response.json()

        snapshots = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for rec in records:
            speed_val = rec.get("speed")
            if speed_val is None:
                continue
            try:
                speed_mph = float(speed_val)
            except (ValueError, TypeError):
                continue

            speed_kmh = round(speed_mph * 1.60934, 2)
            travel_time_sec = int(float(rec.get("travel_time", 0)))
            segment_id = str(rec.get("id", rec.get("link_id", "unknown")))

            recorded_at_raw = rec.get("data_as_of")
            if recorded_at_raw:
                try:
                    recorded_at = datetime.fromisoformat(
                        recorded_at_raw.replace("Z", "+00:00")
                    ).isoformat()
                except Exception:
                    recorded_at = now_iso
            else:
                recorded_at = now_iso

            snapshots.append(
                {
                    "segment_id": segment_id,
                    "speed_mph": speed_mph,
                    "speed_kmh": speed_kmh,
                    "travel_time_seconds": travel_time_sec,
                    "borough": rec.get("borough"),
                    "link_name": rec.get("link_name"),
                    "recorded_at": recorded_at,
                    "source": "socrata_live",
                }
            )

        return snapshots

    def generate_synthetic(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate realistic synthetic traffic speed snapshots when live API is unavailable."""
        snapshots = []
        now_iso = datetime.now(timezone.utc).isoformat()
        selected = SYNTHETIC_TRAFFIC_CORRIDORS[:count]

        for item in selected:
            jitter = random.uniform(-6.0, 6.0)
            speed_mph = max(5.0, round(item["base_speed"] + jitter, 1))
            speed_kmh = round(speed_mph * 1.60934, 2)
            travel_time_sec = int(round((2.5 / speed_mph) * 3600))

            snapshots.append(
                {
                    "segment_id": item["segment_id"],
                    "speed_mph": speed_mph,
                    "speed_kmh": speed_kmh,
                    "travel_time_seconds": travel_time_sec,
                    "borough": item["borough"],
                    "link_name": item["link_name"],
                    "recorded_at": now_iso,
                    "source": "synthetic_fallback",
                }
            )

        return snapshots

    def poll_and_publish(
        self, force_synthetic: bool = False, limit: int = 50
    ) -> Dict[str, Any]:
        """Poll traffic data (or generate synthetic) and publish records to Redpanda."""
        source_label = "synthetic_fallback"
        records: List[Dict[str, Any]] = []

        if not force_synthetic:
            try:
                records = self.poll_live(limit=limit)
                if records:
                    source_label = "socrata_live"
                else:
                    logger.warning(
                        "Socrata API returned 0 records. Falling back to synthetic traffic snapshots."
                    )
                    records = self.generate_synthetic(count=10)
            except Exception as exc:
                logger.warning(
                    "Socrata live traffic fetch failed (%s). Falling back to synthetic traffic snapshots.",
                    exc,
                )
                records = self.generate_synthetic(count=10)
        else:
            records = self.generate_synthetic(count=10)

        published_count = 0
        error_count = 0

        for snapshot in records:
            key_bytes = str(snapshot["segment_id"]).encode("utf-8")
            try:
                self.producer.send(
                    topic=self.topic,
                    key=key_bytes,
                    value=snapshot,
                )
                published_count += 1
            except Exception as exc:
                error_count += 1
                logger.error(
                    "Failed to publish traffic snapshot %s: %s",
                    snapshot.get("segment_id"),
                    exc,
                )

        self.producer.flush()
        return {
            "feed": "traffic",
            "records_polled": len(records),
            "records_published": published_count,
            "errors": error_count,
            "source": source_label,
        }


class TransitPositionsProducer:
    """Producer for MTA GTFS-RT subway transit positions and delay proxies."""

    def __init__(
        self,
        producer: Optional[KafkaProducer] = None,
        broker: Optional[str] = None,
        topic: str = TOPIC_TRANSIT_POSITIONS,
        api_key: Optional[str] = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.settings = get_settings()
        self.broker = broker or self.settings.redpanda_broker
        self.topic = topic
        self.api_key = api_key if api_key is not None else self.settings.mta_api_key
        self.timeout_seconds = timeout_seconds
        self.producer = producer or get_kafka_producer(broker=self.broker)

    def poll_live(self) -> List[Dict[str, Any]]:
        """Fetch live MTA transit status / alerts feeds."""
        if not self.api_key:
            raise ValueError("MTA_API_KEY is not configured.")

        headers = {"x-api-key": self.api_key}
        response = requests.get(
            MTA_SUBWAY_ALERTS_URL,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()

        snapshots = []
        now_iso = datetime.now(timezone.utc).isoformat()
        entities = data.get("entity", [])

        for idx, entity in enumerate(entities):
            alert = entity.get("alert", {})
            informed_entities = alert.get("informed_entity", [{}])
            route_id = (
                informed_entities[0].get("route_id", "NYCT")
                if informed_entities
                else "NYCT"
            )
            header_text = (
                alert.get("header_text", {})
                .get("translation", [{}])[0]
                .get("text", "Transit Alert")
            )

            delay_seconds = 180 if "delay" in header_text.lower() else 0
            congestion = (
                "HEAVY_DELAY"
                if delay_seconds > 120
                else "MODERATE" if delay_seconds > 0 else "NORMAL"
            )

            snapshots.append(
                {
                    "route_id": route_id,
                    "trip_id": f"mta-alert-{entity.get('id', idx)}",
                    "vehicle_id": f"subway-{route_id}",
                    "current_status": (
                        "SERVICE_ALERT" if delay_seconds > 0 else "NORMAL"
                    ),
                    "stop_id": (
                        informed_entities[0].get("stop_id")
                        if informed_entities
                        else None
                    ),
                    "delay_seconds": delay_seconds,
                    "congestion_level": congestion,
                    "recorded_at": now_iso,
                    "source": "mta_gtfs_live",
                }
            )

        return snapshots

    def generate_synthetic(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate realistic synthetic transit line snapshots when live API is unavailable."""
        snapshots = []
        now_iso = datetime.now(timezone.utc).isoformat()
        selected = SYNTHETIC_TRANSIT_ROUTES[:count]

        for item in selected:
            has_delay = random.random() < 0.20
            delay_seconds = random.randint(60, 360) if has_delay else 0
            congestion = (
                "HEAVY_DELAY"
                if delay_seconds > 180
                else "MODERATE" if delay_seconds > 0 else "NORMAL"
            )
            stop_id = random.choice(item["stop_ids"])
            status = random.choice(["IN_TRANSIT_TO", "STOPPED_AT", "INCOMING_AT"])

            snapshots.append(
                {
                    "route_id": item["route_id"],
                    "trip_id": f"trip-{item['route_id']}-{random.randint(1000, 9999)}",
                    "vehicle_id": f"train-{item['route_id']}-{random.randint(100, 999)}",
                    "current_status": status,
                    "stop_id": stop_id,
                    "delay_seconds": delay_seconds,
                    "congestion_level": congestion,
                    "recorded_at": now_iso,
                    "source": "synthetic_fallback",
                }
            )

        return snapshots

    def poll_and_publish(self, force_synthetic: bool = False) -> Dict[str, Any]:
        """Poll transit data (or generate synthetic) and publish records to Redpanda."""
        source_label = "synthetic_fallback"
        records: List[Dict[str, Any]] = []

        if not force_synthetic and self.api_key:
            try:
                records = self.poll_live()
                if records:
                    source_label = "mta_gtfs_live"
                else:
                    logger.info(
                        "MTA API returned empty entity list. Falling back to synthetic transit snapshots."
                    )
                    records = self.generate_synthetic(count=10)
            except Exception as exc:
                logger.warning(
                    "MTA live transit fetch failed (%s). Falling back to synthetic transit snapshots.",
                    exc,
                )
                records = self.generate_synthetic(count=10)
        else:
            if not self.api_key and not force_synthetic:
                logger.info(
                    "MTA_API_KEY is not set. Using representative synthetic transit snapshots."
                )
            records = self.generate_synthetic(count=10)

        published_count = 0
        error_count = 0

        for snapshot in records:
            key_bytes = str(snapshot["route_id"]).encode("utf-8")
            try:
                self.producer.send(
                    topic=self.topic,
                    key=key_bytes,
                    value=snapshot,
                )
                published_count += 1
            except Exception as exc:
                error_count += 1
                logger.error(
                    "Failed to publish transit snapshot %s: %s",
                    snapshot.get("route_id"),
                    exc,
                )

        self.producer.flush()
        return {
            "feed": "transit",
            "records_polled": len(records),
            "records_published": published_count,
            "errors": error_count,
            "source": source_label,
        }


class WeatherSnapshotsProducer:
    """Producer for NYC meteorological observations (OpenWeatherMap API)."""

    def __init__(
        self,
        producer: Optional[KafkaProducer] = None,
        broker: Optional[str] = None,
        topic: str = TOPIC_WEATHER_SNAPSHOTS,
        api_key: Optional[str] = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.settings = get_settings()
        self.broker = broker or self.settings.redpanda_broker
        self.topic = topic
        self.api_key = (
            api_key if api_key is not None else self.settings.openweathermap_api_key
        )
        self.timeout_seconds = timeout_seconds
        self.producer = producer or get_kafka_producer(broker=self.broker)

    def poll_live(self) -> Dict[str, Any]:
        """Fetch current NYC weather observation from OpenWeatherMap."""
        if not self.api_key:
            raise ValueError("OPENWEATHERMAP_API_KEY is not configured.")

        params = {
            "lat": 40.7128,
            "lon": -74.0060,
            "appid": self.api_key,
            "units": "metric",
        }

        response = requests.get(
            OPENWEATHERMAP_CURRENT_URL,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()

        main_data = data.get("main", {})
        weather_list = data.get("weather", [{}])
        weather_item = weather_list[0] if weather_list else {}
        condition = weather_item.get("main", "Clear")

        rain_info = data.get("rain", {})
        snow_info = data.get("snow", {})
        precip_mm = float(rain_info.get("1h", snow_info.get("1h", 0.0)))
        is_precip = precip_mm > 0 or condition.lower() in (
            "rain",
            "snow",
            "drizzle",
            "thunderstorm",
        )

        temp_c = float(main_data.get("temp", 20.0))
        temp_f = round(temp_c * 9 / 5 + 32, 2)
        wind_speed_kmh = round(float(data.get("wind", {}).get("speed", 0.0)) * 3.6, 2)

        return {
            "location": "New York, NY",
            "temp_c": round(temp_c, 2),
            "temp_f": temp_f,
            "humidity_pct": int(main_data.get("humidity", 50)),
            "condition": condition,
            "is_precipitating": is_precip,
            "precipitation_mm_1h": round(precip_mm, 2),
            "wind_speed_kmh": wind_speed_kmh,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "source": "openweathermap_live",
        }

    def generate_synthetic(self) -> Dict[str, Any]:
        """Generate realistic synthetic NYC weather snapshot when live API is unavailable."""
        conditions = ["Clear", "Clouds", "Rain", "Drizzle"]
        chosen_cond = random.choice(conditions)
        is_precip = chosen_cond in ("Rain", "Drizzle")
        precip_mm = round(random.uniform(0.5, 4.0), 2) if is_precip else 0.0

        temp_c = round(random.uniform(14.0, 26.0), 1)
        temp_f = round(temp_c * 9 / 5 + 32, 2)

        return {
            "location": "New York, NY",
            "temp_c": temp_c,
            "temp_f": temp_f,
            "humidity_pct": random.randint(45, 80),
            "condition": chosen_cond,
            "is_precipitating": is_precip,
            "precipitation_mm_1h": precip_mm,
            "wind_speed_kmh": round(random.uniform(8.0, 25.0), 1),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "source": "synthetic_fallback",
        }

    def poll_and_publish(self, force_synthetic: bool = False) -> Dict[str, Any]:
        """Poll weather data (or generate synthetic) and publish record to Redpanda."""
        source_label = "synthetic_fallback"
        snapshot: Dict[str, Any] = {}

        if not force_synthetic and self.api_key:
            try:
                snapshot = self.poll_live()
                source_label = "openweathermap_live"
            except Exception as exc:
                logger.warning(
                    "OpenWeatherMap live fetch failed (%s). Falling back to synthetic weather snapshot.",
                    exc,
                )
                snapshot = self.generate_synthetic()
        else:
            if not self.api_key and not force_synthetic:
                logger.info(
                    "OPENWEATHERMAP_API_KEY is not set. Using representative synthetic weather snapshot."
                )
            snapshot = self.generate_synthetic()

        error_count = 0
        published_count = 0
        key_bytes = b"NYC"

        try:
            self.producer.send(
                topic=self.topic,
                key=key_bytes,
                value=snapshot,
            )
            published_count = 1
        except Exception as exc:
            error_count = 1
            logger.error("Failed to publish weather snapshot: %s", exc)

        self.producer.flush()
        return {
            "feed": "weather",
            "records_polled": 1,
            "records_published": published_count,
            "errors": error_count,
            "source": source_label,
            "payload": snapshot,
        }


class LiveFeedPollerManager:
    """Orchestrator for concurrent live feed polling and Redpanda streaming."""

    def __init__(
        self,
        broker: Optional[str] = None,
        traffic_producer: Optional[TrafficSpeedProducer] = None,
        transit_producer: Optional[TransitPositionsProducer] = None,
        weather_producer: Optional[WeatherSnapshotsProducer] = None,
    ) -> None:
        self.settings = get_settings()
        self.broker = broker or self.settings.redpanda_broker
        self.traffic_producer = traffic_producer or TrafficSpeedProducer(
            broker=self.broker
        )
        self.transit_producer = transit_producer or TransitPositionsProducer(
            broker=self.broker
        )
        self.weather_producer = weather_producer or WeatherSnapshotsProducer(
            broker=self.broker
        )
        self._running = True

    def _setup_signal_handlers(self) -> None:
        def _handle_stop(sig, frame):
            logger.info("Received stop signal %s, stopping live feed pollers...", sig)
            self._running = False

        signal.signal(signal.SIGINT, _handle_stop)
        signal.signal(signal.SIGTERM, _handle_stop)

    def poll_all_once(self, force_synthetic: bool = False) -> Dict[str, Any]:
        """Execute a single poll and publish cycle across all three feeds."""
        logger.info(
            "Starting single-pass poll for traffic, transit, and weather feeds..."
        )
        traffic_res = self.traffic_producer.poll_and_publish(
            force_synthetic=force_synthetic
        )
        transit_res = self.transit_producer.poll_and_publish(
            force_synthetic=force_synthetic
        )
        weather_res = self.weather_producer.poll_and_publish(
            force_synthetic=force_synthetic
        )

        summary = {
            "traffic": traffic_res,
            "transit": transit_res,
            "weather": weather_res,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(
            "Poll cycle complete: traffic=%d (%s), transit=%d (%s), weather=%d (%s)",
            traffic_res["records_published"],
            traffic_res["source"],
            transit_res["records_published"],
            transit_res["source"],
            weather_res["records_published"],
            weather_res["source"],
        )
        return summary

    def run_loop(
        self,
        traffic_interval_sec: float = 30.0,
        transit_interval_sec: float = 30.0,
        weather_interval_sec: float = 300.0,
        force_synthetic: bool = False,
    ) -> None:
        """Run continuous scheduled polling loop until interrupted."""
        self._setup_signal_handlers()
        logger.info(
            "Running live feed poller daemon (traffic=%.1fs, transit=%.1fs, weather=%.1fs)...",
            traffic_interval_sec,
            transit_interval_sec,
            weather_interval_sec,
        )

        last_traffic = 0.0
        last_transit = 0.0
        last_weather = 0.0

        while self._running:
            now = time.time()

            if now - last_traffic >= traffic_interval_sec:
                try:
                    res = self.traffic_producer.poll_and_publish(
                        force_synthetic=force_synthetic
                    )
                    logger.debug("Polled traffic: %s", res)
                    last_traffic = now
                except Exception as exc:
                    logger.error("Error in traffic polling loop: %s", exc)

            if now - last_transit >= transit_interval_sec:
                try:
                    res = self.transit_producer.poll_and_publish(
                        force_synthetic=force_synthetic
                    )
                    logger.debug("Polled transit: %s", res)
                    last_transit = now
                except Exception as exc:
                    logger.error("Error in transit polling loop: %s", exc)

            if now - last_weather >= weather_interval_sec:
                try:
                    res = self.weather_producer.poll_and_publish(
                        force_synthetic=force_synthetic
                    )
                    logger.debug("Polled weather: %s", res)
                    last_weather = now
                except Exception as exc:
                    logger.error("Error in weather polling loop: %s", exc)

            time.sleep(1.0)

        logger.info("Live feed poller loop shut down cleanly.")

    def close(self) -> None:
        """Close producers and flush remaining messages."""
        self.traffic_producer.producer.close()
        self.transit_producer.producer.close()
        self.weather_producer.producer.close()


def main() -> None:
    """CLI entrypoint for running live feed producers."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        description="Live external feed polling producer for Redpanda."
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=None,
        help="Redpanda broker address (e.g. localhost:9092)",
    )
    parser.add_argument(
        "--once", action="store_true", help="Run a single polling pass and exit."
    )
    parser.add_argument(
        "--force-synthetic",
        action="store_true",
        help="Force synthetic fallback generation.",
    )
    parser.add_argument(
        "--traffic-interval",
        type=float,
        default=30.0,
        help="Traffic polling interval in seconds.",
    )
    parser.add_argument(
        "--transit-interval",
        type=float,
        default=30.0,
        help="Transit polling interval in seconds.",
    )
    parser.add_argument(
        "--weather-interval",
        type=float,
        default=300.0,
        help="Weather polling interval in seconds.",
    )

    args = parser.parse_args()

    ensure_topics_exist(broker=args.broker)

    manager = LiveFeedPollerManager(broker=args.broker)

    if args.once:
        summary = manager.poll_all_once(force_synthetic=args.force_synthetic)
        print("Summary:", summary)
    else:
        manager.run_loop(
            traffic_interval_sec=args.traffic_interval,
            transit_interval_sec=args.transit_interval,
            weather_interval_sec=args.weather_interval,
            force_synthetic=args.force_synthetic,
        )

    manager.close()


if __name__ == "__main__":
    main()
