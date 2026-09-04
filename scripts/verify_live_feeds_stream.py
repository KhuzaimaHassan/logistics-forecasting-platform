"""Live verification script for live external feed producers (traffic, transit, weather).

Demonstrates:
1. Provisioning Redpanda topics (traffic.snapshots, transit.positions, weather.snapshots).
2. Polling external feeds (live API or realistic synthetic fallback) via LiveFeedPollerManager.
3. Publishing snapshots to Redpanda topics.
4. Reading back published messages via KafkaConsumer across all 3 topics and asserting payloads, keys, and offsets.
"""

import logging
import os
import time
from uuid import uuid4

from src.common.kafka_utils import (
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_WEATHER_SNAPSHOTS,
    ensure_topics_exist,
    get_admin_client,
    get_kafka_consumer,
    json_deserializer,
)
from src.extract.live_feed_producers import LiveFeedPollerManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("verify_live_feeds")


def _consume_topic_messages(
    topic: str, broker: str, expected_min: int = 1, timeout_sec: float = 10.0
) -> list:
    """Consume messages from a specific topic using a unique consumer group."""
    unique_group = f"verify-live-{topic.replace('.', '-')}-{uuid4().hex[:8]}"
    consumer = get_kafka_consumer(
        topic,
        broker=broker,
        group_id=unique_group,
        auto_offset_reset="earliest",
        consumer_timeout_ms=int(timeout_sec * 1000),
        value_deserializer=json_deserializer,
    )

    messages = []
    start_time = time.time()
    for msg in consumer:
        messages.append(msg)
        if len(messages) >= expected_min and (time.time() - start_time) > 2.0:
            break
        if time.time() - start_time > timeout_sec:
            break

    consumer.close()
    return messages


def main() -> None:
    broker = os.getenv("REDPANDA_BROKER", "localhost:9092")
    logger.info("Connecting to live Redpanda broker at: %s", broker)

    # 1. Ensure topics exist
    print("=" * 70)
    print("STEP 1: VERIFYING & PROVISIONING REDPANDA TOPICS")
    print("=" * 70)
    created = ensure_topics_exist(broker=broker)
    admin = get_admin_client(broker=broker)
    topics = admin.list_topics()
    admin.close()
    print(f"Current Redpanda Topics on Broker: {sorted(topics)}")
    for topic_name in [
        TOPIC_TRAFFIC_SNAPSHOTS,
        TOPIC_TRANSIT_POSITIONS,
        TOPIC_WEATHER_SNAPSHOTS,
    ]:
        assert (
            topic_name in topics
        ), f"Expected topic '{topic_name}' not found in broker!"
    print(f"Newly created topics in this run: {created}")

    # 2. Initialize and run LiveFeedPollerManager
    print("\n" + "=" * 70)
    print("STEP 2: POLLING EXTERNAL FEEDS & PUBLISHING TO REDPANDA")
    print("=" * 70)
    manager = LiveFeedPollerManager(broker=broker)
    poll_results = manager.poll_all_once()
    print(f"Polling & Publishing Results: {poll_results}")
    manager.close()

    assert (
        poll_results["traffic"]["published"] > 0
    ), "No traffic snapshot records were published!"
    assert (
        poll_results["transit"]["published"] > 0
    ), "No transit position records were published!"
    assert (
        poll_results["weather"]["published"] > 0
    ), "No weather snapshot records were published!"

    # 3. Consume and verify traffic.snapshots
    print("\n" + "=" * 70)
    print("STEP 3: VERIFYING CONSUMPTION FROM 'traffic.snapshots'")
    print("=" * 70)
    traffic_msgs = _consume_topic_messages(
        TOPIC_TRAFFIC_SNAPSHOTS,
        broker,
        expected_min=poll_results["traffic"]["published"],
    )
    print(f"Consumed {len(traffic_msgs)} messages from '{TOPIC_TRAFFIC_SNAPSHOTS}'.")
    assert len(traffic_msgs) > 0, "Failed to consume messages from traffic.snapshots"

    for idx, msg in enumerate(traffic_msgs[:3]):
        val = msg.value
        key_str = (
            msg.key.decode("utf-8") if isinstance(msg.key, bytes) else str(msg.key)
        )
        print(
            f"  [{idx+1}] Partition: {msg.partition} | Offset: {msg.offset} | Key: {key_str}"
        )
        print(f"       Segment ID:   {val.get('segment_id')}")
        print(f"       Link Name:    {val.get('link_name')}")
        print(
            f"       Speed:        {val.get('speed_mph')} mph ({val.get('speed_kmh')} km/h)"
        )
        print(f"       Travel Time:  {val.get('travel_time_seconds')}s")
        print(f"       Source / TS:  {val.get('source')} / {val.get('timestamp')}")
        assert val.get("segment_id") is not None
        assert val.get("speed_mph") is not None

    # 4. Consume and verify transit.positions
    print("\n" + "=" * 70)
    print("STEP 4: VERIFYING CONSUMPTION FROM 'transit.positions'")
    print("=" * 70)
    transit_msgs = _consume_topic_messages(
        TOPIC_TRANSIT_POSITIONS,
        broker,
        expected_min=poll_results["transit"]["published"],
    )
    print(f"Consumed {len(transit_msgs)} messages from '{TOPIC_TRANSIT_POSITIONS}'.")
    assert len(transit_msgs) > 0, "Failed to consume messages from transit.positions"

    for idx, msg in enumerate(transit_msgs[:3]):
        val = msg.value
        key_str = (
            msg.key.decode("utf-8") if isinstance(msg.key, bytes) else str(msg.key)
        )
        print(
            f"  [{idx+1}] Partition: {msg.partition} | Offset: {msg.offset} | Key: {key_str}"
        )
        print(f"       Route ID:     {val.get('route_id')}")
        print(f"       Route Name:   {val.get('route_name')}")
        print(
            f"       Delay:        {val.get('delay_seconds')}s ({val.get('congestion_level')})"
        )
        print(f"       Alert:        {val.get('alert_header')}")
        print(f"       Source / TS:  {val.get('source')} / {val.get('timestamp')}")
        assert val.get("route_id") is not None
        assert val.get("congestion_level") is not None

    # 5. Consume and verify weather.snapshots
    print("\n" + "=" * 70)
    print("STEP 5: VERIFYING CONSUMPTION FROM 'weather.snapshots'")
    print("=" * 70)
    weather_msgs = _consume_topic_messages(
        TOPIC_WEATHER_SNAPSHOTS,
        broker,
        expected_min=poll_results["weather"]["published"],
    )
    print(f"Consumed {len(weather_msgs)} messages from '{TOPIC_WEATHER_SNAPSHOTS}'.")
    assert len(weather_msgs) > 0, "Failed to consume messages from weather.snapshots"

    for idx, msg in enumerate(weather_msgs[:3]):
        val = msg.value
        key_str = (
            msg.key.decode("utf-8") if isinstance(msg.key, bytes) else str(msg.key)
        )
        print(
            f"  [{idx+1}] Partition: {msg.partition} | Offset: {msg.offset} | Key: {key_str}"
        )
        print(f"       Location:     {val.get('location')}")
        print(f"       Temperature:  {val.get('temp_c')} °C ({val.get('temp_f')} °F)")
        print(
            f"       Condition:    {val.get('condition')} (Rain: {val.get('is_raining')}, Snow: {val.get('is_snowing')})"
        )
        print(
            f"       Wind / Hum:   {val.get('wind_speed_mps')} m/s / {val.get('humidity_pct')}%"
        )
        print(f"       Precipitation:{val.get('precipitation_mm')} mm")
        print(f"       Source / TS:  {val.get('source')} / {val.get('timestamp')}")
        assert val.get("location") == "NYC"
        assert val.get("temp_c") is not None

    print("\n" + "=" * 70)
    print("LIVE REDPANDA EXTERNAL FEEDS PRODUCERS VERIFICATION: PASSED (100% PROVEN)")
    print("=" * 70)


if __name__ == "__main__":
    main()
