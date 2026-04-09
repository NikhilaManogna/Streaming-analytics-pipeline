import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from utils.logger import get_logger


logger = get_logger("producer")
EVENT_TYPES = ("click", "order", "payment")
INVALID_EVENT_TYPES = ("refund", "signup", "unknown")


def load_config() -> Dict[str, Any]:
    config_path = Path(os.getenv("RTA_CONFIG_PATH", ROOT_DIR / "config" / "config.yaml"))
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    config["kafka"]["brokers"] = os.getenv("RTA_KAFKA_BROKERS", ",".join(config["kafka"]["brokers"])).split(",")
    config["kafka"]["topic"] = os.getenv("RTA_KAFKA_TOPIC", config["kafka"]["topic"])
    config["producer"]["continuous"] = os.getenv(
        "RTA_PRODUCER_CONTINUOUS", str(config["producer"]["continuous"])
    ).lower() == "true"
    config["producer"]["target_events"] = int(
        os.getenv("RTA_PRODUCER_TARGET_EVENTS", config["producer"]["target_events"])
    )
    return config


def _current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()



def _amount_for_event(event_type: str) -> float:
    if event_type == "click":
        return 0.0
    if event_type == "order":
        return round(random.uniform(20.0, 320.0), 2)
    return round(random.uniform(10.0, 180.0), 2)



def generate_event() -> Dict[str, Any]:
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 5000),
        "event_type": event_type,
        "timestamp": _current_timestamp(),
        "amount": _amount_for_event(event_type),
    }



def generate_invalid_event() -> Dict[str, Any]:
    invalid_variant = random.choice(
        [
            "missing_event_id",
            "missing_user_id",
            "invalid_event_type",
            "invalid_timestamp",
            "negative_amount",
        ]
    )

    event = generate_event()
    if invalid_variant == "missing_event_id":
        event["event_id"] = None
    elif invalid_variant == "missing_user_id":
        event["user_id"] = None
    elif invalid_variant == "invalid_event_type":
        event["event_type"] = random.choice(INVALID_EVENT_TYPES)
        event["amount"] = round(random.uniform(5.0, 50.0), 2)
    elif invalid_variant == "invalid_timestamp":
        event["timestamp"] = "not-a-valid-timestamp"
    elif invalid_variant == "negative_amount":
        event["amount"] = -round(random.uniform(1.0, 50.0), 2)

    return event



def create_producer(config: Dict[str, Any]) -> KafkaProducer:
    producer_config = config["producer"]
    return KafkaProducer(
        bootstrap_servers=config["kafka"]["brokers"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks=producer_config["acks"],
        linger_ms=producer_config["linger_ms"],
        batch_size=producer_config["batch_size"],
        retries=producer_config["retries"],
        request_timeout_ms=producer_config["request_timeout_ms"],
        max_in_flight_requests_per_connection=producer_config["max_in_flight_requests_per_connection"],
        compression_type=producer_config["compression_type"],
    )



def wait_for_kafka(config: Dict[str, Any]) -> None:
    brokers = config["kafka"]["brokers"]
    for _ in range(30):
        try:
            producer = KafkaProducer(bootstrap_servers=brokers)
            producer.close()
            return
        except NoBrokersAvailable:
            logger.info("Kafka not ready, retrying in 5 seconds")
            time.sleep(5)
    raise RuntimeError("Kafka did not become ready in time")



def choose_event(recent_valid_events: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
    producer_config = config["producer"]
    duplicate_ratio = float(producer_config["duplicate_ratio"])
    invalid_ratio = float(producer_config["invalid_ratio"])
    selection = random.random()

    if recent_valid_events and selection < duplicate_ratio:
        return random.choice(recent_valid_events).copy()
    if selection < duplicate_ratio + invalid_ratio:
        return generate_invalid_event()
    return generate_event()



def run() -> None:
    config = load_config()
    wait_for_kafka(config)

    kafka_topic = config["kafka"]["topic"]
    producer_config = config["producer"]
    producer = create_producer(config)
    recent_valid_events: List[Dict[str, Any]] = []
    published = 0

    logger.info("Connected to Kafka, publishing to topic '%s'", kafka_topic)

    try:
        while producer_config["continuous"] or published < int(producer_config["target_events"]):
            event = choose_event(recent_valid_events, config)

            for attempt in range(1, int(producer_config["retries"]) + 2):
                try:
                    producer.send(kafka_topic, value=event)
                    break
                except KafkaError:
                    if attempt > int(producer_config["retries"]):
                        logger.exception("Failed to publish event after retries")
                        raise
                    time.sleep(min(0.25 * attempt, 2.0))

            published += 1

            if event.get("event_id") and event.get("event_type") in EVENT_TYPES and event.get("amount") is not None and event["amount"] >= 0:
                recent_valid_events.append(event.copy())
                if len(recent_valid_events) > 500:
                    recent_valid_events.pop(0)

            if published % 1000 == 0:
                producer.flush()
                logger.info("Published %s events", published)

            time.sleep(float(producer_config["interval_seconds"]))

        producer.flush()
        logger.info("Finished publishing %s events", published)
    finally:
        producer.close()


if __name__ == "__main__":
    run()
