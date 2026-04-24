import json
import os
import sys
import time

import requests
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = "movie-events"
PARTITIONS = 3
REPLICATION_FACTOR = 2
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/schemas/movie_event.avsc")


def wait_for_kafka(broker: str, retries: int = 30, delay: int = 3) -> None:
    print(f"Waiting for Kafka at {broker}...")
    for attempt in range(retries):
        try:
            client = AdminClient({"bootstrap.servers": broker})
            meta = client.list_topics(timeout=5)
            print(f"Kafka is ready. Known topics: {list(meta.topics.keys())}")
            return
        except Exception as exc:
            print(f"  Attempt {attempt + 1}/{retries}: {exc}")
            time.sleep(delay)
    print("ERROR: Kafka did not become ready in time.")
    sys.exit(1)


def create_topic(broker: str) -> None:
    client = AdminClient({"bootstrap.servers": broker})
    existing = client.list_topics(timeout=10).topics
    if TOPIC_NAME in existing:
        print(f"Topic '{TOPIC_NAME}' already exists, skipping creation.")
        return

    new_topic = NewTopic(
        TOPIC_NAME,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        config={
            "retention.ms": "604800000",
            "min.insync.replicas": "1",
        },
    )
    futures = client.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created with {PARTITIONS} partitions.")
        except Exception as exc:
            print(f"ERROR creating topic '{topic}': {exc}")
            sys.exit(1)


def wait_for_schema_registry(url: str, retries: int = 30, delay: int = 3) -> None:
    print(f"Waiting for Schema Registry at {url}...")
    for attempt in range(retries):
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            if resp.status_code == 200:
                print("Schema Registry is ready.")
                return
        except Exception as exc:
            print(f"  Attempt {attempt + 1}/{retries}: {exc}")
        time.sleep(delay)
    print("ERROR: Schema Registry did not become ready in time.")
    sys.exit(1)


def register_schema(url: str, schema_path: str) -> None:
    subject = f"{TOPIC_NAME}-value"
    with open(schema_path, "r") as f:
        schema_str = f.read()

    check_url = f"{url}/subjects/{subject}/versions/latest"
    try:
        resp = requests.get(check_url, timeout=5)
        if resp.status_code == 200:
            existing = resp.json()
            print(f"Schema already registered for subject '{subject}' (id={existing.get('id')}, version={existing.get('version')}). Skipping.")
            return
    except Exception:
        pass

    payload = {"schema": schema_str}
    register_url = f"{url}/subjects/{subject}/versions"
    resp = requests.post(
        register_url,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
        timeout=10,
    )
    if resp.status_code in (200, 201):
        schema_id = resp.json().get("id")
        print(f"Schema registered for subject '{subject}' with id={schema_id}.")
    else:
        print(f"ERROR: Failed to register schema (HTTP {resp.status_code}): {resp.text}")
        sys.exit(1)


def main() -> None:
    wait_for_kafka(KAFKA_BROKER)
    create_topic(KAFKA_BROKER)
    wait_for_schema_registry(SCHEMA_REGISTRY_URL)
    register_schema(SCHEMA_REGISTRY_URL, SCHEMA_PATH)
    print("Kafka init complete.")


if __name__ == "__main__":
    main()
