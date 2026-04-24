import io
import json
import logging
import time
from typing import Optional

import fastavro
import requests
from confluent_kafka import KafkaException, Producer

from config import Settings

logger = logging.getLogger(__name__)


class KafkaAvroProducer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: Optional[Producer] = None
        self._parsed_schema: Optional[dict] = None
        self._schema_id: Optional[int] = None

    def _load_avro_schema(self) -> dict:
        if self._parsed_schema is None:
            with open(self._settings.schema_path, "r") as f:
                raw = json.load(f)
            self._parsed_schema = fastavro.parse_schema(raw)
            logger.info(f"Avro schema loaded from {self._settings.schema_path}")
        return self._parsed_schema

    def _fetch_schema_id(self, retries: int = 15, delay: float = 3.0) -> int:
        if self._schema_id is not None:
            return self._schema_id

        subject = f"{self._settings.kafka_topic}-value"
        url = f"{self._settings.schema_registry_url}/subjects/{subject}/versions/latest"

        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    self._schema_id = resp.json()["id"]
                    logger.info(f"Schema id={self._schema_id} fetched from Schema Registry (subject={subject})")
                    return self._schema_id
                logger.warning(
                    f"Schema Registry returned HTTP {resp.status_code} "
                    f"for subject {subject} (attempt {attempt}/{retries})"
                )
            except requests.RequestException as exc:
                logger.warning(f"Schema Registry not reachable (attempt {attempt}/{retries}): {exc}")
            time.sleep(delay)

        raise RuntimeError(
            f"Could not fetch schema id from Schema Registry after {retries} attempts"
        )

    def _avro_encode(self, record: dict) -> bytes:
        schema = self._load_avro_schema()
        schema_id = self._fetch_schema_id()

        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(schema_id.to_bytes(4, "big"))
        fastavro.schemaless_writer(buf, schema, record)
        return buf.getvalue()

    def _get_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self._settings.kafka_broker,
                    "acks": self._settings.kafka_acks,
                    "retries": self._settings.kafka_retries,
                    "retry.backoff.ms": self._settings.kafka_retry_backoff_ms,
                    "enable.idempotence": True,
                }
            )
            logger.info(f"Kafka producer created (broker={self._settings.kafka_broker})")
        return self._producer

    @staticmethod
    def _delivery_report(err, msg) -> None:
        key = msg.key().decode() if msg.key() else "?"
        if err:
            logger.error(f"Delivery failed | key={key} error={err}")
        else:
            logger.info(
                f"Delivered | key={key} topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, record: dict, partition_key: str) -> None:
        payload = self._avro_encode(record)
        producer = self._get_producer()

        delay = self._settings.kafka_retry_backoff_ms / 1000.0
        max_attempts = self._settings.kafka_retries + 1

        for attempt in range(1, max_attempts + 1):
            try:
                producer.produce(
                    topic=self._settings.kafka_topic,
                    key=partition_key.encode(),
                    value=payload,
                    callback=self._delivery_report,
                )
                producer.poll(0)
                logger.info(
                    f"Queued | event_id={record.get('event_id')} "
                    f"event_type={record.get('event_type')} "
                    f"user_id={partition_key}"
                )
                return
            except KafkaException as exc:
                logger.warning(f"KafkaException (attempt {attempt}/{max_attempts}): {exc}")
                if attempt < max_attempts:
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)
                else:
                    raise

    def flush(self, timeout: float = 30.0) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=timeout)
            logger.info(f"Producer flushed (timeout={timeout:.1f}s)")
