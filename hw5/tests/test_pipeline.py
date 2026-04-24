import os
import time
import uuid
from datetime import datetime, timezone

import clickhouse_connect
import pytest
import requests

PRODUCER_URL = os.environ.get("PRODUCER_URL", "http://localhost:8000")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "cinema")

POLL_TIMEOUT = 60
POLL_INTERVAL = 2


@pytest.fixture(scope="session")
def ch_client():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
    )
    yield client
    client.close()


def _wait_for_event(ch_client, event_id: str, timeout: int = POLL_TIMEOUT) -> dict | None:
    deadline = time.time() + timeout
    query = f"""
        SELECT
            event_id,
            user_id,
            movie_id,
            event_type,
            timestamp,
            device_type,
            session_id,
            progress_seconds
        FROM {CLICKHOUSE_DB}.movie_events
        WHERE event_id = '{event_id}'
        LIMIT 1
    """
    while time.time() < deadline:
        result = ch_client.query(query)
        if result.row_count > 0:
            row = result.first_row
            return {
                "event_id": row[0],
                "user_id": row[1],
                "movie_id": row[2],
                "event_type": row[3],
                "timestamp": row[4],
                "device_type": row[5],
                "session_id": row[6],
                "progress_seconds": row[7],
            }
        time.sleep(POLL_INTERVAL)
    return None


class TestPipeline:

    def test_producer_health(self):
        resp = requests.get(f"{PRODUCER_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_publish_view_started_event(self, ch_client):
        event_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        user_id = "test_user_001"
        movie_id = "test_movie_001"
        progress_seconds = 0
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "event_id": event_id,
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": "VIEW_STARTED",
            "timestamp": ts,
            "device_type": "DESKTOP",
            "session_id": session_id,
            "progress_seconds": progress_seconds,
        }

        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 201, f"Unexpected status: {resp.status_code} — {resp.text}"
        assert resp.json()["event_id"] == event_id

        row = _wait_for_event(ch_client, event_id)
        assert row is not None, f"Event {event_id} did not appear in ClickHouse within {POLL_TIMEOUT}s"

        assert row["event_id"] == event_id
        assert row["user_id"] == user_id
        assert row["movie_id"] == movie_id
        assert row["event_type"] == "VIEW_STARTED"
        assert row["device_type"] == "DESKTOP"
        assert row["session_id"] == session_id
        assert row["progress_seconds"] == progress_seconds

    def test_publish_view_finished_event(self, ch_client):
        event_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        progress_seconds = 5400

        payload = {
            "event_id": event_id,
            "user_id": "test_user_002",
            "movie_id": "test_movie_002",
            "event_type": "VIEW_FINISHED",
            "device_type": "TV",
            "session_id": session_id,
            "progress_seconds": progress_seconds,
        }

        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 201

        row = _wait_for_event(ch_client, event_id)
        assert row is not None, f"Event {event_id} not found in ClickHouse"
        assert row["event_type"] == "VIEW_FINISHED"
        assert row["progress_seconds"] == progress_seconds

    def test_publish_searched_event_no_progress(self, ch_client):
        event_id = str(uuid.uuid4())

        payload = {
            "event_id": event_id,
            "user_id": "test_user_003",
            "movie_id": "",
            "event_type": "SEARCHED",
            "device_type": "MOBILE",
            "session_id": str(uuid.uuid4()),
        }

        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 201

        row = _wait_for_event(ch_client, event_id)
        assert row is not None, f"Event {event_id} not found in ClickHouse"
        assert row["event_type"] == "SEARCHED"
        assert row["progress_seconds"] is None

    def test_invalid_event_type_rejected(self):
        payload = {
            "user_id": "test_user_bad",
            "movie_id": "movie_bad",
            "event_type": "INVALID_TYPE",
            "device_type": "DESKTOP",
            "session_id": str(uuid.uuid4()),
        }
        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 422

    def test_invalid_device_type_rejected(self):
        payload = {
            "user_id": "test_user_bad",
            "movie_id": "movie_bad",
            "event_type": "VIEW_STARTED",
            "device_type": "FRIDGE",
            "session_id": str(uuid.uuid4()),
        }
        resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
        assert resp.status_code == 422

    def test_idempotent_republish(self, ch_client):
        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "user_id": "test_user_idem",
            "movie_id": "test_movie_idem",
            "event_type": "LIKED",
            "device_type": "TABLET",
            "session_id": str(uuid.uuid4()),
        }

        for _ in range(2):
            resp = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=10)
            assert resp.status_code == 201

        row = _wait_for_event(ch_client, event_id)
        assert row is not None, f"Event {event_id} not found in ClickHouse"
        assert row["event_type"] == "LIKED"
