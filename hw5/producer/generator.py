import logging
import random
import time
import uuid
from datetime import date, datetime, timezone

from models import DeviceType, EventType, MovieEventRecord

logger = logging.getLogger(__name__)

_MOVIE_IDS: list[str] = [f"movie_{i:03d}" for i in range(1, 21)]
_USER_IDS: list[str] = [f"user_{i:04d}" for i in range(1, 51)]
_DEVICE_TYPES: list[DeviceType] = list(DeviceType)

_SESSION_STEP_MS = 5_000


def _base_ms(target_date: date | None) -> int:
    if target_date is None:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    start_of_day = datetime(target_date.year, target_date.month, target_date.day,
                            tzinfo=timezone.utc)
    offset_seconds = random.randint(0, 86399)
    return int((start_of_day.timestamp() + offset_seconds) * 1000)


def _make_event(
    *,
    event_type: EventType,
    user_id: str,
    movie_id: str,
    session_id: str,
    device_type: DeviceType,
    progress_seconds: int | None,
    timestamp_ms: int,
) -> MovieEventRecord:
    return MovieEventRecord(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        movie_id=movie_id,
        event_type=event_type,
        timestamp=timestamp_ms,
        device_type=device_type,
        session_id=session_id,
        progress_seconds=progress_seconds,
    )


def generate_session(
    user_id: str,
    movie_id: str,
    device_type: DeviceType,
    target_date: date | None = None,
) -> list[MovieEventRecord]:
    session_id = str(uuid.uuid4())
    events: list[MovieEventRecord] = []
    progress = 0
    movie_duration = random.randint(3600, 7200)
    ts = _base_ms(target_date)

    def make(event_type: EventType, progress_s: int | None) -> MovieEventRecord:
        return _make_event(
            event_type=event_type,
            user_id=user_id,
            movie_id=movie_id,
            session_id=session_id,
            device_type=device_type,
            progress_seconds=progress_s,
            timestamp_ms=ts,
        )

    events.append(make(EventType.VIEW_STARTED, progress))
    ts += _SESSION_STEP_MS

    for _ in range(random.randint(0, 2)):
        progress += random.randint(60, 600)
        progress = min(progress, movie_duration - 1)
        events.append(make(EventType.VIEW_PAUSED, progress))
        ts += _SESSION_STEP_MS
        events.append(make(EventType.VIEW_RESUMED, progress))
        ts += _SESSION_STEP_MS

    events.append(make(EventType.VIEW_FINISHED, movie_duration))
    ts += _SESSION_STEP_MS

    if random.random() < 0.3:
        events.append(make(EventType.LIKED, movie_duration))

    return events


def generate_search(user_id: str, device_type: DeviceType, target_date: date | None = None) -> MovieEventRecord:
    return _make_event(
        event_type=EventType.SEARCHED,
        user_id=user_id,
        movie_id="",
        session_id=str(uuid.uuid4()),
        device_type=device_type,
        progress_seconds=None,
        timestamp_ms=_base_ms(target_date),
    )


def generate_events(num_sessions: int, delay_ms: int, target_date: date | None = None) -> list[MovieEventRecord]:
    all_events: list[MovieEventRecord] = []

    for i in range(num_sessions):
        user_id = random.choice(_USER_IDS)
        movie_id = random.choice(_MOVIE_IDS)
        device_type = random.choice(_DEVICE_TYPES)

        if random.random() < 0.2:
            all_events.append(generate_search(user_id, device_type, target_date))

        session_events = generate_session(user_id, movie_id, device_type, target_date)
        all_events.extend(session_events)

        logger.info(
            f"Generated session {i + 1}/{num_sessions} "
            f"user_id={user_id} movie_id={movie_id} "
            f"events={len(session_events)} date={target_date}"
        )

        if delay_ms > 0 and i < num_sessions - 1:
            time.sleep(delay_ms / 1000.0)

    return all_events
